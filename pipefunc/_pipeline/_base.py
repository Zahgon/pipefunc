
from __future__ import annotations

import functools
import inspect
import os
import time
import warnings
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Literal, NamedTuple

import networkx as nx

from pipefunc._pipefunc import NestedPipeFunc, PipeFunc, _maybe_mapspec
from pipefunc._pipefunc_utils import handle_pipefunc_error
from pipefunc._profile import print_profiling_stats
from pipefunc._utils import (
    assert_complete_kwargs,
    at_least_tuple,
    clear_cached_properties,
    ensure_output_names_set,
    is_installed,
    is_running_in_ipynb,
    requires,
)
from pipefunc.cache import DiskCache, HybridCache, LRUCache, SimpleCache
from pipefunc.exceptions import ErrorSnapshot, UnusedParametersError
from pipefunc.lazy import _LazyFunction, task_graph
from pipefunc.map._mapspec import (
    MapSpec,
    mapspec_axes,
    mapspec_dimensions,
    validate_consistent_axes,
)
from pipefunc.map._run import AsyncMap, run_map, run_map_async
from pipefunc.map._run_eager import run_map_eager
from pipefunc.map._run_eager_async import run_map_eager_async
from pipefunc.map._run_info import _handle_cleanup_deprecation
from pipefunc.resources import Resources

from ._autodoc import PipelineDocumentation, format_pipeline_docs
from ._cache import compute_cache_key, create_cache, get_result_from_cache, update_cache
from ._cli import cli
from ._mapspec import (
    add_mapspec_axis,
    create_missing_mapspecs,
    find_non_root_axes,
    replace_none_in_axes,
)
from ._pydantic import pipeline_to_pydantic
from ._simplify import _func_node_colors, _identify_combinable_nodes, simplified_pipeline
from ._validation import (
    validate_consistent_defaults,
    validate_consistent_type_annotations,
    validate_scopes,
    validate_unique_output_names,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Sequence
    from concurrent.futures import Executor
    from pathlib import Path

    import graphviz
    import holoviews as hv
    import IPython.display
    import ipywidgets
    import pydantic
    from rich.table import Table

    from pipefunc._plotting import GraphvizStyle
    from pipefunc._profile import ProfilingStats
    from pipefunc.map._result import ResultDict
    from pipefunc.map._types import UserShapeDict

    from ._types import OUTPUT_TYPE, StorageType


class Pipeline:

    def __init__(
        self,
        functions: list[PipeFunc | tuple[PipeFunc, str | MapSpec]],
        *,
        lazy: bool = False,
        debug: bool | None = None,
        print_error: bool | None = None,
        profile: bool | None = None,
        cache_type: Literal["lru", "hybrid", "disk", "simple"] | None = None,
        cache_kwargs: dict[str, Any] | None = None,
        validate_type_annotations: bool = True,
        scope: str | None = None,
        default_resources: dict[str, Any] | Resources | None = None,
        name: str | None = None,
        description: str | None = None,
    ) -> None:
        """Pipeline class for managing and executing a sequence of functions."""
        self.functions: list[PipeFunc] = []
        self.lazy = lazy
        self._debug = debug
        self._print_error = print_error
        self._profile = profile
        self._default_resources: Resources | None = Resources.maybe_from_dict(default_resources)  # type: ignore[assignment]
        self.validate_type_annotations = validate_type_annotations
        self.name = name
        self.description = description
        for f in functions:
            if isinstance(f, tuple):
                f, mapspec = f  # noqa: PLW2901
            else:
                mapspec = None
            self.add(f, mapspec=mapspec)
        self._cache_type = cache_type
        self._cache_kwargs = cache_kwargs
        if cache_type is None and any(f.cache for f in self.functions):
            cache_type = "lru"
        self.cache = create_cache(cache_type, lazy, cache_kwargs)
        if scope is not None:
            self.update_scope(scope, "*", "*")

    def info(self, *, print_table: bool = False) -> dict[str, tuple[str, ...]] | None:
        """Return information about inputs and outputs of the Pipeline.

        Parameters
        ----------
        print_table
            Whether to print a rich-formatted table to the console. Requires the `rich` package.

        Returns
        -------
        dict or None
            If `print_table` is False, returns a dictionary containing information about
            the inputs and outputs of the Pipeline, with the following keys:

            - ``inputs``: The input arguments of the Pipeline.
            - ``outputs``: The output arguments of the Pipeline.
            - ``intermediate_outputs``: The intermediate output arguments of the Pipeline.
            - ``required_inputs``: The required input arguments of the Pipeline.
            - ``optional_inputs``: The optional input arguments of the Pipeline (see `Pipeline.defaults`).

            If `print_table` is True, prints a rich-formatted table to the console and returns None.

        See Also
        --------
        defaults
            A dictionary with input name to default value mappings.
        leaf_nodes
            The leaf nodes of the pipeline as `PipeFunc` objects.
        root_args
            The root arguments (inputs) required to compute the output of the pipeline.
        print_documentation
            Print formatted documentation of the pipeline to the console.

        """
        inputs = self.root_args()
        outputs = tuple(sorted(n for f in self.leaf_nodes for n in at_least_tuple(f.output_name)))
        intermediate_outputs = tuple(sorted(self.all_output_names - set(outputs)))
        required_inputs = tuple(sorted(arg for arg in inputs if arg not in self.defaults))
        optional_inputs = tuple(sorted(arg for arg in inputs if arg in self.defaults))
        info: dict[str, tuple[str, ...]] = {
            "required_inputs": required_inputs,
            "optional_inputs": optional_inputs,
            "inputs": inputs,
            "intermediate_outputs": intermediate_outputs,
            "outputs": outputs,
        }
        if not print_table:
            return info
        _ = _rich_info_table(info, prints=True)
        return None

    @property
    def profile(self) -> bool | None:
        pass

    @profile.setter
    def profile(self, value: bool | None) -> None:
        pass

    @property
    def debug(self) -> bool | None:
        pass

    @debug.setter
    def debug(self, value: bool | None) -> None:
        pass

    @property
    def print_error(self) -> bool | None:
        pass

    @print_error.setter
    def print_error(self, value: bool | None) -> None:
        pass

    def add(self, f: PipeFunc | Callable, mapspec: str | MapSpec | None = None) -> PipeFunc:
        """Add a function to the pipeline.

        Always creates a copy of the `PipeFunc` instance to avoid side effects.

        Parameters
        ----------
        f
            The function to add to the pipeline.
        profile
            Flag indicating whether profiling information should be collected.
        mapspec
            This is a specification for mapping that dictates how input values should
            be merged together. If ``None``, the default behavior is that the input directly
            maps to the output.

        """
        if isinstance(f, PipeFunc):
            resources = Resources.maybe_with_defaults(f.resources, self._default_resources)
            f: PipeFunc = f.copy(  # type: ignore[no-redef]
                resources=resources,
                mapspec=f.mapspec if mapspec is None else _maybe_mapspec(mapspec),
            )
        elif callable(f):
            f = PipeFunc(
                f,
                output_name=f.__name__,
                mapspec=mapspec,
                resources=self._default_resources,
            )
        else:
            msg = f"`f` must be a `PipeFunc` or callable, got {type(f)}"
            raise TypeError(msg)

        validate_unique_output_names(f.output_name, self.output_to_func)
        self.functions.append(f)
        f._pipelines.add(self)

        if self.profile is not None:
            f.profile = self.profile

        if self.debug is not None:
            f.debug = self.debug

        if self.print_error is not None:
            f.print_error = self.print_error

        self._clear_internal_cache()  # reset cache
        self.validate()
        return f

    def drop(self, *, f: PipeFunc | None = None, output_name: OUTPUT_TYPE | None = None) -> None:
        """Drop a function from the pipeline.

        Parameters
        ----------
        f
            The function to drop from the pipeline.
        output_name
            The name of the output to drop from the pipeline.

        """
        if (f is not None and output_name is not None) or (f is None and output_name is None):
            msg = "Either `f` or `output_name` should be provided."
            raise ValueError(msg)
        if f is not None:
            if f not in self.functions:
                msg = (
                    f"The function `{f}` is not in the pipeline."
                    " Remember that the `PipeFunc` instances are copied on `Pipeline` initialization."
                )
                if f.output_name in self.output_to_func:
                    msg += (
                        f" However, the function with the same output name `{f.output_name!r}` exists in the"
                        f" pipeline, you can access that function via `pipeline[{f.output_name!r}]`."
                    )
                raise ValueError(msg)
            self.functions.remove(f)
        elif output_name is not None:
            f = self.output_to_func[output_name]
            self.drop(f=f)
        self._clear_internal_cache()
        self.validate()

    def replace(self, new: PipeFunc, old: PipeFunc | None = None) -> None:
        """Replace a function in the pipeline with another function.

        Parameters
        ----------
        new
            The function to add to the pipeline.
        old
            The function to replace in the pipeline. If None, ``old`` is
            assumed to be the function with the same output name as ``new``.

        """
        if old is None:
            self.drop(output_name=new.output_name)
        else:
            self.drop(f=old)
        self.add(new)
        self._clear_internal_cache()
        self.validate()

    @functools.cached_property
    def output_to_func(self) -> dict[OUTPUT_TYPE, PipeFunc]:
        pass

    def __getitem__(self, output_name: OUTPUT_TYPE) -> PipeFunc:
        """Return the function corresponding to a specific output name.

        See Also
        --------
        output_to_func
            The mapping from output names to functions.

        """
        if output_name not in self.output_to_func:
            available = list(self.output_to_func.keys())
            msg = f"No function with output name `{output_name!r}` in the pipeline, only `{available}`."
            raise KeyError(msg)
        return self.output_to_func[output_name]

    def __contains__(self, output_name: OUTPUT_TYPE) -> bool:
        """Check if the pipeline contains a function with a specific output name."""
        return output_name in self.output_to_func

    @functools.cached_property
    def node_mapping(self) -> dict[OUTPUT_TYPE, PipeFunc | str]:
        pass

    @functools.cached_property
    def graph(self) -> nx.DiGraph:
        pass

    def func(
        self,
        output_name: OUTPUT_TYPE | list[OUTPUT_TYPE] | None = None,
    ) -> _PipelineAsFunc:
        """Create a composed function that can be called with keyword arguments.

        Parameters
        ----------
        output_name
            The identifier for the return value of the composed function. Can be a
            single output name or a list of output names. If ``None``, the output
            name of the unique leaf node is used, or all leaf nodes if there are
            multiple (returning a tuple of their outputs).

        Returns
        -------
            The composed function that can be called with keyword arguments.

        """
        if output_name is None:
            output_name = self._default_output_name()
        key = tuple(output_name) if isinstance(output_name, list) else output_name
        if f := self._internal_cache.func.get(key):
            return f
        root_args = _root_args(self, output_name)
        assert isinstance(root_args, tuple)
        f = _PipelineAsFunc(self, output_name, root_args=root_args)
        self._internal_cache.func[key] = f
        return f


    def _clear_internal_cache(self) -> None:
        clear_cached_properties(self)
        for f in self.functions:
            f._clear_internal_cache(clear_pipelines=False)

    def __call__(
        self,
        __output_name__: OUTPUT_TYPE | list[OUTPUT_TYPE] | None = None,
        /,
        **kwargs: Any,
    ) -> Any:
        """Call the pipeline for a specific return value.

        Parameters
        ----------
        __output_name__
            The identifier for the return value of the pipeline. Can be a single
            output name or a list of output names (returning a tuple of their
            outputs, like `Pipeline.run`).
            Is None by default, in which case the unique leaf node is used,
            or all leaf nodes if there are multiple (returning a tuple of
            their outputs).
            This parameter is positional-only and the strange name is used
            to avoid conflicts with the ``output_name`` argument that might be
            passed via ``kwargs``.
        kwargs
            Keyword arguments to be passed to the pipeline functions.

        Returns
        -------
            The return value of the pipeline.

        """
        return self.run(__output_name__, kwargs=kwargs)

    def _get_func_args(
        self,
        func: PipeFunc,
        flat_scope_kwargs: dict[str, Any],
        all_results: dict[OUTPUT_TYPE, Any],
        full_output: bool,
        used_parameters: set[str | None],
    ) -> dict[str, Any]:
        func_args = {}
        for arg in func.parameters:
            if arg in func._bound:
                value = func._bound[arg]
            elif arg in flat_scope_kwargs:
                value = flat_scope_kwargs[arg]
            elif arg in self.output_to_func:
                value = self._run(
                    output_name=arg,
                    flat_scope_kwargs=flat_scope_kwargs,
                    all_results=all_results,
                    full_output=full_output,
                    used_parameters=used_parameters,
                )
            elif arg in self.defaults:
                value = self.defaults[arg]
            else:
                msg = f"Missing value for argument `{arg}` in `{func}`."
                raise ValueError(msg)
            func_args[arg] = value
            used_parameters.add(arg)
        func._convert_lazyframe_kwargs(func_args)
        return func_args

    def _current_cache(self) -> LRUCache | HybridCache | DiskCache | SimpleCache | None:
        """Return the cache used by the pipeline."""
        if not isinstance(self.cache, SimpleCache) and (tg := task_graph()) is not None:
            return tg.cache
        return self.cache

    def _run(
        self,
        *,
        output_name: OUTPUT_TYPE,
        flat_scope_kwargs: dict[str, Any],
        all_results: dict[OUTPUT_TYPE, Any],
        full_output: bool,
        used_parameters: set[str | None],
    ) -> Any:
        if output_name in all_results:
            return all_results[output_name]
        func = self.output_to_func[output_name]
        assert func.parameters is not None

        cache = self._current_cache()
        use_cache = (func.cache and cache is not None) or task_graph() is not None
        root_args = self.root_args(output_name)
        result_from_cache = False
        if use_cache:
            assert cache is not None
            cache_key = compute_cache_key(
                func._cache_id,
                self._func_defaults(func) | flat_scope_kwargs | func._bound,
                root_args,
            )
            return_now, result_from_cache = get_result_from_cache(
                func,
                cache,
                cache_key,
                output_name,
                all_results,
                full_output,
                used_parameters,
                self.lazy,
            )
            if return_now:
                return all_results[output_name]

        func_args = self._get_func_args(
            func,
            flat_scope_kwargs,
            all_results,
            full_output,
            used_parameters,
        )

        if result_from_cache:
            assert full_output
            return all_results[output_name]

        start_time = time.perf_counter()
        r = _execute_func(func, func_args, self.lazy)
        if use_cache and cache_key is not None:
            assert cache is not None
            update_cache(cache, cache_key, r, start_time)
        _update_all_results(func, r, output_name, all_results, self.lazy)
        return all_results[output_name]

    def _default_output_name(self) -> OUTPUT_TYPE | list[OUTPUT_TYPE]:
        """Return the output name(s) of the leaf node(s) of the pipeline graph."""
        output_names = [f.output_name for f in self.leaf_nodes]
        return output_names[0] if len(output_names) == 1 else output_names

    def _validate_run_output_name(
        self,
        kwargs: dict[str, Any],
        output_name: OUTPUT_TYPE | list[OUTPUT_TYPE],
    ) -> None:
        if isinstance(output_name, list):
            for name in output_name:
                self._validate_run_output_name(kwargs, name)
            return
        if output_name in kwargs:
            msg = f"The `output_name='{output_name}'` argument cannot be provided in `kwargs={kwargs}`."
            raise ValueError(msg)
        if output_name not in self.output_to_func:
            available = ", ".join(k for k in self.output_to_func if isinstance(k, str))
            msg = (
                f"No function with output name `{output_name}` in the pipeline, only `{available}`."
            )
            raise ValueError(msg)

        if p := self.mapspec_names & set(self.func_dependencies(output_name)):
            inputs = self.mapspec_names & set(self.root_args(output_name))
            msg = (
                f"Cannot execute pipeline to get `{output_name}` because `{p}`"
                f" (depends on `{inputs=}`) have `MapSpec`(s). Use `Pipeline.map` instead."
            )
            raise RuntimeError(msg)

    def run(
        self,
        output_name: OUTPUT_TYPE | list[OUTPUT_TYPE] | None = None,
        *,
        full_output: bool = False,
        kwargs: dict[str, Any],
        allow_unused: bool = False,
    ) -> Any:
        """Execute the pipeline for a specific return value.

        Parameters
        ----------
        output_name
            The identifier for the return value of the pipeline. Can be a single
            output name or a list of output names. If ``None``, the output name
            of the unique leaf node is used, or all leaf nodes if there are
            multiple (returning a tuple of their outputs).
        full_output
            Whether to return the outputs of all function executions
            as a dictionary mapping function names to their return values.
        kwargs
            Keyword arguments to be passed to the pipeline functions.
        allow_unused
            Whether to allow unused keyword arguments. If ``False``, an error
            is raised if any keyword arguments are unused. If ``True``, unused
            keyword arguments are ignored.

        Returns
        -------
            A dictionary mapping function names to their return values
            if ``full_output`` is ``True``. Otherwise, the return value is the
            return value of the pipeline function specified by ``output_name``.
            If ``output_name`` is a list, the return value is a tuple of the
            return values of the pipeline functions.

        """
        if output_name is None:
            output_name = self._default_output_name()
        self._validate_run_output_name(kwargs, output_name)
        self._validate_scoped_parameters(kwargs)
        flat_scope_kwargs = self._flatten_scopes(kwargs)

        all_results: dict[OUTPUT_TYPE, Any] = flat_scope_kwargs.copy()  # type: ignore[assignment]
        used_parameters: set[str | None] = set()
        output_names = [output_name] if not isinstance(output_name, list) else output_name
        for _output_name in output_names:
            self._run(
                output_name=_output_name,
                flat_scope_kwargs=flat_scope_kwargs,
                all_results=all_results,
                full_output=full_output,
                used_parameters=used_parameters,
            )

        if (
            not allow_unused
            and None not in used_parameters
            and (unused := flat_scope_kwargs.keys() - set(used_parameters))
        ):
            unused_str = ", ".join(sorted(unused))
            msg = f"Unused keyword arguments: `{unused_str}`. {kwargs=}, {used_parameters=}"
            raise UnusedParametersError(msg)

        if full_output:
            return all_results
        if isinstance(output_name, list):
            return tuple(all_results[k] for k in output_name)
        return all_results[output_name]

    def map(
        self,
        inputs: dict[str, Any] | pydantic.BaseModel,
        run_folder: str | Path | None = None,
        internal_shapes: UserShapeDict | None = None,
        *,
        output_names: OUTPUT_TYPE | Iterable[OUTPUT_TYPE] | None = None,
        parallel: bool = True,
        executor: Executor | dict[OUTPUT_TYPE, Executor] | None = None,
        chunksizes: int | dict[OUTPUT_TYPE, int | Callable[[int], int] | None] | None = None,
        storage: StorageType | None = None,
        persist_memory: bool = True,
        cleanup: bool | None = None,
        resume: bool = False,
        resume_validation: Literal["auto", "strict", "skip"] = "auto",
        fixed_indices: dict[str, int | slice] | None = None,
        auto_subpipeline: bool = False,
        show_progress: bool | Literal["rich", "ipywidgets", "headless"] | None = None,
        return_results: bool = True,
        error_handling: Literal["raise", "continue"] = "raise",
        scheduling_strategy: Literal["generation", "eager"] = "generation",
    ) -> ResultDict:
        pass

    def map_async(
        self,
        inputs: dict[str, Any] | pydantic.BaseModel,
        run_folder: str | Path | None = None,
        internal_shapes: UserShapeDict | None = None,
        *,
        output_names: OUTPUT_TYPE | Iterable[OUTPUT_TYPE] | None = None,
        executor: Executor | dict[OUTPUT_TYPE, Executor] | None = None,
        chunksizes: int | dict[OUTPUT_TYPE, int | Callable[[int], int] | None] | None = None,
        storage: StorageType | None = None,
        persist_memory: bool = True,
        cleanup: bool | None = None,
        resume: bool = False,
        resume_validation: Literal["auto", "strict", "skip"] = "auto",
        fixed_indices: dict[str, int | slice] | None = None,
        auto_subpipeline: bool = False,
        show_progress: bool | Literal["rich", "ipywidgets", "headless"] | None = None,
        display_widgets: bool = True,
        return_results: bool = True,
        scheduling_strategy: Literal["generation", "eager"] = "generation",
        error_handling: Literal["raise", "continue"] = "raise",
        start: bool = True,
    ) -> AsyncMap:
        pass

    def arg_combinations(self, output_name: OUTPUT_TYPE) -> set[tuple[str, ...]]:
        pass

    def root_args(self, output_name: OUTPUT_TYPE | None = None) -> tuple[str, ...]:
        """Return the root arguments required to compute a specific (or all) output(s).

        Parameters
        ----------
        output_name
            The identifier for the return value of the pipeline. If ``None``,
            the root arguments for all outputs are returned.

        Returns
        -------
            A tuple containing the root arguments required to compute the output.
            The tuple is sorted in alphabetical order.

        """
        if r := self._internal_cache.root_args.get(output_name):
            return r

        if output_name is None:
            root_args = tuple(sorted(self.topological_generations.root_args))
        else:
            all_root_args = set(self.topological_generations.root_args)
            ancestors = nx.ancestors(self.graph, self.output_to_func[output_name])
            root_args_set = {n for n in self.graph.nodes if n in all_root_args and n in ancestors}
            root_args = tuple(sorted(root_args_set))

        self._internal_cache.root_args[output_name] = root_args
        return root_args

    def func_dependencies(self, output_name: OUTPUT_TYPE | PipeFunc) -> list[OUTPUT_TYPE]:
        """Return the functions required to compute a specific output.

        See Also
        --------
        func_dependents

        """
        return _traverse_graph(output_name, "predecessors", self.graph, self.node_mapping)

    def func_dependents(self, name: OUTPUT_TYPE | PipeFunc) -> list[OUTPUT_TYPE]:
        """Return the functions that depend on a specific input/output.

        See Also
        --------
        func_dependencies

        """
        return _traverse_graph(name, "successors", self.graph, self.node_mapping)



    def _func_defaults(self, func: PipeFunc) -> dict[str, Any]:
        """Retrieve defaults for a function, including those set by other functions."""
        if r := self._internal_cache.func_defaults.get(func.output_name):
            return r
        defaults = func.defaults.copy()
        for arg in func.parameters:
            if arg in self.defaults:
                pipeline_default = self.defaults[arg]
                if arg in defaults:
                    assert defaults[arg] == pipeline_default
                    continue
                defaults[arg] = self.defaults[arg]
        self._internal_cache.func_defaults[func.output_name] = defaults
        return defaults

    def update_defaults(
        self,
        defaults: dict[str, Any | dict[str, Any]],
        *,
        overwrite: bool = False,
    ) -> None:
        pass

    def update_renames(
        self,
        renames: dict[str, str],
        *,
        update_from: Literal["current", "original"] = "current",
        overwrite: bool = False,
    ) -> None:
        """Update the renames for the pipeline.

        Automatically traverses the pipeline graph to find all functions that
        the renames can be applied to.

        Parameters
        ----------
        renames
            A dictionary mapping old parameter names to new parameter and output names.
        update_from
            Whether to update the renames from the current parameter names (`PipeFunc.parameters`)
            or from the original parameter names (`PipeFunc.original_parameters`).
        overwrite
            Whether to overwrite the existing renames. If ``False``, the new
            renames will be added to the existing renames.

        """
        unused = set(renames.keys())
        for f in self.functions:
            parameters = tuple(
                f.parameters + at_least_tuple(f.output_name)
                if update_from == "current"
                else tuple(f.original_parameters) + at_least_tuple(f._output_name),
            )
            update = {k: v for k, v in renames.items() if k in parameters}
            unused -= set(update.keys())
            f.update_renames(update, overwrite=overwrite, update_from=update_from)
        self._clear_internal_cache()
        if unused:
            unused_str = ", ".join(sorted(unused))
            msg = f"Unused keyword arguments: `{unused_str}`. These are not settable renames."
            raise ValueError(msg)
        self.validate()

    def update_mapspec_axes(self, renames: dict[str, str]) -> None:
        pass

    def update_scope(
        self,
        scope: str | None,
        inputs: set[str] | Literal["*"] | None = None,
        outputs: set[str] | Literal["*"] | None = None,
        exclude: set[str] | None = None,
    ) -> None:
        pass

    def _validate_scoped_parameters(self, kwargs: dict[str, Any]) -> None:
        """Validate that scoped parameters are not defined in both flattened and nested formats."""
        if overlap := kwargs.keys() & self._flatten_scopes(
            {
                scope: scoped_kwargs
                for scope, scoped_kwargs in kwargs.items()
                if scope in self.scopes
            },
        ):
            overlap_str = ", ".join(sorted(overlap))
            msg = (
                f"Conflicting definitions for `{overlap_str}`:"
                " found both flattened ('scope.param') and nested ({'scope': {'param': ...}}) formats."
            )
            raise ValueError(msg)

    def _flatten_scopes(self, kwargs: dict[str, Any]) -> dict[str, Any]:
        flat_scope_kwargs = kwargs
        for f in self.functions:
            flat_scope_kwargs = f._flatten_scopes(flat_scope_kwargs)
        return flat_scope_kwargs

    @functools.cached_property
    def parameter_annotations(self) -> dict[str, Any]:
        pass

    @functools.cached_property
    def output_annotations(self) -> dict[str, Any]:
        pass

    @functools.cached_property
    def all_arg_combinations(self) -> dict[OUTPUT_TYPE, set[tuple[str, ...]]]:
        pass

    @functools.cached_property
    def all_root_args(self) -> dict[OUTPUT_TYPE, tuple[str, ...]]:
        pass


    def mapspecs(self, *, ordered: bool = True) -> list[MapSpec]:
        """Return the MapSpecs for all functions in the pipeline."""
        functions = self.sorted_functions if ordered else self.functions
        return [f.mapspec for f in functions if f.mapspec]

    @functools.cached_property
    def mapspecs_as_strings(self) -> list[str]:
        pass

    @functools.cached_property
    def mapspec_dimensions(self: Pipeline) -> dict[str, int]:
        """Return the number of dimensions for each array parameter in the pipeline."""
        return mapspec_dimensions(self.mapspecs())

    @functools.cached_property
    def mapspec_axes(self: Pipeline) -> dict[str, tuple[str, ...]]:
        """Return the axes for each array parameter in the pipeline."""
        return mapspec_axes(self.mapspecs())

    def validate(self) -> None:
        """Validate the pipeline (checks its scopes, renames, defaults, mapspec, type hints).

        This is automatically called when the pipeline is created and when calling state
        updating methods like {method}`~Pipeline.update_renames` or
        {method}`~Pipeline.update_defaults`. Should be called manually after e.g.,
        manually updating `pipeline.validate_type_annotations` or changing some other attributes.
        """
        validate_scopes(self.functions)
        validate_consistent_defaults(self.functions, output_to_func=self.output_to_func)
        self._validate_mapspec()
        if self.validate_type_annotations:
            validate_consistent_type_annotations(self.graph)

    def _validate_mapspec(self) -> None:
        """Validate the MapSpecs for all functions in the pipeline."""
        for f in self.functions:
            if f.mapspec and at_least_tuple(f.output_name) != f.mapspec.output_names:
                msg = (
                    f"The output_name of the function `{f}` does not match the output_names"
                    f" in the MapSpec: `{f.output_name}` != `{f.mapspec.output_names}`."
                )
                raise ValueError(msg)
        validate_consistent_axes(self.mapspecs(ordered=False))
        self._autogen_mapspec_axes()

    @functools.cached_property
    def unique_leaf_node(self) -> PipeFunc:
        pass

    @functools.cached_property
    def topological_generations(self) -> Generations:
        pass

    @functools.cached_property
    def sorted_functions(self) -> list[PipeFunc]:
        pass


    def _autogen_mapspec_axes(self) -> set[PipeFunc]:
        """Generate `MapSpec`s for functions that return arrays with ``internal_shapes``."""
        root_args = self.topological_generations.root_args
        mapspecs = self.mapspecs(ordered=False)
        non_root_inputs = find_non_root_axes(mapspecs, root_args)
        output_names = {at_least_tuple(f.output_name) for f in self.functions}
        multi_output_mapping = {n: names for names in output_names for n in names if len(names) > 1}
        replace_none_in_axes(mapspecs, non_root_inputs, multi_output_mapping)  # type: ignore[arg-type]
        return create_missing_mapspecs(self.functions, non_root_inputs)  # type: ignore[arg-type]

    def add_mapspec_axis(self, *parameter: str, axis: str) -> None:
        pass


    def visualize(
        self,
        *,
        backend: Literal["matplotlib", "graphviz", "graphviz_widget", "holoviews"] | None = None,
        **kwargs: Any,
    ) -> Any:
        pass

    def visualize_graphviz(
        self,
        *,
        figsize: tuple[int, int] | int | None = None,
        collapse_scopes: bool | Sequence[str] = False,
        min_arg_group_size: int | None = None,
        hide_default_args: bool = False,
        filename: str | Path | None = None,
        style: GraphvizStyle | None = None,
        orient: Literal["TB", "LR", "BT", "RL"] = "LR",
        graphviz_kwargs: dict[str, Any] | None = None,
        show_legend: bool = True,
        include_full_mapspec: bool = False,
        return_type: Literal["graphviz", "html"] | None = None,
    ) -> graphviz.Digraph | IPython.display.HTML:
        pass

    def visualize_graphviz_widget(
        self,
        *,
        orient: Literal["TB", "LR", "BT", "RL"] = "TB",
        graphviz_kwargs: dict[str, Any] | None = None,
    ) -> ipywidgets.VBox:
        pass

    def visualize_matplotlib(
        self,
        figsize: tuple[int, int] | int = (10, 10),
        filename: str | Path | None = None,
        *,
        color_combinable: bool = False,
        conservatively_combine: bool = False,
        output_name: OUTPUT_TYPE | None = None,
    ) -> None:
        pass

    def visualize_holoviews(self, *, show: bool = False) -> hv.Graph | None:
        pass

    def print_profiling_stats(self) -> None:
        """Display the resource usage report for each function in the pipeline."""
        if not self.profiling_stats:
            msg = "Profiling is not enabled."
            raise ValueError(msg)
        print_profiling_stats(self.profiling_stats)

    def simplified_pipeline(
        self,
        output_name: OUTPUT_TYPE | None = None,
        *,
        conservatively_combine: bool = False,
    ) -> Pipeline:
        pass

    @functools.cached_property
    def leaf_nodes(self) -> list[PipeFunc]:
        pass

    @functools.cached_property
    def root_nodes(self) -> list[PipeFunc]:
        pass

    @property
    def profiling_stats(self) -> dict[str, ProfilingStats]:
        pass

    def __str__(self) -> str:
        """Return a string representation of the pipeline."""
        pipeline_str = "Pipeline:\n"
        for node in self.graph.nodes:
            if isinstance(node, PipeFunc):
                fn = node
                input_args = self.all_arg_combinations[fn.output_name]
                pipeline_str += f"  {fn.output_name} = {fn.__name__}({', '.join(fn.parameters)})\n"
                pipeline_str += f"    Possible input arguments: {input_args}\n"
        return pipeline_str

    def copy(self, **update: Any) -> Pipeline:
        """Return a copy of the pipeline.

        Parameters
        ----------
        update
            Keyword arguments passed to the `Pipeline` constructor instead of the
            original values.

        """
        kwargs = {
            "functions": self.functions,
            "lazy": self.lazy,
            "debug": self._debug,
            "profile": self._profile,
            "print_error": self._print_error,
            "cache_type": self._cache_type,
            "cache_kwargs": self._cache_kwargs,
            "default_resources": self._default_resources,
            "validate_type_annotations": self.validate_type_annotations,
            "name": self.name,
            "description": self.description,
        }
        assert_complete_kwargs(kwargs, Pipeline.__init__, skip={"self", "scope"})
        kwargs.update(update)
        return Pipeline(**kwargs)  # type: ignore[arg-type]

    @property
    def error_snapshot(self) -> ErrorSnapshot | None:
        pass

    def nest_funcs(
        self,
        output_names: OUTPUT_TYPE | Iterable[OUTPUT_TYPE] | Literal["*"],
        new_output_name: OUTPUT_TYPE | None = None,
        function_name: str | None = None,
    ) -> NestedPipeFunc:
        pass

    def join(self, *pipelines: Pipeline | PipeFunc) -> Pipeline:
        """Join multiple pipelines into a single new pipeline.

        The new pipeline has no `default_resources` set, instead, each function has a
        `Resources` attribute that is created via
        ``Resources.maybe_with_defaults(f.resources, pipeline.default_resources)``.

        Parameters
        ----------
        pipelines
            The pipelines to join. Can also be individual `PipeFunc` instances.

        Returns
        -------
            A new pipeline containing all functions from the original pipelines.

        """
        functions = []
        for pipeline in [self, *pipelines]:
            if isinstance(pipeline, Pipeline):
                for f in pipeline.functions:
                    f_new = f.copy(resources=f.resources)
                    functions.append(f_new)
            elif isinstance(pipeline, PipeFunc):
                functions.append(pipeline.copy())
            else:
                msg = "Only `Pipeline` or `PipeFunc` instances can be joined."
                raise TypeError(msg)

        return self.copy(functions=functions, default_resources=None)

    def __or__(self, other: Pipeline | PipeFunc) -> Pipeline:
        """Combine two pipelines using the ``|`` operator.

        See Also
        --------
        join
            The method that is called when using the ``|`` operator.

        Examples
        --------
        >>> pipeline1 = Pipeline([f1, f2])
        >>> pipeline2 = Pipeline([f3, f4])
        >>> combined_pipeline = pipeline1 | pipeline2

        """
        return self.join(other)

    def _connected_components(self) -> list[set[PipeFunc | str]]:
        pass

    def split_disconnected(self: Pipeline, **pipeline_kwargs: Any) -> tuple[Pipeline, ...]:
        pass

    def _axis_in_root_arg(
        self,
        axis: str,
        output_name: OUTPUT_TYPE,
        root_args: tuple[str, ...] | None = None,
        visited: set[OUTPUT_TYPE] | None = None,
        result: set[bool] | None = None,
    ) -> bool:
        if root_args is None:
            root_args = self.root_args(output_name)
        if visited is None:
            visited = set()
        if result is None:
            result = set()
        if output_name in visited:
            return None  # type: ignore[return-value]

        visited.add(output_name)
        visited.update(at_least_tuple(output_name))

        func = self.output_to_func[output_name]
        assert func.mapspec is not None
        if axis not in func.mapspec.output_indices:  # pragma: no cover
            msg = f"Axis `{axis}` not in output indices for `{output_name=}`"
            raise ValueError(msg)

        if axis not in func.mapspec.input_indices:
            result.add(False)  # noqa: FBT003

        axes = self.mapspec_axes
        for name in func.mapspec.input_names:
            if axis not in axes[name]:
                continue
            if name in root_args:
                if axis in axes[name]:
                    result.add(True)  # noqa: FBT003
            else:
                self._axis_in_root_arg(axis, name, root_args, visited, result)

        return all(result)

    def independent_axes_in_mapspecs(self, output_name: OUTPUT_TYPE) -> set[str]:
        """Return the axes that are both in the output and in the root arguments.

        Identifies axes that are cross-products and can be computed independently.
        """
        func = self.output_to_func[output_name]
        if func.mapspec is None:
            return set()
        return {
            axis
            for axis in func.mapspec.output_indices
            if self._axis_in_root_arg(axis, output_name)
        }

    def subpipeline(
        self,
        inputs: set[str] | None = None,
        output_names: OUTPUT_TYPE | Iterable[OUTPUT_TYPE] | None = None,
    ) -> Pipeline:
        """Create a new pipeline containing only the nodes between the specified inputs and outputs.

        Parameters
        ----------
        inputs
            Set of input names to include in the subpipeline. If ``None``, all root nodes of the
            original pipeline will be used as inputs.
        output_names
            Output name(s) to include in the subpipeline. Can be a single output name
            (a ``tuple`` is the output name of a function with multiple outputs, as in
            `Pipeline.run`) or a collection of output names. If ``None``, all leaf nodes
            of the original pipeline will be used as outputs.

        Returns
        -------
            A new pipeline containing only the nodes and connections between the specified
            inputs and outputs.

        Notes
        -----
        The subpipeline is created by copying the original pipeline and then removing the nodes
        that are not part of the path from the specified inputs to the specified outputs. The
        resulting subpipeline will have the same behavior as the original pipeline for the
        selected inputs and outputs.

        If ``inputs`` is provided, the subpipeline will use those nodes as the new root nodes. If
        ``output_names`` is provided, the subpipeline will use those nodes as the new leaf nodes.

        Examples
        --------
        >>> @pipefunc(output_name="y", mapspec="x[i] -> y[i]")
        ... def f(x: int) -> int:
        ...     return x
        ...
        >>> @pipefunc(output_name="z")
        ... def g(y: np.ndarray) -> int:
        ...     return sum(y)
        ...
        >>> pipeline = Pipeline([f, g])
        >>> inputs = {"x": [1, 2, 3]}
        >>> results = pipeline.map(inputs, "tmp_path")
        >>> partial = pipeline.subpipeline({"y"})
        >>> r = partial.map({"y": results["y"].output}, "tmp_path")
        >>> assert len(r) == 1
        >>> assert r["z"].output == 6

        """
        if inputs is None and output_names is None:
            msg = "At least one of `inputs` or `output_names` should be provided."
            raise ValueError(msg)

        output_names = ensure_output_names_set(output_names)
        pipeline = self.copy()

        input_nodes: set[str | PipeFunc] = (
            set(pipeline.topological_generations.root_args)
            if inputs is None
            else {pipeline.node_mapping[n] for n in inputs}
        )
        output_nodes: set[PipeFunc] = (
            set(pipeline.leaf_nodes)
            if output_names is None
            else {pipeline.node_mapping[n] for n in output_names}  # type: ignore[misc]
        )
        between = _find_nodes_between(pipeline.graph, input_nodes, output_nodes)
        drop = [f for f in pipeline.functions if f not in between]
        for f in drop:
            pipeline.drop(f=f)

        if inputs is not None:
            root_args = set(pipeline.topological_generations.root_args)
            new_required_root_args = root_args - pipeline.defaults.keys()
            if not new_required_root_args.issubset(inputs):
                outputs = {f.output_name for f in pipeline.functions}
                msg = (
                    f"Cannot construct a partial pipeline with `{outputs=}`"
                    f" and `{inputs=}`, it would require `{new_required_root_args}`."
                )
                raise ValueError(msg)

        return pipeline

    def _repr_mimebundle_(
        self,
        include: set[str] | None = None,
        exclude: set[str] | None = None,
    ) -> dict[str, str]:  # pragma: no cover
        pass

    def print_documentation(
        self,
        *,
        borders: bool = False,
        skip_optional: bool = False,
        skip_intermediate: bool = True,
        description_table: bool = True,
        parameters_table: bool = True,
        returns_table: bool = True,
        order: Literal["topological", "alphabetical"] = "topological",
    ) -> None:
        pass

    def pydantic_model(self, model_name: str = "InputModel") -> type[pydantic.BaseModel]:
        pass

    def cli(self: Pipeline, description: str | None = None) -> None:
        pass


class Generations(NamedTuple):
    root_args: list[str]
    function_lists: list[list[PipeFunc]]


@dataclass(frozen=True, slots=True, eq=True)
class _Bound:
    name: str
    output_name: OUTPUT_TYPE


@dataclass(frozen=True, slots=True, eq=True)
class _Resources:
    name: str
    output_name: OUTPUT_TYPE


class _PipelineAsFunc:

    __slots__ = ["_call_with_root_args", "output_name", "pipeline", "root_args"]

    def __init__(
        self,
        pipeline: Pipeline,
        output_name: OUTPUT_TYPE | list[OUTPUT_TYPE],
        root_args: tuple[str, ...],
    ) -> None:
        """Initialize the function wrapper."""
        self.pipeline = pipeline
        self.output_name = output_name
        self.root_args = root_args
        self._call_with_root_args: Callable[..., Any] | None = None


    def __call__(self, **kwargs: Any) -> Any:
        """Call the pipeline function with the given arguments.

        Parameters
        ----------
        kwargs
            Keyword arguments to be passed to the pipeline function.

        Returns
        -------
            The return value of the pipeline function.

        """
        return self.pipeline.run(output_name=self.output_name, kwargs=kwargs)

    def call_full_output(self, **kwargs: Any) -> dict[str, Any]:
        pass

    def call_with_dict(self, kwargs: dict[str, Any]) -> Any:
        pass

    def __getstate__(self) -> dict:
        """Prepare the state of the current object for pickling."""
        state = {slot: getattr(self, slot) for slot in self.__slots__}
        state["_call_with_root_args"] = None  # don't pickle the execute method
        return state

    def __setstate__(self, state: dict) -> None:
        """Restore the state of the current object from the provided state."""
        for slot in self.__slots__:
            setattr(self, slot, state[slot])
        self._call_with_root_args = None




def _update_all_results(
    func: PipeFunc,
    r: Any,
    output_name: OUTPUT_TYPE,
    all_results: dict[OUTPUT_TYPE, Any],
    lazy: bool,
) -> None:
    if isinstance(func.output_name, tuple):
        assert func.output_picker is not None
        for name in func.output_name:
            all_results[name] = (
                _LazyFunction(func.output_picker, args=(r, name))
                if lazy
                else func.output_picker(r, name)
            )
        if isinstance(output_name, tuple):
            all_results[func.output_name] = r
    else:
        all_results[func.output_name] = r


def _execute_func(func: PipeFunc, func_args: dict[str, Any], lazy: bool) -> Any:
    if lazy:
        return _LazyFunction(func, kwargs=func_args)
    try:
        return func(**func_args)
    except Exception as e:
        handle_pipefunc_error(e, func, func_args, "raise")
        raise  # pragma: no cover












def _traverse_graph(
    start: OUTPUT_TYPE | PipeFunc,
    direction: Literal["predecessors", "successors"],
    graph: nx.DiGraph,
    node_mapping: dict[OUTPUT_TYPE, PipeFunc | str],
) -> list[OUTPUT_TYPE]:
    visited = set()

    def _traverse(x: OUTPUT_TYPE | PipeFunc) -> list[OUTPUT_TYPE]:
        results = set()
        if isinstance(x, str | tuple):
            x = node_mapping[x]
        for neighbor in getattr(graph, direction)(x):
            if isinstance(neighbor, PipeFunc):
                output_name = neighbor.output_name
                if output_name not in visited:
                    visited.add(output_name)
                    results.add(output_name)
                    results.update(_traverse(neighbor))
        return results  # type: ignore[return-value]

    return sorted(_traverse(start), key=at_least_tuple)


def _find_nodes_between(
    graph: nx.DiGraph,
    input_nodes: set[Any],
    output_nodes: set[Any],
) -> set[Any]:
    reachable_from_inputs = set()
    for input_node in input_nodes:
        reachable_from_inputs.update(nx.descendants(graph, input_node))
    reachable_to_outputs = set()
    for output_node in output_nodes:
        reachable_to_outputs.update(nx.ancestors(graph, output_node))
    reachable_to_outputs.update(output_nodes)
    return reachable_from_inputs & reachable_to_outputs


@dataclass(frozen=True, slots=True)
class _PipelineInternalCache:
    arg_combinations: dict[OUTPUT_TYPE, set[tuple[str, ...]]] = field(default_factory=dict)
    root_args: dict[OUTPUT_TYPE | None, tuple[str, ...]] = field(default_factory=dict)
    func: dict[OUTPUT_TYPE | tuple[OUTPUT_TYPE, ...], _PipelineAsFunc] = field(default_factory=dict)
    func_defaults: dict[OUTPUT_TYPE, dict[str, Any]] = field(default_factory=dict)


def _rich_info_table(info: dict[str, tuple[str, ...]], *, prints: bool = False) -> Table:
    """Create a rich table from a dictionary of information."""
    requires("rich", reason="print_table=True", extras="rich")
    import rich.table

    table = rich.table.Table(title="Pipeline Info", box=rich.box.DOUBLE)
    table.add_column("Category", style="dim", width=20)
    table.add_column("Items")

    for category, items in info.items():
        styles = {"required_inputs": "bold green", "optional_inputs": "bold yellow"}
        table.add_row(category, ", ".join(items), style=styles.get(category))
    if prints:
        console = rich.get_console()
        console.print(table)
    return table


def _root_args(pipeline: Pipeline, output_name: OUTPUT_TYPE | list[OUTPUT_TYPE]) -> tuple[str, ...]:
    if not isinstance(output_name, list):
        return pipeline.root_args(output_name)
    root_args: list[str] = []
    for name in output_name:
        root_args.extend(pipeline.root_args(name))
    return tuple(dict.fromkeys(root_args))


from __future__ import annotations

import contextlib
import dataclasses
import datetime
import functools
import hashlib
import inspect
import os
import warnings
import weakref
from collections import defaultdict
from typing import (
    TYPE_CHECKING,
    Any,
    Generic,
    Literal,
    ParamSpec,
    TypeVar,
    get_args,
    get_origin,
)

import cloudpickle

from pipefunc._profile import ProfilingStats, ResourceProfiler
from pipefunc._utils import (
    assert_complete_kwargs,
    at_least_tuple,
    clear_cached_properties,
    format_function_call,
    is_classmethod,
    is_lazyframe_annotation,
    is_pydantic_base_model,
    requires,
)
from pipefunc.cache import compute_function_hash
from pipefunc.exceptions import ErrorSnapshot, PropagatedErrorSnapshot
from pipefunc.lazy import evaluate_lazy
from pipefunc.map._mapspec import ArraySpec, MapSpec, mapspec_axes
from pipefunc.map._run import _EVALUATED_RESOURCES
from pipefunc.resources import Resources
from pipefunc.typing import NoAnnotation, safe_get_type_hints

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

    import pydantic

    from pipefunc import Pipeline
    from pipefunc._pipeline._types import OUTPUT_TYPE
    from pipefunc.map._types import ShapeTuple

P = ParamSpec("P")
R = TypeVar("R")

MAX_PARAMS_LEN = 15


class PipeFunc(Generic[P, R]):

    __test__ = False

    def __init__(
        self,
        func: Callable[P, R],
        output_name: OUTPUT_TYPE,
        *,
        output_picker: Callable[[Any, str], Any] | None = None,
        renames: dict[str, str] | None = None,
        defaults: dict[str, Any] | None = None,
        bound: dict[str, Any] | None = None,
        profile: bool = False,
        debug: bool = False,
        print_error: bool = True,
        cache: bool = False,
        mapspec: str | MapSpec | None = None,
        internal_shape: int | Literal["?"] | ShapeTuple | None = None,
        post_execution_hook: Callable[[PipeFunc, Any, dict[str, Any]], None] | None = None,
        resources: dict
        | Resources
        | Callable[[dict[str, Any]], Resources | dict[str, Any]]
        | None = None,
        resources_variable: str | None = None,
        resources_scope: Literal["map", "element"] = "map",
        scope: str | None = None,
        variant: str | dict[str | None, str] | None = None,
        variant_group: str | None = None,  # deprecated
    ) -> None:
        """Function wrapper class for pipeline functions with additional attributes."""
        self._pipelines: weakref.WeakSet[Pipeline] = weakref.WeakSet()
        self.func: Callable[P, R] = func
        self.__name__ = _get_name(func)
        doc = getattr(func, "__doc__", None)
        if doc and doc != getattr(type(func), "__doc__", None):
            self.__doc__ = doc
        self._output_name: OUTPUT_TYPE = output_name
        self.debug = debug
        self.print_error = print_error
        self.cache = cache
        self.mapspec = _maybe_mapspec(mapspec)
        self.internal_shape: int | Literal["?"] | ShapeTuple | None = internal_shape
        self.post_execution_hook = post_execution_hook
        self._output_picker: Callable[[Any, str], Any] | None = output_picker
        self.profile = profile
        self._renames: dict[str, str] = renames or {}
        self._defaults: dict[str, Any] = defaults or {}
        self._bound: dict[str, Any] = bound or {}
        self.resources = Resources.maybe_from_dict(resources)
        self.resources_variable = resources_variable
        self.resources_scope: Literal["map", "element"] = resources_scope
        _maybe_variant_group_error(variant_group, variant)
        self.variant = _ensure_variant(variant)
        self.profiling_stats: ProfilingStats | None
        if scope is not None:
            self.update_scope(scope, inputs="*", outputs="*")
        self._validate()
        self.error_snapshot: ErrorSnapshot | None = None

    @property
    def renames(self) -> dict[str, str]:
        pass

    @property
    def bound(self) -> dict[str, Any]:
        pass

    @functools.cached_property
    def output_name(self) -> OUTPUT_TYPE:
        pass


    @property
    def original_parameters(self) -> dict[str, inspect.Parameter]:
        pass

    @functools.cached_property
    def defaults(self) -> dict[str, Any]:
        pass

    @functools.cached_property
    def _inverse_renames(self) -> dict[str, str]:
        pass

    @functools.cached_property
    def output_picker(self) -> Callable[[Any, str], Any] | None:
        """Return the output picker function for the wrapped function.

        The output picker function takes the output of the wrapped function as first
        argument and the ``output_name`` (str) as second argument, and returns the
        desired output.
        """
        if self._output_picker is None and isinstance(self.output_name, tuple):
            return functools.partial(_default_output_picker, output_name=self.output_name)
        return self._output_picker

    def update_defaults(self, defaults: dict[str, Any], *, overwrite: bool = False) -> None:
        pass

    def update_renames(
        self,
        renames: dict[str, str],
        *,
        update_from: Literal["current", "original"] = "current",
        overwrite: bool = False,
    ) -> None:
        """Update renames to function arguments and ``output_name`` for the wrapped function.

        When renaming the ``output_name`` and if it is a tuple of strings, the
        renames must be provided as individual strings in the tuple.

        Parameters
        ----------
        renames
            A dictionary of renames for the function arguments or ``output_name``.
        update_from
            Whether to update the renames from the ``"current"`` parameter names
            (`PipeFunc.parameters`) or from the ``"original"`` parameter names as
            in the function signature (`PipeFunc.original_parameters`). If also updating
            the ``output_name``, original means the name that was provided to the
            `PipeFunc` instance.
        overwrite
            Whether to overwrite the existing renames. If ``False``, the new
            renames will be added to the existing renames.

        """
        assert update_from in ("current", "original")
        assert all(isinstance(k, str) for k in renames.keys())  # noqa: SIM118
        assert all(isinstance(v, str) for v in renames.values())
        allowed_parameters = tuple(
            self.parameters + at_least_tuple(self.output_name)
            if update_from == "current"
            else tuple(self.original_parameters) + at_least_tuple(self._output_name),
        )
        self._validate_update(renames, "renames", allowed_parameters)
        if update_from == "current":
            renames = {
                self._inverse_renames.get(k, k): v
                for k, v in renames.items()
                if k in allowed_parameters
            }
        old_inverse = self._inverse_renames.copy()
        bound_original = {old_inverse.get(k, k): v for k, v in self._bound.items()}
        defaults_original = {old_inverse.get(k, k): v for k, v in self._defaults.items()}
        if overwrite:
            self._renames = renames.copy()
        else:
            self._renames = dict(self._renames, **renames)

        new_defaults = {}
        for name, value in defaults_original.items():
            name = self._renames.get(name, name)  # noqa: PLW2901
            new_defaults[name] = value
        self._defaults = new_defaults

        new_bound = {}
        for name, value in bound_original.items():
            new_name = self._renames.get(name, name)
            new_bound[new_name] = value
        self._bound = new_bound

        if self.mapspec is not None:
            self.mapspec = self.mapspec.rename(old_inverse).rename(self._renames)

        self._clear_internal_cache()
        self._validate()

    def update_scope(
        self,
        scope: str | None,
        inputs: set[str] | Literal["*"] | None = None,
        outputs: set[str] | Literal["*"] | None = None,
        exclude: set[str] | None = None,
    ) -> None:
        pass

    def update_mapspec_axes(self, renames: dict[str, str]) -> None:
        pass

    def update_bound(self, bound: dict[str, Any], *, overwrite: bool = False) -> None:
        pass

    def _clear_internal_cache(self, *, clear_pipelines: bool = True) -> None:
        clear_cached_properties(self, PipeFunc)
        if clear_pipelines:
            for pipeline in self._pipelines:
                pipeline._clear_internal_cache()

    def _validate_update(
        self,
        update: dict[str, Any],
        name: str,
        parameters: tuple[str, ...],
    ) -> None:
        if extra := set(update) - set(parameters):
            msg = (
                f"Unexpected `{name}` arguments: `{extra}`."
                f" The allowed arguments are: `{parameters}`."
                f" The provided arguments are: `{update}`."
            )
            raise ValueError(msg)

        for key, value in update.items():
            _validate_identifier(name, key)
            if name == "renames":
                _validate_identifier(name, value)

    def _validate(self) -> None:
        self._validate_names()
        self._validate_mapspec()

    def _validate_names(self) -> None:
        if common := set(self._defaults) & set(self._bound):
            msg = (
                f"The following parameters are both defaults and bound: `{common}`."
                " This is not allowed."
            )
            raise ValueError(msg)
        if not isinstance(self._output_name, str | tuple):
            msg = (
                f"The `output_name` should be a string or a tuple of strings,"
                f" not {type(self._output_name)}."
            )
            raise TypeError(msg)
        if self.resources_variable is not None:
            try:
                self.original_parameters  # noqa: B018
            except KeyError as e:
                msg = (
                    f"The `resources_variable={self.resources_variable!r}`"
                    " should be a parameter of the function."
                )
                raise ValueError(msg) from e
        if overlap := set(self.parameters) & set(at_least_tuple(self.output_name)):
            msg = (
                "The `output_name` cannot be the same as any of the input"
                f" parameter names. The overlap is: {overlap}"
            )
            raise ValueError(msg)
        if len(self._renames) != len(self._inverse_renames):
            inverse_renames = defaultdict(list)
            for k, v in self._renames.items():
                inverse_renames[v].append(k)
            violations = {k: v for k, v in inverse_renames.items() if len(v) > 1}
            violation_details = "; ".join(f"`{k}: {v}`" for k, v in violations.items())
            msg = (
                f"The `renames` should be a one-to-one mapping. Found violations where "
                f"multiple keys map to the same value: {violation_details}."
            )
            raise ValueError(msg)
        self._validate_update(
            self._renames,
            "renames",
            tuple(self.original_parameters) + at_least_tuple(self._output_name),  # type: ignore[arg-type]
        )
        self._validate_update(self._defaults, "defaults", self.parameters)
        self._validate_update(self._bound, "bound", self.parameters)
        for name in at_least_tuple(self.output_name):
            _validate_identifier("output_name", name)

    @property
    def __wrapped__(self) -> Callable[P, R]:
        """Return the wrapped function, following the convention of `functools.wraps`."""
        return self.func

    def copy(self, **update: Any) -> PipeFunc[P, R]:
        """Create a copy of the `PipeFunc` instance, optionally updating the attributes."""
        kwargs = {
            "func": self.func,
            "output_name": self._output_name,
            "output_picker": self._output_picker,
            "renames": self._renames,
            "defaults": self._defaults,
            "bound": self._bound,
            "profile": self._profile,
            "print_error": self.print_error,
            "debug": self.debug,
            "cache": self.cache,
            "mapspec": self.mapspec,
            "internal_shape": self.internal_shape,
            "post_execution_hook": self.post_execution_hook,
            "resources": self.resources,
            "resources_variable": self.resources_variable,
            "resources_scope": self.resources_scope,
            "variant": self.variant,
            "variant_group": None,  # deprecated
        }
        assert_complete_kwargs(kwargs, PipeFunc.__init__, skip={"self", "scope"})
        kwargs.update(update)
        return PipeFunc(**kwargs)  # type: ignore[arg-type,type-var]

    @functools.cached_property
    def _evaluate_lazy(self) -> bool:
        pass

    def _rename_to_native(self, kwargs: dict[str, Any]) -> dict[str, Any]:
        return {self._inverse_renames.get(k, k): v for k, v in kwargs.items()}

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> R:
        """Call the wrapped function with the given arguments.

        Note that ``renames``, ``defaults``, ``bound``, and scopes are applied,
        so the parameter names may differ from the original function signature
        (which is what type checkers see).

        Returns
        -------
            The return value of the wrapped function.

        """
        return self._call(*args, **kwargs)


    @functools.cached_property
    def __signature__(self) -> inspect.Signature:
        """Return the signature of `__call__` with renamed parameters.

        If *any* of the output annotations are `NoAnnotation`, the return annotation
        is set to `inspect.Parameter.empty`.
        """
        if self._output_picker is None:
            output_annotations = self.output_annotation
            if any(v is NoAnnotation for v in output_annotations.values()):
                return_annotation = inspect.Parameter.empty
            elif isinstance(self.output_name, tuple):
                return_annotations = tuple(output_annotations[name] for name in self.output_name)
                return_annotation = tuple[return_annotations]  # type: ignore[assignment, valid-type]
            else:
                return_annotation = output_annotations[self.output_name]
        else:
            return_annotation = inspect.Parameter.empty
        parameters = [
            inspect.Parameter(
                name=name if "." not in name else _ScopedIdentifier(name),
                kind=inspect.Parameter.POSITIONAL_OR_KEYWORD,
                default=self.defaults.get(name, inspect.Parameter.empty),
                annotation=self.parameter_annotations.get(name, inspect.Parameter.empty),
            )
            for name in self.parameters
            if name not in self.bound
        ]
        return inspect.Signature(parameters, return_annotation=return_annotation)

    @property
    def profile(self) -> bool:
        pass

    @profile.setter
    def profile(self, enable: bool) -> None:
        pass

    @functools.cached_property
    def parameter_scopes(self) -> set[str]:
        pass

    @functools.cached_property
    def unscoped_parameters(self) -> tuple[str, ...]:
        pass

    def _flatten_scopes(self, kwargs: dict[str, Any]) -> dict[str, Any]:
        """Flatten the scopes of the function parameters.

        Flattens `{scope: {name: value}}` to `{f"{scope}.{name}": value}`.

        Examples
        --------
        >>> f_c(x={"a": 1, "b": 1})
        >>> f_c(**{"x.a": 1, "x.b": 1})
        >>> f_c(x=dict(a=1), **{"x.b": 1})

        """
        if not self.parameter_scopes:
            return kwargs

        requires_flattening = self.parameter_scopes & kwargs.keys()
        if not requires_flattening:
            return kwargs

        new_kwargs = {}
        for k, v in kwargs.items():
            if k in self.parameter_scopes:
                new_kwargs.update({f"{k}.{name}": value for name, value in v.items()})
            else:
                new_kwargs[k] = v
        return new_kwargs

    @functools.cached_property
    def parameter_annotations(self) -> dict[str, Any]:
        pass

    @functools.cached_property
    def _lazyframe_parameters(self) -> tuple[str, ...]:
        pass

    def _convert_lazyframe_kwargs(self, kwargs: dict[str, Any]) -> None:
        """Convert `pl.DataFrame` values to `pl.LazyFrame` where the annotation asks for it."""
        if not self._lazyframe_parameters:  # fast path, avoids per-element overhead
            return
        import polars as pl

        for p in self._lazyframe_parameters:
            value = kwargs.get(p)
            if isinstance(value, pl.DataFrame):
                kwargs[p] = value.lazy()

    @functools.cached_property
    def output_annotation(self) -> dict[str, Any]:
        pass


    def _maybe_profiler(self) -> contextlib.AbstractContextManager:
        pass

    def __str__(self) -> str:
        """Return a string representation of the PipeFunc instance.

        Returns
        -------
            A string representation of the PipeFunc instance.

        """
        outputs = ", ".join(at_least_tuple(self.output_name))
        return f"{self.__name__}(...) → {outputs}"

    def __repr__(self) -> str:
        """Return a string representation of the PipeFunc instance.

        Returns
        -------
            A string representation of the PipeFunc instance.

        """
        return f"PipeFunc({self.__name__})"

    def __getstate__(self) -> dict:
        """Prepare the state of the current object for pickling.

        The state includes all picklable instance variables.
        For non-picklable instance variables, they are transformed
        into a picklable form or ignored.

        Returns
        -------
            A dictionary containing the picklable state of the object.

        """
        state = {
            k: v for k, v in self.__dict__.items() if k not in ("func", "_pipelines", "resources")
        }
        state["func"] = cloudpickle.dumps(self.func)
        state["resources"] = (
            cloudpickle.dumps(self.resources) if self.resources is not None else None
        )
        return state

    def __setstate__(self, state: dict) -> None:
        """Restore the state of the current object from the provided state.

        It also handles restoring non-picklable instance variable
        into their original form.

        Parameters
        ----------
        state
            A dictionary containing the picklable state of the object.

        """
        self.__dict__.update(state)
        self._pipelines = weakref.WeakSet()
        self.func = cloudpickle.loads(self.func)
        self.resources = cloudpickle.loads(self.resources) if self.resources is not None else None

    def _validate_mapspec(self) -> None:
        if self.mapspec is None:
            return

        if not isinstance(self.mapspec, MapSpec):  # pragma: no cover
            msg = (
                "The 'mapspec' argument should be an instance of MapSpec,"
                f" not {type(self.mapspec)}."
            )
            raise TypeError(msg)

        mapspec_input_names = set(self.mapspec.input_names)
        if extra := mapspec_input_names - set(self.parameters):
            msg = (
                f"The input of the function `{self.__name__}` should match"
                f" the input of the MapSpec `{self.mapspec}`:"
                f" `{extra} not in {self.parameters}`."
            )
            raise ValueError(msg)

        if bound_inputs := self._bound.keys() & mapspec_input_names:
            msg = (
                f"The bound arguments cannot be part of the MapSpec input names."
                f" The violating bound arguments are: `{bound_inputs}`."
                " Because bound arguments might have the same name in different"
                " functions and the MapSpec input names are unique, the bound"
                " arguments cannot be part of the MapSpec input names."
            )
            raise ValueError(msg)

        mapspec_output_names = set(self.mapspec.output_names)
        output_names = set(at_least_tuple(self.output_name))
        if mapspec_output_names != output_names:
            msg = (
                f"The output of the function `{self.__name__}` should match"
                f" the output of the MapSpec `{self.mapspec}`:"
                f" `{mapspec_output_names} != {output_names}`."
            )
            raise ValueError(msg)

    @functools.cached_property
    def _cache_id(self) -> str:
        pass


def pipefunc(
    output_name: OUTPUT_TYPE,
    *,
    output_picker: Callable[[Any, str], Any] | None = None,
    renames: dict[str, str] | None = None,
    defaults: dict[str, Any] | None = None,
    bound: dict[str, Any] | None = None,
    profile: bool = False,
    debug: bool = False,
    print_error: bool = True,
    cache: bool = False,
    mapspec: str | MapSpec | None = None,
    internal_shape: int | Literal["?"] | ShapeTuple | None = None,
    post_execution_hook: Callable[[PipeFunc, Any, dict[str, Any]], None] | None = None,
    resources: dict
    | Resources
    | Callable[[dict[str, Any]], Resources | dict[str, Any]]
    | None = None,
    resources_variable: str | None = None,
    resources_scope: Literal["map", "element"] = "map",
    scope: str | None = None,
    variant: str | dict[str | None, str] | None = None,
    variant_group: str | None = None,  # deprecated
) -> Callable[[Callable[P, R]], PipeFunc[P, R]]:
    """A decorator that wraps a function in a PipeFunc instance.

    Parameters
    ----------
    output_name
        The identifier for the output of the decorated function.
        Provide a tuple of strings for multiple outputs.
    output_picker
        A function that takes the output of the wrapped function as first argument
        and the ``output_name`` (str) as second argument, and returns the desired output.
        If ``None``, the output of the wrapped function is returned as is.
    renames
        A dictionary for renaming function arguments and outputs. The keys are the
        original names (as defined in the function signature or the ``output_name``),
        and the values are the new names to be used. This allows you to change how
        the function is called without modifying its internal logic. For example,
        ``{"old_name": "new_name"}`` would allow the function to be called with
        ``new_name`` instead of ``old_name``. If renaming the ``output_name``, include it
        in this dictionary as well.
    defaults
        Set defaults for parameters. Overwrites any current defaults. Must be in terms
        of the renamed argument names.
    bound
        Bind arguments to the function. These are arguments that are fixed. Even when
        providing different values, the bound values will be used. Must be in terms of
        the renamed argument names.
    profile
        Flag indicating whether the decorated function should be profiled.
    debug
        Flag indicating whether debug information should be printed.
    print_error
        Flag indicating whether errors raised during the function execution should
        be printed.
    cache
        Flag indicating whether the decorated function should be cached.
    mapspec
        This is a specification for mapping that dictates how input values should
        be merged together. If ``None``, the default behavior is that the input directly
        maps to the output.
    internal_shape
        The shape of the output produced by this function *when it is used within a
        ``mapspec`` context*. Can be an int or a tuple of ints, or "?" for unknown
        dimensions, or a tuple with a mix of both. If not provided, the shape will be
        inferred from the first execution of the function. If provided, the shape will be
        validated against the actual shape of the output. This parameter is required only
        when a `mapspec` like `... -> out[i]` is used, indicating that the shape cannot be
        derived from the inputs. In case there are multiple outputs, provide the shape for
        one of the outputs. This works because the shape of all outputs are required to be
        identical.
    post_execution_hook
        A callback function that is invoked after the function is executed.
        The callback signature is ``hook(func: PipeFunc, result: Any, kwargs: dict) -> None``.
        This hook can be used for logging, visualization of intermediate results,
        debugging, statistics collection, or other side effects. The hook is executed
        synchronously after the function returns but before the result is passed to
        the next function in the pipeline. Keep the hook lightweight to avoid impacting performance.
    resources
        A dictionary or `Resources` instance containing the resources required
        for the function. This can be used to specify the number of CPUs, GPUs,
        memory, wall time, queue, partition, and any extra job scheduler
        arguments. This is *not* used by the `pipefunc` directly but can be
        used by job schedulers to manage the resources required for the
        function. Alternatively, provide a callable that receives a dict with the
        input values and returns a `Resources` instance.
    resources_variable
        If provided, the resources will be passed as the specified argument name to the function.
        This requires that the function has a parameter with the same name. For example,
        if ``resources_variable="resources"``, the function will be called as
        ``func(..., resources=Resources(...))``. This is useful when the function handles internal
        parallelization.
    resources_scope
        Determines how resources are allocated in relation to the mapspec:

        - "map": Allocate resources for the entire mapspec operation (default).
        - "element": Allocate resources for each element in the mapspec.

        If no mapspec is defined, this parameter is ignored.
    scope
        If provided, *all* parameter names and output names of the function will
        be prefixed with the specified scope followed by a dot (``'.'``), e.g., parameter
        ``x`` with scope ``foo`` becomes ``foo.x``. This allows multiple functions in a
        pipeline to have parameters with the same name without conflict. To be selective
        about which parameters and outputs to include in the scope, use the
        `PipeFunc.update_scope` method.

        When providing parameter values for functions that have scopes, they can
        be provided either as a dictionary for the scope, or by using the
        ``f'{scope}.{name}'`` notation. For example,
        a `PipeFunc` instance with scope "foo" and "bar", the parameters
        can be provided as: ``func(foo=dict(a=1, b=2), bar=dict(a=3, b=4))``
        or ``func(**{"foo.a": 1, "foo.b": 2, "bar.a": 3, "bar.b": 4})``.
    variant
        Identifies this function as an alternative implementation in a
        `VariantPipeline` and specifies which variant groups it belongs to.
        When multiple functions share the same `output_name`, variants allow
        selecting which implementation to use during pipeline execution.

        Can be specified in two formats:
        - A string (e.g., ``"fast"``): Places the function in the default unnamed
          group (None) with the specified variant name. Equivalent to ``{None: "fast"}``.
        - A dictionary (e.g., ``{"algorithm": "fast", "optimization": "level1"}``):
          Assigns the function to multiple variant groups simultaneously, with a
          specific variant name in each group.

        Functions with the same `output_name` but different variant specifications
        represent alternative implementations. The {meth}`VariantPipeline.with_variant`
        method selects which variants to use for execution. For example, you might
        have "preprocessing" variants ("v1"/"v2") independent from "computation"
        variants ("fast"/"accurate"), allowing you to select specific combinations
        like ``{"preprocessing": "v1", "computation": "fast"}``.
    variant_group
        DEPRECATED in v0.58.0: Use `variant` instead.

    Returns
    -------
        A wrapped function that takes the original function and ``output_name`` and
        creates a `PipeFunc` instance with the specified return identifier.

    See Also
    --------
    PipeFunc
        A function wrapper class for pipeline functions. Has the same functionality
        as the `pipefunc` decorator but can be used to wrap existing functions.

    Examples
    --------
    >>> @pipefunc(output_name="c")
    ... def add(a, b):
    ...     return a + b
    >>> add(a=1, b=2)
    3
    >>> add.update_renames({"a": "x", "b": "y"})
    >>> add(x=1, y=2)
    3

    """

    def decorator(f: Callable[P, R]) -> PipeFunc[P, R]:
        pass

    return decorator


class NestedPipeFunc(PipeFunc):

    def __init__(
        self,
        pipefuncs: list[PipeFunc],
        output_name: OUTPUT_TYPE | None = None,
        function_name: str | None = None,
        *,
        renames: dict[str, str] | None = None,
        mapspec: str | MapSpec | None = None,
        resources: dict | Resources | None = None,
        resources_scope: Literal["map", "element"] = "map",
        bound: dict[str, Any] | None = None,
        cache: bool | None = None,
        variant: str | dict[str | None, str] | None = None,
        variant_group: str | None = None,  # deprecated
    ) -> None:
        from pipefunc import Pipeline

        self._pipelines: weakref.WeakSet[Pipeline] = weakref.WeakSet()
        _validate_nested_pipefunc(pipefuncs, resources)
        self.resources = _maybe_max_resources(resources, pipefuncs)
        self.resources_scope = resources_scope
        functions = [f.copy(resources=self.resources) for f in pipefuncs]
        self.pipeline = Pipeline(functions)  # type: ignore[arg-type]
        _validate_output_name(output_name, self._all_outputs)
        self._output_name: OUTPUT_TYPE = output_name or self._all_outputs
        self.function_name = function_name
        self.debug = False  # The underlying PipeFuncs will handle this
        self.cache: bool = (
            cache if cache is not None else any(f.cache for f in self.pipeline.functions)
        )
        _maybe_variant_group_error(variant_group, variant)
        self.variant: dict[str | None, str] = _ensure_variant(variant)
        self._output_picker = None
        self._profile = False
        self.print_error = True
        self._renames: dict[str, str] = renames or {}
        self._bound: dict[str, Any] = bound or {}
        self._defaults: dict[str, Any] = {
            k: v
            for k, v in self.pipeline.defaults.items()
            if k in self.parameters and k not in self._bound
        }
        self.resources_variable = None  # not supported in NestedPipeFunc
        self.profiling_stats = None
        self.post_execution_hook = None
        self.internal_shape = None
        self.mapspec = self._combine_mapspecs() if mapspec is None else _maybe_mapspec(mapspec)
        for f in self.pipeline.functions:
            f.mapspec = None  # MapSpec is handled by the NestedPipeFunc
        self._validate()

    def copy(self, **update: Any) -> NestedPipeFunc:
        kwargs = {
            "pipefuncs": self.pipeline.functions,
            "output_name": self._output_name,
            "function_name": self.function_name,
            "renames": self._renames,
            "mapspec": self.mapspec,
            "resources": self.resources,
            "resources_scope": self.resources_scope,
            "bound": self._bound,
            "cache": self.cache,
            "variant": self.variant,
            "variant_group": None,  # deprecated
        }
        assert_complete_kwargs(kwargs, NestedPipeFunc.__init__, skip={"self"})
        kwargs.update(update)
        f = self.__class__(**kwargs)  # type: ignore[arg-type]
        f._defaults = self._defaults.copy()
        f._bound = self._bound.copy()
        f.debug = self.debug
        return f

    @functools.cached_property
    def _cache_id(self) -> str:
        pass




    @functools.cached_property
    def parameter_annotations(self) -> dict[str, Any]:
        pass



    @functools.cached_property
    def func(self) -> Callable[..., tuple[Any, ...]]:  # type: ignore[override]
        outputs = [f.output_name for f in self.pipeline.leaf_nodes]
        func = self.pipeline.func(outputs)
        return _NestedFuncWrapper(func.call_full_output, self._output_name, self.function_name)

    @functools.cached_property
    def __name__(self) -> str:  # type: ignore[override]
        return self.func.__name__

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(pipefuncs={self.pipeline.functions})"




class _NestedFuncWrapper:

    def __init__(
        self,
        func: Callable[..., dict[str, Any]],
        output_name: OUTPUT_TYPE,
        function_name: str | None = None,
    ) -> None:
        self.func: Callable[..., dict[str, Any]] = func
        self.output_name: OUTPUT_TYPE = output_name
        if function_name is not None:
            self.__name__ = function_name
        else:
            self.__name__ = f"NestedPipeFunc_{'_'.join(at_least_tuple(output_name))}"

    def __call__(self, *args: Any, **kwds: Any) -> Any:
        result_dict = self.func(*args, **kwds)
        if isinstance(self.output_name, str):
            return result_dict[self.output_name]
        return tuple(result_dict[name] for name in self.output_name)


def _validate_identifier(name: str, value: Any) -> None:
    if "." in value:
        scope, value = value.split(".", 1)
        _validate_identifier(name, scope)
        _validate_identifier(name, value)
        return
    if not value.isidentifier():
        msg = f"The `{name}` should contain/be valid Python identifier(s), not `{value}`."
        raise ValueError(msg)






def _validate_combinable_mapspecs(mapspecs: list[MapSpec | None]) -> None:
    if any(m is None for m in mapspecs):
        msg = "Cannot combine a mix of None and MapSpec instances."
        raise ValueError(msg)
    assert len(mapspecs) > 1

    first = mapspecs[0]
    assert first is not None
    for m in mapspecs:
        assert m is not None
        if m.input_indices != set(m.output_indices):
            msg = (
                f"Cannot combine MapSpecs with different input and output mappings. Mapspec: `{m}`"
            )
            raise ValueError(msg)
        if m.input_indices != first.input_indices:
            msg = f"Cannot combine MapSpecs with different input mappings. Mapspec: `{m}`"
            raise ValueError(msg)
        if m.output_indices != first.output_indices:
            msg = f"Cannot combine MapSpecs with different output mappings. Mapspec: `{m}`"
            raise ValueError(msg)


def _is_named_tuple(hint: Any) -> bool:
    pass


def _default_output_picker(output: Any, name: str, output_name: OUTPUT_TYPE) -> Any:
    pass






def _maybe_mapspec(mapspec: str | MapSpec | None) -> MapSpec | None:
    """Return either a MapSpec or None, depending on the input."""
    return MapSpec.from_string(mapspec) if isinstance(mapspec, str) else mapspec










def _ensure_variant(variant: str | dict[str | None, str] | None) -> dict[str | None, str]:
    pass




class _ScopedIdentifier(str):

    __slots__ = ()

    def isidentifier(self) -> bool:
        """Check if the string is a valid identifier.

        This method overrides the default isidentifier method to allow
        for scoped identifiers (e.g., "myscope.x").
        """
        if "." not in self:  # pragma: no cover
            return super().isidentifier()
        if self.count(".") != 1:  # pragma: no cover
            return False
        scope, name = self.split(".")
        return scope.isidentifier() and name.isidentifier()

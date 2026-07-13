
from __future__ import annotations

from collections.abc import Hashable, Iterable
from itertools import product
from typing import TYPE_CHECKING, Any

from pipefunc._utils import at_least_tuple, requires

if TYPE_CHECKING:
    from collections.abc import Callable, Generator, Iterator, Mapping, Sequence

    from pipefunc._pipeline._base import OUTPUT_TYPE, Pipeline


def _combined_exclude(
    *func: Callable[[Mapping[str, Any]], bool] | None,
) -> Callable[[Mapping[str, Any]], bool] | None:
    """Combine multiple exclude functions into one."""
    funcs = [f for f in func if f is not None]
    if len(funcs) == 0:
        return None
    if len(funcs) == 1:
        return funcs[0]
    return lambda x: any(func(x) for func in funcs)


def _combine_dicts(*maybe_dict: dict[str, Any] | None) -> dict[str, Any] | None:
    """Combine multiple dictionaries into one."""
    dicts = [d for d in maybe_dict if d is not None]
    if len(dicts) == 0:
        return None
    if len(dicts) == 1:
        return dicts[0]
    assert len(set().union(*dicts)) == sum(len(d) for d in dicts)
    return {k: v for d in dicts for k, v in d.items()}


class Sweep:

    def __init__(
        self,
        items: dict[str, Sequence[Any]],
        dims: list[str | tuple[str, ...]] | None = None,
        exclude: Callable[[Mapping[str, Any]], bool] | None = None,
        constants: Mapping[str, Any] | None = None,
        derivers: dict[str, Callable[[dict[str, Any]], Any]] | None = None,
    ) -> None:
        self.items = items
        self.dims = dims
        self.exclude = exclude
        self.constants = constants
        self.derivers = derivers

    def generate(self) -> Generator[dict[str, Any], None, None]:  # noqa: PLR0912
        pass

    def __iter__(self) -> Iterator[dict[str, Any]]:
        """Return a generator of the sweep."""
        return self.generate()

    def list(self) -> list[dict[str, Any]]:
        pass

    def filtered_sweep(self, keys: Iterable[str]) -> Sweep:  # noqa: PLR0912
        pass

    def __len__(self) -> int:
        """Return the number of unique combinations in the sweep."""
        if self.exclude is not None:
            return len(self.list())
        if self.dims is None or set(self.dims) == self.items.keys():
            total_length = 1
            for value in self.items.values():
                total_length *= len(value)
            return total_length
        total_length = 1
        for dim_group in self.dims:
            dims = at_least_tuple(dim_group)
            group_length = len(self.items[dims[0]])
            total_length *= group_length
        return total_length

    def __add__(self, other: Sweep) -> MultiSweep:
        """Combine this Sweep with another one, creating a MultiSweep."""
        if not isinstance(other, Sweep):  # pragma: no cover
            msg = "Other object must be a `Sweep` or a `MultiSweep` instance."
            raise TypeError(msg)
        return MultiSweep(self, other)

    def combine(self, other: Sweep) -> MultiSweep:
        pass

    def product(self, *others: Sweep) -> Sweep:
        """Create a Cartesian product of this Sweep with other Sweeps.

        Parameters
        ----------
        *others
            One or more Sweep objects to create a Cartesian product with.

        Returns
        -------
            A new Sweep object representing the Cartesian product of the sweeps.

        Examples
        --------
        >>> sweep1 = Sweep({'a': [1, 2], 'b': [3, 4]})
        >>> sweep2 = Sweep({'c': [5, 6]})
        >>> sweep3 = sweep1.product(sweep2)
        >>> sweep3.list()
        [{'a': 1, 'b': 3, 'c': 5}, {'a': 1, 'b': 3, 'c': 6},
         {'a': 1, 'b': 4, 'c': 5}, {'a': 1, 'b': 4, 'c': 6},
         {'a': 2, 'b': 3, 'c': 5}, {'a': 2, 'b': 3, 'c': 6},
         {'a': 2, 'b': 4, 'c': 5}, {'a': 2, 'b': 4, 'c': 6}]

        """
        items = self.items.copy()
        dims = self.dims.copy() if self.dims is not None else None

        for other in others:
            if not isinstance(other, Sweep):  # pragma: no cover
                msg = "All arguments must be Sweep instances."
                raise TypeError(msg)
            items.update(other.items)
            if dims is not None:
                if other.dims is not None:
                    dims.extend(other.dims)
                else:
                    dims.extend(list(other.items.keys()))

        return Sweep(
            items,
            dims=dims,
            exclude=_combined_exclude(self.exclude, other.exclude),
            constants=_combine_dicts(self.constants, other.constants),  # type: ignore[arg-type]
            derivers=_combine_dicts(self.derivers, other.derivers),  # type: ignore[arg-type]
        )

    def add_derivers(self, **derivers: Callable[[dict[str, Any]], Any]) -> Sweep:
        pass


class MultiSweep(Sweep):

    def __init__(self, *sweeps: Sweep) -> None:
        super().__init__(items={})
        self.sweeps = list(sweeps)

    def generate(self) -> Generator[dict[str, Any], None, None]:
        pass

    def __len__(self) -> int:
        """Return the number of unique combinations in the sweep."""
        return sum(len(sweep) for sweep in self.sweeps)

    def list(self) -> list[dict[str, Any]]:
        pass

    def filtered_sweep(self, keys: Iterable[str]) -> MultiSweep:
        pass

    def __add__(self, other: Sweep) -> MultiSweep:
        """Add another sweep to this MultiSweep."""
        if not isinstance(other, Sweep):  # pragma: no cover
            msg = "Other object must be a `Sweep` or a `MultiSweep` instance."
            raise TypeError(msg)
        return self.combine(other)

    def combine(self, other: Sweep) -> MultiSweep:
        pass


def _check_dim_lengths(seqs: Sequence[Sequence[Any]], dims: tuple[str, ...]) -> None:
    pass


def generate_sweep(
    items: dict[str, Sequence[Any]],
    dims: list[str | tuple[str, ...]] | None = None,
    exclude: Callable[[Mapping[str, Any]], bool] | None = None,
    constants: Mapping[str, Any] | None = None,
    derivers: dict[str, Callable[[dict[str, Any]], Any]] | None = None,
) -> list[dict[str, Any]]:
    """Create a sweep of a pipeline.

    Considering certain dimensions to be linked together (zipped) rather than
    forming a full Cartesian product. If 'dims' is not provided, a Cartesian
    product is formed.

    Parameters
    ----------
    items
        A dictionary where the key is the name of the dimension and the
        value is a sequence of values for that dimension.
    dims
        A list of tuples. Each tuple contains names of dimensions that are
        linked together. If not provided, a Cartesian product is formed.
    exclude
        A function that takes a dictionary of dimension values and returns
        True if the combination should be excluded from the sweep.
    constants
        A dictionary with constant values that should be added to each
        combination.
    derivers
        A dictionary of functions to be applied to each
        dict. The dictionary keys are attribute names and the
        values are functions that take a dict as input and return
        a new attribute value. The keys might be a subset of the
        items keys, which means the values will be overwritten.

    Returns
    -------
        A list of dictionaries, each representing a specific combination
        of dimension values.

    Examples
    --------
    >>> items = {'a': [1, 2], 'b': [3, 4], 'c': [5, 6]}
    >>> generate_sweep(items)
    [{'a': 1, 'b': 3, 'c': 5}, {'a': 1, 'b': 3, 'c': 6}, {'a': 1, 'b': 4, 'c': 5}, {'a': 1, 'b': 4, 'c': 6},
     {'a': 2, 'b': 3, 'c': 5}, {'a': 2, 'b': 3, 'c': 6}, {'a': 2, 'b': 4, 'c': 5}, {'a': 2, 'b': 4, 'c': 6}]

    Which is equivalent to the following:

    >>> generate_sweep(items, dims=["a", "b", "c"])
    >>> generate_sweep(items, dims=[("a",), ("b",), ("c",)])

    Or zip together dimensions:

    >>> generate_sweep(items, dims=[('a', 'b'), ('c',)])
    [{'a': 1, 'b': 3, 'c': 5}, {'a': 2, 'b': 4, 'c': 6}]

    """
    return Sweep(items, dims, exclude, constants, derivers).list()


def count_sweep(
    output_name: str,
    sweep: list[dict[str, Any]] | Sweep,
    pipeline: Pipeline,
    *,
    use_pandas: bool = False,
) -> dict[str | tuple[str, ...], dict[tuple[Any, ...], int]]:
    """Count the number of times each argument combination is used.

    Useful for determining which functions to execute first or which
    functions to cache.

    Parameters
    ----------
    output_name
        The name of the output to count the argument combinations for.
    sweep
        A list of dictionaries, each representing a values for which
        the function is called. Or a Sweep object.
    pipeline
        The pipeline to count the argument combinations for.
    use_pandas
        Whether to use pandas to create the counts. Note that this is
        slower than the default method for sweeps <≈ 1e6.

    Returns
    -------
        A dictionary where the keys are the names of the arguments and the
        values are dictionaries where the keys are the argument combinations
        and the values are the number of times that combination is used.

    """
    if isinstance(sweep, Sweep):
        sweep = sweep.list()  # type: ignore[assignment]
    assert isinstance(sweep, Iterable)
    counts: dict[OUTPUT_TYPE, dict[tuple[Any, ...], int]] = {}
    deps = pipeline.func_dependencies(output_name)
    for _output_name in deps:
        arg_combination = pipeline.root_args(_output_name)
        assert isinstance(arg_combination, tuple)
        if use_pandas:
            requires("pandas", reason="count_sweep", extras="pandas")
            import pandas as pd

            df = pd.DataFrame(list(sweep))
            cols = list(arg_combination)
            counts[_output_name] = df[cols].groupby(cols).size().to_dict()  # type: ignore[assignment]
        else:
            _cnt: dict[tuple[Any, ...], int] = {}
            for combo in sweep:
                key = tuple(combo[arg] for arg in arg_combination)
                _cnt[key] = _cnt.get(key, 0) + 1
            counts[_output_name] = _cnt
    return dict(counts)


def set_cache_for_sweep(
    output_name: str,
    pipeline: Pipeline,
    sweep: list[dict[str, Any]],
    min_executions: int = 2,
    *,
    verbose: bool = False,
) -> None:
    """Set the cache for a sweep of a pipeline."""
    pipeline[output_name].cache = False  # type: ignore[union-attr]
    cnt = count_sweep(output_name, sweep, pipeline)
    max_executions = {k: max(v.values()) for k, v in cnt.items()}
    for _output_name, n in max_executions.items():
        enable_cache = n >= min_executions
        func = pipeline[_output_name]
        if verbose:
            print(f"Setting cache for '{_output_name}' to {enable_cache} (n={n})")
        func.cache = enable_cache  # type: ignore[union-attr]

from __future__ import annotations

import functools
from collections import defaultdict
from typing import TYPE_CHECKING, Any, Literal

from pipefunc import PipeFunc, Pipeline
from pipefunc._utils import assert_complete_kwargs, is_installed, is_running_in_ipynb, requires

if TYPE_CHECKING:
    from collections.abc import Callable

    import ipywidgets

    from pipefunc._pipefunc import PipeFunc


class VariantPipeline:

    def __init__(
        self,
        functions: list[PipeFunc],
        *,
        default_variant: str | dict[str | None, str] | None = None,
        lazy: bool = False,
        debug: bool | None = None,
        print_error: bool | None = None,
        profile: bool | None = None,
        cache_type: Literal["lru", "hybrid", "disk", "simple"] | None = None,
        cache_kwargs: dict[str, Any] | None = None,
        validate_type_annotations: bool = True,
        scope: str | None = None,
        default_resources: dict[str, Any] | None = None,
        name: str | None = None,
        description: str | None = None,
    ) -> None:
        """Initialize a VariantPipeline."""
        self.functions = functions
        self.default_variant = default_variant
        self.lazy = lazy
        self.debug = debug
        self.print_error = print_error
        self.profile = profile
        self.cache_type = cache_type
        self.cache_kwargs = cache_kwargs
        self.validate_type_annotations = validate_type_annotations
        self.scope = scope
        self.default_resources = default_resources
        self.name = name
        self.description = description
        if not self.variants_mapping():
            msg = "No variants found in the pipeline. Use a regular `Pipeline` instead."
            raise ValueError(msg)

    def variants_mapping(self) -> dict[str | None, set[str]]:
        pass

    def _variants_mapping_inverse(self) -> dict[str, set[str | None]]:
        pass

    def with_variant(
        self,
        select: str | dict[str | None, str] | None = None,
        **kwargs: Any,
    ) -> Pipeline | VariantPipeline:
        pass

    def _resolve_single_variant(self, select: str) -> dict[str | None, str]:
        pass

    def _select_functions(self, select: dict[str | None, str]) -> list[PipeFunc]:
        pass

    def _check_remaining_variants(self, functions: list[PipeFunc]) -> bool:
        pass

    def copy(self, **kwargs: Any) -> VariantPipeline:
        """Return a copy of the VariantPipeline.

        Parameters
        ----------
        kwargs
            Keyword arguments passed to the `VariantPipeline` constructor instead of the
            original values.

        """
        original_kwargs = {
            "functions": self.functions,
            "lazy": self.lazy,
            "debug": self.debug,
            "print_error": self.print_error,
            "profile": self.profile,
            "cache_type": self.cache_type,
            "cache_kwargs": self.cache_kwargs,
            "validate_type_annotations": self.validate_type_annotations,
            "scope": self.scope,
            "default_resources": self.default_resources,
            "default_variant": self.default_variant,
            "name": self.name,
            "description": self.description,
        }
        assert_complete_kwargs(original_kwargs, VariantPipeline.__init__, skip={"self"})
        original_kwargs.update(kwargs)
        return VariantPipeline(**original_kwargs)  # type: ignore[arg-type]

    @classmethod
    def from_pipelines(
        cls,
        *variant_pipeline: tuple[str, str, Pipeline] | tuple[str, Pipeline],
    ) -> VariantPipeline:
        pass

    def visualize(self, **kwargs: Any) -> Any:
        pass

    def _repr_mimebundle_(
        self,
        include: set[str] | None = None,
        exclude: set[str] | None = None,
    ) -> dict[str, str]:  # pragma: no cover
        pass

    def __getattr__(self, name: str) -> None:
        if name in Pipeline.__dict__:
            unresolved = {
                group: variants
                for group, variants in self.variants_mapping().items()
                if len(variants) > 1
            }

            if unresolved:
                parts = []
                if None in unresolved:
                    parts.append(f"variants {unresolved[None]}")
                parts.extend(
                    f"variant group `{g} = {v}`" for g, v in unresolved.items() if g is not None
                )
                variants_info = f" The {' and '.join(parts)} are not yet resolved."
            else:
                variants_info = ""

            msg = (
                "This is a `VariantPipeline`, not a `Pipeline`."
                f"{variants_info}"
                " Use `VariantPipeline.with_variant(...)` to instanciate a Pipeline first."
                f" Then access `Pipeline.{name}` again."
            )
            raise AttributeError(msg)
        default_msg = f"'VariantPipeline' object has no attribute '{name}'"
        raise AttributeError(default_msg)


def _validate_variants_exist(
    variants_mapping: dict[str | None, set[str]],
    selection: dict[str | None, str],
) -> None:
    pass


def _pipefunc_in_list(func: PipeFunc, funcs: list[PipeFunc]) -> bool:
    pass


def is_identical_pipefunc(first: PipeFunc, second: PipeFunc) -> bool:
    """Check if two PipeFunc instances are identical.

    Note: This is not implemented as PipeFunc.__eq__ to avoid
    hashing issues.
    """
    cls = type(first)
    for attr, value in first.__dict__.items():
        if isinstance(getattr(cls, attr, None), functools.cached_property):
            continue
        if attr == "_pipelines":
            continue
        if value != second.__dict__[attr]:
            return False
    return True


def _create_variant_selection_widget(
    vp: VariantPipeline,
    update_func: Callable[[Pipeline, ipywidgets.Output, Any], None],
    **kwargs: Any,
) -> ipywidgets.VBox:
    pass


def _ensure_dict(default_variant: str | dict[str | None, str] | None) -> dict[str | None, str]:
    pass


def _update_visualization(
    pipeline: Pipeline,
    output: ipywidgets.Output,
    **kwargs: Any,
) -> None:
    pass


def _update_repr_mimebundle(
    pipeline: Pipeline,
    output: ipywidgets.Output,
    **kwargs: Any,
) -> None:  # pragma: no cover
    pass

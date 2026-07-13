from __future__ import annotations

import importlib.util
import warnings
from typing import TYPE_CHECKING, Annotated, Any

import numpy as np

from pipefunc._utils import is_imported, requires

if TYPE_CHECKING:
    from collections.abc import Callable

    from pydantic import BaseModel

    from pipefunc import Pipeline
    from pipefunc.map._mapspec import ArraySpec, MapSpec

has_griffe = importlib.util.find_spec("griffe") is not None


def pipeline_to_pydantic(pipeline: Pipeline, model_name: str = "InputModel") -> type[BaseModel]:
    pass


def maybe_pydantic_model_to_dict(x: dict[str, Any] | BaseModel) -> dict[str, Any]:
    """Convert a Pydantic model to a dictionary if needed."""
    if isinstance(x, dict):
        return x
    if not is_imported("pydantic"):  # pragma: no cover
        msg = "Unknown type, expected a Pydantic model or a dictionary."
        raise ValueError(msg)
    import pydantic

    assert isinstance(x, pydantic.BaseModel)
    return model_dump(x)




def _nested_list_type(ndim: int, inner_type: Any) -> Any:
    pass






def model_dump(model_instance: BaseModel) -> dict[str, Any]:
    """Dump a Pydantic model to a dictionary."""
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=UserWarning)
        return model_instance.model_dump()

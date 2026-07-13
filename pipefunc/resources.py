
from __future__ import annotations

import functools
import inspect
import re
from dataclasses import asdict, dataclass, field
from typing import TYPE_CHECKING, Any, Literal

if TYPE_CHECKING:
    from collections.abc import Callable


@dataclass(frozen=True, eq=True)
class Resources:

    cpus: int | None = None
    cpus_per_node: int | None = None
    nodes: int | None = None
    memory: str | None = None
    gpus: int | None = None
    time: str | None = None
    partition: str | None = None
    extra_args: dict[str, Any] = field(default_factory=dict)
    parallelization_mode: Literal["internal", "external"] = "external"

    def __post_init__(self) -> None:
        """Validate input parameters after initialization.

        Raises
        ------
        ValueError
            If any of the input parameters do not meet the specified constraints.

        """
        if self.cpus is not None and self.cpus <= 0:
            msg = "`cpus` must be a positive integer."
            raise ValueError(msg)
        if self.gpus is not None and self.gpus < 0:
            msg = "`gpus` must be a non-negative integer."
            raise ValueError(msg)
        if self.nodes is not None and self.nodes <= 0:
            msg = "`nodes` must be a positive integer."
            raise ValueError(msg)
        if self.cpus_per_node is not None and self.cpus_per_node <= 0:
            msg = "`cpus_per_node` must be a positive integer."
            raise ValueError(msg)
        if self.memory is not None and not self._is_valid_memory(self.memory):
            msg = f"`memory` must be a valid string (e.g., '2GB', '500MB'), not '{self.memory}'."
            raise ValueError(msg)
        if self.time is not None and not self._is_valid_wall_time(self.time):
            msg = "`time` must be a valid string (e.g., '2:00:00', '48:00:00')."
            raise ValueError(msg)
        if self.nodes and self.cpus:
            msg = (
                "`nodes` and `cpus` cannot be specified together."
                " Either use nodes and `cpus_per_node` or use `cpus` alone."
            )
            raise ValueError(msg)
        if self.cpus_per_node and not self.nodes:
            msg = "`cpus_per_node` must be specified with `nodes`."
            raise ValueError(msg)

    @staticmethod
    def from_dict(data: dict[str, Any]) -> Resources:
        """Create a Resources instance from a dictionary.

        Parameters
        ----------
        data
            A dictionary containing the input parameters for the Resources instance.

        Returns
        -------
            A Resources instance created from the input dictionary.

        """
        assert isinstance(data, dict), "Input data must be a dictionary."
        try:
            return Resources(**data)
        except TypeError as e:
            parameters = list(inspect.signature(Resources.__init__).parameters)
            allowed_args = ", ".join(parameters[1:])
            msg = f"Error creating Resources instance: {e}.\n The following arguments are allowed: `{allowed_args}`"
            raise TypeError(msg) from e

    @staticmethod
    def maybe_from_dict(
        resources: dict[str, Any]
        | Resources
        | Callable[[dict[str, Any]], Resources | dict[str, Any]]
        | None,
    ) -> Resources | Callable[[dict[str, Any]], Resources] | None:
        """Create a Resources instance from a dictionary, if not already an instance and not None."""
        if resources is None:
            return None
        if isinstance(resources, Resources):
            return resources
        if callable(resources):
            return functools.partial(_ensure_resources, resources_callable=resources)
        return Resources.from_dict(resources)




    def to_slurm_options(self) -> str:
        pass

    def update(self, **kwargs: Any) -> Resources:
        """Update the Resources instance with new values.

        Parameters
        ----------
        **kwargs
            Keyword arguments specifying the attributes to update and their new values.

        Returns
        -------
            A new Resources instance with the updated values.

        """
        data = self.__dict__.copy()
        for key, value in kwargs.items():
            if key == "extra_args":
                data["extra_args"] = {**data["extra_args"], **value}
            elif key in data:
                data[key] = value
            else:
                data["extra_args"][key] = value
        return Resources.from_dict(data)

    @staticmethod
    def combine_max(resources_list: list[Resources]) -> Resources:
        pass

    def with_defaults(self, default_resources: Resources | None) -> Resources:
        """Combine the Resources instance with default resources."""
        if default_resources is None:
            return self
        return Resources(**dict(default_resources.dict(), **self.dict()))

    @staticmethod
    def maybe_with_defaults(
        resources: Resources | None | Callable[[dict[str, Any]], Resources],
        default_resources: Resources | None,
    ) -> Resources | Callable[[dict[str, Any]], Resources] | None:
        """Combine the Resources instance with default resources, if provided."""
        if resources is None and default_resources is None:
            return None
        if resources is None:
            return default_resources
        if default_resources is None:
            return resources
        if callable(resources):
            return functools.partial(
                _delayed_resources_with_defaults,
                _resources=resources,
                _default_resources=default_resources,
            )
        return resources.with_defaults(default_resources)

    def dict(self) -> dict[str, Any]:
        pass





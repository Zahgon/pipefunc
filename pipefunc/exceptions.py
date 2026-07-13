
from __future__ import annotations

import datetime
import getpass
import os
import platform
import traceback
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Literal

import cloudpickle

from pipefunc._utils import get_local_ip

Reason = Literal["input_is_error", "array_contains_errors"]

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

    from pipefunc._error_handling import ErrorInfo


class UnusedParametersError(ValueError):
    pass




@dataclass
class ErrorSnapshot:

    function: Callable[..., Any]
    exception: Exception
    args: tuple[Any, ...]
    kwargs: dict[str, Any]
    traceback: str = field(init=False)
    timestamp: str = field(default_factory=_timestamp)
    user: str = field(default_factory=getpass.getuser)
    machine: str = field(default_factory=platform.node)
    ip_address: str = field(default_factory=get_local_ip)
    current_directory: str = field(default_factory=os.getcwd)

    def __post_init__(self) -> None:
        """Initialize the error snapshot with a formatted traceback."""
        tb = traceback.format_exception(
            type(self.exception),
            self.exception,
            self.exception.__traceback__,
        )
        self.traceback = "".join(tb)

    def __repr__(self) -> str:
        """Return a concise representation for use in arrays and containers."""
        func_name = getattr(self.function, "__name__", "?")
        exc_type = type(self.exception).__name__
        return f"ErrorSnapshot({func_name!r}, {exc_type}: {self.exception})"

    def __str__(self) -> str:
        """Return a detailed string representation of the error snapshot."""
        args_repr = ", ".join(repr(a) for a in self.args)
        kwargs_repr = ", ".join(f"{k}={v!r}" for k, v in self.kwargs.items())
        func_name = f"{self.function.__module__}.{self.function.__qualname__}"

        return (
            "ErrorSnapshot:\n"
            "--------------\n"
            f"- 🛠 Function: {func_name}\n"
            f"- 🚨 Exception type: {type(self.exception).__name__}\n"
            f"- 💥 Exception message: {self.exception}\n"
            f"- 📋 Args: ({args_repr})\n"
            f"- 🗂 Kwargs: {{{kwargs_repr}}}\n"
            f"- 🕒 Timestamp: {self.timestamp}\n"
            f"- 👤 User: {self.user}\n"
            f"- 💻 Machine: {self.machine}\n"
            f"- 📡 IP Address: {self.ip_address}\n"
            f"- 📂 Current Directory: {self.current_directory}\n"
            "\n"
            "🔁 Reproduce the error by calling `error_snapshot.reproduce()`.\n"
            "📄 Or see the full stored traceback using `error_snapshot.traceback`.\n"
            "🔍 Inspect `error_snapshot.args` and `error_snapshot.kwargs`.\n"
            "💾 Or save the error to a file using `error_snapshot.save_to_file(filename)`"
            " and load it using `ErrorSnapshot.load_from_file(filename)`."
        )

    def reproduce(self) -> Any | None:
        pass

    def save_to_file(self, filename: str | Path) -> None:
        pass

    @classmethod
    def load_from_file(cls, filename: str | Path) -> ErrorSnapshot:
        pass


    def __getstate__(self) -> dict[str, Any]:
        """Custom pickling to handle function references using cloudpickle."""
        from pipefunc._error_handling import cloudpickle_function_state

        return cloudpickle_function_state(self.__dict__.copy(), "function")

    def __setstate__(self, state: dict[str, Any]) -> None:
        """Custom unpickling to restore function references."""
        from pipefunc._error_handling import cloudunpickle_function_state

        self.__dict__.update(cloudunpickle_function_state(state, "function"))


@dataclass
class PropagatedErrorSnapshot:

    error_info: dict[str, ErrorInfo]  # parameter -> error details
    skipped_function: Callable[..., Any]
    reason: Reason  # normalized reason label
    attempted_kwargs: dict[str, Any]  # kwargs that were not errors
    timestamp: str = field(default_factory=_timestamp)

    def __repr__(self) -> str:
        """Return a concise representation for use in arrays and containers."""
        func_name = getattr(self.skipped_function, "__name__", str(self.skipped_function))
        return f"PropagatedErrorSnapshot({func_name!r}, reason={self.reason!r})"

    def __str__(self) -> str:
        """Return a detailed string representation of the propagated error snapshot."""
        func_name = getattr(self.skipped_function, "__name__", str(self.skipped_function))
        error_summary = []
        for param, info in self.error_info.items():
            if info.type == "full":
                error_summary.append(f"{param} (complete failure)")
            else:
                error_summary.append(f"{param} ({info.error_count} errors in array)")

        return (
            f"PropagatedErrorSnapshot: Function '{func_name}' was skipped\n"
            f"Reason: {self.reason}\n"
            f"Errors in: {', '.join(error_summary)}"
        )

    def __getstate__(self) -> dict[str, Any]:
        """Custom pickling to handle function references using cloudpickle."""
        from pipefunc._error_handling import cloudpickle_function_state

        state = cloudpickle_function_state(self.__dict__.copy(), "skipped_function")
        state["error_info"] = self._pickle_error_info(self.error_info)
        return state

    def __setstate__(self, state: dict[str, Any]) -> None:
        """Custom unpickling to restore function references."""
        from pipefunc._error_handling import cloudunpickle_function_state

        state = cloudunpickle_function_state(state, "skipped_function")
        state["error_info"] = self._unpickle_error_info(state["error_info"])
        self.__dict__.update(state)

    def _pickle_error_info(
        self,
        error_info: dict[str, ErrorInfo],
    ) -> dict[str, dict[str, Any]]:
        pass

    def _unpickle_error_info(
        self,
        pickled_info: dict[str, dict[str, Any]],
    ) -> dict[str, ErrorInfo]:
        pass

    def get_root_causes(self) -> list[ErrorSnapshot]:
        pass

from __future__ import annotations

import asyncio
import html
import importlib.util
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal

import IPython.display
import ipywidgets

from .helpers import hide, show

has_rich = importlib.util.find_spec("rich") is not None


StatusType = Literal["initializing", "running", "done", "cancelled", "failed"]

INTERVAL_SHORT = 0.1  # Default interval
INTERVAL_MEDIUM = 1.0
INTERVAL_LONG = 10.0
INTERVAL_VERY_LONG = 60.0

THRESHOLD_SHORT = 10
THRESHOLD_MEDIUM = 100
THRESHOLD_LONG = 1000


@dataclass(frozen=True)
class StyleInfo:

    color: str
    icon: str
    message: str


_STYLES: dict[StatusType, StyleInfo] = {
    "initializing": StyleInfo(
        color="#3498db",  # Blue
        icon="⏳",
        message="Task is initializing...",
    ),
    "running": StyleInfo(
        color="#3498db",  # Blue
        icon="⚙️",
        message="Task is running...",
    ),
    "done": StyleInfo(
        color="#2ecc71",  # Green
        icon="✅",
        message="Task completed successfully",
    ),
    "cancelled": StyleInfo(
        color="#f39c12",  # Orange
        icon="⚠️",
        message="Task was cancelled",
    ),
    "failed": StyleInfo(
        color="#e74c3c",  # Red
        icon="❌",
        message="Task failed",
    ),
}


class AsyncTaskStatusWidget:

    def __init__(
        self,
        initial_update_interval: float = INTERVAL_SHORT,
        *,
        display: bool = True,
    ) -> None:
        """Initialize the status widget.

        Parameters
        ----------
        initial_update_interval
            Initial interval for status updates in seconds
        display
            Whether to display the widget immediately

        """
        self._status_html_widget = ipywidgets.HTML()
        self._traceback_widget = ipywidgets.Output(layout=ipywidgets.Layout(display="none"))
        self._traceback_button = ipywidgets.Button(
            description="Show traceback",
            button_style="info",
            icon="search",
            layout=ipywidgets.Layout(width="100%", display="none"),
        )
        self._traceback_button.add_class("custom-button")
        self._traceback_button.on_click(self._toggle_traceback)

        self.widget = ipywidgets.VBox(
            [self._status_html_widget, self._traceback_button, self._traceback_widget],
        )

        self._traceback_visible = False
        self._start_time = time.monotonic()
        self._start_datetime = datetime.now().strftime("%H:%M:%S")  # noqa: DTZ005
        self._update_interval = initial_update_interval
        self._task: asyncio.Task | None = None
        self._update_timer: asyncio.Task | None = None
        self._exception: Exception | None = None

        self._refresh_display("initializing")
        if display:  # pragma: no cover
            self.display()

    def _get_elapsed_time(self) -> float:
        """Get elapsed time in seconds since widget creation."""
        return time.monotonic() - self._start_time

    def _get_style(self, status: StatusType) -> StyleInfo:
        """Get style information for the given status."""
        return _STYLES.get(status, _STYLES["running"])

    def _toggle_traceback(self, _: dict[str, Any]) -> None:
        pass

    def _create_status_html(
        self,
        style: StyleInfo,
        elapsed: float,
        error: Exception | None = None,
    ) -> str:
        """Create HTML for the status display."""
        container_style = "; ".join(
            [
                "display: flex",
                "flex-direction: column",
                "font-size: 13px",
                "padding: 5px 8px",
                "border-radius: 4px",
                "background-color: #f8f9fa",
                f"border-left: 3px solid {style.color}",
                "margin-bottom: 5px",
            ],
        )

        html_content = f"""
        <div style="{container_style}">
            <div style="display: flex; align-items: center; width: 100%">
                <span style="color: {style.color}; font-weight: bold; margin-right: 10px;">
                    {style.icon} {style.message}
                </span>
                <span style="color: #666; margin-left: auto; font-size: 12px;">
                    Started: {self._start_datetime} | Elapsed: {elapsed:.1f}s
                </span>
            </div>
        """

        if error is not None:
            html_content += f"""
            <div style="color: #e74c3c; font-weight: bold; margin-top: 8px; font-size: 14px;">
                {type(error).__name__}: {html.escape(str(error))}
            </div>
            """

        html_content += "</div>"
        return html_content

    def _refresh_display(self, status: StatusType, error: Exception | None = None) -> None:
        """Refresh the display with current status."""
        style = self._get_style(status)
        elapsed = self._get_elapsed_time()
        status_html = self._create_status_html(style, elapsed, error)
        self._status_html_widget.value = status_html

        if status == "failed" and error is not None:
            self._exception = error
            show(self._traceback_button)
        else:
            hide(self._traceback_button)
            hide(self._traceback_widget)

    def _print_traceback(self) -> None:
        pass

    def _adjust_update_interval(self, elapsed: float) -> None:
        """Adjust the update interval based on elapsed time."""
        if elapsed < THRESHOLD_SHORT:
            pass
        elif elapsed < THRESHOLD_MEDIUM:
            self._update_interval = INTERVAL_MEDIUM
        elif elapsed < THRESHOLD_LONG:
            self._update_interval = INTERVAL_LONG
        else:
            self._update_interval = INTERVAL_VERY_LONG

    async def _update_periodically(self) -> None:
        """Periodically update the widget while the task is running."""
        assert self._task is not None

        try:
            while not self._task.done():
                self._refresh_display("running")
                await asyncio.sleep(self._update_interval)
                elapsed = self._get_elapsed_time()
                self._adjust_update_interval(elapsed)
        except asyncio.CancelledError:
            pass
        except Exception as e:  # noqa: BLE001  # pragma: no cover
            print(f"Error in periodic update: {e}")

    def _stop_periodic_updates(self) -> None:
        pass

    def update_status(self, future: asyncio.Future) -> None:
        pass

    def display(self) -> None:
        """Display the widget in the current cell."""
        IPython.display.display(self.widget)

    def attach_task(self, task: asyncio.Task) -> None:
        """Attach the widget to a task for monitoring.

        Parameters
        ----------
        task
            The asyncio.Task to monitor

        """
        self._task = task
        task.add_done_callback(self.update_status)
        self._refresh_display("running")
        self._start_periodic_updates()

    def _start_periodic_updates(self) -> None:
        """Start the periodic update timer."""
        try:
            loop = asyncio.get_event_loop()
            self._update_timer = loop.create_task(self._update_periodically())
        except RuntimeError:
            print("Could not start periodic updates - event loop not available")
        except Exception as e:  # noqa: BLE001
            print(f"Error starting periodic updates: {e}")

from __future__ import annotations

import hashlib
import textwrap
import time
from typing import TYPE_CHECKING, Any

import IPython.display
import ipywidgets as widgets

from pipefunc._utils import at_least_tuple
from pipefunc._widgets.progress_base import ProgressTrackerBase

from .helpers import hide, show

if TYPE_CHECKING:
    import asyncio
    from collections.abc import Callable

    from pipefunc._pipeline._types import OUTPUT_TYPE
    from pipefunc.map._progress import Status


_IPYWIDGETS_MAJOR_VERSION = int(widgets.__version__.split(".")[0])


def _span(class_name: str, value: str) -> str:
    return f'<span class="{class_name}">{value}</span>'














def _get_scope_hue(output_name: OUTPUT_TYPE) -> int | None:
    """Extract scope and calculate a consistent hue value from it."""
    output_name = at_least_tuple(output_name)
    all_have_scope = all("." in name for name in output_name)
    if not all_have_scope:
        return None

    scope = output_name[0].split(".")[0]
    hash_value = int(hashlib.md5(scope.encode()).hexdigest(), 16)  # noqa: S324
    return hash_value % 360




def _scope_background_color_css(hue: int) -> str:
    return textwrap.dedent(
        f"""
        .scope-bg-{hue} {{
            background-color: hsla({hue}, 70%, 95%, 0.75);
        }}
        """,
    )


class IPyWidgetsProgressTracker(ProgressTrackerBase):

    def __init__(
        self,
        progress_dict: dict[OUTPUT_TYPE, Status],
        task: asyncio.Task[Any] | None = None,
        *,
        target_progress_change: float = 0.05,
        auto_update: bool = True,
        in_async: bool = True,
    ) -> None:
        super().__init__(
            progress_dict,
            task,
            target_progress_change=target_progress_change,
            auto_update=auto_update,
            in_async=in_async,
        )
        self._widgets = self._create_widgets()

    def update_progress(self, _: Any = None, *, force: bool = False) -> None:
        """Update the progress values and labels."""
        t_start = time.monotonic()
        return_early = self._should_throttle_update(force)
        for name, status in self.progress_dict.items():
            if status.progress == 0 or name in self._marked_completed:
                continue
            if return_early and status.progress < 1.0:
                return
            progress_bar = self._progress_bars[name]
            progress_bar.value = status.progress
            if status.progress >= 1.0:
                progress_bar.bar_style = "success" if status.n_failed == 0 else "danger"
                progress_bar.remove_class("animated-progress")
                progress_bar.add_class("completed-progress")
                self._marked_completed.add(name)
                if status.progress == 1.0:  # Newly completed
                    self._progress_vboxes[name].add_class("pulse-animation")
            else:
                progress_bar.remove_class("completed-progress")
                progress_bar.add_class("animated-progress")
            self._update_labels(name, status)
        if self._all_completed():
            self._mark_completed()
        self.last_update_time = time.monotonic()
        self._update_sync_interval(self.last_update_time - t_start)

    def _status_text(self, status: Status) -> str:
        completed = f"✅ {status.n_completed:,}"
        failed = f"❌ {status.n_failed:,}"
        left = f"⏳ {status.n_left:,}"
        if status.n_failed == 0:
            return f"{completed} | {left}"
        return f"{completed} | {failed} | {left}"

    def _update_labels(self, name: OUTPUT_TYPE, status: Status) -> None:
        assert status.progress > 0
        iterations_label = self._status_text(status)
        labels = self._labels[name]
        labels["percentage"].value = _span(
            "percent-label",
            f"{status.progress * 100:.1f}% | {iterations_label}",
        )
        elapsed_time = status.elapsed_time()
        if status.end_time is not None:
            eta = "Completed"
        else:
            estimated_time_left = status.remaining_time(elapsed_time=elapsed_time)
            eta = f"ETA: {estimated_time_left:.2f} sec"
        speed = f"{status.n_attempted / elapsed_time:,.2f}" if elapsed_time > 0 else "∞"
        labels["speed"].value = _span("speed-label", f"Speed: {speed} iterations/sec")
        labels["estimated_time"].value = _span(
            "estimate-label",
            f"Elapsed: {elapsed_time:.2f} sec | {eta}",
        )

    def _update_auto_update_interval_text(self, new_interval: float) -> None:
        self._auto_update_interval_label.value = _span(
            "interval-label",
            f"Auto-update every: {new_interval:.2f} sec",
        )

    def _mark_completed(self) -> None:
        if self._completed:
            return
        self._completed = True
        if self.auto_update:
            self._toggle_auto_update()
        if any(status.n_failed > 0 for status in self.progress_dict.values()):
            msg = "Completed with errors ❌"
        else:
            msg = "Completed all tasks 🎉"
        self._auto_update_interval_label.value = _span("interval-label", msg)
        for button in self._buttons.values():
            button.disabled = True

    def _set_auto_update(self, value: bool) -> None:
        """Set the auto-update feature to the given value."""
        super()._set_auto_update(value)
        if not hasattr(self, "_buttons"):
            return
        self._buttons["toggle_auto_update"].description = (
            "Stop Auto-Update" if self.auto_update else "Start Auto-Update"
        )
        self._buttons["toggle_auto_update"].button_style = (
            "danger" if self.auto_update else "success"
        )
        if self.task:
            show(self._buttons_box)
            show(self._auto_update_interval_label)

    def _cancel_calculation(self, _: Any) -> None:
        pass



    def _create_widgets(self) -> widgets.VBox:
        pass

    def _style(self) -> IPython.display.HTML:
        style = textwrap.dedent(
            """
            <style>
            .progress {
                border-radius: 5px;
                box-shadow: 0 0 10px rgba(0, 0, 0, 0.1);
            }
            .progress-bar {
                border-radius: 5px;
                transition: width 0.4s ease;
                background-image: linear-gradient(
                    -45deg,
                    rgba(255, 255, 255, 0.15) 25%,
                    transparent 25%,
                    transparent 50%,
                    rgba(255, 255, 255, 0.15) 50%,
                    rgba(255, 255, 255, 0.15) 75%,
                    transparent 75%,
                    transparent
                );
                background-size: 40px 40px;
            }
            .animated-progress .progress-bar {
                animation: stripes 1s linear infinite;
            }
            .completed-progress .progress-bar {
                animation: none;
            }
            @keyframes stripes {
                0% {
                    background-position: 0 0;
                }
                100% {
                    background-position: 40px 0;
                }
            }

            /* Pulse animation for completed tasks */
            @keyframes balanced-pulse {
                0% {
                    box-shadow: 0 0 0 0 rgba(46, 204, 113, 0.4);
                    border-color: rgba(46, 204, 113, 0.8);
                    transform: scale(1);
                }
                40% {
                    box-shadow: 0 0 0 5px rgba(46, 204, 113, 0.2);
                    border-color: rgba(46, 204, 113, 0.9);
                    transform: scale(1.005);
                }
                100% {
                    box-shadow: 0 0 0 0 rgba(46, 204, 113, 0);
                    border-color: inherit;
                    transform: scale(1);
                }
            }
            .pulse-animation {
                animation: balanced-pulse 1.5s ease-out 1;
            }

            .percent-label {
                margin-left: 10px;
                font-weight: bold;
                color: #3366cc;
            }
            .estimate-label {
                font-style: italic;
                color: #666;
            }
            .speed-label {
                font-weight: bold;
                color: #009900;
            }
            .interval-label {
                font-weight: bold;
                color: #990000;
            }
            .widget-label {
                margin-bottom: 5px;
                color: #333;
                font-family: monospace;
            }
            .widget-button {
                margin-top: 5px;
            }
            .progress-vbox {
                border-radius: 10px;
                box-shadow: 0 0 10px rgba(0, 0, 0, 0.1);
                transition: all 0.2s ease-in-out;
                position: relative;
                top: 0;
            }
            .progress-vbox:hover {
                transform: scale(1.005);
                box-shadow: 0 5px 15px rgba(0, 0, 0, 0.2);
                top: -2px;
            }
            """,
        )
        hues = {hue for name in self.progress_dict if (hue := _get_scope_hue(name)) is not None}
        for hue in hues:
            style += _scope_background_color_css(hue)
        style += "</style>"
        return IPython.display.HTML(style)

    def display(self) -> None:
        IPython.display.display(self._style(), self._widgets)

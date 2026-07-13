from __future__ import annotations

import functools
import html
import inspect
import re
import warnings
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Literal, NamedTuple

import networkx as nx
import numpy as np
from networkx.drawing.nx_agraph import graphviz_layout

from pipefunc._pipefunc import NestedPipeFunc, PipeFunc
from pipefunc._pipeline._base import _Bound, _Resources
from pipefunc._plotting_utils import (
    CollapsedScope,
    GroupedArgs,
    all_unique_output_scopes,
    collapsed_scope_graph,
    create_grouped_parameter_graph,
    hide_default_args_graph,
)
from pipefunc._utils import at_least_tuple, is_running_in_ipynb, requires
from pipefunc.typing import NoAnnotation, type_as_string

if TYPE_CHECKING:
    from collections.abc import Sequence
    from pathlib import Path

    import graphviz
    import graphviz_anywidget
    import holoviews as hv
    import IPython.display
    import ipywidgets
    import matplotlib.pyplot as plt

_empty = inspect.Parameter.empty
MAX_LABEL_LENGTH = 20
_AUTO_GROUP_ARGS_THRESHOLD = 10


def _get_graph_layout(graph: nx.DiGraph) -> dict:
    pass


def _trim(s: Any, max_len: int = MAX_LABEL_LENGTH) -> str:
    pass


@dataclass
class _Nodes:

    arg: list[str] = field(default_factory=list)
    grouped_args: list[GroupedArgs] = field(default_factory=list)
    func: list[PipeFunc] = field(default_factory=list)
    nested_func: list[NestedPipeFunc] = field(default_factory=list)
    collapsed_scope: list[CollapsedScope] = field(default_factory=list)
    bound: list[_Bound] = field(default_factory=list)
    resources: list[_Resources] = field(default_factory=list)

    def append(self, node: Any) -> None:
        """Appends a node to the appropriate list based on its type."""
        if isinstance(node, str):
            self.arg.append(node)
        elif isinstance(node, GroupedArgs):
            self.grouped_args.append(node)
        elif isinstance(node, CollapsedScope):
            self.collapsed_scope.append(node)
        elif isinstance(node, NestedPipeFunc):
            self.nested_func.append(node)
        elif isinstance(node, PipeFunc):
            self.func.append(node)
        elif isinstance(node, _Bound):
            self.bound.append(node)
        elif isinstance(node, _Resources):
            self.resources.append(node)
        else:  # pragma: no cover
            msg = "Should not happen. Please report this as a bug."
            raise TypeError(msg)

    @classmethod
    def from_graph(cls, graph: nx.DiGraph) -> _Nodes:
        pass


def _all_type_annotations(graph: nx.DiGraph) -> dict[str, type]:
    pass


class _Labels(NamedTuple):
    outputs: dict
    outputs_mapspec: dict
    inputs: dict
    inputs_mapspec: dict
    bound: dict
    resources: dict
    arg_mapspec: dict

    @classmethod
    def from_graph(cls, graph: nx.DiGraph) -> _Labels:  # noqa: PLR0912
        pass


NodeType = str | PipeFunc | _Bound | _Resources | NestedPipeFunc | CollapsedScope | GroupedArgs


def _generate_node_label(
    node: NodeType,
    hints: dict[str, type],
    defaults: dict[str, Any] | None,
    arg_mapspec: dict[str, str],
    include_full_mapspec: bool,
) -> str:
    pass


_COLORS = {
    "skyblue": "#87CEEB",
    "blue": "#0000FF",
    "lightgreen": "#90EE90",
    "darkgreen": "#006400",
    "red": "#FF0000",
    "orange": "#FFA500",
}


@dataclass
class GraphvizStyle:

    arg_node_color: str = _COLORS["lightgreen"]
    func_node_color: str = _COLORS["skyblue"]
    nested_func_node_color: str = _COLORS["red"]
    bound_node_color: str = _COLORS["red"]
    resources_node_color: str = _COLORS["orange"]
    collapsed_scope_node_color: str = _COLORS["blue"]
    grouped_args_node_color: str = _COLORS["lightgreen"]
    arg_edge_color: str | None = None  # default is arg_node_color
    output_edge_color: str | None = None  # default is func_node_color
    bound_edge_color: str | None = None  # default is bound_node_color
    resources_edge_color: str | None = None  # default is resources_node_color
    input_mapspec_edge_color: str = _COLORS["darkgreen"]
    output_mapspec_edge_color: str = _COLORS["blue"]
    grouped_args_edge_color: str | None = None  # default uses node color
    font_name: str = "Helvetica"
    font_size: int = 12
    edge_font_size: int = 10
    legend_font_size: int = 20
    font_color: str = "black"
    legend_background_color: str = "lightgrey"
    background_color: str | None = None
    legend_border_color: str = "black"


def visualize_graphviz(  # noqa: PLR0912, C901, PLR0915
    graph: nx.DiGraph,
    defaults: dict[str, Any] | None = None,
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
    graph: nx.DiGraph,
    defaults: dict[str, Any] | None = None,
    *,
    orient: Literal["TB", "LR", "BT", "RL"] = "TB",
    graphviz_kwargs: dict[str, Any] | None = None,
) -> ipywidgets.VBox:
    pass






def _extra_controls_factory(  # noqa: PLR0915
    graphviz_anywidget: graphviz_anywidget.GraphvizAnyWidget,
    *,
    graph: nx.DiGraph,
    defaults: dict[str, Any] | None,
    orient: Literal["TB", "LR", "BT", "RL"],
    graphviz_kwargs: dict[str, Any] | None,
) -> ipywidgets.HBox:
    pass


def visualize_matplotlib(
    graph: nx.DiGraph,
    figsize: tuple[int, int] | int = (10, 10),
    filename: str | Path | None = None,
    func_node_colors: str | list[str] | None = None,
) -> plt.Figure:
    pass


def visualize_holoviews(graph: nx.DiGraph, *, show: bool = False) -> hv.Graph | None:
    pass

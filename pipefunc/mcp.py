
import json
import sys
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Literal, cast

import fastmcp
import pydantic
from rich.console import Console

from pipefunc import _run_status
from pipefunc._pipeline._autodoc import PipelineDocumentation, format_pipeline_docs
from pipefunc._pipeline._base import Pipeline
from pipefunc._utils import requires
from pipefunc.map._mapspec import MapSpec
from pipefunc.map._run_eager_async import AsyncMap

if sys.version_info < (3, 12):  # pragma: no cover
    from typing_extensions import TypedDict
else:  # pragma: no cover
    from typing import TypedDict


@dataclass
class JobInfo:

    runner: AsyncMap
    started_at: datetime
    run_folder: str
    status: Literal["running", "completed", "cancelled"]
    pipeline_name: str


job_registry: dict[str, JobInfo] = {}

_DEFAULT_PIPELINE_NAME = "Unnamed Pipeline"
_DEFAULT_PIPELINE_DESCRIPTION = "No description provided."

_PIPEFUNC_INSTRUCTIONS = """\
This MCP server executes pipefunc computational pipelines.
pipefunc creates function pipelines as DAGs where functions are automatically connected based on input/output dependencies.
See https://pipefunc.readthedocs.io/en/latest/ and https://github.com/pipefunc/pipefunc for more information.

<general>
## CORE CONCEPTS:
- Pipeline: A sequence of interconnected Python functions forming a computational workflow. Dependencies are handled automatically.
- MapSpec: A string (e.g., "x[i] -> y[i]") that defines how arrays are mapped between functions, enabling powerful, parallel parameter sweeps.

## EXECUTION MODES:
Two execution modes are available:

1. Synchronous (execute_pipeline_sync):
   - Blocks until completion and returns results immediately
   - Ideal for single calculations, small datasets, and interactive use.

2. Asynchronous (execute_pipeline_async):
   - Returns a `job_id` immediately for tracking background execution.
   - Ideal for large datasets, long-running computations, and parameter sweeps.
   - Workflow: Start job -> Get `job_id` -> Check status.
   - IMPORTANT: Always IMMEDIATELY check the job status with check_job_status after starting a new job!

## JOB MANAGEMENT (for Async):
- `check_job_status(job_id)`: Monitor progress and get results when complete.
- `list_jobs()`: See all running/completed jobs.
- `cancel_job(job_id)`: Stop a running job.

## EXECUTION PARAMETERS:
- inputs: Dictionary with parameter values (single values or arrays)
- parallel: Boolean (default true) - enables parallel execution
- run_folder: Optional string - directory to save intermediate results

## MAPSPEC REFERENCE:
`MapSpec` is the key to unlocking parallel execution and parameter sweeps.

Basic Syntax:
`"input1[index1], input2[index2] -> output1[index3]"`

Index Rules (How to control sweeps):
- Different Indices (`a[i]`, `b[j]`): Creates a cross-product. The pipeline runs for every combination of elements from `a` and `b`.
- Same Index (`x[i]`, `y[i]`): Zips the inputs. The pipeline pairs elements `x[0]` with `y[0]`, `x[1]` with `y[1]`, etc. The arrays must have the same length.
- No Index: A single value is treated as a constant and used in every computation of the sweep.

Common `MapSpec` Patterns & Examples:
- `"x[i] -> y[i]"`: Element-wise. Processes each element of `x` independently.
  - *Example Input*: `{"x": [1, 2, 3]}`
- `"x[i], y[i] -> z[i]"`: Zipped. Pairs elements from `x` and `y`.
  - *Example Input*: `{"x": [1, 2], "y": [10, 20]}`
- `"a[i], b[j] -> c[i, j]"`: Cross-product. Combines every element of `a` with every element of `b`.
  - *Example Input*: `{"a": [1, 2], "b": [10, 20]}` results in 4 runs.
- `"x[i, :] -> y[i]"`: Reduction. Aggregates data across a dimension (the `:`). This is typically an internal pipeline step.
- `"... -> x[i]"`: Dynamic Axis Generation. A function that generates an array from a scalar input. This is typically an internal pipeline step.

## OUTPUT FORMAT:
Returns dictionary with all pipeline outputs. Each output contains:
- "output": Computed result (converted to JSON-compatible format)
- "shape": Array dimensions (if applicable)

## JOB MANAGEMENT:
- check_job_status: Monitor progress and get results when complete
- list_jobs: See all running/completed jobs
- cancel_job: Stop a running job

</general>
"""

_PIPELINE_EXECUTE_DESCRIPTION_TEMPLATE = """\
Executes the '{pipeline_name}' pipeline.

## 1. Pipeline Purpose

{pipeline_description}

## 2. Pipeline Information

{pipeline_info}

## 3. How to Provide Inputs

{input_format_section}

## 4. How Arrays are Processed (`MapSpec` rules)

{mapspec_section}

## 5. Detailed Parameter and Output Reference

{documentation}
"""


_PIPELINE_ASYNC_EXECUTE_DESCRIPTION_EXTRA = """\
This tool returns a job ID and run folder.

Use the job ID to:
- check_job_status: Monitor progress and get results when complete
- list_jobs: See all running/completed jobs
- cancel_job: Stop a running job

IMPORTANT:
- Whenever starting a new job, ALWAYS immediately check the job status with check_job_status.
"""


def _get_pipeline_documentation(pipeline: Pipeline) -> str:
    pass


def _get_pipeline_info_summary(pipeline_name: str, pipeline: Pipeline) -> str:
    pass


def _get_input_format_section(pipeline: Pipeline) -> str:
    pass


def _is_root_mapspec(mapspec: MapSpec, root_args: tuple[str, ...]) -> bool:
    pass


def _get_mapspec_section(pipeline: Pipeline) -> str:
    pass


def _format_tool_description(pipeline: Pipeline) -> str:
    pass


def build_mcp_server(pipeline: Pipeline, **fast_mcp_kwargs: Any) -> fastmcp.FastMCP:
    pass












class _RunEntry(TypedDict):
    last_modified: str
    run_folder: str
    all_complete: bool
    total_outputs: int
    completed_outputs: int
    pipefunc_version: str


class _Runs(TypedDict):
    runs: list[_RunEntry]
    total_count: int
    folder: str
    scanned_directories: int
    error: str | None


def _list_historical_runs(folder: str = "runs", max_runs: int | None = None) -> _Runs:
    """List all historical pipeline runs from disk, sorted by modification time."""
    return cast(_Runs, _run_status.list_historical_runs(folder, max_runs))


def _run_info(run_folder: str) -> dict[str, Any]:
    return _run_status.run_info(run_folder)


def _load_outputs(run_folder: str, output_names: list[str] | None = None) -> dict[str, Any]:
    return _run_status.load_outputs(run_folder, output_names)

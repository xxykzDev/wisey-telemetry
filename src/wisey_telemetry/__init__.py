# wisey_telemetry/__init__.py

from .telemetry import (
    init_telemetry,
    instrument_app,
    get_tracer,
    start_trace_span,
    trace_function,
)

__all__ = [
    "init_telemetry",
    "instrument_app",
    "get_tracer",
    "start_trace_span",
    "trace_function"
]

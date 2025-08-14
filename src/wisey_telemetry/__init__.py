# wisey_telemetry/__init__.py

from .telemetry import (
    init_telemetry,
    instrument_app,
    get_tracer,
    start_trace_span,
    trace_function,
)

from .kafka_metrics import (
    KafkaMetrics,
    init_kafka_metrics,
    get_kafka_metrics
)

__all__ = [
    "init_telemetry",
    "instrument_app",
    "get_tracer",
    "start_trace_span",
    "trace_function",
    "KafkaMetrics",
    "init_kafka_metrics",
    "get_kafka_metrics"
]

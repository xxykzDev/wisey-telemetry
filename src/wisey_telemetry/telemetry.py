import os
import logging
from typing import Optional, Callable, Any
from contextlib import contextmanager

from fastapi import FastAPI
from opentelemetry import trace
from opentelemetry.trace import Span, Status, StatusCode
from opentelemetry.sdk.resources import Resource, SERVICE_NAME
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.jaeger.thrift import JaegerExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

logger = logging.getLogger(__name__)

__all__ = [
    "init_telemetry",
    "instrument_app",
    "get_tracer",
    "start_trace_span",
    "trace_function"
]

def init_telemetry(service_name: str):
    """
    Inicializa el TracerProvider con el exportador Jaeger.
    """
    jaeger_host = os.getenv("JAEGER_HOST", "localhost")
    jaeger_port = int(os.getenv("JAEGER_PORT", "6831"))

    resource = Resource(attributes={SERVICE_NAME: service_name})

    provider = TracerProvider(resource=resource)
    trace.set_tracer_provider(provider)

    jaeger_exporter = JaegerExporter(
        agent_host_name=jaeger_host,
        agent_port=jaeger_port,
    )

    span_processor = BatchSpanProcessor(jaeger_exporter)
    provider.add_span_processor(span_processor)

    logger.info(f"✅ Telemetría inicializada para: {service_name}")


def instrument_app(app: FastAPI):
    """
    Instrumenta una aplicación FastAPI automáticamente.
    """
    FastAPIInstrumentor.instrument_app(app)
    logger.info("🚀 FastAPI instrumentada con OpenTelemetry")


def get_tracer(name: Optional[str] = None):
    """
    Obtiene un tracer global para uso general.
    """
    return trace.get_tracer(name or "wisey-tracer")


@contextmanager
def start_trace_span(name: str, attrs: Optional[dict] = None) -> Span:
    """
    Context manager para crear spans con atributos opcionales.
    Maneja también errores y los registra.
    """
    tracer = get_tracer()
    with tracer.start_as_current_span(name) as span:
        if attrs:
            for k, v in attrs.items():
                span.set_attribute(k, v)
        try:
            yield span
        except Exception as e:
            logger.exception(f"❌ Error en span '{name}': {e}")
            span.record_exception(e)
            span.set_status(Status(StatusCode.ERROR, str(e)))
            raise


def trace_function(name: str) -> Callable:
    """
    Decorador para trazar funciones automáticamente.
    """
    def decorator(func: Callable) -> Callable:
        def wrapper(*args, **kwargs) -> Any:
            with start_trace_span(name) as span:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    logger.exception(f"❌ Error en función '{name}': {e}")
                    span.record_exception(e)
                    span.set_status(Status(StatusCode.ERROR, str(e)))
                    raise
        return wrapper
    return decorator

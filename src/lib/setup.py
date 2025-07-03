import os
from fastapi import FastAPI
from opentelemetry import trace
from opentelemetry.sdk.resources import Resource, SERVICE_NAME
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.jaeger.thrift import JaegerExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

__all__ = ["init_telemetry", "instrument_app"]


def init_telemetry(service_name: str):
    """
    Inicializa el TracerProvider con el exportador Jaeger.

    :param service_name: Nombre del servicio para distinguir en el trace backend.
    """
    jaeger_host = os.getenv("JAEGER_HOST", "localhost")
    jaeger_port = int(os.getenv("JAEGER_PORT", "6831"))

    # Recurso con nombre del servicio
    resource = Resource(attributes={
        SERVICE_NAME: service_name
    })

    provider = TracerProvider(resource=resource)
    trace.set_tracer_provider(provider)

    jaeger_exporter = JaegerExporter(
        agent_host_name=jaeger_host,
        agent_port=jaeger_port,
    )

    span_processor = BatchSpanProcessor(jaeger_exporter)
    provider.add_span_processor(span_processor)


def instrument_app(app: FastAPI):
    """
    Instrumenta una aplicación FastAPI automáticamente.
    Si no usás FastAPI, no necesitás llamar esto.
    """
    FastAPIInstrumentor.instrument_app(app)

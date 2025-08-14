"""
Kafka-specific metrics and telemetry for Wisey
"""
import time
import logging
from typing import Dict, Any, Optional
from contextlib import contextmanager
from opentelemetry import metrics
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.exporter.prometheus import PrometheusMetricReader
from prometheus_client import start_http_server

logger = logging.getLogger(__name__)

class KafkaMetrics:
    """Kafka metrics collector for monitoring producer/consumer health"""
    
    def __init__(self, service_name: str):
        self.service_name = service_name
        self.meter = None
        self._init_metrics()
        
    def _init_metrics(self):
        """Initialize OpenTelemetry metrics"""
        # Create a PrometheusMetricReader
        reader = PrometheusMetricReader()
        provider = MeterProvider(metric_readers=[reader])
        metrics.set_meter_provider(provider)
        
        # Get a meter
        self.meter = metrics.get_meter(self.service_name)
        
        # Create counters and histograms
        self.messages_sent = self.meter.create_counter(
            name="kafka_messages_sent_total",
            description="Total number of messages sent to Kafka",
            unit="messages"
        )
        
        self.messages_failed = self.meter.create_counter(
            name="kafka_messages_failed_total",
            description="Total number of failed message sends",
            unit="messages"
        )
        
        self.reconnections = self.meter.create_counter(
            name="kafka_producer_reconnections_total",
            description="Total number of producer reconnections",
            unit="reconnections"
        )
        
        self.send_duration = self.meter.create_histogram(
            name="kafka_send_duration_seconds",
            description="Duration of Kafka send operations",
            unit="seconds"
        )
        
        self.retry_attempts = self.meter.create_histogram(
            name="kafka_retry_attempts",
            description="Number of retry attempts per message",
            unit="attempts"
        )
        
        self.producer_health = self.meter.create_up_down_counter(
            name="kafka_producer_health",
            description="Producer health status (1=healthy, 0=unhealthy)",
            unit="status"
        )
        
        logger.info(f"📊 Kafka metrics initialized for {self.service_name}")
    
    def record_message_sent(self, topic: str, success: bool = True):
        """Record a message send attempt"""
        labels = {"topic": topic, "service": self.service_name}
        
        if success:
            self.messages_sent.add(1, labels)
        else:
            self.messages_failed.add(1, labels)
    
    def record_reconnection(self):
        """Record a producer reconnection"""
        labels = {"service": self.service_name}
        self.reconnections.add(1, labels)
    
    def record_send_duration(self, duration: float, topic: str):
        """Record the duration of a send operation"""
        labels = {"topic": topic, "service": self.service_name}
        self.send_duration.record(duration, labels)
    
    def record_retry_attempts(self, attempts: int, topic: str):
        """Record the number of retry attempts"""
        labels = {"topic": topic, "service": self.service_name}
        self.retry_attempts.record(attempts, labels)
    
    def set_producer_health(self, healthy: bool):
        """Set producer health status"""
        labels = {"service": self.service_name}
        # Set to 1 for healthy, -1 to decrement (making it 0)
        value = 1 if healthy else -1
        self.producer_health.add(value, labels)
    
    @contextmanager
    def measure_send_time(self, topic: str):
        """Context manager to measure send operation time"""
        start_time = time.time()
        try:
            yield
            duration = time.time() - start_time
            self.record_send_duration(duration, topic)
            self.record_message_sent(topic, success=True)
        except Exception as e:
            duration = time.time() - start_time
            self.record_send_duration(duration, topic)
            self.record_message_sent(topic, success=False)
            raise
    
    def start_metrics_server(self, port: int = 8000):
        """Start Prometheus metrics HTTP server"""
        try:
            start_http_server(port)
            logger.info(f"📊 Prometheus metrics server started on port {port}")
        except Exception as e:
            logger.error(f"Failed to start metrics server: {e}")


# Global metrics instance
_kafka_metrics: Optional[KafkaMetrics] = None

def init_kafka_metrics(service_name: str) -> KafkaMetrics:
    """Initialize global Kafka metrics instance"""
    global _kafka_metrics
    if _kafka_metrics is None:
        _kafka_metrics = KafkaMetrics(service_name)
    return _kafka_metrics

def get_kafka_metrics() -> Optional[KafkaMetrics]:
    """Get the global Kafka metrics instance"""
    return _kafka_metrics
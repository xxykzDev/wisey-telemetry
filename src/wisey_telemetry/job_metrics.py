"""
Job-specific metrics and telemetry for Wisey
"""
import time
import logging
from typing import Dict, Any, Optional
from contextlib import contextmanager
from datetime import datetime, timedelta
from opentelemetry import metrics
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.exporter.prometheus import PrometheusMetricReader
from prometheus_client import start_http_server

logger = logging.getLogger(__name__)

class JobMetrics:
    """Job metrics collector for monitoring async job operations"""
    
    def __init__(self, service_name: str):
        self.service_name = service_name
        self.meter = None
        self._init_metrics()
        
    def _init_metrics(self):
        """Initialize OpenTelemetry metrics for jobs"""
        # Create a PrometheusMetricReader
        reader = PrometheusMetricReader()
        provider = MeterProvider(metric_readers=[reader])
        metrics.set_meter_provider(provider)
        
        # Get a meter
        self.meter = metrics.get_meter(f"{self.service_name}.jobs")
        
        # Job counters
        self.jobs_created = self.meter.create_counter(
            name="jobs_created_total",
            description="Total number of jobs created",
            unit="jobs"
        )
        
        self.jobs_completed = self.meter.create_counter(
            name="jobs_completed_total",
            description="Total number of jobs completed successfully",
            unit="jobs"
        )
        
        self.jobs_failed = self.meter.create_counter(
            name="jobs_failed_total",
            description="Total number of jobs that failed",
            unit="jobs"
        )
        
        self.jobs_cancelled = self.meter.create_counter(
            name="jobs_cancelled_total",
            description="Total number of jobs cancelled",
            unit="jobs"
        )
        
        self.jobs_expired = self.meter.create_counter(
            name="jobs_expired_total",
            description="Total number of jobs that expired",
            unit="jobs"
        )
        
        # Job duration histogram
        self.job_duration = self.meter.create_histogram(
            name="job_duration_seconds",
            description="Duration of job execution",
            unit="seconds"
        )
        
        # Queue size gauge
        self.queue_size = self.meter.create_up_down_counter(
            name="job_queue_size",
            description="Current number of jobs in queue",
            unit="jobs"
        )
        
        # WebSocket metrics
        self.ws_connections = self.meter.create_up_down_counter(
            name="websocket_connections",
            description="Current number of WebSocket connections",
            unit="connections"
        )
        
        self.ws_subscriptions = self.meter.create_up_down_counter(
            name="websocket_subscriptions",
            description="Current number of job subscriptions",
            unit="subscriptions"
        )
        
        self.ws_messages_sent = self.meter.create_counter(
            name="websocket_messages_sent_total",
            description="Total WebSocket messages sent",
            unit="messages"
        )
        
        self.ws_heartbeats = self.meter.create_counter(
            name="websocket_heartbeats_total",
            description="Total heartbeat messages sent",
            unit="heartbeats"
        )
        
        # Polling metrics
        self.poll_requests = self.meter.create_counter(
            name="job_poll_requests_total",
            description="Total number of job polling requests",
            unit="requests"
        )
        
        # Cache metrics
        self.cache_hits = self.meter.create_counter(
            name="job_cache_hits_total",
            description="Total number of cache hits for job results",
            unit="hits"
        )
        
        self.cache_misses = self.meter.create_counter(
            name="job_cache_misses_total",
            description="Total number of cache misses for job results",
            unit="misses"
        )
        
        # Idempotency metrics
        self.idempotent_requests = self.meter.create_counter(
            name="idempotent_requests_total",
            description="Total number of idempotent requests",
            unit="requests"
        )
        
        self.idempotent_hits = self.meter.create_counter(
            name="idempotent_hits_total",
            description="Total number of idempotent key hits",
            unit="hits"
        )
        
        logger.info(f"📊 Job metrics initialized for {self.service_name}")
    
    def record_job_created(self, job_type: str):
        """Record a job creation"""
        labels = {"job_type": job_type, "service": self.service_name}
        self.jobs_created.add(1, labels)
        self.queue_size.add(1, {"service": self.service_name})
    
    def record_job_completed(self, job_type: str, duration: float):
        """Record a successful job completion"""
        labels = {"job_type": job_type, "service": self.service_name}
        self.jobs_completed.add(1, labels)
        self.job_duration.record(duration, labels)
        self.queue_size.add(-1, {"service": self.service_name})
    
    def record_job_failed(self, job_type: str, error_code: str):
        """Record a job failure"""
        labels = {
            "job_type": job_type,
            "error_code": error_code,
            "service": self.service_name
        }
        self.jobs_failed.add(1, labels)
        self.queue_size.add(-1, {"service": self.service_name})
    
    def record_job_cancelled(self, job_type: str):
        """Record a job cancellation"""
        labels = {"job_type": job_type, "service": self.service_name}
        self.jobs_cancelled.add(1, labels)
        self.queue_size.add(-1, {"service": self.service_name})
    
    def record_job_expired(self, job_type: str):
        """Record a job expiration"""
        labels = {"job_type": job_type, "service": self.service_name}
        self.jobs_expired.add(1, labels)
        self.queue_size.add(-1, {"service": self.service_name})
    
    def record_ws_connection(self, delta: int):
        """Record WebSocket connection change"""
        self.ws_connections.add(delta, {"service": self.service_name})
    
    def record_ws_subscription(self, delta: int):
        """Record WebSocket subscription change"""
        self.ws_subscriptions.add(delta, {"service": self.service_name})
    
    def record_ws_message(self, message_type: str):
        """Record WebSocket message sent"""
        labels = {"type": message_type, "service": self.service_name}
        self.ws_messages_sent.add(1, labels)
    
    def record_ws_heartbeat(self):
        """Record WebSocket heartbeat"""
        self.ws_heartbeats.add(1, {"service": self.service_name})
    
    def record_poll_request(self, job_type: str):
        """Record a polling request"""
        labels = {"job_type": job_type, "service": self.service_name}
        self.poll_requests.add(1, labels)
    
    def record_cache_hit(self, cache_type: str = "result"):
        """Record a cache hit"""
        labels = {"type": cache_type, "service": self.service_name}
        self.cache_hits.add(1, labels)
    
    def record_cache_miss(self, cache_type: str = "result"):
        """Record a cache miss"""
        labels = {"type": cache_type, "service": self.service_name}
        self.cache_misses.add(1, labels)
    
    def record_idempotent_request(self):
        """Record an idempotent request"""
        self.idempotent_requests.add(1, {"service": self.service_name})
    
    def record_idempotent_hit(self):
        """Record an idempotent key hit"""
        self.idempotent_hits.add(1, {"service": self.service_name})
    
    @contextmanager
    def measure_job_duration(self, job_type: str):
        """Context manager to measure job execution time"""
        start_time = time.time()
        try:
            yield
            duration = time.time() - start_time
            self.record_job_completed(job_type, duration)
        except Exception as e:
            duration = time.time() - start_time
            self.job_duration.record(duration, {
                "job_type": job_type,
                "status": "failed",
                "service": self.service_name
            })
            raise
    
    def get_stats(self) -> Dict[str, Any]:
        """Get current metrics stats (for debugging)"""
        # This would need to query the actual metrics
        # For now, return a placeholder
        return {
            "service": self.service_name,
            "timestamp": datetime.utcnow().isoformat(),
            "status": "metrics_active"
        }


# Global metrics instance
_job_metrics: Optional[JobMetrics] = None

def init_job_metrics(service_name: str) -> JobMetrics:
    """Initialize global job metrics instance"""
    global _job_metrics
    if _job_metrics is None:
        _job_metrics = JobMetrics(service_name)
    return _job_metrics

def get_job_metrics() -> Optional[JobMetrics]:
    """Get the global job metrics instance"""
    return _job_metrics
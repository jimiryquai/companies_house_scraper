"""Metrics server for Companies House streaming monitoring.

Exposes Prometheus metrics and health endpoints for Grafana.
"""

import asyncio
import logging
import os
import sqlite3
from datetime import datetime
from typing import Any

from fastapi import FastAPI, HTTPException, Response  # type: ignore[import-not-found]
from prometheus_client import (  # type: ignore[import-not-found]
    CONTENT_TYPE_LATEST,
    Counter,
    Gauge,
    Histogram,
    generate_latest,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Companies House Metrics")

# Prometheus metrics
events_processed = Counter("streaming_events_processed_total", "Total events processed")
companies_updated = Counter("streaming_companies_updated_total", "Total companies updated")
active_companies = Gauge("streaming_active_companies", "Number of companies in strike-off status")
processing_latency = Histogram("streaming_processing_latency_seconds", "Event processing latency")
connection_status = Gauge(
    "streaming_connection_status", "Connection status (1=connected, 0=disconnected)"
)
error_count = Counter("streaming_errors_total", "Total errors", ["error_type"])
database_size = Gauge("streaming_database_size_bytes", "Database file size")
events_per_minute = Gauge("streaming_events_per_minute", "Events processed per minute")
last_event_timestamp = Gauge("streaming_last_event_timestamp", "Timestamp of last processed event")

# Configuration
DATABASE_PATH = os.environ.get("DATABASE_PATH", "/opt/render/project/data/companies.db")
METRICS_INTERVAL = int(os.environ.get("METRICS_INTERVAL", 60))


class MetricsCollector:
    """Collects metrics from the database and streaming service."""

    def __init__(self) -> None:
        """Initialize the metrics collector."""
        self.last_event_count = 0
        self.last_check_time = datetime.now()

    def get_database_metrics(self) -> dict[str, Any]:
        """Collect metrics from the database."""
        metrics = {}

        try:
            conn = sqlite3.connect(DATABASE_PATH)
            cursor = conn.cursor()

            # Count active strike-off companies
            cursor.execute("""
                SELECT COUNT(*) FROM companies
                WHERE company_status = 'active-proposal-to-strike-off'
            """)
            metrics["active_companies"] = cursor.fetchone()[0]

            # Count total events
            cursor.execute("SELECT COUNT(*) FROM stream_events")
            metrics["total_events"] = cursor.fetchone()[0]

            # Count events in last minute
            cursor.execute("""
                SELECT COUNT(*) FROM stream_events
                WHERE created_at > datetime('now', '-1 minute')
            """)
            metrics["events_last_minute"] = cursor.fetchone()[0]

            # Get last event time
            cursor.execute("""
                SELECT MAX(created_at) FROM stream_events
            """)
            last_event = cursor.fetchone()[0]
            if last_event:
                metrics["last_event_time"] = datetime.fromisoformat(last_event)

            # Count unique companies updated today
            cursor.execute("""
                SELECT COUNT(DISTINCT company_number) FROM stream_events
                WHERE DATE(created_at) = DATE('now')
            """)
            metrics["companies_updated_today"] = cursor.fetchone()[0]

            # Get average processing latency
            cursor.execute("""
                SELECT AVG(JULIANDAY(processed_at) - JULIANDAY(created_at)) * 86400
                FROM stream_events
                WHERE processed_at IS NOT NULL
                AND created_at > datetime('now', '-1 hour')
            """)
            latency = cursor.fetchone()[0]
            metrics["avg_latency"] = latency if latency else 0

            # Get error counts by type
            cursor.execute("""
                SELECT event_type, COUNT(*)
                FROM stream_events
                WHERE event_type LIKE '%error%'
                GROUP BY event_type
            """)
            metrics["errors"] = dict(cursor.fetchall())

            conn.close()

            # Get database file size
            if os.path.exists(DATABASE_PATH):
                metrics["database_size"] = os.path.getsize(DATABASE_PATH)

        except Exception as e:
            logger.error(f"Error collecting database metrics: {e}")
            metrics["error"] = str(e)

        return metrics

    def update_prometheus_metrics(self, metrics: dict[str, Any]) -> None:
        """Update Prometheus metrics with collected data."""
        if "active_companies" in metrics:
            active_companies.set(metrics["active_companies"])

        if "events_last_minute" in metrics:
            events_per_minute.set(metrics["events_last_minute"])

        if "last_event_time" in metrics:
            last_event_timestamp.set(metrics["last_event_time"].timestamp())

        if "avg_latency" in metrics:
            processing_latency.observe(metrics["avg_latency"])

        if "database_size" in metrics:
            database_size.set(metrics["database_size"])

        if "errors" in metrics:
            for error_type, count in metrics["errors"].items():
                error_count.labels(error_type=error_type).inc(count)

    async def collect_metrics_loop(self) -> None:
        """Continuously collect metrics."""
        while True:
            try:
                metrics = self.get_database_metrics()
                self.update_prometheus_metrics(metrics)
                logger.info(f"Metrics updated: {metrics.get('events_last_minute', 0)} events/min")
            except Exception as e:
                logger.error(f"Error in metrics collection: {e}")

            await asyncio.sleep(METRICS_INTERVAL)


# Initialize collector
collector = MetricsCollector()


@app.on_event("startup")  # type: ignore[misc]
async def startup_event() -> None:
    """Start metrics collection on startup."""
    asyncio.create_task(collector.collect_metrics_loop())


@app.get("/")  # type: ignore[misc]
async def root() -> dict[str, Any]:
    """Root endpoint with service info."""
    return {
        "service": "Companies House Metrics Server",
        "endpoints": {
            "/health": "Health check",
            "/metrics": "Prometheus metrics",
            "/stats": "Current statistics",
        },
    }


@app.get("/health")  # type: ignore[misc]
async def health() -> dict[str, Any]:
    """Health check endpoint."""
    try:
        # Check database connectivity
        conn = sqlite3.connect(DATABASE_PATH)
        conn.execute("SELECT 1")
        conn.close()

        return {"status": "healthy", "timestamp": datetime.now().isoformat()}
    except Exception as e:
        raise HTTPException(status_code=503, detail={"status": "unhealthy", "error": str(e)}) from e


@app.get("/metrics")  # type: ignore[misc]
async def metrics() -> Response:
    """Prometheus metrics endpoint."""
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/stats")  # type: ignore[misc]
async def stats() -> dict[str, Any]:
    """Current statistics endpoint."""
    metrics = collector.get_database_metrics()

    return {
        "timestamp": datetime.now().isoformat(),
        "active_companies": metrics.get("active_companies", 0),
        "events_per_minute": metrics.get("events_last_minute", 0),
        "companies_updated_today": metrics.get("companies_updated_today", 0),
        "average_latency_seconds": metrics.get("avg_latency", 0),
        "database_size_mb": metrics.get("database_size", 0) / (1024 * 1024),
        "last_event": metrics.get("last_event_time", "N/A"),
        "errors": metrics.get("errors", {}),
    }


if __name__ == "__main__":
    import uvicorn  # type: ignore[import-not-found]

    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)  # noqa: S104

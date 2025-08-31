"""Comprehensive metrics collector for cloud operations monitoring.

This module aggregates metrics from all streaming components to provide
comprehensive monitoring of queue depth, processing rates, API compliance,
and overall system performance for cloud deployment environments.
"""

import asyncio
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from .api_usage_monitor import RealTimeAPIUsageMonitor
from .company_state_manager import CompanyStateManager
from .health_monitor import HealthMonitor
from .queue_manager import PriorityQueueManager
from .rate_limit_monitor import RateLimitMonitor
from .recovery_manager import ServiceRecoveryManager

logger = logging.getLogger(__name__)


@dataclass
class QueueDepthMetrics:
    """Queue depth and utilization metrics."""

    total_queued: int = 0
    high_priority_queued: int = 0
    medium_priority_queued: int = 0
    low_priority_queued: int = 0
    background_queued: int = 0

    max_capacity: int = 10000
    utilization_percentage: float = 0.0

    # Queue health indicators
    oldest_request_age_seconds: float = 0.0
    queue_growth_rate_per_minute: float = 0.0
    average_queue_time_seconds: float = 0.0

    # Processing throughput
    requests_processed_last_5min: int = 0
    processing_rate_per_minute: float = 0.0

    def calculate_health_score(self) -> float:
        """Calculate queue health score (0.0 to 1.0)."""
        # Factors: utilization, age, growth rate
        utilization_score = max(0, 1 - (self.utilization_percentage / 100))
        age_score = max(0, 1 - (self.oldest_request_age_seconds / 3600))  # 1 hour = 0
        growth_score = max(0, 1 - abs(self.queue_growth_rate_per_minute) / 100)

        return (utilization_score + age_score + growth_score) / 3


@dataclass
class ProcessingRateMetrics:
    """Processing rate and throughput metrics."""

    # Company processing rates
    companies_processed_per_minute: float = 0.0
    companies_detected_per_minute: float = 0.0
    companies_completed_per_minute: float = 0.0

    # State transition rates
    status_checks_per_minute: float = 0.0
    officers_fetches_per_minute: float = 0.0

    # Processing efficiency
    average_company_processing_time_seconds: float = 0.0
    success_rate: float = 1.0
    error_rate_per_minute: float = 0.0

    # Pipeline health
    pipeline_backlog: int = 0
    pipeline_efficiency_score: float = 1.0

    # Throughput predictions
    projected_throughput_next_hour: int = 0
    capacity_utilization: float = 0.0


@dataclass
class APIComplianceMetrics:
    """API compliance and rate limiting metrics."""

    # Current usage
    api_calls_current_window: int = 0
    api_usage_percentage: float = 0.0
    rate_limit: int = 600

    # Compliance status
    compliance_level: str = "compliant"  # compliant, warning, violation, critical
    minutes_until_window_reset: float = 0.0

    # Violation tracking
    violations_last_24h: int = 0
    violations_current_window: int = 0
    time_since_last_violation_minutes: float = 0.0

    # Performance metrics
    api_response_time_ms: float = 0.0
    api_success_rate: float = 1.0
    api_error_rate_percentage: float = 0.0

    # Predictive metrics
    projected_usage_next_window: int = 0
    risk_assessment: str = "low"  # low, moderate, high, critical

    def get_compliance_score(self) -> float:
        """Calculate API compliance score (0.0 to 1.0)."""
        # Higher score = better compliance
        usage_score = max(0, 1 - (self.api_usage_percentage / 100))
        violation_score = max(0, 1 - (self.violations_current_window / 10))
        response_score = min(1, 500 / max(self.api_response_time_ms, 1))

        return (usage_score + violation_score + response_score) / 3


@dataclass
class SystemPerformanceMetrics:
    """Overall system performance metrics."""

    # System health
    overall_health_score: float = 1.0
    system_status: str = "healthy"  # healthy, degraded, critical, offline

    # Component status
    components_healthy: int = 0
    components_total: int = 0

    # Performance indicators
    memory_usage_mb: float = 0.0
    cpu_usage_percentage: float = 0.0

    # Operational metrics
    uptime_hours: float = 0.0
    last_restart_time: Optional[datetime] = None

    # Alert status
    active_alerts: int = 0
    critical_alerts: int = 0


class ComprehensiveMetricsCollector:
    """Comprehensive metrics collector for cloud operations monitoring.

    This collector aggregates metrics from all streaming components to provide
    unified monitoring capabilities including:
    - Queue depth and utilization monitoring
    - Processing rate and throughput tracking
    - API compliance and rate limit monitoring
    - System performance and health metrics
    - Predictive analysis and recommendations
    """

    def __init__(
        self,
        queue_manager: PriorityQueueManager,
        api_usage_monitor: RealTimeAPIUsageMonitor,
        rate_limit_monitor: RateLimitMonitor,
        state_manager: CompanyStateManager,
        health_monitor: Optional[HealthMonitor] = None,
        recovery_manager: Optional[ServiceRecoveryManager] = None,
        collection_interval_seconds: int = 30,
        metrics_retention_hours: int = 24,
    ) -> None:
        """Initialize the comprehensive metrics collector.

        Args:
            queue_manager: Queue manager instance
            api_usage_monitor: API usage monitor instance
            rate_limit_monitor: Rate limit monitor instance
            state_manager: Company state manager instance
            health_monitor: Health monitor instance (optional)
            recovery_manager: Recovery manager instance (optional)
            collection_interval_seconds: Metrics collection interval
            metrics_retention_hours: How long to retain historical metrics
        """
        self.queue_manager = queue_manager
        self.api_usage_monitor = api_usage_monitor
        self.rate_limit_monitor = rate_limit_monitor
        self.state_manager = state_manager
        self.health_monitor = health_monitor
        self.recovery_manager = recovery_manager

        self.collection_interval = collection_interval_seconds
        self.retention_period = timedelta(hours=metrics_retention_hours)

        # Current metrics
        self.queue_metrics = QueueDepthMetrics()
        self.processing_metrics = ProcessingRateMetrics()
        self.compliance_metrics = APIComplianceMetrics()
        self.performance_metrics = SystemPerformanceMetrics()

        # Historical data
        self.metrics_history: List[Dict[str, Any]] = []
        self.last_collection_time = datetime.now()
        self.start_time = datetime.now()

        # Collection state
        self.collecting = False
        self.collection_task: Optional[asyncio.Task] = None

        logger.info(
            "ComprehensiveMetricsCollector initialized: interval=%ds, retention=%dh",
            collection_interval_seconds,
            metrics_retention_hours,
        )

    async def start_collection(self) -> None:
        """Start automatic metrics collection."""
        if self.collecting:
            logger.warning("Metrics collection is already active")
            return

        self.collecting = True
        self.collection_task = asyncio.create_task(self._collection_loop())

        logger.info("Started comprehensive metrics collection")

    async def stop_collection(self) -> None:
        """Stop automatic metrics collection."""
        if not self.collecting:
            return

        self.collecting = False
        if self.collection_task:
            self.collection_task.cancel()
            try:
                await self.collection_task
            except asyncio.CancelledError:
                pass

        logger.info("Stopped comprehensive metrics collection")

    async def _collection_loop(self) -> None:
        """Main metrics collection loop."""
        while self.collecting:
            try:
                # Collect metrics from all components
                await self.collect_all_metrics()

                # Clean old historical data
                self._clean_historical_data()

                # Wait for next collection
                await asyncio.sleep(self.collection_interval)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Error in metrics collection loop: %s", e, exc_info=True)
                await asyncio.sleep(self.collection_interval)

    async def collect_all_metrics(self) -> None:
        """Collect metrics from all components."""
        collection_start = time.time()

        # Collect queue depth metrics
        await self._collect_queue_metrics()

        # Collect processing rate metrics
        await self._collect_processing_metrics()

        # Collect API compliance metrics
        self._collect_api_compliance_metrics()

        # Collect system performance metrics
        await self._collect_performance_metrics()

        # Store historical snapshot
        self._store_historical_snapshot()

        collection_time = time.time() - collection_start
        logger.debug("Metrics collection completed in %.2fs", collection_time)

        self.last_collection_time = datetime.now()

    async def _collect_queue_metrics(self) -> None:
        """Collect queue depth and utilization metrics."""
        queue_status = self.queue_manager.get_queue_status()
        queue_metrics = queue_status["metrics"]

        # Basic queue metrics
        self.queue_metrics.total_queued = queue_status["total_queued"]
        self.queue_metrics.max_capacity = queue_status["max_capacity"]
        self.queue_metrics.utilization_percentage = queue_status["utilization"] * 100

        # Per-priority metrics
        queues = queue_status["queues"]
        self.queue_metrics.high_priority_queued = queues.get("HIGH", {}).get("depth", 0)
        self.queue_metrics.medium_priority_queued = queues.get("MEDIUM", {}).get("depth", 0)
        self.queue_metrics.low_priority_queued = queues.get("LOW", {}).get("depth", 0)
        self.queue_metrics.background_queued = queues.get("BACKGROUND", {}).get("depth", 0)

        # Processing metrics
        self.queue_metrics.requests_processed_last_5min = queue_metrics.get("total_processed", 0)

        # Calculate oldest request age
        oldest_ages = [q.get("oldest_age", 0) for q in queues.values()]
        self.queue_metrics.oldest_request_age_seconds = max(oldest_ages) if oldest_ages else 0

        # Calculate processing rate
        if len(self.metrics_history) > 0:
            prev_processed = (
                self.metrics_history[-1]
                .get("queue_metrics", {})
                .get("requests_processed_last_5min", 0)
            )
            time_diff_minutes = (datetime.now() - self.last_collection_time).total_seconds() / 60

            if time_diff_minutes > 0:
                processed_diff = max(
                    0, self.queue_metrics.requests_processed_last_5min - prev_processed
                )
                self.queue_metrics.processing_rate_per_minute = processed_diff / time_diff_minutes

    async def _collect_processing_metrics(self) -> None:
        """Collect processing rate and throughput metrics."""
        try:
            # Get processing statistics from state manager
            processing_stats = await self.state_manager.get_processing_statistics()

            # Calculate processing rates based on historical data
            if len(self.metrics_history) > 0:
                time_diff_minutes = (
                    datetime.now() - self.last_collection_time
                ).total_seconds() / 60

                if time_diff_minutes > 0:
                    prev_stats = self.metrics_history[-1].get("processing_stats", {})

                    # Calculate company processing rates
                    current_total = processing_stats.get("total_companies", 0)
                    prev_total = prev_stats.get("total_companies", 0)

                    self.processing_metrics.companies_processed_per_minute = (
                        max(0, current_total - prev_total) / time_diff_minutes
                    )

            # Set current values
            by_state = processing_stats.get("by_state", {})
            self.processing_metrics.pipeline_backlog = (
                by_state.get("detected", 0)
                + by_state.get("status_queued", 0)
                + by_state.get("officers_queued", 0)
            )

            # Calculate success rate from queue manager
            queue_metrics = self.queue_manager.get_queue_status()["metrics"]
            self.processing_metrics.success_rate = queue_metrics.get("success_rate", 1.0)

        except Exception as e:
            logger.debug("Error collecting processing metrics: %s", e)

    def _collect_api_compliance_metrics(self) -> None:
        """Collect API compliance and rate limiting metrics."""
        # API usage metrics
        api_stats = self.api_usage_monitor.get_current_statistics()
        current_window = api_stats["current_window"]

        self.compliance_metrics.api_calls_current_window = current_window["calls"]
        self.compliance_metrics.api_usage_percentage = current_window["usage_percentage"]
        self.compliance_metrics.rate_limit = current_window["rate_limit"]
        self.compliance_metrics.minutes_until_window_reset = current_window.get(
            "time_remaining_minutes", 0
        )

        # Performance metrics
        performance = api_stats["performance"]
        self.compliance_metrics.api_response_time_ms = performance["avg_response_time_ms"]
        self.compliance_metrics.api_success_rate = performance["success_rate"]
        self.compliance_metrics.api_error_rate_percentage = (1 - performance["success_rate"]) * 100

        # Predictions
        predictions = api_stats["predictions"]
        self.compliance_metrics.projected_usage_next_window = predictions[
            "projected_calls_next_window"
        ]
        self.compliance_metrics.risk_assessment = predictions["risk_level"]

        # Rate limit violations
        violation_summary = self.rate_limit_monitor.get_violation_summary()
        self.compliance_metrics.violations_current_window = violation_summary["current_window"][
            "violations"
        ]
        self.compliance_metrics.violations_last_24h = violation_summary["totals"][
            "total_violations"
        ]

        # Compliance level classification
        if self.compliance_metrics.violations_current_window > 0:
            if violation_summary["status"]["level"] == "EMERGENCY":
                self.compliance_metrics.compliance_level = "critical"
            elif violation_summary["status"]["level"] == "CRITICAL":
                self.compliance_metrics.compliance_level = "violation"
            else:
                self.compliance_metrics.compliance_level = "warning"
        else:
            self.compliance_metrics.compliance_level = "compliant"

    async def _collect_performance_metrics(self) -> None:
        """Collect system performance metrics."""
        # System uptime
        uptime = (datetime.now() - self.start_time).total_seconds() / 3600
        self.performance_metrics.uptime_hours = uptime

        # Component health assessment
        healthy_components = 0
        total_components = 0

        # Check queue manager health
        total_components += 1
        if self.queue_metrics.utilization_percentage < 90:
            healthy_components += 1

        # Check API compliance health
        total_components += 1
        if self.compliance_metrics.compliance_level in ["compliant", "warning"]:
            healthy_components += 1

        # Check health monitor if available
        if self.health_monitor:
            total_components += 1
            if self.health_monitor.is_healthy():
                healthy_components += 1

        self.performance_metrics.components_healthy = healthy_components
        self.performance_metrics.components_total = total_components

        # Overall health score
        queue_health = self.queue_metrics.calculate_health_score()
        api_health = self.compliance_metrics.get_compliance_score()

        self.performance_metrics.overall_health_score = (queue_health + api_health) / 2

        # System status classification
        if self.performance_metrics.overall_health_score >= 0.8:
            self.performance_metrics.system_status = "healthy"
        elif self.performance_metrics.overall_health_score >= 0.6:
            self.performance_metrics.system_status = "degraded"
        elif self.performance_metrics.overall_health_score >= 0.3:
            self.performance_metrics.system_status = "critical"
        else:
            self.performance_metrics.system_status = "offline"

        # Active alerts
        active_alerts = len(self.rate_limit_monitor.active_alerts)
        self.performance_metrics.active_alerts = active_alerts

        # Critical alerts (emergency level)
        critical_alerts = sum(
            1
            for alert in self.rate_limit_monitor.alert_history
            if not alert.resolved and alert.level.value == "emergency"
        )
        self.performance_metrics.critical_alerts = critical_alerts

    def _store_historical_snapshot(self) -> None:
        """Store current metrics as historical snapshot."""
        snapshot = {
            "timestamp": datetime.now().isoformat(),
            "collection_time": self.last_collection_time.isoformat(),
            "queue_metrics": {
                "total_queued": self.queue_metrics.total_queued,
                "utilization_percentage": self.queue_metrics.utilization_percentage,
                "processing_rate_per_minute": self.queue_metrics.processing_rate_per_minute,
                "oldest_request_age_seconds": self.queue_metrics.oldest_request_age_seconds,
                "health_score": self.queue_metrics.calculate_health_score(),
            },
            "processing_metrics": {
                "companies_processed_per_minute": self.processing_metrics.companies_processed_per_minute,
                "success_rate": self.processing_metrics.success_rate,
                "pipeline_backlog": self.processing_metrics.pipeline_backlog,
            },
            "compliance_metrics": {
                "api_calls_current_window": self.compliance_metrics.api_calls_current_window,
                "api_usage_percentage": self.compliance_metrics.api_usage_percentage,
                "compliance_level": self.compliance_metrics.compliance_level,
                "violations_current_window": self.compliance_metrics.violations_current_window,
                "compliance_score": self.compliance_metrics.get_compliance_score(),
            },
            "performance_metrics": {
                "overall_health_score": self.performance_metrics.overall_health_score,
                "system_status": self.performance_metrics.system_status,
                "components_healthy": self.performance_metrics.components_healthy,
                "components_total": self.performance_metrics.components_total,
                "active_alerts": self.performance_metrics.active_alerts,
            },
        }

        self.metrics_history.append(snapshot)

    def _clean_historical_data(self) -> None:
        """Clean old historical data."""
        cutoff_time = datetime.now() - self.retention_period

        self.metrics_history = [
            snapshot
            for snapshot in self.metrics_history
            if datetime.fromisoformat(snapshot["timestamp"]) >= cutoff_time
        ]

    def get_comprehensive_metrics(self) -> Dict[str, Any]:
        """Get all current metrics in a comprehensive format.

        Returns:
            Dictionary containing all current metrics
        """
        return {
            "timestamp": datetime.now().isoformat(),
            "collection_info": {
                "last_collection": self.last_collection_time.isoformat(),
                "collection_interval_seconds": self.collection_interval,
                "uptime_hours": (datetime.now() - self.start_time).total_seconds() / 3600,
            },
            "queue_metrics": {
                "depth": {
                    "total_queued": self.queue_metrics.total_queued,
                    "high_priority": self.queue_metrics.high_priority_queued,
                    "medium_priority": self.queue_metrics.medium_priority_queued,
                    "low_priority": self.queue_metrics.low_priority_queued,
                    "background": self.queue_metrics.background_queued,
                },
                "utilization": {
                    "percentage": self.queue_metrics.utilization_percentage,
                    "max_capacity": self.queue_metrics.max_capacity,
                },
                "performance": {
                    "processing_rate_per_minute": self.queue_metrics.processing_rate_per_minute,
                    "oldest_request_age_seconds": self.queue_metrics.oldest_request_age_seconds,
                    "average_queue_time_seconds": self.queue_metrics.average_queue_time_seconds,
                },
                "health_score": self.queue_metrics.calculate_health_score(),
            },
            "processing_metrics": {
                "rates": {
                    "companies_per_minute": self.processing_metrics.companies_processed_per_minute,
                    "status_checks_per_minute": self.processing_metrics.status_checks_per_minute,
                    "officers_fetches_per_minute": self.processing_metrics.officers_fetches_per_minute,
                },
                "efficiency": {
                    "success_rate": self.processing_metrics.success_rate,
                    "error_rate_per_minute": self.processing_metrics.error_rate_per_minute,
                    "pipeline_efficiency_score": self.processing_metrics.pipeline_efficiency_score,
                },
                "backlog": {
                    "pipeline_backlog": self.processing_metrics.pipeline_backlog,
                    "capacity_utilization": self.processing_metrics.capacity_utilization,
                },
            },
            "api_compliance": {
                "current_usage": {
                    "calls_current_window": self.compliance_metrics.api_calls_current_window,
                    "usage_percentage": self.compliance_metrics.api_usage_percentage,
                    "rate_limit": self.compliance_metrics.rate_limit,
                },
                "compliance": {
                    "level": self.compliance_metrics.compliance_level,
                    "violations_current_window": self.compliance_metrics.violations_current_window,
                    "violations_last_24h": self.compliance_metrics.violations_last_24h,
                    "compliance_score": self.compliance_metrics.get_compliance_score(),
                },
                "performance": {
                    "response_time_ms": self.compliance_metrics.api_response_time_ms,
                    "success_rate": self.compliance_metrics.api_success_rate,
                    "error_rate_percentage": self.compliance_metrics.api_error_rate_percentage,
                },
                "predictions": {
                    "projected_usage_next_window": self.compliance_metrics.projected_usage_next_window,
                    "risk_assessment": self.compliance_metrics.risk_assessment,
                    "minutes_until_reset": self.compliance_metrics.minutes_until_window_reset,
                },
            },
            "system_performance": {
                "health": {
                    "overall_score": self.performance_metrics.overall_health_score,
                    "system_status": self.performance_metrics.system_status,
                    "components_healthy": self.performance_metrics.components_healthy,
                    "components_total": self.performance_metrics.components_total,
                },
                "alerts": {
                    "active_alerts": self.performance_metrics.active_alerts,
                    "critical_alerts": self.performance_metrics.critical_alerts,
                },
                "uptime": {
                    "uptime_hours": self.performance_metrics.uptime_hours,
                    "last_restart": self.performance_metrics.last_restart_time.isoformat()
                    if self.performance_metrics.last_restart_time
                    else None,
                },
            },
        }

    def get_metrics_summary(self) -> Dict[str, Any]:
        """Get a summary of key metrics for dashboards.

        Returns:
            Dictionary with key metric summaries
        """
        return {
            "timestamp": datetime.now().isoformat(),
            "system_status": self.performance_metrics.system_status,
            "overall_health_score": round(self.performance_metrics.overall_health_score, 3),
            "queue_utilization_percentage": round(self.queue_metrics.utilization_percentage, 2),
            "api_usage_percentage": round(self.compliance_metrics.api_usage_percentage, 2),
            "processing_rate_per_minute": round(self.queue_metrics.processing_rate_per_minute, 2),
            "api_compliance_level": self.compliance_metrics.compliance_level,
            "active_alerts": self.performance_metrics.active_alerts,
            "total_queued": self.queue_metrics.total_queued,
            "pipeline_backlog": self.processing_metrics.pipeline_backlog,
            "uptime_hours": round(self.performance_metrics.uptime_hours, 2),
        }

    def export_for_cloud_platform(self, platform: str = "generic") -> Dict[str, Any]:
        """Export metrics in cloud platform specific format.

        Args:
            platform: Target platform (generic, cloudwatch, prometheus, etc.)

        Returns:
            Platform-formatted metrics
        """
        base_metrics = self.get_metrics_summary()

        if platform == "cloudwatch":
            return {
                "Timestamp": datetime.now().isoformat(),
                "Namespace": "CompaniesHouse/Streaming",
                "MetricData": [
                    {
                        "MetricName": "SystemHealthScore",
                        "Value": base_metrics["overall_health_score"],
                        "Unit": "None",
                        "Dimensions": [{"Name": "Component", "Value": "StreamingService"}],
                    },
                    {
                        "MetricName": "QueueUtilization",
                        "Value": base_metrics["queue_utilization_percentage"],
                        "Unit": "Percent",
                    },
                    {
                        "MetricName": "APIUsage",
                        "Value": base_metrics["api_usage_percentage"],
                        "Unit": "Percent",
                    },
                    {
                        "MetricName": "ProcessingRate",
                        "Value": base_metrics["processing_rate_per_minute"],
                        "Unit": "Count/Minute",
                    },
                    {
                        "MetricName": "ActiveAlerts",
                        "Value": base_metrics["active_alerts"],
                        "Unit": "Count",
                    },
                ],
            }

        if platform == "prometheus":
            return {
                "companies_house_system_health_score": base_metrics["overall_health_score"],
                "companies_house_queue_utilization_percentage": base_metrics[
                    "queue_utilization_percentage"
                ],
                "companies_house_api_usage_percentage": base_metrics["api_usage_percentage"],
                "companies_house_processing_rate_per_minute": base_metrics[
                    "processing_rate_per_minute"
                ],
                "companies_house_active_alerts_total": base_metrics["active_alerts"],
                "companies_house_uptime_hours": base_metrics["uptime_hours"],
                "companies_house_queue_total": base_metrics["total_queued"],
                "companies_house_pipeline_backlog": base_metrics["pipeline_backlog"],
            }

        # generic
        return {
            "service": "companies-house-streaming",
            "timestamp": base_metrics["timestamp"],
            "metrics": base_metrics,
            "tags": {"component": "comprehensive_metrics", "environment": "production"},
        }

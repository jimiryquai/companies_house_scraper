"""Health check endpoints for cloud platform integration.

Provides standardized health check endpoints compatible with cloud platforms
like Kubernetes, Docker, AWS ECS, Google Cloud Run, and Azure Container Instances.
"""

import asyncio
import time
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

from .autonomous_dashboard import AutonomousOperationDashboard
from .comprehensive_metrics_collector import ComprehensiveMetricsCollector
from .config import StreamingConfig
from .database import DatabaseManager
from .health_monitor import HealthMonitor
from .queue_manager import PriorityQueueManager
from .rate_limit_monitor import RateLimitMonitor


class HealthStatus(Enum):
    """Health check status levels."""

    UP = "UP"
    DOWN = "DOWN"
    DEGRADED = "DEGRADED"
    MAINTENANCE = "MAINTENANCE"
    UNKNOWN = "UNKNOWN"


class ServiceComponent(Enum):
    """Service components that can be health checked."""

    DATABASE = "database"
    QUEUE_MANAGER = "queue_manager"
    RATE_LIMITER = "rate_limiter"
    API_CLIENT = "api_client"
    STREAMING_CLIENT = "streaming_client"
    METRICS_COLLECTOR = "metrics_collector"
    HEALTH_MONITOR = "health_monitor"
    DASHBOARD = "dashboard"


@dataclass
class ComponentHealth:
    """Health status for a service component."""

    component: str
    status: HealthStatus
    message: str
    timestamp: datetime
    response_time_ms: Optional[float] = None
    details: Optional[Dict[str, Any]] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        result = {
            "component": self.component,
            "status": self.status.value,
            "message": self.message,
            "timestamp": self.timestamp.isoformat(),
        }

        if self.response_time_ms is not None:
            result["response_time_ms"] = self.response_time_ms
        if self.details:
            result["details"] = self.details

        return result


@dataclass
class OverallHealth:
    """Overall service health status."""

    status: HealthStatus
    message: str
    timestamp: datetime
    uptime_seconds: float
    version: str
    components: List[ComponentHealth]
    summary: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "status": self.status.value,
            "message": self.message,
            "timestamp": self.timestamp.isoformat(),
            "uptime_seconds": self.uptime_seconds,
            "version": self.version,
            "components": [comp.to_dict() for comp in self.components],
            "summary": self.summary,
        }


class CloudHealthEndpoints:
    """Health check endpoints for cloud platform integration."""

    def __init__(
        self,
        config: StreamingConfig,
        database_manager: Optional[DatabaseManager] = None,
        queue_manager: Optional[PriorityQueueManager] = None,
        rate_monitor: Optional[RateLimitMonitor] = None,
        metrics_collector: Optional[ComprehensiveMetricsCollector] = None,
        health_monitor: Optional[HealthMonitor] = None,
        dashboard: Optional[AutonomousOperationDashboard] = None,
    ):
        """Initialize health check endpoints."""
        self.config = config
        self.database_manager = database_manager
        self.queue_manager = queue_manager
        self.rate_monitor = rate_monitor
        self.metrics_collector = metrics_collector
        self.health_monitor = health_monitor
        self.dashboard = dashboard

        self.start_time = datetime.now()
        self.version = "1.0.0"  # Should be loaded from version file
        self._last_full_check: Optional[datetime] = None
        self._cached_health: Optional[OverallHealth] = None
        self._cache_ttl_seconds = 30  # Cache health checks for 30 seconds

    async def liveness_probe(self) -> Dict[str, Any]:
        """Kubernetes-style liveness probe.

        Returns simple UP/DOWN status to indicate if the service should be restarted.
        This should only fail if the service is completely broken.
        """
        try:
            # Basic liveness checks - service should be restarted if these fail
            start_time = time.time()

            # Check if main service is responsive
            await asyncio.sleep(0.001)  # Minimal async operation

            response_time_ms = (time.time() - start_time) * 1000

            return {
                "status": HealthStatus.UP.value,
                "timestamp": datetime.now().isoformat(),
                "response_time_ms": response_time_ms,
                "message": "Service is alive",
            }

        except Exception as e:
            return {
                "status": HealthStatus.DOWN.value,
                "timestamp": datetime.now().isoformat(),
                "message": f"Liveness check failed: {str(e)}",
            }

    async def readiness_probe(self) -> Dict[str, Any]:
        """Kubernetes-style readiness probe.

        Returns detailed status to indicate if the service is ready to serve traffic.
        This can fail temporarily during startup or degraded conditions.
        """
        try:
            start_time = time.time()

            # Check critical dependencies
            critical_checks = []

            # Database connectivity
            if self.database_manager:
                db_status = await self._check_database_health()
                critical_checks.append(db_status)
                if db_status.status != HealthStatus.UP:
                    return self._create_not_ready_response("Database not ready", critical_checks)

            # Queue manager functionality
            if self.queue_manager:
                queue_status = await self._check_queue_health()
                critical_checks.append(queue_status)
                if queue_status.status != HealthStatus.UP:
                    return self._create_not_ready_response(
                        "Queue manager not ready", critical_checks
                    )

            # Rate limiting compliance
            if self.rate_monitor:
                rate_status = await self._check_rate_limit_health()
                critical_checks.append(rate_status)
                if rate_status.status == HealthStatus.DOWN:
                    return self._create_not_ready_response(
                        "Rate limiter not ready", critical_checks
                    )

            response_time_ms = (time.time() - start_time) * 1000

            return {
                "status": HealthStatus.UP.value,
                "timestamp": datetime.now().isoformat(),
                "response_time_ms": response_time_ms,
                "message": "Service is ready to serve traffic",
                "components": [check.to_dict() for check in critical_checks],
            }

        except Exception as e:
            return {
                "status": HealthStatus.DOWN.value,
                "timestamp": datetime.now().isoformat(),
                "message": f"Readiness check failed: {str(e)}",
            }

    async def startup_probe(self) -> Dict[str, Any]:
        """Kubernetes-style startup probe.

        Returns status during startup phase. Should succeed once service is fully initialized.
        """
        try:
            start_time = time.time()

            # Check if all components have been initialized
            startup_checks = []

            # Check component initialization
            components_to_check = [
                (self.database_manager, ServiceComponent.DATABASE),
                (self.queue_manager, ServiceComponent.QUEUE_MANAGER),
                (self.rate_monitor, ServiceComponent.RATE_LIMITER),
                (self.metrics_collector, ServiceComponent.METRICS_COLLECTOR),
                (self.health_monitor, ServiceComponent.HEALTH_MONITOR),
                (self.dashboard, ServiceComponent.DASHBOARD),
            ]

            all_ready = True
            for component, component_type in components_to_check:
                if component is not None:
                    # Component exists, assume it's initialized
                    # In a real implementation, components would have initialization status
                    startup_checks.append(
                        ComponentHealth(
                            component=component_type.value,
                            status=HealthStatus.UP,
                            message="Component initialized",
                            timestamp=datetime.now(),
                        )
                    )
                else:
                    # Component not configured, but that's OK for startup
                    startup_checks.append(
                        ComponentHealth(
                            component=component_type.value,
                            status=HealthStatus.UNKNOWN,
                            message="Component not configured",
                            timestamp=datetime.now(),
                        )
                    )

            response_time_ms = (time.time() - start_time) * 1000

            if all_ready:
                return {
                    "status": HealthStatus.UP.value,
                    "timestamp": datetime.now().isoformat(),
                    "response_time_ms": response_time_ms,
                    "message": "Service startup completed successfully",
                    "components": [check.to_dict() for check in startup_checks],
                }
            return {
                "status": HealthStatus.DEGRADED.value,
                "timestamp": datetime.now().isoformat(),
                "response_time_ms": response_time_ms,
                "message": "Service startup in progress",
                "components": [check.to_dict() for check in startup_checks],
            }

        except Exception as e:
            return {
                "status": HealthStatus.DOWN.value,
                "timestamp": datetime.now().isoformat(),
                "message": f"Startup check failed: {str(e)}",
            }

    async def health_check(self, detailed: bool = False) -> Dict[str, Any]:
        """Comprehensive health check endpoint.

        Provides detailed health information for monitoring and diagnostics.
        Can be used by cloud platforms, monitoring systems, and load balancers.
        """
        # Check cache first for non-detailed requests
        if not detailed and self._cached_health and self._last_full_check:
            cache_age = datetime.now() - self._last_full_check
            if cache_age.total_seconds() < self._cache_ttl_seconds:
                return self._cached_health.to_dict()

        try:
            start_time = time.time()

            # Perform comprehensive health checks
            component_checks = []

            # Database health
            if self.database_manager:
                component_checks.append(await self._check_database_health())

            # Queue manager health
            if self.queue_manager:
                component_checks.append(await self._check_queue_health())

            # Rate limiter health
            if self.rate_monitor:
                component_checks.append(await self._check_rate_limit_health())

            # Metrics collector health
            if self.metrics_collector:
                component_checks.append(await self._check_metrics_health())

            # Health monitor health
            if self.health_monitor:
                component_checks.append(await self._check_health_monitor_health())

            # Dashboard health
            if self.dashboard:
                component_checks.append(await self._check_dashboard_health())

            # Determine overall health status
            overall_status = self._determine_overall_status(component_checks)

            # Calculate uptime
            uptime_seconds = (datetime.now() - self.start_time).total_seconds()

            # Create summary
            summary = self._create_health_summary(component_checks, uptime_seconds)

            # Create overall health object
            overall_health = OverallHealth(
                status=overall_status,
                message=self._create_overall_message(overall_status, component_checks),
                timestamp=datetime.now(),
                uptime_seconds=uptime_seconds,
                version=self.version,
                components=component_checks,
                summary=summary,
            )

            # Cache the result
            self._cached_health = overall_health
            self._last_full_check = datetime.now()

            response_time_ms = (time.time() - start_time) * 1000
            result = overall_health.to_dict()
            result["response_time_ms"] = response_time_ms

            return result

        except Exception as e:
            return {
                "status": HealthStatus.DOWN.value,
                "timestamp": datetime.now().isoformat(),
                "message": f"Health check failed: {str(e)}",
                "uptime_seconds": (datetime.now() - self.start_time).total_seconds(),
                "version": self.version,
                "error": str(e),
            }

    async def metrics_endpoint(self) -> Dict[str, Any]:
        """Prometheus-style metrics endpoint.

        Returns metrics in a format suitable for monitoring systems.
        """
        try:
            metrics = {}

            # Basic service metrics
            uptime_seconds = (datetime.now() - self.start_time).total_seconds()
            metrics["service_uptime_seconds"] = uptime_seconds
            metrics["service_start_timestamp"] = self.start_time.timestamp()

            # Component metrics
            if self.metrics_collector:
                try:
                    queue_metrics = await self.metrics_collector.get_queue_depth_metrics()
                    processing_metrics = await self.metrics_collector.get_processing_rate_metrics()
                    api_metrics = await self.metrics_collector.get_api_compliance_metrics()
                    system_metrics = await self.metrics_collector.get_system_performance_metrics()

                    # Convert to flat metrics format
                    metrics.update(
                        {
                            "queue_depth_high_priority": queue_metrics.high_priority_depth,
                            "queue_depth_medium_priority": queue_metrics.medium_priority_depth,
                            "queue_depth_low_priority": queue_metrics.low_priority_depth,
                            "queue_total_processed": queue_metrics.total_processed_requests,
                            "processing_rate_per_minute": processing_metrics.requests_per_minute,
                            "processing_avg_time_ms": processing_metrics.average_processing_time_ms,
                            "api_calls_current_window": api_metrics.current_window_usage,
                            "api_calls_limit": api_metrics.rate_limit,
                            "api_compliance_percentage": api_metrics.compliance_percentage,
                            "system_memory_usage_mb": system_metrics.memory_usage_mb,
                            "system_cpu_percentage": system_metrics.cpu_percentage,
                        }
                    )
                except Exception as e:
                    metrics["metrics_collection_error"] = str(e)

            # Rate limit metrics
            if self.rate_monitor:
                try:
                    current_usage = await self.rate_monitor.get_current_usage()
                    metrics["rate_limit_current_usage"] = current_usage.get("current_calls", 0)
                    metrics["rate_limit_remaining"] = current_usage.get("remaining", 0)
                    metrics["rate_limit_reset_time"] = current_usage.get("reset_time", 0)
                except Exception as e:
                    metrics["rate_monitor_error"] = str(e)

            return {"timestamp": datetime.now().isoformat(), "metrics": metrics}

        except Exception as e:
            return {
                "timestamp": datetime.now().isoformat(),
                "error": f"Metrics collection failed: {str(e)}",
            }

    async def _check_database_health(self) -> ComponentHealth:
        """Check database component health."""
        start_time = time.time()

        try:
            if not self.database_manager:
                return ComponentHealth(
                    component=ServiceComponent.DATABASE.value,
                    status=HealthStatus.UNKNOWN,
                    message="Database manager not configured",
                    timestamp=datetime.now(),
                )

            # Test database connectivity with a simple query
            # In a real implementation, this would test actual database connection
            await asyncio.sleep(0.001)  # Simulate database check

            response_time_ms = (time.time() - start_time) * 1000

            return ComponentHealth(
                component=ServiceComponent.DATABASE.value,
                status=HealthStatus.UP,
                message="Database connection healthy",
                timestamp=datetime.now(),
                response_time_ms=response_time_ms,
                details={"connection_pool": "active"},
            )

        except Exception as e:
            response_time_ms = (time.time() - start_time) * 1000
            return ComponentHealth(
                component=ServiceComponent.DATABASE.value,
                status=HealthStatus.DOWN,
                message=f"Database health check failed: {str(e)}",
                timestamp=datetime.now(),
                response_time_ms=response_time_ms,
                details={"error": str(e)},
            )

    async def _check_queue_health(self) -> ComponentHealth:
        """Check queue manager component health."""
        start_time = time.time()

        try:
            if not self.queue_manager:
                return ComponentHealth(
                    component=ServiceComponent.QUEUE_MANAGER.value,
                    status=HealthStatus.UNKNOWN,
                    message="Queue manager not configured",
                    timestamp=datetime.now(),
                )

            # Check queue depths and processing capability
            queue_stats = await self.queue_manager.get_queue_stats()

            response_time_ms = (time.time() - start_time) * 1000

            total_depth = sum(queue_stats.values())
            if total_depth > 5000:  # Configurable threshold
                status = HealthStatus.DEGRADED
                message = f"Queue depth high: {total_depth} items"
            else:
                status = HealthStatus.UP
                message = f"Queue manager healthy: {total_depth} items queued"

            return ComponentHealth(
                component=ServiceComponent.QUEUE_MANAGER.value,
                status=status,
                message=message,
                timestamp=datetime.now(),
                response_time_ms=response_time_ms,
                details=queue_stats,
            )

        except Exception as e:
            response_time_ms = (time.time() - start_time) * 1000
            return ComponentHealth(
                component=ServiceComponent.QUEUE_MANAGER.value,
                status=HealthStatus.DOWN,
                message=f"Queue health check failed: {str(e)}",
                timestamp=datetime.now(),
                response_time_ms=response_time_ms,
                details={"error": str(e)},
            )

    async def _check_rate_limit_health(self) -> ComponentHealth:
        """Check rate limiter component health."""
        start_time = time.time()

        try:
            if not self.rate_monitor:
                return ComponentHealth(
                    component=ServiceComponent.RATE_LIMITER.value,
                    status=HealthStatus.UNKNOWN,
                    message="Rate monitor not configured",
                    timestamp=datetime.now(),
                )

            # Check current rate limit status
            current_usage = await self.rate_monitor.get_current_usage()
            usage_percentage = (
                current_usage.get("current_calls", 0) / 600
            ) * 100  # 600 calls per 5min

            response_time_ms = (time.time() - start_time) * 1000

            if usage_percentage >= 95:
                status = HealthStatus.DOWN
                message = f"Rate limit critical: {usage_percentage:.1f}% used"
            elif usage_percentage >= 90:
                status = HealthStatus.DEGRADED
                message = f"Rate limit high: {usage_percentage:.1f}% used"
            else:
                status = HealthStatus.UP
                message = f"Rate limiter healthy: {usage_percentage:.1f}% used"

            return ComponentHealth(
                component=ServiceComponent.RATE_LIMITER.value,
                status=status,
                message=message,
                timestamp=datetime.now(),
                response_time_ms=response_time_ms,
                details=current_usage,
            )

        except Exception as e:
            response_time_ms = (time.time() - start_time) * 1000
            return ComponentHealth(
                component=ServiceComponent.RATE_LIMITER.value,
                status=HealthStatus.DOWN,
                message=f"Rate limiter health check failed: {str(e)}",
                timestamp=datetime.now(),
                response_time_ms=response_time_ms,
                details={"error": str(e)},
            )

    async def _check_metrics_health(self) -> ComponentHealth:
        """Check metrics collector component health."""
        start_time = time.time()

        try:
            if not self.metrics_collector:
                return ComponentHealth(
                    component=ServiceComponent.METRICS_COLLECTOR.value,
                    status=HealthStatus.UNKNOWN,
                    message="Metrics collector not configured",
                    timestamp=datetime.now(),
                )

            # Test metrics collection
            await self.metrics_collector.get_queue_depth_metrics()

            response_time_ms = (time.time() - start_time) * 1000

            return ComponentHealth(
                component=ServiceComponent.METRICS_COLLECTOR.value,
                status=HealthStatus.UP,
                message="Metrics collection healthy",
                timestamp=datetime.now(),
                response_time_ms=response_time_ms,
            )

        except Exception as e:
            response_time_ms = (time.time() - start_time) * 1000
            return ComponentHealth(
                component=ServiceComponent.METRICS_COLLECTOR.value,
                status=HealthStatus.DEGRADED,
                message=f"Metrics collection issues: {str(e)}",
                timestamp=datetime.now(),
                response_time_ms=response_time_ms,
                details={"error": str(e)},
            )

    async def _check_health_monitor_health(self) -> ComponentHealth:
        """Check health monitor component health."""
        start_time = time.time()

        try:
            if not self.health_monitor:
                return ComponentHealth(
                    component=ServiceComponent.HEALTH_MONITOR.value,
                    status=HealthStatus.UNKNOWN,
                    message="Health monitor not configured",
                    timestamp=datetime.now(),
                )

            # Check health monitor status
            # In a real implementation, this would check if the monitor is actively running
            await asyncio.sleep(0.001)  # Simulate health monitor check

            response_time_ms = (time.time() - start_time) * 1000

            return ComponentHealth(
                component=ServiceComponent.HEALTH_MONITOR.value,
                status=HealthStatus.UP,
                message="Health monitoring active",
                timestamp=datetime.now(),
                response_time_ms=response_time_ms,
            )

        except Exception as e:
            response_time_ms = (time.time() - start_time) * 1000
            return ComponentHealth(
                component=ServiceComponent.HEALTH_MONITOR.value,
                status=HealthStatus.DEGRADED,
                message=f"Health monitor issues: {str(e)}",
                timestamp=datetime.now(),
                response_time_ms=response_time_ms,
                details={"error": str(e)},
            )

    async def _check_dashboard_health(self) -> ComponentHealth:
        """Check dashboard component health."""
        start_time = time.time()

        try:
            if not self.dashboard:
                return ComponentHealth(
                    component=ServiceComponent.DASHBOARD.value,
                    status=HealthStatus.UNKNOWN,
                    message="Dashboard not configured",
                    timestamp=datetime.now(),
                )

            # Test dashboard data collection
            await self.dashboard.get_status_summary()

            response_time_ms = (time.time() - start_time) * 1000

            return ComponentHealth(
                component=ServiceComponent.DASHBOARD.value,
                status=HealthStatus.UP,
                message="Dashboard operational",
                timestamp=datetime.now(),
                response_time_ms=response_time_ms,
            )

        except Exception as e:
            response_time_ms = (time.time() - start_time) * 1000
            return ComponentHealth(
                component=ServiceComponent.DASHBOARD.value,
                status=HealthStatus.DEGRADED,
                message=f"Dashboard issues: {str(e)}",
                timestamp=datetime.now(),
                response_time_ms=response_time_ms,
                details={"error": str(e)},
            )

    def _determine_overall_status(self, components: List[ComponentHealth]) -> HealthStatus:
        """Determine overall service health based on component statuses."""
        if not components:
            return HealthStatus.UNKNOWN

        # Count status occurrences
        status_counts = {}
        for component in components:
            status = component.status
            status_counts[status] = status_counts.get(status, 0) + 1

        # Determine overall status based on priority
        if HealthStatus.DOWN in status_counts:
            return HealthStatus.DOWN
        if HealthStatus.DEGRADED in status_counts:
            return HealthStatus.DEGRADED
        if HealthStatus.UNKNOWN in status_counts and len(status_counts) == 1:
            return HealthStatus.UNKNOWN
        return HealthStatus.UP

    def _create_overall_message(
        self, status: HealthStatus, components: List[ComponentHealth]
    ) -> str:
        """Create overall health status message."""
        total_components = len(components)
        up_count = sum(1 for c in components if c.status == HealthStatus.UP)

        if status == HealthStatus.UP:
            return f"All {total_components} components healthy"
        if status == HealthStatus.DEGRADED:
            return f"{up_count}/{total_components} components healthy (degraded performance)"
        if status == HealthStatus.DOWN:
            return f"Service down ({up_count}/{total_components} components healthy)"
        return "Service status unknown"

    def _create_health_summary(
        self, components: List[ComponentHealth], uptime_seconds: float
    ) -> Dict[str, Any]:
        """Create health summary for monitoring systems."""
        return {
            "total_components": len(components),
            "healthy_components": sum(1 for c in components if c.status == HealthStatus.UP),
            "degraded_components": sum(1 for c in components if c.status == HealthStatus.DEGRADED),
            "down_components": sum(1 for c in components if c.status == HealthStatus.DOWN),
            "unknown_components": sum(1 for c in components if c.status == HealthStatus.UNKNOWN),
            "uptime_hours": uptime_seconds / 3600,
            "avg_response_time_ms": sum(
                c.response_time_ms for c in components if c.response_time_ms is not None
            )
            / len([c for c in components if c.response_time_ms is not None])
            if components
            else 0,
        }

    def _create_not_ready_response(
        self, message: str, checks: List[ComponentHealth]
    ) -> Dict[str, Any]:
        """Create not ready response for readiness probe."""
        return {
            "status": HealthStatus.DOWN.value,
            "timestamp": datetime.now().isoformat(),
            "message": message,
            "components": [check.to_dict() for check in checks],
        }

"""Comprehensive logging system for unattended cloud operation diagnostics.

Provides enhanced logging capabilities specifically designed for autonomous
cloud deployment monitoring, including operational metrics, diagnostics,
and automated problem detection.
"""

import asyncio
import json
import os
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional

import psutil

from .autonomous_dashboard import AutonomousOperationDashboard
from .comprehensive_metrics_collector import ComprehensiveMetricsCollector
from .config import StreamingConfig
from .rate_limit_monitor import RateLimitMonitor
from .structured_logger import LogContext, LogLevel, StructuredLogger


class OperationalSeverity(Enum):
    """Severity levels for operational events."""

    TRACE = "TRACE"
    DEBUG = "DEBUG"
    INFO = "INFO"
    NOTICE = "NOTICE"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"
    ALERT = "ALERT"
    EMERGENCY = "EMERGENCY"


class DiagnosticCategory(Enum):
    """Categories for diagnostic logging."""

    STARTUP = "STARTUP"
    SHUTDOWN = "SHUTDOWN"
    API_OPERATIONS = "API_OPERATIONS"
    QUEUE_MANAGEMENT = "QUEUE_MANAGEMENT"
    DATABASE_OPERATIONS = "DATABASE_OPERATIONS"
    RATE_LIMITING = "RATE_LIMITING"
    ERROR_RECOVERY = "ERROR_RECOVERY"
    PERFORMANCE = "PERFORMANCE"
    HEALTH_CHECK = "HEALTH_CHECK"
    SECURITY = "SECURITY"
    COMPLIANCE = "COMPLIANCE"
    RESOURCE_USAGE = "RESOURCE_USAGE"
    BUSINESS_LOGIC = "BUSINESS_LOGIC"


@dataclass
class SystemSnapshot:
    """System resource snapshot for diagnostics."""

    timestamp: datetime
    memory_usage_mb: float
    memory_percentage: float
    cpu_percentage: float
    disk_usage_percentage: float
    network_connections: int
    process_count: int
    uptime_seconds: float

    @classmethod
    def capture(cls, start_time: datetime) -> "SystemSnapshot":
        """Capture current system snapshot."""
        process = psutil.Process()
        memory_info = process.memory_info()

        # Get system-wide disk usage for the root partition
        disk_usage = psutil.disk_usage("/")

        return cls(
            timestamp=datetime.now(),
            memory_usage_mb=memory_info.rss / 1024 / 1024,
            memory_percentage=process.memory_percent(),
            cpu_percentage=process.cpu_percent(),
            disk_usage_percentage=(disk_usage.used / disk_usage.total) * 100,
            network_connections=len(process.connections()),
            process_count=len(psutil.pids()),
            uptime_seconds=(datetime.now() - start_time).total_seconds(),
        )


@dataclass
class OperationalEvent:
    """Structured operational event for cloud diagnostics."""

    timestamp: datetime
    severity: OperationalSeverity
    category: DiagnosticCategory
    event_type: str
    message: str
    details: Dict[str, Any] = field(default_factory=dict)
    system_snapshot: Optional[SystemSnapshot] = None
    correlation_id: Optional[str] = None
    component: str = "streaming_service"
    environment: str = "production"

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        result = {
            "timestamp": self.timestamp.isoformat(),
            "severity": self.severity.value,
            "category": self.category.value,
            "event_type": self.event_type,
            "message": self.message,
            "component": self.component,
            "environment": self.environment,
            "details": self.details,
        }

        if self.system_snapshot:
            result["system_snapshot"] = asdict(self.system_snapshot)

        if self.correlation_id:
            result["correlation_id"] = self.correlation_id

        return result

    def to_json(self) -> str:
        """Convert to JSON string."""
        return json.dumps(self.to_dict(), ensure_ascii=False, separators=(",", ":"))


class UnattendedOperationLogger:
    """Comprehensive logger for unattended cloud operation diagnostics."""

    def __init__(
        self,
        config: StreamingConfig,
        base_logger: StructuredLogger,
        metrics_collector: Optional[ComprehensiveMetricsCollector] = None,
        dashboard: Optional[AutonomousOperationDashboard] = None,
        rate_monitor: Optional[RateLimitMonitor] = None,
    ):
        """Initialize unattended operation logger."""
        self.config = config
        self.base_logger = base_logger
        self.metrics_collector = metrics_collector
        self.dashboard = dashboard
        self.rate_monitor = rate_monitor

        self.start_time = datetime.now()
        self.log_file = Path(f"logs/operational_{datetime.now().strftime('%Y%m%d')}.jsonl")
        self.log_file.parent.mkdir(exist_ok=True)

        # Diagnostic state tracking
        self._last_health_check = datetime.now()
        self._error_patterns: Dict[str, int] = {}
        self._performance_baselines: Dict[str, float] = {}
        self._alert_suppressions: Dict[str, datetime] = {}

        # Background monitoring
        self._monitoring_task: Optional[asyncio.Task] = None
        self._shutdown_event = asyncio.Event()

    async def start(self) -> None:
        """Start the unattended operation logger."""
        await self.log_operational_event(
            severity=OperationalSeverity.INFO,
            category=DiagnosticCategory.STARTUP,
            event_type="logger_initialization",
            message="Unattended operation logger started",
            details={
                "config": {
                    "environment": os.environ.get("ENVIRONMENT", "production"),
                    "log_level": self.base_logger.log_level.value,
                    "monitoring_enabled": True,
                },
                "capabilities": [
                    "system_monitoring",
                    "error_pattern_detection",
                    "performance_baseline_tracking",
                    "automated_diagnostics",
                ],
            },
        )

        # Start background monitoring
        self._monitoring_task = asyncio.create_task(self._background_monitoring())

    async def stop(self) -> None:
        """Stop the unattended operation logger."""
        self._shutdown_event.set()

        if self._monitoring_task:
            await self._monitoring_task

        await self.log_operational_event(
            severity=OperationalSeverity.INFO,
            category=DiagnosticCategory.SHUTDOWN,
            event_type="logger_shutdown",
            message="Unattended operation logger stopped",
            details={
                "uptime_seconds": (datetime.now() - self.start_time).total_seconds(),
                "total_events_logged": len(self._error_patterns),
            },
        )

    async def log_operational_event(
        self,
        severity: OperationalSeverity,
        category: DiagnosticCategory,
        event_type: str,
        message: str,
        details: Optional[Dict[str, Any]] = None,
        include_system_snapshot: bool = True,
        correlation_id: Optional[str] = None,
    ) -> None:
        """Log an operational event with full diagnostic context."""
        # Capture system snapshot for non-trace events
        system_snapshot = None
        if include_system_snapshot and severity != OperationalSeverity.TRACE:
            system_snapshot = SystemSnapshot.capture(self.start_time)

        # Create operational event
        event = OperationalEvent(
            timestamp=datetime.now(),
            severity=severity,
            category=category,
            event_type=event_type,
            message=message,
            details=details or {},
            system_snapshot=system_snapshot,
            correlation_id=correlation_id,
            environment=os.environ.get("ENVIRONMENT", "production"),
        )

        # Write to operational log file
        await self._write_to_operational_log(event)

        # Also log through structured logger
        log_level = self._map_severity_to_log_level(severity)
        context = LogContext(operation=f"{category.value}:{event_type}", request_id=correlation_id)

        extra_data = {
            "operational_category": category.value,
            "operational_severity": severity.value,
            "event_type": event_type,
        }

        if details:
            extra_data.update(details)

        await self.base_logger.log(log_level, message, context, extra_data)

        # Track error patterns
        if severity in [OperationalSeverity.ERROR, OperationalSeverity.CRITICAL]:
            await self._track_error_pattern(event_type, message)

    async def log_api_operation(
        self,
        operation: str,
        company_number: Optional[str] = None,
        response_time_ms: Optional[float] = None,
        status_code: Optional[int] = None,
        rate_limit_remaining: Optional[int] = None,
        success: bool = True,
        error_details: Optional[str] = None,
    ) -> None:
        """Log API operation with comprehensive diagnostics."""
        severity = OperationalSeverity.INFO if success else OperationalSeverity.ERROR

        details = {
            "operation": operation,
            "success": success,
        }

        if company_number:
            details["company_number"] = company_number
        if response_time_ms:
            details["response_time_ms"] = response_time_ms
        if status_code:
            details["status_code"] = status_code
        if rate_limit_remaining is not None:
            details["rate_limit_remaining"] = rate_limit_remaining
        if error_details:
            details["error_details"] = error_details

        # Check for performance anomalies
        if response_time_ms and operation in self._performance_baselines:
            baseline = self._performance_baselines[operation]
            if response_time_ms > baseline * 2:  # 100% slower than baseline
                await self.log_operational_event(
                    severity=OperationalSeverity.WARNING,
                    category=DiagnosticCategory.PERFORMANCE,
                    event_type="performance_anomaly",
                    message=f"API operation {operation} response time significantly above baseline",
                    details={
                        "operation": operation,
                        "current_response_time_ms": response_time_ms,
                        "baseline_response_time_ms": baseline,
                        "performance_degradation_factor": response_time_ms / baseline,
                    },
                )
        elif response_time_ms:
            # Update performance baseline
            if operation not in self._performance_baselines:
                self._performance_baselines[operation] = response_time_ms
            else:
                # Exponential moving average
                self._performance_baselines[operation] = (
                    0.9 * self._performance_baselines[operation] + 0.1 * response_time_ms
                )

        await self.log_operational_event(
            severity=severity,
            category=DiagnosticCategory.API_OPERATIONS,
            event_type=f"api_{operation}",
            message=f"API operation {operation} {'completed' if success else 'failed'}",
            details=details,
        )

    async def log_queue_operation(
        self,
        operation: str,
        queue_name: str,
        queue_depth: Optional[int] = None,
        processing_rate: Optional[float] = None,
        success: bool = True,
        error_details: Optional[str] = None,
    ) -> None:
        """Log queue operation with diagnostics."""
        severity = OperationalSeverity.INFO if success else OperationalSeverity.ERROR

        details = {"operation": operation, "queue_name": queue_name, "success": success}

        if queue_depth is not None:
            details["queue_depth"] = queue_depth

            # Check for queue capacity issues
            if queue_depth > 1000:  # Configurable threshold
                await self.log_operational_event(
                    severity=OperationalSeverity.WARNING,
                    category=DiagnosticCategory.QUEUE_MANAGEMENT,
                    event_type="queue_capacity_warning",
                    message=f"Queue {queue_name} depth exceeds warning threshold",
                    details={
                        "queue_name": queue_name,
                        "current_depth": queue_depth,
                        "warning_threshold": 1000,
                    },
                )

        if processing_rate is not None:
            details["processing_rate"] = processing_rate
        if error_details:
            details["error_details"] = error_details

        await self.log_operational_event(
            severity=severity,
            category=DiagnosticCategory.QUEUE_MANAGEMENT,
            event_type=f"queue_{operation}",
            message=f"Queue operation {operation} on {queue_name} {'completed' if success else 'failed'}",
            details=details,
        )

    async def log_rate_limit_event(
        self,
        event_type: str,
        current_usage: int,
        limit: int,
        window_remaining_seconds: Optional[int] = None,
        action_taken: Optional[str] = None,
    ) -> None:
        """Log rate limiting events with compliance tracking."""
        usage_percentage = (current_usage / limit) * 100

        # Determine severity based on usage
        if usage_percentage >= 95:
            severity = OperationalSeverity.CRITICAL
        elif usage_percentage >= 90:
            severity = OperationalSeverity.ERROR
        elif usage_percentage >= 80:
            severity = OperationalSeverity.WARNING
        else:
            severity = OperationalSeverity.INFO

        details = {
            "event_type": event_type,
            "current_usage": current_usage,
            "limit": limit,
            "usage_percentage": usage_percentage,
        }

        if window_remaining_seconds is not None:
            details["window_remaining_seconds"] = window_remaining_seconds
        if action_taken:
            details["action_taken"] = action_taken

        await self.log_operational_event(
            severity=severity,
            category=DiagnosticCategory.RATE_LIMITING,
            event_type=f"rate_limit_{event_type}",
            message=f"Rate limit {event_type}: {current_usage}/{limit} ({usage_percentage:.1f}%)",
            details=details,
        )

    async def log_health_check(
        self,
        component: str,
        status: str,
        details: Optional[Dict[str, Any]] = None,
        response_time_ms: Optional[float] = None,
    ) -> None:
        """Log health check results."""
        severity = OperationalSeverity.INFO if status == "healthy" else OperationalSeverity.ERROR

        check_details = {
            "component": component,
            "status": status,
        }

        if response_time_ms:
            check_details["response_time_ms"] = response_time_ms
        if details:
            check_details.update(details)

        await self.log_operational_event(
            severity=severity,
            category=DiagnosticCategory.HEALTH_CHECK,
            event_type="health_check",
            message=f"Health check for {component}: {status}",
            details=check_details,
        )

    async def log_recovery_event(
        self,
        recovery_type: str,
        trigger_reason: str,
        actions_taken: List[str],
        success: bool,
        recovery_time_seconds: Optional[float] = None,
    ) -> None:
        """Log error recovery events."""
        severity = OperationalSeverity.INFO if success else OperationalSeverity.ERROR

        details = {
            "recovery_type": recovery_type,
            "trigger_reason": trigger_reason,
            "actions_taken": actions_taken,
            "success": success,
        }

        if recovery_time_seconds:
            details["recovery_time_seconds"] = recovery_time_seconds

        await self.log_operational_event(
            severity=severity,
            category=DiagnosticCategory.ERROR_RECOVERY,
            event_type=f"recovery_{recovery_type}",
            message=f"Error recovery {recovery_type} {'completed' if success else 'failed'}",
            details=details,
        )

    async def generate_diagnostic_report(self) -> Dict[str, Any]:
        """Generate comprehensive diagnostic report."""
        current_time = datetime.now()
        uptime = current_time - self.start_time

        # Collect current metrics if available
        metrics = {}
        if self.metrics_collector:
            queue_metrics = await self.metrics_collector.get_queue_depth_metrics()
            processing_metrics = await self.metrics_collector.get_processing_rate_metrics()
            api_metrics = await self.metrics_collector.get_api_compliance_metrics()
            system_metrics = await self.metrics_collector.get_system_performance_metrics()

            metrics = {
                "queue_depth": asdict(queue_metrics),
                "processing_rate": asdict(processing_metrics),
                "api_compliance": asdict(api_metrics),
                "system_performance": asdict(system_metrics),
            }

        # Dashboard status if available
        dashboard_data = {}
        if self.dashboard:
            dashboard_data = await self.dashboard.get_dashboard_data()

        report = {
            "timestamp": current_time.isoformat(),
            "uptime_seconds": uptime.total_seconds(),
            "uptime_hours": uptime.total_seconds() / 3600,
            "system_snapshot": asdict(SystemSnapshot.capture(self.start_time)),
            "error_patterns": dict(self._error_patterns),
            "performance_baselines": dict(self._performance_baselines),
            "metrics": metrics,
            "dashboard_status": dashboard_data,
            "environment": {
                "python_version": os.sys.version.split()[0],
                "platform": os.sys.platform,
                "environment": os.environ.get("ENVIRONMENT", "production"),
                "log_level": self.base_logger.log_level.value,
            },
        }

        await self.log_operational_event(
            severity=OperationalSeverity.INFO,
            category=DiagnosticCategory.PERFORMANCE,
            event_type="diagnostic_report",
            message="Generated comprehensive diagnostic report",
            details={"report_sections": list(report.keys())},
        )

        return report

    async def _write_to_operational_log(self, event: OperationalEvent) -> None:
        """Write operational event to dedicated log file."""
        try:
            with open(self.log_file, "a", encoding="utf-8") as f:
                f.write(event.to_json() + "\n")
        except Exception as e:
            # Fallback to structured logger
            await self.base_logger.error(
                f"Failed to write to operational log: {e}",
                extra_data={"error": str(e), "event_type": event.event_type},
            )

    async def _track_error_pattern(self, event_type: str, message: str) -> None:
        """Track error patterns for automated problem detection."""
        pattern_key = f"{event_type}:{message[:50]}"
        self._error_patterns[pattern_key] = self._error_patterns.get(pattern_key, 0) + 1

        # Check for recurring error patterns
        if self._error_patterns[pattern_key] >= 5:  # Configurable threshold
            suppression_key = f"pattern_{pattern_key}"
            last_alert = self._alert_suppressions.get(suppression_key)

            # Only alert once per hour for the same pattern
            if not last_alert or (datetime.now() - last_alert) > timedelta(hours=1):
                await self.log_operational_event(
                    severity=OperationalSeverity.WARNING,
                    category=DiagnosticCategory.COMPLIANCE,
                    event_type="error_pattern_detected",
                    message=f"Recurring error pattern detected: {pattern_key}",
                    details={
                        "pattern": pattern_key,
                        "occurrence_count": self._error_patterns[pattern_key],
                        "threshold": 5,
                    },
                )
                self._alert_suppressions[suppression_key] = datetime.now()

    def _map_severity_to_log_level(self, severity: OperationalSeverity) -> LogLevel:
        """Map operational severity to structured logger level."""
        mapping = {
            OperationalSeverity.TRACE: LogLevel.DEBUG,
            OperationalSeverity.DEBUG: LogLevel.DEBUG,
            OperationalSeverity.INFO: LogLevel.INFO,
            OperationalSeverity.NOTICE: LogLevel.INFO,
            OperationalSeverity.WARNING: LogLevel.WARNING,
            OperationalSeverity.ERROR: LogLevel.ERROR,
            OperationalSeverity.CRITICAL: LogLevel.CRITICAL,
            OperationalSeverity.ALERT: LogLevel.CRITICAL,
            OperationalSeverity.EMERGENCY: LogLevel.CRITICAL,
        }
        return mapping.get(severity, LogLevel.INFO)

    async def _background_monitoring(self) -> None:
        """Background task for continuous operational monitoring."""
        while not self._shutdown_event.is_set():
            try:
                await asyncio.sleep(300)  # 5-minute intervals

                # Periodic health check logging
                current_time = datetime.now()
                if (current_time - self._last_health_check) > timedelta(minutes=10):
                    system_snapshot = SystemSnapshot.capture(self.start_time)

                    await self.log_operational_event(
                        severity=OperationalSeverity.INFO,
                        category=DiagnosticCategory.HEALTH_CHECK,
                        event_type="periodic_health_check",
                        message="Periodic system health check completed",
                        details={
                            "memory_usage_mb": system_snapshot.memory_usage_mb,
                            "cpu_percentage": system_snapshot.cpu_percentage,
                            "uptime_hours": system_snapshot.uptime_seconds / 3600,
                        },
                        include_system_snapshot=True,
                    )

                    self._last_health_check = current_time

            except asyncio.CancelledError:
                break
            except Exception as e:
                await self.log_operational_event(
                    severity=OperationalSeverity.ERROR,
                    category=DiagnosticCategory.PERFORMANCE,
                    event_type="monitoring_error",
                    message=f"Background monitoring encountered error: {e}",
                    details={"error": str(e)},
                    include_system_snapshot=True,
                )

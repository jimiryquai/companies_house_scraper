"""Connection Status Reporter

Provides detailed connection status reporting for the streaming client,
including connection history, performance analysis, and alerting capabilities.
"""

import asyncio
import json
import logging
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

logger = logging.getLogger(__name__)


@dataclass
class ConnectionEvent:
    """Represents a connection-related event."""

    timestamp: datetime
    event_type: str  # 'connect', 'disconnect', 'reconnect', 'failure'
    details: Dict[str, Any]
    duration_seconds: Optional[float] = None
    error_message: Optional[str] = None


@dataclass
class ConnectionReport:
    """Comprehensive connection status report."""

    report_timestamp: datetime
    report_period_hours: float
    connection_summary: Dict[str, Any]
    performance_metrics: Dict[str, Any]
    reliability_metrics: Dict[str, Any]
    recent_events: List[ConnectionEvent]
    issues_detected: List[str]
    recommendations: List[str]


class ConnectionStatusReporter:
    """Provides comprehensive connection status reporting and analysis.

    Tracks connection events, analyzes patterns, generates reports,
    and provides alerting for connection issues.
    """

    def __init__(self, health_monitor: Any, report_directory: Optional[str] = None):
        """Initialize connection status reporter.

        Args:
            health_monitor: HealthMonitor instance to get metrics from
            report_directory: Optional directory to save reports
        """
        self.health_monitor = health_monitor
        self.report_directory = Path(report_directory) if report_directory else None

        # Event tracking
        self.connection_events: List[ConnectionEvent] = []
        self.max_events_history = 1000

        # Performance tracking
        self.connection_times: List[float] = []
        self.disconnect_reasons: Dict[str, int] = {}

        # Reporting configuration
        self.reporting_enabled = True
        self.auto_save_reports = bool(report_directory)

        # Alert thresholds
        self.alert_thresholds = {
            "max_connection_failures_per_hour": 5,
            "max_avg_connection_time_seconds": 30.0,
            "min_connection_success_rate": 0.95,
            "max_disconnection_frequency_per_hour": 10,
        }

        # Alert callbacks
        self.alert_callbacks: List[Callable[..., None]] = []

        if self.report_directory:
            self.report_directory.mkdir(parents=True, exist_ok=True)
            logger.info(
                f"Connection status reporter initialized with report directory: {self.report_directory}"
            )

    def register_alert_callback(self, callback: Callable[..., None]) -> None:
        """Register callback for connection alerts."""
        self.alert_callbacks.append(callback)
        logger.info("Registered connection alert callback")

    def record_connection_event(
        self,
        event_type: str,
        details: Optional[Dict[str, Any]] = None,
        error_message: Optional[str] = None,
        duration: Optional[float] = None,
    ) -> None:
        """Record a connection-related event.

        Args:
            event_type: Type of event ('connect', 'disconnect', 'failure', etc.)
            details: Additional event details
            error_message: Error message if applicable
            duration: Duration in seconds if applicable
        """
        event = ConnectionEvent(
            timestamp=datetime.now(),
            event_type=event_type,
            details=details or {},
            duration_seconds=duration,
            error_message=error_message,
        )

        self.connection_events.append(event)

        # Maintain history size
        if len(self.connection_events) > self.max_events_history:
            self.connection_events = self.connection_events[-self.max_events_history :]

        # Update tracking metrics
        self._update_tracking_metrics(event)

        # Check for alerts
        self._check_alerts(event)

        logger.debug(f"Recorded connection event: {event_type}")

    def _update_tracking_metrics(self, event: ConnectionEvent) -> None:
        """Update internal tracking metrics based on event."""
        if event.event_type == "connect" and event.duration_seconds is not None:
            self.connection_times.append(event.duration_seconds)
            # Keep only recent connection times
            if len(self.connection_times) > 100:
                self.connection_times = self.connection_times[-100:]

        elif event.event_type == "disconnect" and event.error_message:
            reason = event.error_message[:50]  # First 50 chars as key
            self.disconnect_reasons[reason] = self.disconnect_reasons.get(reason, 0) + 1

    def _check_alerts(self, event: ConnectionEvent) -> None:
        """Check if event triggers any alerts."""
        now = datetime.now()

        # Check for high failure rate
        if event.event_type in ["failure", "disconnect"]:
            recent_failures = [
                e
                for e in self.connection_events
                if e.event_type in ["failure", "disconnect"]
                and (now - e.timestamp).total_seconds() < 3600
            ]

            if len(recent_failures) >= self.alert_thresholds["max_connection_failures_per_hour"]:
                self._trigger_alert(
                    "high_failure_rate",
                    f"High connection failure rate: {len(recent_failures)} failures in last hour",
                )

        # Check for frequent disconnections
        recent_disconnects = [
            e
            for e in self.connection_events
            if e.event_type == "disconnect" and (now - e.timestamp).total_seconds() < 3600
        ]

        if len(recent_disconnects) >= self.alert_thresholds["max_disconnection_frequency_per_hour"]:
            self._trigger_alert(
                "frequent_disconnections",
                f"Frequent disconnections: {len(recent_disconnects)} in last hour",
            )

        # Check connection success rate
        recent_connect_attempts = [
            e
            for e in self.connection_events
            if e.event_type in ["connect", "failure"] and (now - e.timestamp).total_seconds() < 3600
        ]

        if len(recent_connect_attempts) >= 5:  # Minimum sample size
            successes = len([e for e in recent_connect_attempts if e.event_type == "connect"])
            success_rate = successes / len(recent_connect_attempts)

            if success_rate < self.alert_thresholds["min_connection_success_rate"]:
                self._trigger_alert(
                    "low_success_rate", f"Low connection success rate: {success_rate:.1%}"
                )

    def _trigger_alert(self, alert_type: str, message: str) -> None:
        """Trigger an alert through registered callbacks."""
        alert_data = {
            "type": alert_type,
            "message": message,
            "timestamp": datetime.now().isoformat(),
            "client_status": self.health_monitor.get_health_summary(),
        }

        logger.warning(f"Connection alert triggered: {message}")

        for callback in self.alert_callbacks:
            try:
                if asyncio.iscoroutinefunction(callback):
                    # Schedule coroutine for async callbacks
                    asyncio.create_task(callback(alert_data))
                else:
                    callback(alert_data)
            except Exception as e:
                logger.error(f"Error in alert callback: {e}")

    def get_connection_summary(self, hours_back: float = 24) -> Dict[str, Any]:
        """Get connection summary for the specified time period.

        Args:
            hours_back: Number of hours to look back

        Returns:
            Connection summary with metrics and statistics
        """
        cutoff_time = datetime.now() - timedelta(hours=hours_back)
        recent_events = [e for e in self.connection_events if e.timestamp > cutoff_time]

        # Count events by type
        event_counts: Dict[str, int] = {}
        for event in recent_events:
            event_counts[event.event_type] = event_counts.get(event.event_type, 0) + 1

        # Calculate connection success rate
        connect_attempts = event_counts.get("connect", 0) + event_counts.get("failure", 0)
        success_rate = (
            (event_counts.get("connect", 0) / connect_attempts) if connect_attempts > 0 else 0
        )

        # Calculate average connection time
        recent_connection_times = [
            e.duration_seconds
            for e in recent_events
            if e.event_type == "connect" and e.duration_seconds is not None
        ]
        avg_connection_time = (
            sum(recent_connection_times) / len(recent_connection_times)
            if recent_connection_times
            else 0
        )

        # Calculate uptime
        uptime_seconds: float = 0
        current_connection_start = None

        for event in sorted(recent_events, key=lambda x: x.timestamp):
            if event.event_type == "connect":
                current_connection_start = event.timestamp
            elif event.event_type == "disconnect" and current_connection_start:
                uptime_seconds = (
                    uptime_seconds + (event.timestamp - current_connection_start).total_seconds()
                )
                current_connection_start = None

        # Add current connection time if still connected
        if current_connection_start and self.health_monitor.client.is_connected:
            uptime_seconds = (
                uptime_seconds + (datetime.now() - current_connection_start).total_seconds()
            )

        total_period_seconds = hours_back * 3600
        uptime_percentage = (
            (uptime_seconds / total_period_seconds) * 100 if total_period_seconds > 0 else 0
        )

        return {
            "period_hours": hours_back,
            "total_events": len(recent_events),
            "event_counts": event_counts,
            "connection_attempts": connect_attempts,
            "success_rate": success_rate,
            "average_connection_time_seconds": avg_connection_time,
            "uptime_seconds": uptime_seconds,
            "uptime_percentage": uptime_percentage,
            "current_status": {
                "connected": self.health_monitor.client.is_connected,
                "health_status": self.health_monitor.current_status.value,
                "last_heartbeat": self.health_monitor.client.last_heartbeat.isoformat()
                if self.health_monitor.client.last_heartbeat
                else None,
            },
        }

    def get_performance_analysis(self, hours_back: float = 24) -> Dict[str, Any]:
        """Analyze connection performance over the specified period.

        Args:
            hours_back: Number of hours to analyze

        Returns:
            Performance analysis with trends and patterns
        """
        cutoff_time = datetime.now() - timedelta(hours=hours_back)
        recent_events = [e for e in self.connection_events if e.timestamp > cutoff_time]

        # Connection time analysis
        connection_times = [
            e.duration_seconds
            for e in recent_events
            if e.event_type == "connect" and e.duration_seconds is not None
        ]

        connection_stats = {}
        if connection_times:
            connection_stats = {
                "count": len(connection_times),
                "average": sum(connection_times) / len(connection_times),
                "min": min(connection_times),
                "max": max(connection_times),
                "median": sorted(connection_times)[len(connection_times) // 2],
            }

        # Failure pattern analysis
        failures = [e for e in recent_events if e.event_type in ["failure", "disconnect"]]
        failure_patterns: Dict[int, int] = {}

        for failure in failures:
            hour = failure.timestamp.hour
            failure_patterns[hour] = failure_patterns.get(hour, 0) + 1

        # Disconnect reason analysis
        disconnect_analysis = dict(self.disconnect_reasons)

        return {
            "analysis_period_hours": hours_back,
            "connection_time_stats": connection_stats,
            "failure_patterns_by_hour": failure_patterns,
            "disconnect_reasons": disconnect_analysis,
            "performance_grade": self._calculate_performance_grade(connection_stats, len(failures)),
        }

    def _calculate_performance_grade(
        self, connection_stats: Dict[str, Any], failure_count: int
    ) -> str:
        """Calculate overall performance grade (A-F)."""
        score = 100

        if connection_stats:
            # Deduct points for slow connections
            avg_time = connection_stats["average"]
            if avg_time > 30:
                score -= 30
            elif avg_time > 10:
                score -= 15
            elif avg_time > 5:
                score -= 5

        # Deduct points for failures
        score -= min(failure_count * 5, 50)  # Max 50 points deduction for failures

        if score >= 95:
            return "A"
        if score >= 85:
            return "B"
        if score >= 75:
            return "C"
        if score >= 65:
            return "D"
        return "F"

    def generate_comprehensive_report(self, hours_back: float = 24) -> ConnectionReport:
        """Generate comprehensive connection status report.

        Args:
            hours_back: Number of hours to include in the report

        Returns:
            Complete connection report
        """
        cutoff_time = datetime.now() - timedelta(hours=hours_back)
        recent_events = [e for e in self.connection_events if e.timestamp > cutoff_time]

        # Generate report sections
        connection_summary = self.get_connection_summary(hours_back)
        performance_metrics = self.get_performance_analysis(hours_back)

        # Calculate reliability metrics
        reliability_metrics = {
            "mtbf_hours": self._calculate_mtbf(recent_events),
            "mttr_seconds": self._calculate_mttr(recent_events),
            "availability_percentage": connection_summary["uptime_percentage"],
        }

        # Detect issues
        issues = self._detect_issues(connection_summary, performance_metrics)

        # Generate recommendations
        recommendations = self._generate_recommendations(issues, performance_metrics)

        return ConnectionReport(
            report_timestamp=datetime.now(),
            report_period_hours=hours_back,
            connection_summary=connection_summary,
            performance_metrics=performance_metrics,
            reliability_metrics=reliability_metrics,
            recent_events=recent_events[-20:],  # Last 20 events
            issues_detected=issues,
            recommendations=recommendations,
        )

    def _calculate_mtbf(self, events: List[ConnectionEvent]) -> float:
        """Calculate Mean Time Between Failures."""
        failures = [e for e in events if e.event_type in ["failure", "disconnect"]]
        if len(failures) < 2:
            return float("inf")

        failure_times = [e.timestamp for e in failures]
        failure_times.sort()

        intervals = []
        for i in range(1, len(failure_times)):
            interval = (failure_times[i] - failure_times[i - 1]).total_seconds() / 3600
            intervals.append(interval)

        return sum(intervals) / len(intervals) if intervals else float("inf")

    def _calculate_mttr(self, events: List[ConnectionEvent]) -> float:
        """Calculate Mean Time To Recovery."""
        recovery_times = []
        failure_time = None

        for event in sorted(events, key=lambda x: x.timestamp):
            if event.event_type in ["failure", "disconnect"]:
                failure_time = event.timestamp
            elif event.event_type == "connect" and failure_time:
                recovery_time = (event.timestamp - failure_time).total_seconds()
                recovery_times.append(recovery_time)
                failure_time = None

        return sum(recovery_times) / len(recovery_times) if recovery_times else 0

    def _detect_issues(self, summary: Dict[str, Any], performance: Dict[str, Any]) -> List[str]:
        """Detect issues based on metrics."""
        issues = []

        if summary["success_rate"] < 0.95:
            issues.append(f"Low connection success rate: {summary['success_rate']:.1%}")

        if summary["uptime_percentage"] < 95:
            issues.append(f"Low uptime: {summary['uptime_percentage']:.1f}%")

        if performance["connection_time_stats"]:
            avg_time = performance["connection_time_stats"]["average"]
            if avg_time > 30:
                issues.append(f"Slow connection times: {avg_time:.1f}s average")

        if performance["performance_grade"] in ["D", "F"]:
            issues.append(f"Poor performance grade: {performance['performance_grade']}")

        return issues

    def _generate_recommendations(
        self, issues: List[str], performance: Dict[str, Any]
    ) -> List[str]:
        """Generate recommendations based on detected issues."""
        recommendations = []

        for issue in issues:
            if "success rate" in issue.lower():
                recommendations.append("Check network connectivity and API endpoint availability")
                recommendations.append("Review authentication credentials and permissions")

            elif "uptime" in issue.lower():
                recommendations.append("Implement more aggressive reconnection strategy")
                recommendations.append("Add redundant connection paths if available")

            elif "connection times" in issue.lower():
                recommendations.append("Investigate network latency and bandwidth")
                recommendations.append("Consider connection pooling or keep-alive optimization")

            elif "performance grade" in issue.lower():
                recommendations.append("Review overall system health and resource usage")
                recommendations.append("Consider upgrading infrastructure or connection parameters")

        if not issues:
            recommendations.append(
                "Connection performance is good - maintain current configuration"
            )

        return list(set(recommendations))  # Remove duplicates

    async def save_report(
        self, report: ConnectionReport, filename: Optional[str] = None
    ) -> Optional[Path]:
        """Save connection report to file.

        Args:
            report: Report to save
            filename: Optional filename (auto-generated if not provided)

        Returns:
            Path to saved file or None if saving disabled
        """
        if not self.auto_save_reports or not self.report_directory:
            return None

        if not filename:
            timestamp = report.report_timestamp.strftime("%Y%m%d_%H%M%S")
            filename = f"connection_report_{timestamp}.json"

        file_path = self.report_directory / filename

        try:
            # Convert report to dict for JSON serialization
            report_dict = asdict(report)

            # Convert datetime objects to ISO format
            def convert_datetimes(obj: Any) -> Any:
                if isinstance(obj, dict):
                    return {k: convert_datetimes(v) for k, v in obj.items()}
                if isinstance(obj, list):
                    return [convert_datetimes(item) for item in obj]
                if isinstance(obj, datetime):
                    return obj.isoformat()
                return obj

            report_dict = convert_datetimes(report_dict)

            with open(file_path, "w") as f:
                json.dump(report_dict, f, indent=2)

            logger.info(f"Connection report saved to: {file_path}")
            return file_path

        except Exception as e:
            logger.error(f"Failed to save connection report: {e}")
            return None

    def get_recent_events(self, count: int = 10) -> List[Dict[str, Any]]:
        """Get recent connection events."""
        recent = self.connection_events[-count:] if self.connection_events else []
        return [
            {
                "timestamp": event.timestamp.isoformat(),
                "type": event.event_type,
                "details": event.details,
                "duration_seconds": event.duration_seconds,
                "error_message": event.error_message,
            }
            for event in recent
        ]

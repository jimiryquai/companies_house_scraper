"""Rate limit violation monitoring and alerting system.

This module provides comprehensive monitoring and alerting for rate limit violations
to ensure early detection and response in cloud deployment environments.
"""

import json
import logging
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

logger = logging.getLogger(__name__)


class AlertLevel(Enum):
    """Alert severity levels."""

    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"
    EMERGENCY = "emergency"


@dataclass
class RateLimitAlert:
    """Rate limit violation alert."""

    alert_id: str
    level: AlertLevel
    title: str
    message: str
    timestamp: datetime
    context: Dict[str, Any] = field(default_factory=dict)
    resolved: bool = False
    resolved_at: Optional[datetime] = None


class RateLimitMonitor:
    """Advanced rate limit violation monitoring and alerting system.

    This monitor provides:
    - Real-time tracking of rate limit violations
    - Sliding window analysis for trend detection
    - Configurable alerting thresholds
    - Alert suppression to prevent spam
    - Cloud deployment ready logging
    """

    def __init__(
        self,
        alert_callback: Optional[Callable[[RateLimitAlert], None]] = None,
        window_size_minutes: int = 15,
        warning_threshold: int = 5,
        critical_threshold: int = 10,
        emergency_threshold: int = 20,
    ) -> None:
        """Initialize the rate limit monitor.

        Args:
            alert_callback: Function to call when alerts are triggered
            window_size_minutes: Size of sliding window for analysis
            warning_threshold: 429s in window to trigger WARNING
            critical_threshold: 429s in window to trigger CRITICAL
            emergency_threshold: 429s in window to trigger EMERGENCY
        """
        self.alert_callback = alert_callback or self._default_alert_handler
        self.window_size = timedelta(minutes=window_size_minutes)
        self.warning_threshold = warning_threshold
        self.critical_threshold = critical_threshold
        self.emergency_threshold = emergency_threshold

        # Sliding window for 429 tracking
        self.violation_window: deque = deque()

        # Alert tracking
        self.active_alerts: Dict[str, RateLimitAlert] = {}
        self.alert_history: List[RateLimitAlert] = []
        self.alert_suppression: Dict[str, datetime] = {}

        # Metrics
        self.total_violations = 0
        self.alerts_sent = 0
        self.last_violation_time: Optional[datetime] = None

        logger.info(
            "RateLimitMonitor initialized: window=%dm, thresholds=W:%d/C:%d/E:%d",
            window_size_minutes,
            warning_threshold,
            critical_threshold,
            emergency_threshold,
        )

    def _default_alert_handler(self, alert: RateLimitAlert) -> None:
        """Default alert handler that logs to console.

        Args:
            alert: Alert to handle
        """
        log_level = {
            AlertLevel.INFO: logging.INFO,
            AlertLevel.WARNING: logging.WARNING,
            AlertLevel.CRITICAL: logging.ERROR,
            AlertLevel.EMERGENCY: logging.CRITICAL,
        }[alert.level]

        logger.log(
            log_level,
            "🚨 RATE LIMIT ALERT [%s] %s: %s (context: %s)",
            alert.level.value.upper(),
            alert.title,
            alert.message,
            json.dumps(alert.context, default=str),
        )

    def _clean_violation_window(self) -> None:
        """Remove old violations from sliding window."""
        cutoff_time = datetime.now() - self.window_size
        while self.violation_window and self.violation_window[0] < cutoff_time:
            self.violation_window.popleft()

    def _get_current_violation_count(self) -> int:
        """Get current violation count in sliding window."""
        self._clean_violation_window()
        return len(self.violation_window)

    def _should_suppress_alert(self, alert_type: str, min_interval_minutes: int = 5) -> bool:
        """Check if alert should be suppressed to prevent spam.

        Args:
            alert_type: Type of alert to check
            min_interval_minutes: Minimum interval between alerts of same type

        Returns:
            True if alert should be suppressed
        """
        if alert_type not in self.alert_suppression:
            return False

        last_alert_time = self.alert_suppression[alert_type]
        min_interval = timedelta(minutes=min_interval_minutes)

        return datetime.now() - last_alert_time < min_interval

    def _create_alert(
        self, level: AlertLevel, title: str, message: str, context: Optional[Dict[str, Any]] = None
    ) -> RateLimitAlert:
        """Create a new alert.

        Args:
            level: Alert severity level
            title: Alert title
            message: Alert message
            context: Additional context information

        Returns:
            Created alert
        """
        alert_id = f"{level.value}_{int(time.time())}"
        alert = RateLimitAlert(
            alert_id=alert_id,
            level=level,
            title=title,
            message=message,
            timestamp=datetime.now(),
            context=context or {},
        )

        # Track alert
        self.active_alerts[alert_id] = alert
        self.alert_history.append(alert)
        self.alert_suppression[f"{level.value}_threshold"] = datetime.now()
        self.alerts_sent += 1

        return alert

    def record_violation(
        self,
        company_number: str,
        request_id: str,
        endpoint: str,
        context: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Record a rate limit violation (429 response).

        Args:
            company_number: Company that received 429
            request_id: Request that received 429
            endpoint: API endpoint that returned 429
            context: Additional context information
        """
        violation_time = datetime.now()
        self.violation_window.append(violation_time)
        self.total_violations += 1
        self.last_violation_time = violation_time

        # Get current violation count in window
        current_count = self._get_current_violation_count()

        # Create alert context
        alert_context = {
            "company_number": company_number,
            "request_id": request_id,
            "endpoint": endpoint,
            "violations_in_window": current_count,
            "window_minutes": self.window_size.total_seconds() / 60,
            "total_violations": self.total_violations,
            **(context or {}),
        }

        logger.debug(
            "Rate limit violation recorded: company=%s, endpoint=%s, window_count=%d/%d, total=%d",
            company_number,
            endpoint,
            current_count,
            self.emergency_threshold,
            self.total_violations,
        )

        # Check thresholds and create alerts
        if current_count >= self.emergency_threshold:
            if not self._should_suppress_alert("emergency_threshold", min_interval_minutes=10):
                alert = self._create_alert(
                    AlertLevel.EMERGENCY,
                    "EMERGENCY: Excessive Rate Limit Violations",
                    f"{current_count} rate limit violations in {self.window_size.total_seconds() / 60:.0f} minutes "
                    f"(threshold: {self.emergency_threshold}). System may be banned!",
                    alert_context,
                )
                self.alert_callback(alert)

        elif current_count >= self.critical_threshold:
            if not self._should_suppress_alert("critical_threshold", min_interval_minutes=5):
                alert = self._create_alert(
                    AlertLevel.CRITICAL,
                    "CRITICAL: High Rate Limit Violations",
                    f"{current_count} rate limit violations in {self.window_size.total_seconds() / 60:.0f} minutes "
                    f"(threshold: {self.critical_threshold}). Immediate action required.",
                    alert_context,
                )
                self.alert_callback(alert)

        elif current_count >= self.warning_threshold:
            if not self._should_suppress_alert("warning_threshold", min_interval_minutes=2):
                alert = self._create_alert(
                    AlertLevel.WARNING,
                    "WARNING: Elevated Rate Limit Violations",
                    f"{current_count} rate limit violations in {self.window_size.total_seconds() / 60:.0f} minutes "
                    f"(threshold: {self.warning_threshold}). Monitor closely.",
                    alert_context,
                )
                self.alert_callback(alert)

    def record_successful_request(self, company_number: str, endpoint: str) -> None:
        """Record a successful request (helps with recovery detection).

        Args:
            company_number: Company that made successful request
            endpoint: API endpoint that succeeded
        """
        # Check if we can resolve any active alerts due to successful requests
        current_count = self._get_current_violation_count()

        # If violation count has dropped significantly, create recovery alert
        if (
            current_count < self.warning_threshold
            and len(self.active_alerts) > 0
            and self.last_violation_time
            and datetime.now() - self.last_violation_time > timedelta(minutes=5)
        ):
            # Resolve active alerts
            resolved_count = 0
            for alert_id, alert in list(self.active_alerts.items()):
                if not alert.resolved:
                    alert.resolved = True
                    alert.resolved_at = datetime.now()
                    resolved_count += 1

            # Clear active alerts
            self.active_alerts.clear()

            if resolved_count > 0:
                recovery_alert = self._create_alert(
                    AlertLevel.INFO,
                    "RECOVERY: Rate Limit Violations Resolved",
                    f"Rate limit situation has improved. {resolved_count} alerts resolved. "
                    f"Current violations in window: {current_count}",
                    {
                        "company_number": company_number,
                        "endpoint": endpoint,
                        "resolved_alerts": resolved_count,
                        "current_violations": current_count,
                    },
                )
                self.alert_callback(recovery_alert)

    def get_violation_summary(self) -> Dict[str, Any]:
        """Get comprehensive violation summary.

        Returns:
            Dictionary with violation statistics
        """
        current_count = self._get_current_violation_count()

        return {
            "current_window": {
                "violations": current_count,
                "window_size_minutes": self.window_size.total_seconds() / 60,
                "rate_per_minute": current_count / (self.window_size.total_seconds() / 60)
                if current_count > 0
                else 0,
            },
            "thresholds": {
                "warning": self.warning_threshold,
                "critical": self.critical_threshold,
                "emergency": self.emergency_threshold,
            },
            "status": {
                "level": (
                    "EMERGENCY"
                    if current_count >= self.emergency_threshold
                    else "CRITICAL"
                    if current_count >= self.critical_threshold
                    else "WARNING"
                    if current_count >= self.warning_threshold
                    else "NORMAL"
                ),
                "active_alerts": len(self.active_alerts),
                "last_violation": self.last_violation_time.isoformat()
                if self.last_violation_time
                else None,
            },
            "totals": {
                "total_violations": self.total_violations,
                "alerts_sent": self.alerts_sent,
                "alerts_in_history": len(self.alert_history),
            },
            "timestamp": datetime.now().isoformat(),
        }

    def get_recent_alerts(self, hours: int = 24) -> List[Dict[str, Any]]:
        """Get recent alerts within specified time period.

        Args:
            hours: Number of hours to look back

        Returns:
            List of recent alert dictionaries
        """
        cutoff_time = datetime.now() - timedelta(hours=hours)

        recent_alerts = [
            {
                "alert_id": alert.alert_id,
                "level": alert.level.value,
                "title": alert.title,
                "message": alert.message,
                "timestamp": alert.timestamp.isoformat(),
                "resolved": alert.resolved,
                "resolved_at": alert.resolved_at.isoformat() if alert.resolved_at else None,
                "context": alert.context,
            }
            for alert in self.alert_history
            if alert.timestamp >= cutoff_time
        ]

        return sorted(recent_alerts, key=lambda x: x["timestamp"], reverse=True)

    def reset_monitoring(self) -> None:
        """Reset monitoring state (useful for testing or after maintenance)."""
        self.violation_window.clear()
        self.active_alerts.clear()
        self.alert_suppression.clear()
        self.last_violation_time = None

        logger.info("Rate limit monitoring state reset")

    def get_metrics(self) -> Dict[str, Any]:
        """Get comprehensive monitoring metrics.

        Returns:
            Dictionary with all monitoring metrics
        """
        return {
            "rate_limit_monitor": {
                "summary": self.get_violation_summary(),
                "configuration": {
                    "window_size_minutes": self.window_size.total_seconds() / 60,
                    "warning_threshold": self.warning_threshold,
                    "critical_threshold": self.critical_threshold,
                    "emergency_threshold": self.emergency_threshold,
                },
                "recent_alerts": self.get_recent_alerts(hours=1),  # Last hour
            }
        }

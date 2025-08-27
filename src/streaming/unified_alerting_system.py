"""Unified alerting system for rate limit violations and processing failures.

This module consolidates all alerting capabilities from various components
into a unified system for comprehensive monitoring and notification of
rate limit violations, processing failures, and operational issues.
"""

import asyncio
import json
import logging
import smtplib
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from enum import Enum
from typing import Any, Dict, List, Optional, Set

import aiohttp

from .comprehensive_metrics_collector import ComprehensiveMetricsCollector
from .rate_limit_monitor import RateLimitMonitor

logger = logging.getLogger(__name__)


class AlertSeverity(Enum):
    """Alert severity levels."""

    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"
    EMERGENCY = "emergency"


class AlertCategory(Enum):
    """Alert categories."""

    RATE_LIMITS = "rate_limits"
    PROCESSING_FAILURES = "processing_failures"
    QUEUE_CAPACITY = "queue_capacity"
    API_COMPLIANCE = "api_compliance"
    SYSTEM_HEALTH = "system_health"
    RECOVERY_STATUS = "recovery_status"


@dataclass
class UnifiedAlert:
    """Unified alert structure."""

    alert_id: str
    timestamp: datetime
    severity: AlertSeverity
    category: AlertCategory
    title: str
    message: str
    context: Dict[str, Any] = field(default_factory=dict)
    source_component: str = ""
    resolved: bool = False
    resolved_at: Optional[datetime] = None
    acknowledgment_required: bool = False
    acknowledged: bool = False
    acknowledged_by: str = ""
    acknowledged_at: Optional[datetime] = None
    escalation_level: int = 0
    escalation_history: List[datetime] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        """Convert alert to dictionary."""
        return {
            "alert_id": self.alert_id,
            "timestamp": self.timestamp.isoformat(),
            "severity": self.severity.value,
            "category": self.category.value,
            "title": self.title,
            "message": self.message,
            "context": self.context,
            "source_component": self.source_component,
            "resolved": self.resolved,
            "resolved_at": self.resolved_at.isoformat() if self.resolved_at else None,
            "acknowledgment_required": self.acknowledgment_required,
            "acknowledged": self.acknowledged,
            "acknowledged_by": self.acknowledged_by,
            "acknowledged_at": self.acknowledged_at.isoformat() if self.acknowledged_at else None,
            "escalation_level": self.escalation_level,
        }


@dataclass
class AlertingConfiguration:
    """Configuration for the unified alerting system."""

    # Email settings
    email_enabled: bool = False
    smtp_server: str = ""
    smtp_port: int = 587
    smtp_username: str = ""
    smtp_password: str = ""
    email_recipients: List[str] = field(default_factory=list)

    # Webhook settings
    webhook_enabled: bool = False
    webhook_urls: List[str] = field(default_factory=list)
    webhook_timeout_seconds: int = 30

    # Slack settings
    slack_enabled: bool = False
    slack_webhook_url: str = ""
    slack_channel: str = "#alerts"

    # PagerDuty settings
    pagerduty_enabled: bool = False
    pagerduty_integration_key: str = ""

    # Alert thresholds
    rate_limit_warning_threshold: int = 5
    rate_limit_critical_threshold: int = 15
    rate_limit_emergency_threshold: int = 30

    processing_failure_threshold: int = 10
    queue_capacity_warning_threshold: float = 0.8
    queue_capacity_critical_threshold: float = 0.95

    # Escalation settings
    escalation_enabled: bool = True
    escalation_interval_minutes: int = 30
    max_escalation_level: int = 3


class UnifiedAlertingSystem:
    """Unified alerting system for comprehensive monitoring and notifications.

    This system consolidates alerting from all components and provides:
    - Unified alert format and processing
    - Multiple notification channels (email, webhook, Slack, PagerDuty)
    - Alert escalation and acknowledgment
    - Alert suppression and deduplication
    - Integration with existing monitoring components
    """

    def __init__(
        self,
        config: AlertingConfiguration,
        rate_limit_monitor: RateLimitMonitor,
        metrics_collector: ComprehensiveMetricsCollector,
    ) -> None:
        """Initialize the unified alerting system.

        Args:
            config: Alerting configuration
            rate_limit_monitor: Rate limit monitor instance
            metrics_collector: Metrics collector instance
        """
        self.config = config
        self.rate_limit_monitor = rate_limit_monitor
        self.metrics_collector = metrics_collector

        # Alert storage
        self.active_alerts: Dict[str, UnifiedAlert] = {}
        self.alert_history: List[UnifiedAlert] = []

        # Alert suppression
        self.suppressed_alerts: Set[str] = set()
        self.last_alert_times: Dict[str, datetime] = {}

        # Escalation tracking
        self.escalation_tasks: Dict[str, asyncio.Task] = {}

        # Notification handlers
        self.notification_handlers = {
            "email": self._send_email_notification,
            "webhook": self._send_webhook_notification,
            "slack": self._send_slack_notification,
            "pagerduty": self._send_pagerduty_notification,
        }

        # Setup rate limit monitor callback
        self.rate_limit_monitor.alert_callback = self._handle_rate_limit_alert

        logger.info("UnifiedAlertingSystem initialized")

    async def create_alert(
        self,
        severity: AlertSeverity,
        category: AlertCategory,
        title: str,
        message: str,
        context: Optional[Dict[str, Any]] = None,
        source_component: str = "",
        acknowledgment_required: bool = None,
    ) -> UnifiedAlert:
        """Create and process a new alert.

        Args:
            severity: Alert severity level
            category: Alert category
            title: Alert title
            message: Alert message
            context: Additional context information
            source_component: Component that generated the alert
            acknowledgment_required: Whether acknowledgment is required

        Returns:
            Created alert
        """
        # Generate alert ID
        alert_id = f"{category.value}_{severity.value}_{int(time.time())}"

        # Determine if acknowledgment is required
        if acknowledgment_required is None:
            acknowledgment_required = severity in [AlertSeverity.CRITICAL, AlertSeverity.EMERGENCY]

        # Create alert
        alert = UnifiedAlert(
            alert_id=alert_id,
            timestamp=datetime.now(),
            severity=severity,
            category=category,
            title=title,
            message=message,
            context=context or {},
            source_component=source_component,
            acknowledgment_required=acknowledgment_required,
        )

        # Check for alert suppression
        if self._should_suppress_alert(alert):
            logger.debug("Alert suppressed: %s", alert_id)
            return alert

        # Store alert
        self.active_alerts[alert_id] = alert
        self.alert_history.append(alert)

        # Send notifications
        await self._send_notifications(alert)

        # Start escalation if required
        if self.config.escalation_enabled and acknowledgment_required:
            await self._start_escalation(alert)

        logger.info("Alert created: %s - %s", alert_id, title)
        return alert

    def _handle_rate_limit_alert(self, rate_limit_alert_data: Dict[str, Any]) -> None:
        """Handle rate limit alerts from the rate limit monitor."""
        # Convert rate limit alert to unified alert
        severity_mapping = {
            "info": AlertSeverity.INFO,
            "warning": AlertSeverity.WARNING,
            "critical": AlertSeverity.CRITICAL,
            "emergency": AlertSeverity.EMERGENCY,
        }

        severity = severity_mapping.get(
            rate_limit_alert_data.get("level", "info"), AlertSeverity.INFO
        )

        # Create unified alert
        asyncio.create_task(
            self.create_alert(
                severity=severity,
                category=AlertCategory.RATE_LIMITS,
                title=f"Rate Limit {rate_limit_alert_data.get('level', 'Unknown').upper()}",
                message=f"API usage: {rate_limit_alert_data.get('current_calls', 0)} calls "
                f"({rate_limit_alert_data.get('usage_percentage', 0):.1f}% of limit)",
                context=rate_limit_alert_data,
                source_component="rate_limit_monitor",
            )
        )

    async def check_processing_failures(self) -> None:
        """Check for processing failures and create alerts if needed."""
        try:
            metrics = self.metrics_collector.get_comprehensive_metrics()

            # Check queue processing failures
            queue_metrics = metrics["queue_metrics"]
            processing_metrics = metrics["processing_metrics"]

            # Check processing success rate
            success_rate = processing_metrics["efficiency"]["success_rate"]
            if success_rate < 0.9:  # Less than 90% success rate
                await self.create_alert(
                    severity=AlertSeverity.WARNING if success_rate > 0.8 else AlertSeverity.ERROR,
                    category=AlertCategory.PROCESSING_FAILURES,
                    title="Low Processing Success Rate",
                    message=f"Processing success rate: {success_rate:.1%}",
                    context={"success_rate": success_rate, "queue_metrics": queue_metrics},
                    source_component="metrics_collector",
                )

            # Check queue utilization
            utilization = queue_metrics["utilization"]["percentage"]
            if utilization >= self.config.queue_capacity_critical_threshold * 100:
                await self.create_alert(
                    severity=AlertSeverity.CRITICAL,
                    category=AlertCategory.QUEUE_CAPACITY,
                    title="Queue Capacity Critical",
                    message=f"Queue utilization: {utilization:.1f}%",
                    context={"utilization": utilization, "queue_metrics": queue_metrics},
                    source_component="metrics_collector",
                )
            elif utilization >= self.config.queue_capacity_warning_threshold * 100:
                await self.create_alert(
                    severity=AlertSeverity.WARNING,
                    category=AlertCategory.QUEUE_CAPACITY,
                    title="Queue Capacity High",
                    message=f"Queue utilization: {utilization:.1f}%",
                    context={"utilization": utilization},
                    source_component="metrics_collector",
                )

            # Check system health
            health_score = metrics["system_performance"]["health"]["overall_score"]
            if health_score < 0.5:
                await self.create_alert(
                    severity=AlertSeverity.CRITICAL,
                    category=AlertCategory.SYSTEM_HEALTH,
                    title="System Health Critical",
                    message=f"Overall health score: {health_score:.2f}",
                    context={
                        "health_score": health_score,
                        "system_metrics": metrics["system_performance"],
                    },
                    source_component="metrics_collector",
                )
            elif health_score < 0.7:
                await self.create_alert(
                    severity=AlertSeverity.WARNING,
                    category=AlertCategory.SYSTEM_HEALTH,
                    title="System Health Degraded",
                    message=f"Overall health score: {health_score:.2f}",
                    context={"health_score": health_score},
                    source_component="metrics_collector",
                )

        except Exception as e:
            logger.error("Error checking processing failures: %s", e)

    def _should_suppress_alert(self, alert: UnifiedAlert) -> bool:
        """Check if alert should be suppressed."""
        # Create suppression key
        suppression_key = f"{alert.category.value}_{alert.severity.value}"

        # Check if similar alert was sent recently
        if suppression_key in self.last_alert_times:
            last_alert_time = self.last_alert_times[suppression_key]
            if datetime.now() - last_alert_time < timedelta(minutes=5):
                return True  # Suppress if similar alert sent within 5 minutes

        # Update last alert time
        self.last_alert_times[suppression_key] = datetime.now()
        return False

    async def _send_notifications(self, alert: UnifiedAlert) -> None:
        """Send notifications for an alert."""
        notification_tasks = []

        # Send email notification
        if self.config.email_enabled:
            notification_tasks.append(self._send_email_notification(alert))

        # Send webhook notifications
        if self.config.webhook_enabled:
            notification_tasks.append(self._send_webhook_notification(alert))

        # Send Slack notification
        if self.config.slack_enabled:
            notification_tasks.append(self._send_slack_notification(alert))

        # Send PagerDuty notification
        if self.config.pagerduty_enabled and alert.severity in [
            AlertSeverity.CRITICAL,
            AlertSeverity.EMERGENCY,
        ]:
            notification_tasks.append(self._send_pagerduty_notification(alert))

        # Execute all notifications concurrently
        if notification_tasks:
            await asyncio.gather(*notification_tasks, return_exceptions=True)

    async def _send_email_notification(self, alert: UnifiedAlert) -> None:
        """Send email notification."""
        try:
            if not self.config.email_recipients:
                return

            # Create email message
            msg = MIMEMultipart()
            msg["From"] = self.config.smtp_username
            msg["To"] = ", ".join(self.config.email_recipients)
            msg["Subject"] = f"[{alert.severity.value.upper()}] {alert.title}"

            # Email body
            body = f"""
Alert Details:
- Severity: {alert.severity.value.upper()}
- Category: {alert.category.value}
- Timestamp: {alert.timestamp.isoformat()}
- Source: {alert.source_component}

Message:
{alert.message}

Context:
{json.dumps(alert.context, indent=2, default=str)}

Alert ID: {alert.alert_id}
"""

            msg.attach(MIMEText(body, "plain"))

            # Send email
            server = smtplib.SMTP(self.config.smtp_server, self.config.smtp_port)
            server.starttls()
            server.login(self.config.smtp_username, self.config.smtp_password)
            server.sendmail(
                self.config.smtp_username, self.config.email_recipients, msg.as_string()
            )
            server.quit()

            logger.info("Email notification sent for alert: %s", alert.alert_id)

        except Exception as e:
            logger.error("Failed to send email notification: %s", e)

    async def _send_webhook_notification(self, alert: UnifiedAlert) -> None:
        """Send webhook notifications."""
        try:
            if not self.config.webhook_urls:
                return

            payload = {
                "alert": alert.to_dict(),
                "service": "companies-house-streaming",
                "timestamp": datetime.now().isoformat(),
            }

            async with aiohttp.ClientSession() as session:
                for webhook_url in self.config.webhook_urls:
                    try:
                        async with session.post(
                            webhook_url,
                            json=payload,
                            timeout=aiohttp.ClientTimeout(
                                total=self.config.webhook_timeout_seconds
                            ),
                        ) as response:
                            if response.status == 200:
                                logger.debug("Webhook notification sent to: %s", webhook_url)
                            else:
                                logger.warning(
                                    "Webhook notification failed: %s - %s",
                                    webhook_url,
                                    response.status,
                                )
                    except Exception as e:
                        logger.error("Webhook notification error for %s: %s", webhook_url, e)

        except Exception as e:
            logger.error("Failed to send webhook notifications: %s", e)

    async def _send_slack_notification(self, alert: UnifiedAlert) -> None:
        """Send Slack notification."""
        try:
            if not self.config.slack_webhook_url:
                return

            # Color based on severity
            color_map = {
                AlertSeverity.INFO: "#36a64f",
                AlertSeverity.WARNING: "#ff9500",
                AlertSeverity.ERROR: "#ff0000",
                AlertSeverity.CRITICAL: "#ff0000",
                AlertSeverity.EMERGENCY: "#8B0000",
            }

            payload = {
                "channel": self.config.slack_channel,
                "username": "Companies House Monitor",
                "attachments": [
                    {
                        "color": color_map.get(alert.severity, "#36a64f"),
                        "title": f"{alert.severity.value.upper()}: {alert.title}",
                        "text": alert.message,
                        "fields": [
                            {"title": "Category", "value": alert.category.value, "short": True},
                            {"title": "Source", "value": alert.source_component, "short": True},
                            {"title": "Alert ID", "value": alert.alert_id, "short": True},
                            {
                                "title": "Timestamp",
                                "value": alert.timestamp.isoformat(),
                                "short": True,
                            },
                        ],
                    }
                ],
            }

            async with aiohttp.ClientSession() as session:
                async with session.post(self.config.slack_webhook_url, json=payload) as response:
                    if response.status == 200:
                        logger.debug("Slack notification sent for alert: %s", alert.alert_id)
                    else:
                        logger.warning("Slack notification failed: %s", response.status)

        except Exception as e:
            logger.error("Failed to send Slack notification: %s", e)

    async def _send_pagerduty_notification(self, alert: UnifiedAlert) -> None:
        """Send PagerDuty notification."""
        try:
            if not self.config.pagerduty_integration_key:
                return

            payload = {
                "routing_key": self.config.pagerduty_integration_key,
                "event_action": "trigger",
                "dedup_key": alert.alert_id,
                "payload": {
                    "summary": f"{alert.severity.value.upper()}: {alert.title}",
                    "source": "companies-house-streaming",
                    "severity": "critical"
                    if alert.severity == AlertSeverity.EMERGENCY
                    else "error",
                    "custom_details": {
                        "message": alert.message,
                        "category": alert.category.value,
                        "source_component": alert.source_component,
                        "context": alert.context,
                    },
                },
            }

            async with aiohttp.ClientSession() as session:
                async with session.post(
                    "https://events.pagerduty.com/v2/enqueue", json=payload
                ) as response:
                    if response.status == 202:
                        logger.debug("PagerDuty notification sent for alert: %s", alert.alert_id)
                    else:
                        logger.warning("PagerDuty notification failed: %s", response.status)

        except Exception as e:
            logger.error("Failed to send PagerDuty notification: %s", e)

    async def _start_escalation(self, alert: UnifiedAlert) -> None:
        """Start alert escalation process."""
        escalation_task = asyncio.create_task(self._escalation_loop(alert))
        self.escalation_tasks[alert.alert_id] = escalation_task

    async def _escalation_loop(self, alert: UnifiedAlert) -> None:
        """Alert escalation loop."""
        try:
            while (
                not alert.acknowledged
                and not alert.resolved
                and alert.escalation_level < self.config.max_escalation_level
            ):
                # Wait for escalation interval
                await asyncio.sleep(self.config.escalation_interval_minutes * 60)

                # Check if alert is still active
                if alert.acknowledged or alert.resolved:
                    break

                # Escalate alert
                alert.escalation_level += 1
                alert.escalation_history.append(datetime.now())

                # Send escalated notification
                escalated_alert = UnifiedAlert(
                    alert_id=f"{alert.alert_id}_escalation_{alert.escalation_level}",
                    timestamp=datetime.now(),
                    severity=AlertSeverity.EMERGENCY,
                    category=alert.category,
                    title=f"ESCALATED: {alert.title}",
                    message=f"Alert not acknowledged after {alert.escalation_level * self.config.escalation_interval_minutes} minutes\n\nOriginal: {alert.message}",
                    context={
                        **alert.context,
                        "escalation_level": alert.escalation_level,
                        "original_alert_id": alert.alert_id,
                    },
                    source_component=alert.source_component,
                )

                await self._send_notifications(escalated_alert)

                logger.warning(
                    "Alert escalated: %s (level %d)", alert.alert_id, alert.escalation_level
                )

        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error("Error in escalation loop for alert %s: %s", alert.alert_id, e)

    async def acknowledge_alert(self, alert_id: str, acknowledged_by: str) -> bool:
        """Acknowledge an alert.

        Args:
            alert_id: Alert ID to acknowledge
            acknowledged_by: Who acknowledged the alert

        Returns:
            True if alert was acknowledged successfully
        """
        if alert_id not in self.active_alerts:
            return False

        alert = self.active_alerts[alert_id]
        alert.acknowledged = True
        alert.acknowledged_by = acknowledged_by
        alert.acknowledged_at = datetime.now()

        # Cancel escalation
        if alert_id in self.escalation_tasks:
            self.escalation_tasks[alert_id].cancel()
            del self.escalation_tasks[alert_id]

        logger.info("Alert acknowledged: %s by %s", alert_id, acknowledged_by)
        return True

    async def resolve_alert(self, alert_id: str) -> bool:
        """Resolve an alert.

        Args:
            alert_id: Alert ID to resolve

        Returns:
            True if alert was resolved successfully
        """
        if alert_id not in self.active_alerts:
            return False

        alert = self.active_alerts[alert_id]
        alert.resolved = True
        alert.resolved_at = datetime.now()

        # Remove from active alerts
        del self.active_alerts[alert_id]

        # Cancel escalation
        if alert_id in self.escalation_tasks:
            self.escalation_tasks[alert_id].cancel()
            del self.escalation_tasks[alert_id]

        logger.info("Alert resolved: %s", alert_id)
        return True

    def get_active_alerts(self) -> List[Dict[str, Any]]:
        """Get all active alerts."""
        return [alert.to_dict() for alert in self.active_alerts.values()]

    def get_alert_summary(self) -> Dict[str, Any]:
        """Get summary of alert status."""
        active_by_severity = {}
        for severity in AlertSeverity:
            active_by_severity[severity.value] = sum(
                1 for alert in self.active_alerts.values() if alert.severity == severity
            )

        return {
            "total_active": len(self.active_alerts),
            "active_by_severity": active_by_severity,
            "unacknowledged": sum(
                1
                for alert in self.active_alerts.values()
                if alert.acknowledgment_required and not alert.acknowledged
            ),
            "escalated": sum(
                1 for alert in self.active_alerts.values() if alert.escalation_level > 0
            ),
            "total_alerts_24h": len(
                [
                    alert
                    for alert in self.alert_history
                    if datetime.now() - alert.timestamp < timedelta(hours=24)
                ]
            ),
        }

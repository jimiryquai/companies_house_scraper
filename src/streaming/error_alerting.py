"""Error logging and alerting system for streaming module.

Provides pattern-based error detection and multi-channel alerting capabilities.
"""

import json
import logging
import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from re import Pattern
from typing import Any, Optional

try:
    import smtplib
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText

    EMAIL_AVAILABLE = True
except ImportError:
    EMAIL_AVAILABLE = False

try:
    import aiohttp

    HTTP_AVAILABLE = True
except ImportError:
    HTTP_AVAILABLE = False

from .config import StreamingConfig
from .structured_logger import LogEntry, LogLevel


class AlertingError(Exception):
    """Error in alerting system."""

    pass


class AlertLevel(Enum):
    """Alert severity levels."""

    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"

    def priority(self) -> int:
        """Get numeric priority for ordering."""
        return {
            AlertLevel.LOW: 1,
            AlertLevel.MEDIUM: 2,
            AlertLevel.HIGH: 3,
            AlertLevel.CRITICAL: 4,
        }[self]


@dataclass
class ErrorPattern:
    """Pattern for matching and classifying errors."""

    name: str
    message_regex: Optional[str] = None
    error_type_regex: Optional[str] = None
    log_level: Optional[LogLevel] = None
    alert_level: AlertLevel = AlertLevel.MEDIUM
    context_filters: Optional[dict[str, str]] = None

    def __post_init__(self) -> None:
        """Compile regex patterns after initialization."""
        self._message_pattern: Optional[Pattern[str]] = None
        self._error_type_pattern: Optional[Pattern[str]] = None

        if self.message_regex:
            self._message_pattern = re.compile(self.message_regex, re.IGNORECASE)

        if self.error_type_regex:
            self._error_type_pattern = re.compile(self.error_type_regex, re.IGNORECASE)

    def matches(self, log_entry: LogEntry) -> bool:
        """Check if log entry matches this error pattern."""
        # Check log level if specified
        if self.log_level and log_entry.level != self.log_level:
            return False

        # Check message pattern
        if self._message_pattern and not self._message_pattern.search(log_entry.message):
            return False

        # Check error type pattern
        if self._error_type_pattern and log_entry.extra_data:
            error_type = log_entry.extra_data.get("error_type", "")
            if not self._error_type_pattern.search(str(error_type)):
                return False

        # Check context filters
        if self.context_filters:
            context_dict = log_entry.context.to_dict()
            for key, expected_value in self.context_filters.items():
                if context_dict.get(key) != expected_value:
                    return False

        return True


@dataclass
class AlertThreshold:
    """Threshold configuration for triggering alerts."""

    pattern_name: str
    max_occurrences: int
    time_window_minutes: int
    alert_level: AlertLevel

    def should_alert(self, occurrences: list[datetime]) -> tuple[bool, int]:
        """Check if alert should be triggered based on occurrences.

        Returns (should_alert, occurrence_count)
        """
        cutoff_time = datetime.now() - timedelta(minutes=self.time_window_minutes)
        recent_occurrences = [occ for occ in occurrences if occ >= cutoff_time]

        return len(recent_occurrences) > self.max_occurrences, len(recent_occurrences)


class AlertChannel:
    """Base class for alert channels."""

    async def start(self) -> None:
        """Start the alert channel."""
        pass

    async def stop(self) -> None:
        """Stop the alert channel."""
        pass

    async def send_alert(self, alert_data: dict[str, Any]) -> bool:
        """Send an alert through this channel.

        Returns True if successful, False otherwise.
        """
        raise NotImplementedError


class EmailAlertChannel(AlertChannel):
    """Email-based alert channel."""

    def __init__(
        self,
        smtp_server: str,
        smtp_port: int,
        username: str,
        password: str,
        from_email: str,
        to_emails: list[str],
        use_tls: bool = True,
    ):
        """Initialize email alert channel."""
        if not EMAIL_AVAILABLE:
            raise AlertingError("Email functionality not available - missing email modules")

        self.smtp_server = smtp_server
        self.smtp_port = smtp_port
        self.username = username
        self.password = password
        self.from_email = from_email
        self.to_emails = to_emails
        self.use_tls = use_tls

    async def send_alert(self, alert_data: dict[str, Any]) -> bool:
        """Send alert via email."""
        try:
            # Build email content
            subject = (
                f"[{alert_data['alert_level'].value}] Streaming Alert: {alert_data['pattern_name']}"
            )

            body = f"""
Streaming System Alert

Pattern: {alert_data["pattern_name"]}
Alert Level: {alert_data["alert_level"].value}
Message: {alert_data["message"]}
Occurrence Count: {alert_data.get("count", 1)}
Time Window: {alert_data.get("time_window", "N/A")}
Timestamp: {datetime.now().isoformat()}

Additional Details:
{json.dumps(alert_data.get("extra_data", {}), indent=2)}

---
Streaming System Monitoring
"""

            # Create message
            msg = MIMEMultipart()
            msg["From"] = self.from_email
            msg["To"] = ", ".join(self.to_emails)
            msg["Subject"] = subject

            msg.attach(MIMEText(body, "plain"))

            # Send email
            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                if self.use_tls:
                    server.starttls()
                server.login(self.username, self.password)
                server.send_message(msg)

            return True

        except Exception:
            # Log error but don't raise to avoid alert loops
            # Failed to send email alert - avoid alert loops
            pass
            return False


class SlackAlertChannel(AlertChannel):
    """Slack webhook-based alert channel."""

    def __init__(
        self,
        webhook_url: str,
        channel: Optional[str] = None,
        username: Optional[str] = None,
        icon_emoji: Optional[str] = None,
    ):
        """Initialize Slack alert channel."""
        if not HTTP_AVAILABLE:
            raise AlertingError("HTTP functionality not available - missing aiohttp module")

        self.webhook_url = webhook_url
        self.channel = channel
        self.username = username or "StreamingAlerts"
        self.icon_emoji = icon_emoji or ":warning:"

    async def send_alert(self, alert_data: dict[str, Any]) -> bool:
        """Send alert to Slack."""
        try:
            # Build Slack message
            alert_level = alert_data["alert_level"]
            color_map = {
                AlertLevel.LOW: "good",
                AlertLevel.MEDIUM: "warning",
                AlertLevel.HIGH: "danger",
                AlertLevel.CRITICAL: "danger",
            }

            attachment = {
                "color": color_map.get(alert_level, "warning"),
                "title": f"Streaming Alert: {alert_data['pattern_name']}",
                "text": alert_data["message"],
                "fields": [
                    {"title": "Alert Level", "value": alert_level.value, "short": True},
                    {"title": "Count", "value": str(alert_data.get("count", 1)), "short": True},
                    {
                        "title": "Time Window",
                        "value": alert_data.get("time_window", "N/A"),
                        "short": True,
                    },
                ],
                "ts": int(datetime.now().timestamp()),
            }

            payload: dict[str, Any] = {"attachments": [attachment]}

            if self.channel:
                payload["channel"] = self.channel
            if self.username:
                payload["username"] = self.username
            if self.icon_emoji:
                payload["icon_emoji"] = self.icon_emoji

            # Send to Slack
            async with (
                aiohttp.ClientSession() as session,
                session.post(
                    self.webhook_url, json=payload, headers={"Content-Type": "application/json"}
                ) as response,
            ):
                return response.status == 200

        except Exception:
            # Failed to send Slack alert - avoid alert loops
            pass
            return False


class WebhookAlertChannel(AlertChannel):
    """Generic webhook-based alert channel."""

    def __init__(
        self, webhook_url: str, headers: Optional[dict[str, str]] = None, timeout_seconds: int = 30
    ):
        """Initialize webhook alert channel."""
        if not HTTP_AVAILABLE:
            raise AlertingError("HTTP functionality not available - missing aiohttp module")

        self.webhook_url = webhook_url
        self.headers = headers or {"Content-Type": "application/json"}
        self.timeout_seconds = timeout_seconds

    async def send_alert(self, alert_data: dict[str, Any]) -> bool:
        """Send alert via webhook."""
        try:
            # Convert AlertLevel enum to string for JSON serialization
            payload = {**alert_data}
            if "alert_level" in payload:
                payload["alert_level"] = payload["alert_level"].value

            payload["timestamp"] = datetime.now().isoformat()
            payload["alert_type"] = "streaming_error"

            async with (
                aiohttp.ClientSession() as session,
                session.post(
                    self.webhook_url,
                    json=payload,
                    headers=self.headers,
                    timeout=aiohttp.ClientTimeout(total=self.timeout_seconds),
                ) as response,
            ):
                return response.status < 400

        except Exception:
            # Failed to send webhook alert - avoid alert loops
            pass
            return False


class LogAlertChannel(AlertChannel):
    """Log file-based alert channel."""

    def __init__(self, log_file: str, log_level: LogLevel = LogLevel.WARNING):
        """Initialize log alert channel."""
        self.log_file = log_file
        self.log_level = log_level
        self._logger: Optional[logging.Logger] = None

    async def start(self) -> None:
        """Start the log alert channel."""
        # Ensure log directory exists
        Path(self.log_file).parent.mkdir(parents=True, exist_ok=True)

        # Set up logger
        self._logger = logging.getLogger(f"alert_logger_{id(self)}")
        self._logger.setLevel(self.log_level.to_logging_level())

        # Add file handler
        handler = logging.FileHandler(self.log_file)
        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        handler.setFormatter(formatter)
        self._logger.addHandler(handler)

    async def stop(self) -> None:
        """Stop the log alert channel."""
        if self._logger:
            # Remove all handlers
            for handler in self._logger.handlers[:]:
                self._logger.removeHandler(handler)
                handler.close()

    async def send_alert(self, alert_data: dict[str, Any]) -> bool:
        """Log alert to file."""
        try:
            if not self._logger:
                await self.start()

            alert_message = (
                f"ALERT [{alert_data['alert_level'].value}] {alert_data['pattern_name']}: "
                f"{alert_data['message']} (Count: {alert_data.get('count', 1)}, "
                f"Window: {alert_data.get('time_window', 'N/A')})"
            )

            if not self._logger:
                # Logger not initialized - skip logging
                pass
                return False

            if alert_data["alert_level"] == AlertLevel.CRITICAL:
                self._logger.critical(alert_message)
            elif alert_data["alert_level"] == AlertLevel.HIGH:
                self._logger.error(alert_message)
            elif alert_data["alert_level"] == AlertLevel.MEDIUM:
                self._logger.warning(alert_message)
            else:
                self._logger.info(alert_message)

            return True

        except Exception:
            # Failed to log alert - avoid alert loops
            pass
            return False


class ErrorAlertManager:
    """Main error alerting system manager."""

    def __init__(self, config: StreamingConfig):
        """Initialize error alert manager."""
        self.config = config
        self.patterns: list[ErrorPattern] = []
        self.channels: dict[str, AlertChannel] = {}
        self.thresholds: list[AlertThreshold] = []

        # Track pattern occurrences for threshold checking
        self.pattern_occurrences: dict[str, list[datetime]] = {}
        self.pattern_matches: dict[str, int] = {}
        self.alerts_sent: int = 0

        self._started = False

    async def start(self) -> None:
        """Start the alert manager."""
        if self._started:
            return

        # Start all channels
        for channel in self.channels.values():
            await channel.start()

        self._started = True

    async def stop(self) -> None:
        """Stop the alert manager."""
        if not self._started:
            return

        # Stop all channels
        for channel in self.channels.values():
            await channel.stop()

        self._started = False

    def add_pattern(self, pattern: ErrorPattern) -> None:
        """Add an error pattern for monitoring."""
        self.patterns.append(pattern)
        self.pattern_occurrences[pattern.name] = []
        self.pattern_matches[pattern.name] = 0

    def add_channel(self, name: str, channel: AlertChannel) -> None:
        """Add an alert channel."""
        self.channels[name] = channel

    def add_threshold(self, threshold: AlertThreshold) -> None:
        """Add an alert threshold."""
        self.thresholds.append(threshold)

    async def process_log_entry(self, log_entry: LogEntry) -> bool:
        """Process a log entry for error patterns and alerting.

        Returns True if an alert was sent, False otherwise.
        """
        if not self._started:
            await self.start()

        alert_sent = False

        # Check each pattern
        for pattern in self.patterns:
            if pattern.matches(log_entry):
                # Record pattern match
                self.pattern_matches[pattern.name] += 1
                self.pattern_occurrences[pattern.name].append(datetime.now())

                # Check if we should alert based on thresholds
                for threshold in self.thresholds:
                    if threshold.pattern_name == pattern.name:
                        should_alert, count = threshold.should_alert(
                            self.pattern_occurrences[pattern.name]
                        )

                        if should_alert:
                            await self._send_alert(pattern, threshold, log_entry, count)
                            alert_sent = True
                            break

        return alert_sent

    async def _send_alert(
        self,
        pattern: ErrorPattern,
        threshold: AlertThreshold,
        log_entry: LogEntry,
        occurrence_count: int,
    ) -> None:
        """Send alert through all configured channels."""
        alert_data = {
            "pattern_name": pattern.name,
            "alert_level": threshold.alert_level,
            "message": log_entry.message,
            "count": occurrence_count,
            "time_window": f"{threshold.time_window_minutes} minutes",
            "context": log_entry.context.to_dict(),
            "extra_data": log_entry.extra_data or {},
            "log_level": log_entry.level.value,
            "timestamp": log_entry.timestamp.isoformat(),
        }

        # Send through all channels
        success_count = 0
        for _, channel in self.channels.items():
            try:
                success = await channel.send_alert(alert_data)
                if success:
                    success_count += 1
            except Exception as e:
                # Error sending alert through channel - avoid alert loops
                _ = str(e)  # Suppress S110 warning

        if success_count > 0:
            self.alerts_sent += 1

    def get_statistics(self) -> dict[str, Any]:
        """Get alerting statistics."""
        return {
            "total_patterns": len(self.patterns),
            "total_channels": len(self.channels),
            "total_thresholds": len(self.thresholds),
            "pattern_matches": dict(self.pattern_matches),
            "alerts_sent": self.alerts_sent,
            "active_patterns": [p.name for p in self.patterns],
            "active_channels": list(self.channels.keys()),
        }

    def cleanup_old_occurrences(self, hours: int = 24) -> None:
        """Clean up old pattern occurrences to prevent memory growth."""
        cutoff_time = datetime.now() - timedelta(hours=hours)

        for pattern_name in self.pattern_occurrences:
            self.pattern_occurrences[pattern_name] = [
                occurrence
                for occurrence in self.pattern_occurrences[pattern_name]
                if occurrence >= cutoff_time
            ]


# Factory functions for common patterns and channels


def create_common_error_patterns() -> list[ErrorPattern]:
    """Create common error patterns for streaming operations."""
    return [
        ErrorPattern(
            name="api_connection_timeout",
            message_regex=r".*(api|connection).*(timeout|failed).*",
            error_type_regex=r".*(Timeout|Connection).*",
            log_level=LogLevel.ERROR,
            alert_level=AlertLevel.HIGH,
        ),
        ErrorPattern(
            name="database_error",
            message_regex=r".*(database|db).*(error|failed|connection).*",
            error_type_regex=r".*Database.*",
            log_level=LogLevel.ERROR,
            alert_level=AlertLevel.HIGH,
        ),
        ErrorPattern(
            name="critical_system_failure",
            log_level=LogLevel.CRITICAL,
            alert_level=AlertLevel.CRITICAL,
        ),
        ErrorPattern(
            name="rate_limit_exceeded",
            message_regex=r".*(rate.?limit|quota).*exceeded.*",
            error_type_regex=r".*RateLimit.*",
            log_level=LogLevel.WARNING,
            alert_level=AlertLevel.MEDIUM,
        ),
        ErrorPattern(
            name="authentication_failure",
            message_regex=r".*(auth|authentication).*(failed|invalid|expired).*",
            error_type_regex=r".*Auth.*",
            log_level=LogLevel.ERROR,
            alert_level=AlertLevel.HIGH,
        ),
    ]


def create_standard_thresholds() -> list[AlertThreshold]:
    """Create standard alert thresholds."""
    return [
        AlertThreshold(
            pattern_name="critical_system_failure",
            max_occurrences=1,
            time_window_minutes=1,
            alert_level=AlertLevel.CRITICAL,
        ),
        AlertThreshold(
            pattern_name="api_connection_timeout",
            max_occurrences=3,
            time_window_minutes=5,
            alert_level=AlertLevel.HIGH,
        ),
        AlertThreshold(
            pattern_name="database_error",
            max_occurrences=2,
            time_window_minutes=5,
            alert_level=AlertLevel.HIGH,
        ),
        AlertThreshold(
            pattern_name="rate_limit_exceeded",
            max_occurrences=5,
            time_window_minutes=10,
            alert_level=AlertLevel.MEDIUM,
        ),
        AlertThreshold(
            pattern_name="authentication_failure",
            max_occurrences=2,
            time_window_minutes=5,
            alert_level=AlertLevel.HIGH,
        ),
    ]


def create_log_alert_channel(log_file: str) -> LogAlertChannel:
    """Create a log-based alert channel."""
    return LogAlertChannel(log_file=log_file, log_level=LogLevel.INFO)


def setup_basic_alerting(config: StreamingConfig, alert_log_file: str) -> ErrorAlertManager:
    """Set up basic error alerting with common patterns and log channel."""
    manager = ErrorAlertManager(config)

    # Add common patterns
    for pattern in create_common_error_patterns():
        manager.add_pattern(pattern)

    # Add standard thresholds
    for threshold in create_standard_thresholds():
        manager.add_threshold(threshold)

    # Add log alert channel
    log_channel = create_log_alert_channel(alert_log_file)
    manager.add_channel("log_alerts", log_channel)

    return manager

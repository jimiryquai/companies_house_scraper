"""Real-time API usage monitoring for Companies House rate limit compliance.

This module provides comprehensive real-time monitoring of API usage within
5-minute windows to ensure strict compliance with Companies House API limits
and prevent rate limit violations in cloud deployment environments.
"""

import asyncio
import json
import logging
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


class UsageLevel(Enum):
    """API usage level classifications."""

    SAFE = "safe"  # Under 60% of limit
    MODERATE = "moderate"  # 60-80% of limit
    HIGH = "high"  # 80-95% of limit
    CRITICAL = "critical"  # 95%+ of limit
    EXCEEDED = "exceeded"  # Over limit


@dataclass
class APICall:
    """Record of an API call for usage tracking."""

    timestamp: datetime
    endpoint: str
    company_number: str
    request_id: str
    response_code: int
    processing_time_ms: float
    success: bool


@dataclass
class UsageWindow:
    """5-minute usage window tracking."""

    start_time: datetime
    end_time: datetime
    calls: List[APICall] = field(default_factory=list)
    call_count: int = 0
    successful_calls: int = 0
    failed_calls: int = 0
    avg_response_time_ms: float = 0.0

    def add_call(self, api_call: APICall) -> None:
        """Add an API call to this window."""
        self.calls.append(api_call)
        self.call_count += 1

        if api_call.success:
            self.successful_calls += 1
        else:
            self.failed_calls += 1

        # Update average response time
        total_time = sum(call.processing_time_ms for call in self.calls)
        self.avg_response_time_ms = total_time / len(self.calls)

    def get_summary(self) -> Dict[str, Any]:
        """Get summary of this usage window."""
        return {
            "start_time": self.start_time.isoformat(),
            "end_time": self.end_time.isoformat(),
            "total_calls": self.call_count,
            "successful_calls": self.successful_calls,
            "failed_calls": self.failed_calls,
            "success_rate": self.successful_calls / max(self.call_count, 1),
            "avg_response_time_ms": self.avg_response_time_ms,
            "calls_per_minute": self.call_count / 5.0,
        }


@dataclass
class UsageStatistics:
    """Comprehensive usage statistics."""

    current_window_calls: int = 0
    rate_limit: int = 600  # Companies House limit: 600 calls per 5 minutes
    usage_percentage: float = 0.0
    usage_level: UsageLevel = UsageLevel.SAFE

    # Historical tracking
    total_calls_today: int = 0
    total_violations_today: int = 0
    peak_usage_today: int = 0

    # Performance metrics
    avg_response_time_ms: float = 0.0
    success_rate: float = 1.0

    # Predictions
    projected_calls_next_5min: int = 0
    risk_level: UsageLevel = UsageLevel.SAFE

    last_updated: datetime = field(default_factory=datetime.now)

    def update(self, current_calls: int, avg_response_time: float, success_rate: float) -> None:
        """Update statistics with current data."""
        self.current_window_calls = current_calls
        self.usage_percentage = (current_calls / self.rate_limit) * 100
        self.avg_response_time_ms = avg_response_time
        self.success_rate = success_rate
        self.last_updated = datetime.now()

        # Update usage level
        if self.usage_percentage >= 100:
            self.usage_level = UsageLevel.EXCEEDED
        elif self.usage_percentage >= 95:
            self.usage_level = UsageLevel.CRITICAL
        elif self.usage_percentage >= 80:
            self.usage_level = UsageLevel.HIGH
        elif self.usage_percentage >= 60:
            self.usage_level = UsageLevel.MODERATE
        else:
            self.usage_level = UsageLevel.SAFE

        # Update peak usage
        if current_calls > self.peak_usage_today:
            self.peak_usage_today = current_calls


class RealTimeAPIUsageMonitor:
    """Real-time API usage monitor with 5-minute window tracking.

    This monitor provides:
    - Real-time tracking of API calls within 5-minute windows
    - Usage level classification (Safe/Moderate/High/Critical/Exceeded)
    - Predictive analysis for upcoming windows
    - Historical usage tracking and reporting
    - Automatic alerting when approaching limits
    - Export capabilities for cloud monitoring platforms
    """

    def __init__(
        self,
        rate_limit: int = 600,
        window_duration_minutes: int = 5,
        alert_callback: Optional[callable] = None,
        persistence_path: Optional[Path] = None,
    ) -> None:
        """Initialize the real-time usage monitor.

        Args:
            rate_limit: API rate limit (default: 600 calls per 5 minutes)
            window_duration_minutes: Duration of monitoring window
            alert_callback: Function to call for usage alerts
            persistence_path: Path to store usage data
        """
        self.rate_limit = rate_limit
        self.window_duration = timedelta(minutes=window_duration_minutes)
        self.alert_callback = alert_callback
        self.persistence_path = persistence_path

        # Current tracking window
        self.current_window = self._create_new_window()

        # Sliding window for calls (last 5 minutes)
        self.api_calls: deque[APICall] = deque()

        # Usage statistics
        self.statistics = UsageStatistics(rate_limit=rate_limit)

        # Historical data
        self.completed_windows: List[UsageWindow] = []
        self.daily_totals: Dict[str, int] = {}  # date -> call count

        # Monitoring state
        self.monitoring_active = False
        self.monitoring_task: Optional[asyncio.Task] = None

        # Performance tracking
        self.response_times: deque[float] = deque(maxlen=1000)

        logger.info(
            "RealTimeAPIUsageMonitor initialized: rate_limit=%d, window=%dm",
            rate_limit,
            window_duration_minutes,
        )

    def _create_new_window(self) -> UsageWindow:
        """Create a new usage tracking window."""
        now = datetime.now()
        return UsageWindow(start_time=now, end_time=now + self.window_duration)

    async def start_monitoring(self) -> None:
        """Start real-time monitoring."""
        if self.monitoring_active:
            logger.warning("API usage monitoring is already active")
            return

        self.monitoring_active = True
        self.monitoring_task = asyncio.create_task(self._monitoring_loop())

        logger.info("Started real-time API usage monitoring")

    async def stop_monitoring(self) -> None:
        """Stop real-time monitoring."""
        if not self.monitoring_active:
            return

        self.monitoring_active = False
        if self.monitoring_task:
            self.monitoring_task.cancel()
            try:
                await self.monitoring_task
            except asyncio.CancelledError:
                pass

        logger.info("Stopped real-time API usage monitoring")

    async def _monitoring_loop(self) -> None:
        """Main monitoring loop."""
        while self.monitoring_active:
            try:
                # Update statistics
                self._update_statistics()

                # Check if window has expired
                await self._check_window_rotation()

                # Clean old data
                self._clean_old_data()

                # Check for alerts
                self._check_usage_alerts()

                # Persist data if configured
                if self.persistence_path:
                    await self._persist_usage_data()

                # Wait before next check
                await asyncio.sleep(10)  # Check every 10 seconds

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Error in usage monitoring loop: %s", e, exc_info=True)
                await asyncio.sleep(10)

    def record_api_call(
        self,
        endpoint: str,
        company_number: str,
        request_id: str,
        response_code: int,
        processing_time_ms: float,
    ) -> None:
        """Record an API call for usage tracking.

        Args:
            endpoint: API endpoint called
            company_number: Company number requested
            request_id: Unique request identifier
            response_code: HTTP response code
            processing_time_ms: Request processing time in milliseconds
        """
        api_call = APICall(
            timestamp=datetime.now(),
            endpoint=endpoint,
            company_number=company_number,
            request_id=request_id,
            response_code=response_code,
            processing_time_ms=processing_time_ms,
            success=200 <= response_code < 300,
        )

        # Add to sliding window
        self.api_calls.append(api_call)

        # Add to current tracking window
        self.current_window.add_call(api_call)

        # Update response time tracking
        self.response_times.append(processing_time_ms)

        # Update daily totals
        today = datetime.now().strftime("%Y-%m-%d")
        self.daily_totals[today] = self.daily_totals.get(today, 0) + 1

        logger.debug(
            "API call recorded: %s, company=%s, response=%d, time=%.1fms",
            endpoint,
            company_number,
            response_code,
            processing_time_ms,
        )

    def _update_statistics(self) -> None:
        """Update usage statistics."""
        # Clean sliding window (keep only last 5 minutes)
        cutoff_time = datetime.now() - self.window_duration
        while self.api_calls and self.api_calls[0].timestamp < cutoff_time:
            self.api_calls.popleft()

        # Calculate current metrics
        current_calls = len(self.api_calls)

        avg_response_time = (
            sum(self.response_times) / len(self.response_times) if self.response_times else 0.0
        )

        successful_calls = sum(1 for call in self.api_calls if call.success)
        success_rate = successful_calls / max(current_calls, 1)

        # Update statistics
        self.statistics.update(current_calls, avg_response_time, success_rate)

        # Predict next window usage
        self._update_predictions()

    def _update_predictions(self) -> None:
        """Update usage predictions."""
        if len(self.completed_windows) < 2:
            return

        # Simple prediction based on recent trend
        recent_windows = self.completed_windows[-5:]  # Last 5 windows
        recent_calls = [w.call_count for w in recent_windows]

        # Calculate trend
        if len(recent_calls) >= 3:
            trend = (recent_calls[-1] - recent_calls[0]) / len(recent_calls)
            predicted = max(0, int(recent_calls[-1] + trend))

            self.statistics.projected_calls_next_5min = predicted

            # Assess risk level
            projected_percentage = (predicted / self.rate_limit) * 100

            if projected_percentage >= 95:
                self.statistics.risk_level = UsageLevel.CRITICAL
            elif projected_percentage >= 80:
                self.statistics.risk_level = UsageLevel.HIGH
            elif projected_percentage >= 60:
                self.statistics.risk_level = UsageLevel.MODERATE
            else:
                self.statistics.risk_level = UsageLevel.SAFE

    async def _check_window_rotation(self) -> None:
        """Check if the current window should be rotated."""
        now = datetime.now()

        if now >= self.current_window.end_time:
            # Complete current window
            self.completed_windows.append(self.current_window)

            # Keep only recent windows for analysis
            if len(self.completed_windows) > 100:
                self.completed_windows = self.completed_windows[-50:]

            logger.info(
                "Window completed: %d calls in 5min (%.1f%% of limit)",
                self.current_window.call_count,
                (self.current_window.call_count / self.rate_limit) * 100,
            )

            # Create new window
            self.current_window = self._create_new_window()

    def _clean_old_data(self) -> None:
        """Clean old data to prevent memory growth."""
        # Keep only last 24 hours of daily totals
        cutoff_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        self.daily_totals = {
            date: count for date, count in self.daily_totals.items() if date >= cutoff_date
        }

    def _check_usage_alerts(self) -> None:
        """Check for usage alerts and notify if needed."""
        if not self.alert_callback:
            return

        usage_level = self.statistics.usage_level
        current_calls = self.statistics.current_window_calls

        # Alert on high usage
        if usage_level in [UsageLevel.HIGH, UsageLevel.CRITICAL, UsageLevel.EXCEEDED]:
            alert_data = {
                "level": usage_level.value,
                "current_calls": current_calls,
                "rate_limit": self.rate_limit,
                "usage_percentage": self.statistics.usage_percentage,
                "projected_calls": self.statistics.projected_calls_next_5min,
                "timestamp": datetime.now().isoformat(),
            }

            try:
                self.alert_callback(alert_data)
            except Exception as e:
                logger.error("Error in usage alert callback: %s", e)

    async def _persist_usage_data(self) -> None:
        """Persist usage data to storage."""
        if not self.persistence_path:
            return

        try:
            usage_data = {
                "timestamp": datetime.now().isoformat(),
                "current_statistics": self.get_current_statistics(),
                "recent_windows": [w.get_summary() for w in self.completed_windows[-10:]],
                "daily_totals": self.daily_totals,
            }

            # Ensure directory exists
            self.persistence_path.parent.mkdir(parents=True, exist_ok=True)

            # Write data
            with open(self.persistence_path, "w") as f:
                json.dump(usage_data, f, indent=2)

        except Exception as e:
            logger.error("Error persisting usage data: %s", e)

    def get_current_statistics(self) -> Dict[str, Any]:
        """Get current usage statistics.

        Returns:
            Dictionary with current usage statistics
        """
        return {
            "current_window": {
                "calls": self.statistics.current_window_calls,
                "rate_limit": self.statistics.rate_limit,
                "usage_percentage": round(self.statistics.usage_percentage, 2),
                "usage_level": self.statistics.usage_level.value,
                "time_remaining_minutes": (
                    self.current_window.end_time - datetime.now()
                ).total_seconds()
                / 60,
            },
            "performance": {
                "avg_response_time_ms": round(self.statistics.avg_response_time_ms, 2),
                "success_rate": round(self.statistics.success_rate, 4),
            },
            "predictions": {
                "projected_calls_next_window": self.statistics.projected_calls_next_5min,
                "risk_level": self.statistics.risk_level.value,
            },
            "daily_summary": {
                "total_calls_today": self.statistics.total_calls_today,
                "peak_usage_today": self.statistics.peak_usage_today,
                "violations_today": self.statistics.total_violations_today,
            },
            "last_updated": self.statistics.last_updated.isoformat(),
        }

    def get_usage_report(self, hours: int = 24) -> Dict[str, Any]:
        """Get comprehensive usage report.

        Args:
            hours: Number of hours to include in report

        Returns:
            Comprehensive usage report
        """
        cutoff_time = datetime.now() - timedelta(hours=hours)
        recent_windows = [w for w in self.completed_windows if w.start_time >= cutoff_time]

        # Calculate aggregated metrics
        total_calls = sum(w.call_count for w in recent_windows)
        total_successful = sum(w.successful_calls for w in recent_windows)
        total_failed = sum(w.failed_calls for w in recent_windows)

        avg_calls_per_window = total_calls / max(len(recent_windows), 1)
        peak_window_usage = max((w.call_count for w in recent_windows), default=0)

        usage_levels = [(w.call_count / self.rate_limit) * 100 for w in recent_windows]

        return {
            "report_period": {
                "hours": hours,
                "windows_analyzed": len(recent_windows),
                "start_time": cutoff_time.isoformat(),
                "end_time": datetime.now().isoformat(),
            },
            "usage_summary": {
                "total_calls": total_calls,
                "successful_calls": total_successful,
                "failed_calls": total_failed,
                "success_rate": total_successful / max(total_calls, 1),
                "avg_calls_per_window": round(avg_calls_per_window, 2),
                "peak_window_usage": peak_window_usage,
                "peak_usage_percentage": round((peak_window_usage / self.rate_limit) * 100, 2),
            },
            "usage_distribution": {
                "safe_windows": len([p for p in usage_levels if p < 60]),
                "moderate_windows": len([p for p in usage_levels if 60 <= p < 80]),
                "high_windows": len([p for p in usage_levels if 80 <= p < 95]),
                "critical_windows": len([p for p in usage_levels if 95 <= p < 100]),
                "exceeded_windows": len([p for p in usage_levels if p >= 100]),
            },
            "current_status": self.get_current_statistics(),
            "recommendations": self._generate_recommendations(),
        }

    def _generate_recommendations(self) -> List[str]:
        """Generate usage optimization recommendations."""
        recommendations = []

        # Based on current usage level
        if self.statistics.usage_level == UsageLevel.EXCEEDED:
            recommendations.append("URGENT: Rate limit exceeded - implement immediate throttling")
            recommendations.append("Consider implementing request queuing to spread load")
        elif self.statistics.usage_level == UsageLevel.CRITICAL:
            recommendations.append("WARNING: Approaching rate limit - reduce API requests")
            recommendations.append("Implement backoff strategies for non-critical operations")
        elif self.statistics.usage_level == UsageLevel.HIGH:
            recommendations.append("High usage detected - monitor closely")
            recommendations.append("Consider optimizing request patterns")

        # Based on predictions
        if self.statistics.risk_level in [UsageLevel.HIGH, UsageLevel.CRITICAL]:
            recommendations.append("Projected high usage in next window - prepare throttling")

        # Based on performance
        if self.statistics.success_rate < 0.95:
            recommendations.append(
                f"Low success rate ({self.statistics.success_rate:.2%}) - investigate errors"
            )

        if self.statistics.avg_response_time_ms > 1000:
            recommendations.append("High response times detected - API may be under stress")

        return recommendations

    def export_for_cloud_monitoring(self) -> Dict[str, Any]:
        """Export metrics in cloud monitoring platform format.

        Returns:
            Metrics formatted for cloud monitoring platforms
        """
        stats = self.get_current_statistics()

        return {
            "timestamp": datetime.now().isoformat(),
            "service": "companies-house-streaming",
            "metrics": {
                "api_usage_current_calls": stats["current_window"]["calls"],
                "api_usage_percentage": stats["current_window"]["usage_percentage"],
                "api_usage_level": stats["current_window"]["usage_level"],
                "api_response_time_ms": stats["performance"]["avg_response_time_ms"],
                "api_success_rate": stats["performance"]["success_rate"],
                "api_calls_projected_next_window": stats["predictions"][
                    "projected_calls_next_window"
                ],
                "api_risk_level": stats["predictions"]["risk_level"],
                "api_daily_calls": stats["daily_summary"]["total_calls_today"],
                "api_peak_usage_today": stats["daily_summary"]["peak_usage_today"],
            },
            "dimensions": {
                "component": "api_usage_monitor",
                "environment": "production",
                "window_duration_minutes": self.window_duration.total_seconds() / 60,
            },
        }

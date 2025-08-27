"""Tests for real-time API usage monitoring."""

import asyncio
import json
import tempfile
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from src.streaming.api_usage_monitor import (
    APICall,
    RealTimeAPIUsageMonitor,
    UsageLevel,
    UsageStatistics,
    UsageWindow,
)


@pytest.fixture
def temp_persistence_path():
    """Create temporary path for persistence testing."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        yield Path(tmp_dir) / "usage_data.json"


@pytest.fixture
def usage_monitor():
    """Create usage monitor for testing."""
    return RealTimeAPIUsageMonitor(
        rate_limit=600,  # Standard Companies House limit
        window_duration_minutes=5,
        alert_callback=None,
    )


@pytest.fixture
def usage_monitor_with_persistence(temp_persistence_path):
    """Create usage monitor with persistence for testing."""
    return RealTimeAPIUsageMonitor(
        rate_limit=600, window_duration_minutes=5, persistence_path=temp_persistence_path
    )


class TestAPICall:
    """Test APICall data structure."""

    def test_api_call_creation(self):
        """Test APICall object creation."""
        timestamp = datetime.now()
        call = APICall(
            timestamp=timestamp,
            endpoint="/company/{company_number}",
            company_number="12345678",
            request_id="req_123",
            response_code=200,
            processing_time_ms=150.5,
            success=True,
        )

        assert call.timestamp == timestamp
        assert call.endpoint == "/company/{company_number}"
        assert call.company_number == "12345678"
        assert call.request_id == "req_123"
        assert call.response_code == 200
        assert call.processing_time_ms == 150.5
        assert call.success is True


class TestUsageWindow:
    """Test UsageWindow functionality."""

    def test_usage_window_creation(self):
        """Test creating a new usage window."""
        start_time = datetime.now()
        end_time = start_time + timedelta(minutes=5)

        window = UsageWindow(start_time=start_time, end_time=end_time)

        assert window.start_time == start_time
        assert window.end_time == end_time
        assert window.call_count == 0
        assert window.successful_calls == 0
        assert window.failed_calls == 0
        assert len(window.calls) == 0

    def test_adding_calls_to_window(self):
        """Test adding API calls to a window."""
        window = UsageWindow(
            start_time=datetime.now(), end_time=datetime.now() + timedelta(minutes=5)
        )

        # Add successful call
        successful_call = APICall(
            timestamp=datetime.now(),
            endpoint="/company/12345678",
            company_number="12345678",
            request_id="req_1",
            response_code=200,
            processing_time_ms=100.0,
            success=True,
        )
        window.add_call(successful_call)

        assert window.call_count == 1
        assert window.successful_calls == 1
        assert window.failed_calls == 0
        assert window.avg_response_time_ms == 100.0

        # Add failed call
        failed_call = APICall(
            timestamp=datetime.now(),
            endpoint="/company/87654321",
            company_number="87654321",
            request_id="req_2",
            response_code=429,
            processing_time_ms=50.0,
            success=False,
        )
        window.add_call(failed_call)

        assert window.call_count == 2
        assert window.successful_calls == 1
        assert window.failed_calls == 1
        assert window.avg_response_time_ms == 75.0  # (100 + 50) / 2

    def test_window_summary(self):
        """Test getting window summary."""
        start_time = datetime.now()
        window = UsageWindow(start_time=start_time, end_time=start_time + timedelta(minutes=5))

        # Add some calls
        for i in range(3):
            call = APICall(
                timestamp=datetime.now(),
                endpoint=f"/company/{i}",
                company_number=f"{i}",
                request_id=f"req_{i}",
                response_code=200 if i < 2 else 429,
                processing_time_ms=100.0,
                success=i < 2,
            )
            window.add_call(call)

        summary = window.get_summary()

        assert summary["total_calls"] == 3
        assert summary["successful_calls"] == 2
        assert summary["failed_calls"] == 1
        assert summary["success_rate"] == 2 / 3
        assert summary["calls_per_minute"] == 0.6  # 3 calls / 5 minutes
        assert "start_time" in summary
        assert "end_time" in summary


class TestUsageStatistics:
    """Test UsageStatistics functionality."""

    def test_usage_statistics_update(self):
        """Test updating usage statistics."""
        stats = UsageStatistics(rate_limit=600)

        # Test safe usage
        stats.update(current_calls=100, avg_response_time=150.0, success_rate=0.98)

        assert stats.current_window_calls == 100
        assert stats.usage_percentage == 100 / 600 * 100  # ~16.67%
        assert stats.usage_level == UsageLevel.SAFE
        assert stats.avg_response_time_ms == 150.0
        assert stats.success_rate == 0.98

        # Test high usage
        stats.update(current_calls=500, avg_response_time=200.0, success_rate=0.95)

        assert stats.current_window_calls == 500
        assert stats.usage_percentage == 500 / 600 * 100  # ~83.33%
        assert stats.usage_level == UsageLevel.HIGH

        # Test critical usage
        stats.update(current_calls=580, avg_response_time=300.0, success_rate=0.90)

        assert stats.usage_level == UsageLevel.CRITICAL

        # Test exceeded usage
        stats.update(current_calls=620, avg_response_time=400.0, success_rate=0.85)

        assert stats.usage_level == UsageLevel.EXCEEDED


class TestRealTimeAPIUsageMonitor:
    """Test RealTimeAPIUsageMonitor functionality."""

    def test_monitor_initialization(self, usage_monitor):
        """Test monitor initialization."""
        assert usage_monitor.rate_limit == 600
        assert usage_monitor.window_duration == timedelta(minutes=5)
        assert not usage_monitor.monitoring_active
        assert usage_monitor.statistics.rate_limit == 600
        assert len(usage_monitor.api_calls) == 0
        assert len(usage_monitor.completed_windows) == 0

    def test_recording_api_calls(self, usage_monitor):
        """Test recording API calls."""
        # Record some API calls
        usage_monitor.record_api_call(
            endpoint="/company/12345678",
            company_number="12345678",
            request_id="req_1",
            response_code=200,
            processing_time_ms=150.0,
        )

        assert len(usage_monitor.api_calls) == 1
        assert usage_monitor.current_window.call_count == 1
        assert len(usage_monitor.response_times) == 1

        # Record another call
        usage_monitor.record_api_call(
            endpoint="/company/87654321",
            company_number="87654321",
            request_id="req_2",
            response_code=429,
            processing_time_ms=100.0,
        )

        assert len(usage_monitor.api_calls) == 2
        assert usage_monitor.current_window.call_count == 2

    def test_usage_statistics_update(self, usage_monitor):
        """Test automatic statistics updates."""
        # Record calls
        for i in range(50):
            usage_monitor.record_api_call(
                endpoint=f"/company/{i}",
                company_number=f"{i:08d}",
                request_id=f"req_{i}",
                response_code=200,
                processing_time_ms=100.0 + (i % 50),
            )

        # Update statistics
        usage_monitor._update_statistics()

        stats = usage_monitor.statistics
        assert stats.current_window_calls == 50
        assert stats.usage_percentage == 50 / 600 * 100
        assert stats.usage_level == UsageLevel.SAFE
        assert stats.success_rate == 1.0

    def test_usage_level_classification(self, usage_monitor):
        """Test usage level classification."""
        test_cases = [
            (100, UsageLevel.SAFE),  # ~16.7%
            (400, UsageLevel.MODERATE),  # ~66.7%
            (500, UsageLevel.HIGH),  # ~83.3%
            (580, UsageLevel.CRITICAL),  # ~96.7%
            (650, UsageLevel.EXCEEDED),  # ~108.3%
        ]

        for call_count, expected_level in test_cases:
            # Clear previous calls
            usage_monitor.api_calls.clear()
            usage_monitor.current_window = usage_monitor._create_new_window()

            # Record calls
            for i in range(call_count):
                usage_monitor.record_api_call(
                    endpoint=f"/test/{i}",
                    company_number=f"{i:08d}",
                    request_id=f"req_{i}",
                    response_code=200,
                    processing_time_ms=100.0,
                )

            # Update and check
            usage_monitor._update_statistics()
            assert usage_monitor.statistics.usage_level == expected_level

    def test_current_statistics_retrieval(self, usage_monitor):
        """Test getting current statistics."""
        # Add some calls
        for i in range(300):
            usage_monitor.record_api_call(
                endpoint=f"/company/{i}",
                company_number=f"{i:08d}",
                request_id=f"req_{i}",
                response_code=200 if i % 10 != 9 else 429,  # 90% success rate
                processing_time_ms=120.0,
            )

        usage_monitor._update_statistics()
        stats = usage_monitor.get_current_statistics()

        # Verify structure
        assert "current_window" in stats
        assert "performance" in stats
        assert "predictions" in stats
        assert "daily_summary" in stats
        assert "last_updated" in stats

        # Verify values
        current_window = stats["current_window"]
        assert current_window["calls"] == 300
        assert current_window["rate_limit"] == 600
        assert current_window["usage_percentage"] == 50.0
        assert current_window["usage_level"] == "safe"  # 50% is still in safe range (< 60%)

        performance = stats["performance"]
        assert performance["avg_response_time_ms"] == 120.0
        assert performance["success_rate"] == 0.9

    @pytest.mark.asyncio
    async def test_window_rotation(self, usage_monitor):
        """Test automatic window rotation."""
        # Set a very short window for testing
        usage_monitor.window_duration = timedelta(seconds=0.1)
        usage_monitor.current_window.end_time = datetime.now() + timedelta(seconds=0.1)

        # Add calls to current window
        for i in range(5):
            usage_monitor.record_api_call(
                endpoint=f"/test/{i}",
                company_number=f"{i:08d}",
                request_id=f"req_{i}",
                response_code=200,
                processing_time_ms=100.0,
            )

        assert usage_monitor.current_window.call_count == 5
        assert len(usage_monitor.completed_windows) == 0

        # Wait for window to expire
        await asyncio.sleep(0.2)

        # Check window rotation
        await usage_monitor._check_window_rotation()

        assert len(usage_monitor.completed_windows) == 1
        assert usage_monitor.completed_windows[0].call_count == 5
        assert usage_monitor.current_window.call_count == 0

    def test_usage_predictions(self, usage_monitor):
        """Test usage predictions."""
        # Create some completed windows with trend
        window_counts = [100, 120, 140, 160, 180]

        for count in window_counts:
            window = UsageWindow(
                start_time=datetime.now() - timedelta(minutes=30),
                end_time=datetime.now() - timedelta(minutes=25),
            )
            # Simulate calls in window
            window.call_count = count
            usage_monitor.completed_windows.append(window)

        # Update predictions
        usage_monitor._update_predictions()

        # Should predict increasing trend
        assert usage_monitor.statistics.projected_calls_next_5min > 180

        # Risk level should be appropriate for prediction
        if usage_monitor.statistics.projected_calls_next_5min > 570:  # 95% of 600
            assert usage_monitor.statistics.risk_level == UsageLevel.CRITICAL

    def test_usage_report_generation(self, usage_monitor):
        """Test comprehensive usage report generation."""
        # Create historical data
        for i in range(10):
            window = UsageWindow(
                start_time=datetime.now() - timedelta(hours=i + 1),
                end_time=datetime.now() - timedelta(hours=i + 1) + timedelta(minutes=5),
            )
            window.call_count = 100 + (i * 20)  # Increasing usage
            window.successful_calls = int(window.call_count * 0.95)
            window.failed_calls = window.call_count - window.successful_calls
            usage_monitor.completed_windows.append(window)

        # Generate report
        report = usage_monitor.get_usage_report(hours=12)

        # Verify report structure
        assert "report_period" in report
        assert "usage_summary" in report
        assert "usage_distribution" in report
        assert "current_status" in report
        assert "recommendations" in report

        # Verify calculations
        usage_summary = report["usage_summary"]
        assert usage_summary["total_calls"] > 0
        assert 0 <= usage_summary["success_rate"] <= 1
        assert usage_summary["peak_window_usage"] >= usage_summary["avg_calls_per_window"]

    def test_cloud_monitoring_export(self, usage_monitor):
        """Test export for cloud monitoring platforms."""
        # Add some usage data
        for i in range(200):
            usage_monitor.record_api_call(
                endpoint=f"/company/{i}",
                company_number=f"{i:08d}",
                request_id=f"req_{i}",
                response_code=200,
                processing_time_ms=150.0,
            )

        usage_monitor._update_statistics()

        # Export for cloud monitoring
        export_data = usage_monitor.export_for_cloud_monitoring()

        # Verify structure
        assert "timestamp" in export_data
        assert "service" in export_data
        assert "metrics" in export_data
        assert "dimensions" in export_data

        # Verify metrics
        metrics = export_data["metrics"]
        required_metrics = [
            "api_usage_current_calls",
            "api_usage_percentage",
            "api_usage_level",
            "api_response_time_ms",
            "api_success_rate",
            "api_calls_projected_next_window",
            "api_risk_level",
            "api_daily_calls",
            "api_peak_usage_today",
        ]

        for metric in required_metrics:
            assert metric in metrics

        # Verify dimensions
        dimensions = export_data["dimensions"]
        assert dimensions["component"] == "api_usage_monitor"
        assert "environment" in dimensions
        assert dimensions["window_duration_minutes"] == 5

    @pytest.mark.asyncio
    async def test_alert_callback(self, usage_monitor):
        """Test usage alert callback functionality."""
        alerts_received = []

        def alert_callback(alert_data):
            alerts_received.append(alert_data)

        usage_monitor.alert_callback = alert_callback

        # Generate high usage to trigger alert
        for i in range(550):  # Should trigger CRITICAL alert
            usage_monitor.record_api_call(
                endpoint=f"/test/{i}",
                company_number=f"{i:08d}",
                request_id=f"req_{i}",
                response_code=200,
                processing_time_ms=100.0,
            )

        # Update statistics and check alerts
        usage_monitor._update_statistics()
        usage_monitor._check_usage_alerts()

        # Should have received at least one alert
        assert len(alerts_received) >= 1

        # Verify alert structure
        alert = alerts_received[0]
        assert "level" in alert
        assert "current_calls" in alert
        assert "rate_limit" in alert
        assert "usage_percentage" in alert
        assert "timestamp" in alert

        assert alert["level"] in ["high", "critical", "exceeded"]
        assert alert["current_calls"] >= 550

    @pytest.mark.asyncio
    async def test_monitoring_loop(self, usage_monitor):
        """Test the monitoring loop functionality."""
        # Start monitoring
        await usage_monitor.start_monitoring()

        assert usage_monitor.monitoring_active
        assert usage_monitor.monitoring_task is not None

        # Add some calls during monitoring
        for i in range(10):
            usage_monitor.record_api_call(
                endpoint=f"/test/{i}",
                company_number=f"{i:08d}",
                request_id=f"req_{i}",
                response_code=200,
                processing_time_ms=100.0,
            )

        # Let monitoring loop run briefly
        await asyncio.sleep(0.1)

        # Stop monitoring
        await usage_monitor.stop_monitoring()

        assert not usage_monitor.monitoring_active

    @pytest.mark.asyncio
    async def test_persistence(self, usage_monitor_with_persistence):
        """Test usage data persistence."""
        monitor = usage_monitor_with_persistence

        # Add usage data
        for i in range(100):
            monitor.record_api_call(
                endpoint=f"/test/{i}",
                company_number=f"{i:08d}",
                request_id=f"req_{i}",
                response_code=200,
                processing_time_ms=120.0,
            )

        monitor._update_statistics()

        # Persist data
        await monitor._persist_usage_data()

        # Verify file was created
        assert monitor.persistence_path.exists()

        # Verify data content
        with open(monitor.persistence_path) as f:
            data = json.load(f)

        assert "timestamp" in data
        assert "current_statistics" in data
        assert "recent_windows" in data
        assert "daily_totals" in data

        # Verify statistics were saved
        stats = data["current_statistics"]
        assert stats["current_window"]["calls"] == 100


class TestUsageRecommendations:
    """Test usage optimization recommendations."""

    def test_recommendations_for_different_usage_levels(self, usage_monitor):
        """Test recommendations for different usage levels."""
        # Test exceeded usage
        usage_monitor.statistics.usage_level = UsageLevel.EXCEEDED
        recommendations = usage_monitor._generate_recommendations()

        assert any("URGENT" in rec for rec in recommendations)
        assert any("throttling" in rec for rec in recommendations)

        # Test critical usage
        usage_monitor.statistics.usage_level = UsageLevel.CRITICAL
        usage_monitor.statistics.risk_level = UsageLevel.CRITICAL
        recommendations = usage_monitor._generate_recommendations()

        assert any("WARNING" in rec for rec in recommendations)
        assert any("reduce API requests" in rec for rec in recommendations)

        # Test high usage
        usage_monitor.statistics.usage_level = UsageLevel.HIGH
        recommendations = usage_monitor._generate_recommendations()

        assert any("High usage" in rec for rec in recommendations)
        assert any("monitor closely" in rec for rec in recommendations)

        # Test performance recommendations
        usage_monitor.statistics.success_rate = 0.8  # Low success rate
        usage_monitor.statistics.avg_response_time_ms = 1500  # High response time

        recommendations = usage_monitor._generate_recommendations()

        assert any("success rate" in rec for rec in recommendations)
        assert any("response times" in rec for rec in recommendations)

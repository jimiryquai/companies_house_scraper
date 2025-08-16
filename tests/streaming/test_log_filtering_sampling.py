"""
Tests for log filtering and sampling functionality in streaming module.
"""

import os
import tempfile
from datetime import datetime, timedelta
from typing import Any
from unittest.mock import patch

import pytest

from src.streaming.config import StreamingConfig
from src.streaming.log_filtering import (
    FilteredLogger,
    LevelFilter,
    PatternFilter,
    RandomSampler,
    RateLimitFilter,
    TimeBasedSampler,
    VolumeBasedSampler,
)
from src.streaming.structured_logger import LogContext, LogEntry, LogLevel, StructuredLogger


@pytest.fixture
def config() -> Any:
    """Create test configuration."""
    return StreamingConfig(
        streaming_api_key="test-api-key-123456", database_path=":memory:", batch_size=10
    )


@pytest.fixture
def temp_log_file() -> Any:
    """Create a temporary log file for testing."""
    with tempfile.NamedTemporaryFile(suffix=".log", delete=False) as temp_file:
        temp_path = temp_file.name

    yield temp_path

    # Cleanup
    if os.path.exists(temp_path):
        os.unlink(temp_path)


@pytest.fixture
def sample_log_entries() -> Any:
    """Sample log entries for testing."""
    return [
        LogEntry(
            timestamp=datetime.now(),
            level=LogLevel.DEBUG,
            message="Debug message",
            context=LogContext(operation="debug_op"),
            extra_data={"debug": True},
        ),
        LogEntry(
            timestamp=datetime.now(),
            level=LogLevel.INFO,
            message="Info message",
            context=LogContext(operation="info_op"),
            extra_data={"info": True},
        ),
        LogEntry(
            timestamp=datetime.now(),
            level=LogLevel.WARNING,
            message="Warning message",
            context=LogContext(operation="warning_op"),
            extra_data={"warning": True},
        ),
        LogEntry(
            timestamp=datetime.now(),
            level=LogLevel.ERROR,
            message="Error message",
            context=LogContext(operation="error_op"),
            extra_data={"error": True},
        ),
        LogEntry(
            timestamp=datetime.now(),
            level=LogLevel.CRITICAL,
            message="Critical message",
            context=LogContext(operation="critical_op"),
            extra_data={"critical": True},
        ),
    ]


class TestLevelFilter:
    """Test LevelFilter functionality."""

    def test_level_filter_creation(self) -> None:
        """Test LevelFilter creation."""
        filter_obj = LevelFilter(min_level=LogLevel.WARNING)

        assert filter_obj.min_level == LogLevel.WARNING

    def test_level_filter_accepts_above_threshold(self, sample_log_entries: Any) -> None:
        """Test level filter accepts logs above threshold."""
        filter_obj = LevelFilter(min_level=LogLevel.WARNING)

        # Should accept WARNING, ERROR, CRITICAL
        warning_entry = sample_log_entries[2]  # WARNING
        error_entry = sample_log_entries[3]  # ERROR
        critical_entry = sample_log_entries[4]  # CRITICAL

        assert filter_obj.should_log(warning_entry) is True
        assert filter_obj.should_log(error_entry) is True
        assert filter_obj.should_log(critical_entry) is True

    def test_level_filter_rejects_below_threshold(self, sample_log_entries: Any) -> None:
        """Test level filter rejects logs below threshold."""
        filter_obj = LevelFilter(min_level=LogLevel.WARNING)

        # Should reject DEBUG, INFO
        debug_entry = sample_log_entries[0]  # DEBUG
        info_entry = sample_log_entries[1]  # INFO

        assert filter_obj.should_log(debug_entry) is False
        assert filter_obj.should_log(info_entry) is False


class TestPatternFilter:
    """Test PatternFilter functionality."""

    def test_pattern_filter_creation(self) -> None:
        """Test PatternFilter creation."""
        filter_obj = PatternFilter(
            include_patterns=[r".*important.*"], exclude_patterns=[r".*debug.*"]
        )

        assert len(filter_obj.include_patterns) == 1
        assert len(filter_obj.exclude_patterns) == 1

    def test_pattern_filter_include_match(self) -> None:
        """Test pattern filter with include patterns."""
        filter_obj = PatternFilter(include_patterns=[r".*important.*"])

        matching_entry = LogEntry(
            timestamp=datetime.now(),
            level=LogLevel.INFO,
            message="This is an important message",
            context=LogContext(),
        )

        non_matching_entry = LogEntry(
            timestamp=datetime.now(),
            level=LogLevel.INFO,
            message="This is a regular message",
            context=LogContext(),
        )

        assert filter_obj.should_log(matching_entry) is True
        assert filter_obj.should_log(non_matching_entry) is False

    def test_pattern_filter_exclude_match(self) -> None:
        """Test pattern filter with exclude patterns."""
        filter_obj = PatternFilter(exclude_patterns=[r".*debug.*"])

        excluded_entry = LogEntry(
            timestamp=datetime.now(),
            level=LogLevel.DEBUG,
            message="This is a debug message",
            context=LogContext(),
        )

        included_entry = LogEntry(
            timestamp=datetime.now(),
            level=LogLevel.INFO,
            message="This is an info message",
            context=LogContext(),
        )

        assert filter_obj.should_log(excluded_entry) is False
        assert filter_obj.should_log(included_entry) is True

    def test_pattern_filter_include_and_exclude(self) -> None:
        """Test pattern filter with both include and exclude patterns."""
        filter_obj = PatternFilter(include_patterns=[r".*api.*"], exclude_patterns=[r".*debug.*"])

        # Should be included (matches api pattern, doesn't match debug)
        api_entry = LogEntry(
            timestamp=datetime.now(),
            level=LogLevel.INFO,
            message="API call successful",
            context=LogContext(),
        )

        # Should be excluded (matches api pattern but also matches debug)
        api_debug_entry = LogEntry(
            timestamp=datetime.now(),
            level=LogLevel.DEBUG,
            message="API debug information",
            context=LogContext(),
        )

        # Should be excluded (doesn't match api pattern)
        other_entry = LogEntry(
            timestamp=datetime.now(),
            level=LogLevel.INFO,
            message="Other message",
            context=LogContext(),
        )

        assert filter_obj.should_log(api_entry) is True
        assert filter_obj.should_log(api_debug_entry) is False
        assert filter_obj.should_log(other_entry) is False


class TestRateLimitFilter:
    """Test RateLimitFilter functionality."""

    @pytest.mark.asyncio
    async def test_rate_limit_filter_creation(self) -> None:
        """Test RateLimitFilter creation."""
        filter_obj = RateLimitFilter(max_logs_per_minute=60, burst_size=10)

        assert filter_obj.max_logs_per_minute == 60
        assert filter_obj.burst_size == 10

    @pytest.mark.asyncio
    async def test_rate_limit_filter_allows_under_limit(self, sample_log_entries: Any) -> None:
        """Test rate limit filter allows logs under limit."""
        filter_obj = RateLimitFilter(
            max_logs_per_minute=100,  # High limit
            burst_size=20,
        )

        # Should allow multiple entries
        for entry in sample_log_entries:
            assert filter_obj.should_log(entry) is True

    @pytest.mark.asyncio
    async def test_rate_limit_filter_blocks_over_limit(self) -> None:
        """Test rate limit filter blocks logs over limit."""
        filter_obj = RateLimitFilter(
            max_logs_per_minute=2,  # Very low limit
            burst_size=1,
        )

        entry = LogEntry(
            timestamp=datetime.now(),
            level=LogLevel.INFO,
            message="Test message",
            context=LogContext(),
        )

        # First few should be allowed
        assert filter_obj.should_log(entry) is True

        # Subsequent ones should be blocked
        for _ in range(10):
            filter_obj.should_log(entry)
            # Some may be blocked due to rate limiting

        # Check that some were blocked
        assert filter_obj.blocked_count > 0

    @pytest.mark.asyncio
    async def test_rate_limit_filter_resets_over_time(self) -> None:
        """Test rate limit filter resets over time."""
        filter_obj = RateLimitFilter(max_logs_per_minute=1, burst_size=1)

        entry = LogEntry(
            timestamp=datetime.now(),
            level=LogLevel.INFO,
            message="Test message",
            context=LogContext(),
        )

        # Use up the limit
        assert filter_obj.should_log(entry) is True
        assert filter_obj.should_log(entry) is False  # Should be blocked

        # Simulate time passing by manually resetting (in real use, time would pass)
        filter_obj.last_reset = datetime.now() - timedelta(minutes=2)
        filter_obj.current_count = 0

        # Should allow again
        assert filter_obj.should_log(entry) is True


class TestRandomSampler:
    """Test RandomSampler functionality."""

    def test_random_sampler_creation(self) -> None:
        """Test RandomSampler creation."""
        sampler = RandomSampler(sample_rate=0.5)

        assert sampler.sample_rate == 0.5

    def test_random_sampler_rate_zero(self) -> None:
        """Test RandomSampler with 0% sample rate."""
        sampler = RandomSampler(sample_rate=0.0)

        entry = LogEntry(
            timestamp=datetime.now(),
            level=LogLevel.INFO,
            message="Test message",
            context=LogContext(),
        )

        # Should never sample
        for _ in range(100):
            assert sampler.should_sample(entry) is False

    def test_random_sampler_rate_one(self) -> None:
        """Test RandomSampler with 100% sample rate."""
        sampler = RandomSampler(sample_rate=1.0)

        entry = LogEntry(
            timestamp=datetime.now(),
            level=LogLevel.INFO,
            message="Test message",
            context=LogContext(),
        )

        # Should always sample
        for _ in range(100):
            assert sampler.should_sample(entry) is True

    @patch("random.random")
    def test_random_sampler_deterministic(self, mock_random: Any) -> None:
        """Test RandomSampler with deterministic random values."""
        sampler = RandomSampler(sample_rate=0.5)

        entry = LogEntry(
            timestamp=datetime.now(),
            level=LogLevel.INFO,
            message="Test message",
            context=LogContext(),
        )

        # Mock random to return 0.3 (< 0.5, should sample)
        mock_random.return_value = 0.3
        assert sampler.should_sample(entry) is True

        # Mock random to return 0.7 (> 0.5, should not sample)
        mock_random.return_value = 0.7
        assert sampler.should_sample(entry) is False


class TestTimeBasedSampler:
    """Test TimeBasedSampler functionality."""

    def test_time_based_sampler_creation(self) -> None:
        """Test TimeBasedSampler creation."""
        sampler = TimeBasedSampler(interval_seconds=60, max_per_interval=10)

        assert sampler.interval_seconds == 60
        assert sampler.max_per_interval == 10

    def test_time_based_sampler_under_limit(self) -> None:
        """Test TimeBasedSampler when under limit."""
        sampler = TimeBasedSampler(
            interval_seconds=60,
            max_per_interval=100,  # High limit
        )

        entry = LogEntry(
            timestamp=datetime.now(),
            level=LogLevel.INFO,
            message="Test message",
            context=LogContext(),
        )

        # Should sample multiple entries
        for _ in range(10):
            assert sampler.should_sample(entry) is True

    def test_time_based_sampler_over_limit(self) -> None:
        """Test TimeBasedSampler when over limit."""
        sampler = TimeBasedSampler(
            interval_seconds=60,
            max_per_interval=2,  # Low limit
        )

        entry = LogEntry(
            timestamp=datetime.now(),
            level=LogLevel.INFO,
            message="Test message",
            context=LogContext(),
        )

        # First 2 should be sampled
        assert sampler.should_sample(entry) is True
        assert sampler.should_sample(entry) is True

        # Subsequent ones should be rejected
        assert sampler.should_sample(entry) is False
        assert sampler.should_sample(entry) is False


class TestVolumeBasedSampler:
    """Test VolumeBasedSampler functionality."""

    def test_volume_based_sampler_creation(self) -> None:
        """Test VolumeBasedSampler creation."""
        sampler = VolumeBasedSampler(
            low_volume_threshold=10,
            high_volume_threshold=100,
            low_volume_rate=1.0,
            high_volume_rate=0.1,
        )

        assert sampler.low_volume_threshold == 10
        assert sampler.high_volume_threshold == 100
        assert sampler.low_volume_rate == 1.0
        assert sampler.high_volume_rate == 0.1

    @patch("random.random")
    def test_volume_based_sampler_low_volume(self, mock_random: Any) -> None:
        """Test VolumeBasedSampler during low volume."""
        sampler = VolumeBasedSampler(
            low_volume_threshold=10,
            high_volume_threshold=100,
            low_volume_rate=1.0,
            high_volume_rate=0.1,
        )

        entry = LogEntry(
            timestamp=datetime.now(),
            level=LogLevel.INFO,
            message="Test message",
            context=LogContext(),
        )

        # Simulate low volume (5 recent logs)
        sampler.recent_log_count = 5

        # Should use high sample rate (1.0) during low volume
        mock_random.return_value = 0.5
        assert sampler.should_sample(entry) is True

    @patch("random.random")
    def test_volume_based_sampler_high_volume(self, mock_random: Any) -> None:
        """Test VolumeBasedSampler during high volume."""
        sampler = VolumeBasedSampler(
            low_volume_threshold=10,
            high_volume_threshold=100,
            low_volume_rate=1.0,
            high_volume_rate=0.1,
        )

        entry = LogEntry(
            timestamp=datetime.now(),
            level=LogLevel.INFO,
            message="Test message",
            context=LogContext(),
        )

        # Simulate high volume by adding many entries to recent_logs
        now = datetime.now()
        for i in range(150):
            sampler.recent_logs.append(now - timedelta(seconds=i))

        # Should use low sample rate (0.1) during high volume
        mock_random.return_value = 0.05  # < 0.1, should sample
        assert sampler.should_sample(entry) is True

        mock_random.return_value = 0.2  # > 0.1, should not sample
        assert sampler.should_sample(entry) is False


class TestFilteredLogger:
    """Test FilteredLogger functionality."""

    @pytest.mark.asyncio
    async def test_filtered_logger_creation(self, config: Any, temp_log_file: Any) -> None:
        """Test FilteredLogger creation."""
        base_logger = StructuredLogger(
            config=config, log_file=temp_log_file, log_level=LogLevel.DEBUG
        )

        filtered_logger = FilteredLogger(base_logger=base_logger)

        assert filtered_logger.base_logger == base_logger
        assert len(filtered_logger.filters) == 0
        assert len(filtered_logger.samplers) == 0

    @pytest.mark.asyncio
    async def test_filtered_logger_add_filter(self, config: Any, temp_log_file: Any) -> None:
        """Test adding filters to FilteredLogger."""
        base_logger = StructuredLogger(
            config=config, log_file=temp_log_file, log_level=LogLevel.DEBUG
        )

        filtered_logger = FilteredLogger(base_logger=base_logger)

        level_filter = LevelFilter(min_level=LogLevel.WARNING)
        filtered_logger.add_filter(level_filter)

        assert len(filtered_logger.filters) == 1
        assert filtered_logger.filters[0] == level_filter

    @pytest.mark.asyncio
    async def test_filtered_logger_add_sampler(self, config: Any, temp_log_file: Any) -> None:
        """Test adding samplers to FilteredLogger."""
        base_logger = StructuredLogger(
            config=config, log_file=temp_log_file, log_level=LogLevel.DEBUG
        )

        filtered_logger = FilteredLogger(base_logger=base_logger)

        random_sampler = RandomSampler(sample_rate=0.5)
        filtered_logger.add_sampler(random_sampler)

        assert len(filtered_logger.samplers) == 1
        assert filtered_logger.samplers[0] == random_sampler

    @pytest.mark.asyncio
    async def test_filtered_logger_filtering(self, config: Any, temp_log_file: Any) -> None:
        """Test that FilteredLogger applies filters correctly."""
        base_logger = StructuredLogger(
            config=config, log_file=temp_log_file, log_level=LogLevel.DEBUG
        )
        await base_logger.start()

        filtered_logger = FilteredLogger(base_logger=base_logger)

        # Add filter that only allows WARNING and above
        level_filter = LevelFilter(min_level=LogLevel.WARNING)
        filtered_logger.add_filter(level_filter)

        # Add sampler that samples everything
        sampler = RandomSampler(sample_rate=1.0)
        filtered_logger.add_sampler(sampler)

        await filtered_logger.start()

        # Try to log at different levels
        await filtered_logger.debug("Debug message")  # Should be filtered out
        await filtered_logger.info("Info message")  # Should be filtered out
        await filtered_logger.warning("Warning message")  # Should pass through
        await filtered_logger.error("Error message")  # Should pass through

        await filtered_logger.stop()
        await base_logger.stop()

        # Check log file
        with open(temp_log_file) as f:
            content = f.read()
            assert "Debug message" not in content
            assert "Info message" not in content
            assert "Warning message" in content
            assert "Error message" in content

    @pytest.mark.asyncio
    async def test_filtered_logger_sampling(self, config: Any, temp_log_file: Any) -> None:
        """Test that FilteredLogger applies sampling correctly."""
        base_logger = StructuredLogger(
            config=config, log_file=temp_log_file, log_level=LogLevel.DEBUG
        )
        await base_logger.start()

        filtered_logger = FilteredLogger(base_logger=base_logger)

        # Add sampler that samples nothing
        sampler = RandomSampler(sample_rate=0.0)
        filtered_logger.add_sampler(sampler)

        await filtered_logger.start()

        # Try to log multiple messages
        for i in range(10):
            await filtered_logger.info(f"Message {i}")

        await filtered_logger.stop()
        await base_logger.stop()

        # Check that no messages were logged due to sampling
        with open(temp_log_file) as f:
            content = f.read()
            assert "Message" not in content

    @pytest.mark.asyncio
    async def test_filtered_logger_statistics(self, config: Any, temp_log_file: Any) -> None:
        """Test FilteredLogger statistics collection."""
        base_logger = StructuredLogger(
            config=config, log_file=temp_log_file, log_level=LogLevel.DEBUG
        )
        await base_logger.start()

        filtered_logger = FilteredLogger(base_logger=base_logger)

        # Add filter that blocks DEBUG logs
        level_filter = LevelFilter(min_level=LogLevel.INFO)
        filtered_logger.add_filter(level_filter)

        # Add sampler that samples 50%
        sampler = RandomSampler(sample_rate=0.5)
        filtered_logger.add_sampler(sampler)

        await filtered_logger.start()

        # Generate various log entries
        for _ in range(10):
            await filtered_logger.debug("Debug message")  # Will be filtered
            await filtered_logger.info("Info message")  # May be sampled

        stats = filtered_logger.get_statistics()

        assert stats is not None and stats["total_processed"] == 20
        assert stats["filtered_out"] >= 10  # At least debug messages
        assert stats["sampled_out"] >= 0  # Some may be sampled out
        assert stats["logged"] >= 0  # Some may get through

        await filtered_logger.stop()
        await base_logger.stop()


class TestIntegration:
    """Test integration scenarios."""

    @pytest.mark.asyncio
    async def test_complex_filtering_and_sampling(self, config: Any, temp_log_file: Any) -> None:
        """Test complex scenario with multiple filters and samplers."""
        base_logger = StructuredLogger(
            config=config, log_file=temp_log_file, log_level=LogLevel.DEBUG
        )
        await base_logger.start()

        filtered_logger = FilteredLogger(base_logger=base_logger)

        # Add multiple filters
        level_filter = LevelFilter(min_level=LogLevel.INFO)
        pattern_filter = PatternFilter(include_patterns=[r".*important.*"])
        rate_filter = RateLimitFilter(max_logs_per_minute=30, burst_size=5)

        filtered_logger.add_filter(level_filter)
        filtered_logger.add_filter(pattern_filter)
        filtered_logger.add_filter(rate_filter)

        # Add sampler
        sampler = RandomSampler(sample_rate=0.8)  # 80% sample rate
        filtered_logger.add_sampler(sampler)

        await filtered_logger.start()

        # Generate test logs
        test_messages = [
            ("DEBUG", "Debug message"),  # Filtered by level
            ("INFO", "Regular info message"),  # Filtered by pattern
            ("INFO", "Important info message"),  # Should pass filters
            ("WARNING", "Important warning"),  # Should pass filters
            ("ERROR", "Important error occurred"),  # Should pass filters
        ]

        for level_str, message in test_messages:
            level = LogLevel[level_str]
            if level == LogLevel.DEBUG:
                await filtered_logger.debug(message)
            elif level == LogLevel.INFO:
                await filtered_logger.info(message)
            elif level == LogLevel.WARNING:
                await filtered_logger.warning(message)
            elif level == LogLevel.ERROR:
                await filtered_logger.error(message)

        stats = filtered_logger.get_statistics()

        # Verify that filtering and sampling occurred
        assert stats is not None and stats["total_processed"] == 5
        assert stats["filtered_out"] >= 2  # At least debug + non-important info

        await filtered_logger.stop()
        await base_logger.stop()

        # Verify that only important messages made it through
        with open(temp_log_file) as f:
            content = f.read()
            assert "Debug message" not in content
            assert "Regular info message" not in content
            # Important messages may or may not be there due to sampling

    @pytest.mark.asyncio
    async def test_high_volume_scenario(self, config: Any, temp_log_file: Any) -> None:
        """Test filtering and sampling under high volume."""
        base_logger = StructuredLogger(
            config=config, log_file=temp_log_file, log_level=LogLevel.DEBUG
        )
        await base_logger.start()

        filtered_logger = FilteredLogger(base_logger=base_logger)

        # Add volume-based sampler
        volume_sampler = VolumeBasedSampler(
            low_volume_threshold=10,
            high_volume_threshold=50,
            low_volume_rate=1.0,
            high_volume_rate=0.1,
        )
        filtered_logger.add_sampler(volume_sampler)

        await filtered_logger.start()

        # Generate high volume of logs
        for i in range(100):
            await filtered_logger.info(f"High volume message {i}")

        stats = filtered_logger.get_statistics()

        # Should have processed all messages
        assert stats is not None and stats["total_processed"] == 100

        # Should have sampled out many due to high volume
        assert stats["sampled_out"] > 50

        await filtered_logger.stop()
        await base_logger.stop()

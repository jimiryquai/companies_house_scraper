"""
Tests for streaming configuration management.
"""

import pytest
from pydantic import ValidationError

from src.streaming.config import StreamingConfig, load_streaming_config


class TestStreamingConfig:
    """Test StreamingConfig validation and loading."""

    def test_valid_config(self) -> None:
        """Test that valid configuration is accepted."""
        config = StreamingConfig(
            streaming_api_key="test-api-key-123456",
            rest_api_key="test-rest-api-key-123456",
            api_base_url="https://stream.companieshouse.gov.uk",
            connection_timeout=30,
            max_retries=5,
            log_level="INFO",
        )

        assert config.streaming_api_key == "test-api-key-123456"
        assert config.api_base_url == "https://stream.companieshouse.gov.uk"
        assert config.connection_timeout == 30
        assert config.max_retries == 5
        assert config.log_level == "INFO"

    def test_invalid_api_key(self) -> None:
        """Test that invalid API key raises validation error."""
        with pytest.raises(ValidationError) as exc_info:
            StreamingConfig(streaming_api_key="short", rest_api_key="test-rest-key-123456")

        assert "Invalid streaming API key" in str(exc_info.value)

    def test_invalid_max_retries(self) -> None:
        """Test that invalid max_retries raises validation error."""
        with pytest.raises(ValidationError) as exc_info:
            StreamingConfig(
                streaming_api_key="test-api-key-123456",
                rest_api_key="test-rest-key-123456",
                max_retries=15,
            )

        assert "max_retries must be between 1 and 10" in str(exc_info.value)

    def test_invalid_log_level(self) -> None:
        """Test that invalid log level raises validation error."""
        with pytest.raises(ValidationError) as exc_info:
            StreamingConfig(
                streaming_api_key="test-api-key-123456",
                rest_api_key="test-rest-key-123456",
                log_level="INVALID",
            )

        assert "log_level must be one of" in str(exc_info.value)

    def test_default_values(self) -> None:
        """Test that default values are correctly set."""
        config = StreamingConfig(
            streaming_api_key="test-api-key-123456", rest_api_key="test-rest-key-123456"
        )

        assert config.api_base_url == "https://stream.companieshouse.gov.uk"
        assert config.connection_timeout == 30
        assert config.max_retries == 5
        assert config.initial_backoff == 1.0
        assert config.max_backoff == 300.0
        assert config.batch_size == 100
        assert config.health_check_interval == 60
        assert config.database_path == "companies.db"
        assert config.log_level == "INFO"
        assert config.log_file is None

    def test_environment_variable_loading(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test that configuration loads from environment variables."""
        monkeypatch.setenv("STREAMING_STREAMING_API_KEY", "env-api-key-123456")
        monkeypatch.setenv("STREAMING_REST_API_KEY", "env-rest-api-key-123456")
        monkeypatch.setenv("STREAMING_CONNECTION_TIMEOUT", "60")
        monkeypatch.setenv("STREAMING_LOG_LEVEL", "DEBUG")

        config = load_streaming_config()

        assert config.streaming_api_key == "env-api-key-123456"
        assert config.connection_timeout == 60
        assert config.log_level == "DEBUG"

    def test_load_streaming_config_function(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test the load_streaming_config function."""
        monkeypatch.setenv("STREAMING_STREAMING_API_KEY", "function-test-key-123456")
        monkeypatch.setenv("STREAMING_REST_API_KEY", "function-rest-key-123456")

        config = load_streaming_config()

        assert isinstance(config, StreamingConfig)
        assert config.streaming_api_key == "function-test-key-123456"

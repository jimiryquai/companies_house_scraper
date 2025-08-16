"""Configuration management for Companies House Streaming API integration."""

import os
from typing import Optional

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class StreamingConfig(BaseSettings):
    """Configuration for Companies House Streaming API."""

    # API Configuration
    streaming_api_key: str = Field(..., description="Companies House streaming API key")
    api_base_url: str = Field(
        default="https://stream.companieshouse.gov.uk", description="Base URL for streaming API"
    )

    # Connection Configuration
    connection_timeout: int = Field(default=30, description="Connection timeout in seconds")
    max_retries: int = Field(default=5, description="Maximum connection retry attempts")
    initial_backoff: float = Field(default=1.0, description="Initial backoff delay in seconds")
    max_backoff: float = Field(default=300.0, description="Maximum backoff delay in seconds")

    # Processing Configuration
    batch_size: int = Field(default=100, description="Number of events to process in batch")
    health_check_interval: int = Field(default=60, description="Health check interval in seconds")

    # Rate Limiting Configuration
    rate_limit_requests_per_minute: int = Field(
        default=60, description="Maximum requests per minute for rate limiting"
    )

    # Database Configuration
    database_path: str = Field(default="companies.db", description="SQLite database path")

    # Logging Configuration
    log_level: str = Field(default="INFO", description="Logging level")
    log_file: Optional[str] = Field(default=None, description="Log file path")

    model_config = SettingsConfigDict(env_prefix="STREAMING_", case_sensitive=False)

    @field_validator("streaming_api_key")
    @classmethod
    def validate_api_key(cls, v: str) -> str:
        if not v or len(v) < 10:
            raise ValueError("Invalid streaming API key")
        return v

    @field_validator("max_retries")
    @classmethod
    def validate_max_retries(cls, v: int) -> int:
        if v < 1 or v > 10:
            raise ValueError("max_retries must be between 1 and 10")
        return v

    @field_validator("log_level")
    @classmethod
    def validate_log_level(cls, v: str) -> str:
        valid_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        if v.upper() not in valid_levels:
            raise ValueError(f"log_level must be one of {valid_levels}")
        return v.upper()


def load_streaming_config() -> StreamingConfig:
    """Load streaming configuration from environment variables and defaults."""
    # Required parameter must be provided
    api_key = os.environ.get("STREAMING_STREAMING_API_KEY", os.environ.get("STREAMING_API_KEY", ""))
    if not api_key:
        raise ValueError("STREAMING_API_KEY environment variable is required")
    return StreamingConfig(streaming_api_key=api_key)

"""Configuration models for Snov.io integration.

This module defines comprehensive configuration models that extend
existing streaming configuration patterns for Snov.io operations.
"""

import os
from typing import Optional

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class SnovioConfig(BaseSettings):
    """Configuration for Snov.io API integration.

    Extends the existing StreamingConfig pattern with Snov.io specific
    configuration options for API access, webhook handling, and credit management.
    """

    # API Authentication
    snov_client_id: str = Field(..., description="Snov.io OAuth client ID")
    snov_client_secret: str = Field(..., description="Snov.io OAuth client secret")
    snov_api_base_url: str = Field(
        default="https://api.snov.io/v1", description="Snov.io API base URL"
    )

    # Webhook Configuration
    webhook_base_url: str = Field(..., description="Base URL for webhook endpoints")
    webhook_secret: str = Field(..., description="Webhook signature verification secret")
    webhook_timeout: int = Field(default=30, description="Webhook request timeout in seconds")

    # Credit Management
    credit_threshold_warning: int = Field(
        default=100, description="Credit level to trigger warnings"
    )
    credit_threshold_critical: int = Field(
        default=20, description="Credit level to pause non-critical operations"
    )
    credit_check_interval: int = Field(
        default=300, description="Credit check interval in seconds (5 minutes)"
    )
    max_daily_credit_consumption: Optional[int] = Field(
        None, description="Maximum credits to consume per day (None = unlimited)"
    )

    # Rate Limiting and Retry Configuration
    api_rate_limit_per_second: int = Field(
        default=10, description="Maximum API requests per second"
    )
    max_retry_attempts: int = Field(default=3, description="Maximum retry attempts for API calls")
    retry_base_delay: float = Field(
        default=1.0, description="Base delay for retry attempts in seconds"
    )
    retry_max_delay: float = Field(
        default=60.0, description="Maximum delay for retry attempts in seconds"
    )

    # Operational Configuration
    queue_batch_size: int = Field(default=50, description="Batch size for queue processing")
    webhook_processing_timeout: int = Field(
        default=300, description="Timeout for webhook processing in seconds"
    )
    domain_search_timeout: int = Field(
        default=120, description="Timeout for domain search operations in seconds"
    )
    email_finder_timeout: int = Field(
        default=60, description="Timeout for email finder operations in seconds"
    )

    # Cache and Performance
    cache_ttl_minutes: int = Field(
        default=1440, description="Cache TTL in minutes (24 hours default)"
    )
    enable_result_caching: bool = Field(default=True, description="Enable caching of API results")
    max_concurrent_requests: int = Field(default=5, description="Maximum concurrent API requests")

    # Monitoring and Alerting
    enable_operational_metrics: bool = Field(
        default=True, description="Enable operational metrics collection"
    )
    metrics_retention_days: int = Field(default=30, description="Metrics retention period in days")
    alert_on_credit_threshold: bool = Field(
        default=True, description="Send alerts when credit thresholds are reached"
    )
    alert_on_api_errors: bool = Field(default=True, description="Send alerts on API error rates")

    # Database Configuration (extends StreamingConfig)
    database_connection_timeout: int = Field(
        default=30, description="Database connection timeout in seconds"
    )
    database_query_timeout: int = Field(default=60, description="Database query timeout in seconds")

    model_config = SettingsConfigDict(env_prefix="SNOV_", case_sensitive=False)

    @field_validator("snov_client_id")
    @classmethod
    def validate_client_id(cls, v: str) -> str:
        """Validate Snov.io client ID."""
        if not v or len(v) < 10:
            raise ValueError("Invalid Snov.io client ID")
        return v

    @field_validator("snov_client_secret")
    @classmethod
    def validate_client_secret(cls, v: str) -> str:
        """Validate Snov.io client secret."""
        if not v or len(v) < 20:
            raise ValueError("Invalid Snov.io client secret")
        return v

    @field_validator("webhook_base_url")
    @classmethod
    def validate_webhook_base_url(cls, v: str) -> str:
        """Validate webhook base URL."""
        if not v.startswith(("http://", "https://")):
            raise ValueError("Webhook base URL must include protocol")
        return v.rstrip("/")

    @field_validator("webhook_secret")
    @classmethod
    def validate_webhook_secret(cls, v: str) -> str:
        """Validate webhook secret."""
        if not v or len(v) < 16:
            raise ValueError("Webhook secret must be at least 16 characters")
        return v

    @field_validator("credit_threshold_warning")
    @classmethod
    def validate_credit_threshold_warning(cls, v: int) -> int:
        """Validate warning credit threshold."""
        if v < 0:
            raise ValueError("Credit threshold cannot be negative")
        return v

    @field_validator("credit_threshold_critical")
    @classmethod
    def validate_credit_threshold_critical(cls, v: int) -> int:
        """Validate critical credit threshold."""
        if v < 0:
            raise ValueError("Critical credit threshold cannot be negative")
        return v

    @field_validator("api_rate_limit_per_second")
    @classmethod
    def validate_api_rate_limit(cls, v: int) -> int:
        """Validate API rate limit."""
        if v < 1 or v > 100:
            raise ValueError("API rate limit must be between 1 and 100 requests per second")
        return v

    @field_validator("max_retry_attempts")
    @classmethod
    def validate_max_retry_attempts(cls, v: int) -> int:
        """Validate max retry attempts."""
        if v < 0 or v > 10:
            raise ValueError("Max retry attempts must be between 0 and 10")
        return v

    @field_validator("queue_batch_size")
    @classmethod
    def validate_queue_batch_size(cls, v: int) -> int:
        """Validate queue batch size."""
        if v < 1 or v > 1000:
            raise ValueError("Queue batch size must be between 1 and 1000")
        return v

    @field_validator("max_concurrent_requests")
    @classmethod
    def validate_max_concurrent_requests(cls, v: int) -> int:
        """Validate max concurrent requests."""
        if v < 1 or v > 50:
            raise ValueError("Max concurrent requests must be between 1 and 50")
        return v

    def get_webhook_url(self, endpoint: str) -> str:
        """Get full webhook URL for an endpoint.

        Args:
            endpoint: Webhook endpoint path

        Returns:
            Full webhook URL
        """
        return f"{self.webhook_base_url.rstrip('/')}/{endpoint.lstrip('/')}"

    def is_credit_level_critical(self, current_credits: int) -> bool:
        """Check if current credit level is critical.

        Args:
            current_credits: Current available credits

        Returns:
            True if credits are at critical level
        """
        return current_credits <= self.credit_threshold_critical

    def is_credit_level_warning(self, current_credits: int) -> bool:
        """Check if current credit level warrants warning.

        Args:
            current_credits: Current available credits

        Returns:
            True if credits are at warning level
        """
        return current_credits <= self.credit_threshold_warning


class WebhookConfig(BaseSettings):
    """Configuration specific to webhook handling."""

    # Webhook Endpoints
    domain_search_webhook_path: str = Field(
        default="/webhooks/snov/domain-search",
        description="Webhook path for domain search completions",
    )
    email_finder_webhook_path: str = Field(
        default="/webhooks/snov/email-finder",
        description="Webhook path for email finder completions",
    )
    bulk_operation_webhook_path: str = Field(
        default="/webhooks/snov/bulk-operations",
        description="Webhook path for bulk operation completions",
    )

    # Security and Validation
    require_signature_verification: bool = Field(
        default=True, description="Require webhook signature verification"
    )
    max_webhook_body_size: int = Field(
        default=1024 * 1024, description="Maximum webhook body size in bytes (1MB)"
    )
    webhook_ip_whitelist: Optional[str] = Field(
        None, description="Comma-separated list of allowed IP addresses"
    )

    # Processing Configuration
    webhook_queue_size: int = Field(default=1000, description="Maximum webhook queue size")
    webhook_worker_count: int = Field(default=3, description="Number of webhook processing workers")
    failed_webhook_retry_count: int = Field(
        default=3, description="Number of retries for failed webhook processing"
    )

    model_config = SettingsConfigDict(env_prefix="WEBHOOK_", case_sensitive=False)

    def get_allowed_ips(self) -> list[str]:
        """Get list of allowed IP addresses for webhooks.

        Returns:
            List of allowed IP addresses
        """
        if not self.webhook_ip_whitelist:
            return []
        return [ip.strip() for ip in self.webhook_ip_whitelist.split(",")]


class IntegrationConfig(BaseSettings):
    """Configuration for Companies House + Snov.io integration."""

    # Integration Strategy
    enable_automatic_domain_discovery: bool = Field(
        default=True, description="Automatically discover domains for new companies"
    )
    enable_automatic_email_discovery: bool = Field(
        default=True, description="Automatically discover emails for officers"
    )
    enable_webhook_processing: bool = Field(
        default=True, description="Enable webhook-based result processing"
    )

    # Discovery Thresholds
    min_confidence_score_domain: float = Field(
        default=0.6, description="Minimum confidence score for domain acceptance"
    )
    min_confidence_score_email: float = Field(
        default=0.5, description="Minimum confidence score for email acceptance"
    )
    max_domains_per_company: int = Field(
        default=5, description="Maximum domains to store per company"
    )
    max_emails_per_officer: int = Field(
        default=3, description="Maximum emails to store per officer"
    )

    # Processing Priorities
    prioritize_strike_off_companies: bool = Field(
        default=True, description="Give priority to companies with strike-off status"
    )
    delay_bulk_operations_hours: int = Field(
        default=2, description="Hours to delay bulk operations after company discovery"
    )

    # Quality Control
    enable_email_verification: bool = Field(
        default=True, description="Enable email verification for discovered emails"
    )
    skip_disposable_emails: bool = Field(
        default=True, description="Skip storing disposable email addresses"
    )
    require_domain_verification: bool = Field(
        default=False, description="Require domain verification before email discovery"
    )

    model_config = SettingsConfigDict(env_prefix="INTEGRATION_", case_sensitive=False)

    @field_validator("min_confidence_score_domain", "min_confidence_score_email")
    @classmethod
    def validate_confidence_score(cls, v: float) -> float:
        """Validate confidence score range."""
        if v < 0.0 or v > 1.0:
            raise ValueError("Confidence score must be between 0.0 and 1.0")
        return v

    @field_validator("max_domains_per_company", "max_emails_per_officer")
    @classmethod
    def validate_max_items(cls, v: int) -> int:
        """Validate maximum item counts."""
        if v < 1 or v > 100:
            raise ValueError("Maximum items must be between 1 and 100")
        return v


def load_snov_config() -> SnovioConfig:
    """Load Snov.io configuration from environment variables and defaults.

    Returns:
        Configured SnovioConfig instance

    Raises:
        ValueError: If required configuration is missing
    """
    # Check required environment variables
    required_vars = [
        "SNOV_CLIENT_ID",
        "SNOV_CLIENT_SECRET",
        "SNOV_WEBHOOK_BASE_URL",
        "SNOV_WEBHOOK_SECRET",
    ]

    missing_vars = []
    for var in required_vars:
        if not os.environ.get(var):
            missing_vars.append(var)

    if missing_vars:
        raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")

    return SnovioConfig()


def load_webhook_config() -> WebhookConfig:
    """Load webhook configuration from environment variables and defaults.

    Returns:
        Configured WebhookConfig instance
    """
    return WebhookConfig()


def load_integration_config() -> IntegrationConfig:
    """Load integration configuration from environment variables and defaults.

    Returns:
        Configured IntegrationConfig instance
    """
    return IntegrationConfig()


def load_all_configs() -> tuple[SnovioConfig, WebhookConfig, IntegrationConfig]:
    """Load all Snov.io related configurations.

    Returns:
        Tuple of (SnovioConfig, WebhookConfig, IntegrationConfig)
    """
    return (load_snov_config(), load_webhook_config(), load_integration_config())

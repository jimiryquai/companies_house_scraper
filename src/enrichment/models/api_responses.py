"""Pydantic models for Snov.io API responses.

This module defines comprehensive data models for all Snov.io API responses,
providing type safety, validation, and documentation for API interactions.
"""

from datetime import datetime
from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel, Field, field_validator


class EmailData(BaseModel):
    """Individual email data from Snov.io API responses."""

    email: str = Field(..., description="Email address found")
    first_name: Optional[str] = Field(None, description="First name associated with email")
    last_name: Optional[str] = Field(None, description="Last name associated with email")
    position: Optional[str] = Field(None, description="Job position/title")
    type: Optional[str] = Field(None, description="Email type (personal, work, etc.)")
    confidence: Optional[int] = Field(None, description="Confidence score (0-100)")
    sources: Optional[List[str]] = Field(None, description="Data sources for email")


class DomainSearchResponse(BaseModel):
    """Response model for Snov.io Domain Search API.

    Used for finding emails associated with a specific domain.
    Maps to company_domains and officer_emails database tables.
    """

    success: bool = Field(..., description="Whether the request was successful")
    domain: str = Field(..., description="Domain that was searched")
    webmail: bool = Field(default=False, description="Whether domain uses webmail")
    result: int = Field(default=0, description="Number of emails found")
    emails: List[EmailData] = Field(default_factory=list, description="List of found emails")
    count: int = Field(default=0, description="Total count of results")
    limit: int = Field(default=100, description="Limit applied to search")
    credits_left: Optional[int] = Field(None, description="Remaining API credits")
    request_id: Optional[str] = Field(None, description="Snov.io request ID for tracking")

    @field_validator("domain")
    @classmethod
    def validate_domain(cls, v: str) -> str:
        """Validate domain format."""
        if not v or not v.strip():
            raise ValueError("Domain cannot be empty")
        # Basic domain validation
        v = v.strip().lower()
        if not v.replace("-", "").replace(".", "").replace("_", "").replace("/", "").isalnum():
            raise ValueError("Invalid domain format")
        return v

    @field_validator("emails")
    @classmethod
    def validate_emails(cls, v: List[EmailData]) -> List[EmailData]:
        """Validate email data list."""
        # Remove duplicates based on email address
        seen_emails = set()
        unique_emails = []
        for email_data in v:
            if email_data.email not in seen_emails:
                seen_emails.add(email_data.email)
                unique_emails.append(email_data)
        return unique_emails


class EmailFinderResponse(BaseModel):
    """Response model for Snov.io Email Finder API.

    Used for finding specific person's email based on name and domain.
    Maps to officer_emails database table.
    """

    success: bool = Field(..., description="Whether the request was successful")
    email: Optional[str] = Field(None, description="Found email address")
    first_name: Optional[str] = Field(None, description="First name used in search")
    last_name: Optional[str] = Field(None, description="Last name used in search")
    domain: str = Field(..., description="Domain used in search")
    confidence: Optional[int] = Field(None, description="Confidence score (0-100)")
    sources: Optional[List[str]] = Field(None, description="Data sources for email")
    position: Optional[str] = Field(None, description="Job position found")
    credits_left: Optional[int] = Field(None, description="Remaining API credits")
    request_id: Optional[str] = Field(None, description="Snov.io request ID for tracking")

    @field_validator("email")
    @classmethod
    def validate_email(cls, v: Optional[str]) -> Optional[str]:
        """Validate email format."""
        if v is None:
            return v
        v = v.strip().lower()
        if "@" not in v or "." not in v:
            raise ValueError("Invalid email format")
        return v

    @field_validator("confidence")
    @classmethod
    def validate_confidence(cls, v: Optional[int]) -> Optional[int]:
        """Validate confidence score range."""
        if v is not None and (v < 0 or v > 100):
            raise ValueError("Confidence score must be between 0 and 100")
        return v


class WebhookEventData(BaseModel):
    """Webhook event data payload."""

    request_id: str = Field(..., description="Original request ID")
    status: str = Field(..., description="Operation status (completed, failed, etc.)")
    results_count: int = Field(default=0, description="Number of results found")
    credits_used: int = Field(default=0, description="Credits consumed by operation")
    error_message: Optional[str] = Field(None, description="Error message if failed")
    download_url: Optional[str] = Field(None, description="URL to download results")


class WebhookEventPayload(BaseModel):
    """Complete webhook event payload from Snov.io.

    Used for processing webhook notifications from bulk operations.
    Maps to snov_webhooks database table.
    """

    event_type: str = Field(..., description="Type of webhook event")
    event_id: str = Field(..., description="Unique event identifier")
    timestamp: datetime = Field(..., description="Event timestamp")
    data: WebhookEventData = Field(..., description="Event data payload")
    signature: Optional[str] = Field(None, description="Webhook signature for verification")

    @field_validator("event_type")
    @classmethod
    def validate_event_type(cls, v: str) -> str:
        """Validate event type."""
        valid_types = [
            "bulk_email_finder_completed",
            "bulk_domain_search_completed",
            "email_verification_completed",
            "operation_failed",
        ]
        if v not in valid_types:
            raise ValueError(f"Invalid event type. Must be one of: {valid_types}")
        return v


class CreditUsageResponse(BaseModel):
    """Response model for credit usage tracking.

    Used for monitoring API credit consumption.
    Maps to snov_credit_usage database table.
    """

    credits_left: int = Field(..., description="Remaining API credits")
    credits_used: int = Field(default=0, description="Credits used in current operation")
    monthly_limit: Optional[int] = Field(None, description="Monthly credit limit")
    reset_date: Optional[datetime] = Field(None, description="Credit reset date")
    operation_type: str = Field(..., description="Type of operation consuming credits")
    timestamp: datetime = Field(default_factory=datetime.utcnow, description="Usage timestamp")

    @field_validator("credits_left")
    @classmethod
    def validate_credits_left(cls, v: int) -> int:
        """Validate credits remaining."""
        if v < 0:
            raise ValueError("Credits left cannot be negative")
        return v

    @field_validator("operation_type")
    @classmethod
    def validate_operation_type(cls, v: str) -> str:
        """Validate operation type."""
        valid_operations = [
            "domain_search",
            "email_finder",
            "email_verifier",
            "bulk_email_finder",
            "bulk_domain_search",
        ]
        if v not in valid_operations:
            raise ValueError(f"Invalid operation type. Must be one of: {valid_operations}")
        return v


class BulkOperationResponse(BaseModel):
    """Response model for bulk operations (async operations with webhooks)."""

    success: bool = Field(..., description="Whether the request was initiated successfully")
    request_id: str = Field(..., description="Request ID for tracking")
    estimated_credits: int = Field(..., description="Estimated credits to be consumed")
    webhook_url: Optional[str] = Field(None, description="Webhook URL for completion notification")
    status: str = Field(default="pending", description="Current operation status")
    estimated_completion: Optional[datetime] = Field(None, description="Estimated completion time")

    @field_validator("status")
    @classmethod
    def validate_status(cls, v: str) -> str:
        """Validate operation status."""
        valid_statuses = ["pending", "processing", "completed", "failed", "cancelled"]
        if v not in valid_statuses:
            raise ValueError(f"Invalid status. Must be one of: {valid_statuses}")
        return v


class ErrorResponse(BaseModel):
    """Error response from Snov.io API."""

    success: bool = Field(default=False, description="Success flag (always False for errors)")
    error: str = Field(..., description="Error message")
    error_code: Optional[str] = Field(None, description="Snov.io error code")
    details: Optional[Dict[str, Any]] = Field(None, description="Additional error details")
    request_id: Optional[str] = Field(None, description="Request ID if available")


# Union type for all possible API responses
SnovioAPIResponse = Union[
    DomainSearchResponse,
    EmailFinderResponse,
    WebhookEventPayload,
    CreditUsageResponse,
    BulkOperationResponse,
    ErrorResponse,
]

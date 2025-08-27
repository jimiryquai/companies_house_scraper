"""Database ORM models for Snov.io integration.

This module defines comprehensive database models that map to the
Snov.io integration tables, providing type safety and validation
for database operations.
"""

import sqlite3
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel, Field, field_validator


class CompanyDomain(BaseModel):
    """Model for company_domains table.

    Represents domains associated with companies discovered through
    Snov.io domain search or manual entry.
    """

    id: Optional[int] = Field(None, description="Primary key")
    company_id: str = Field(..., description="Companies House company number")
    domain: str = Field(..., description="Domain name")
    confidence_score: Optional[float] = Field(None, description="Confidence score (0.0-1.0)")
    discovery_method: str = Field(..., description="How domain was discovered")
    discovered_at: datetime = Field(
        default_factory=datetime.utcnow, description="Discovery timestamp"
    )
    last_verified: Optional[datetime] = Field(None, description="Last verification timestamp")
    is_primary: bool = Field(default=False, description="Whether this is the primary domain")
    status: str = Field(default="active", description="Domain status")
    created_at: datetime = Field(
        default_factory=datetime.utcnow, description="Record creation timestamp"
    )
    updated_at: datetime = Field(
        default_factory=datetime.utcnow, description="Last update timestamp"
    )

    @field_validator("domain")
    @classmethod
    def validate_domain(cls, v: str) -> str:
        """Validate domain format."""
        v = v.strip().lower()
        if not v:
            raise ValueError("Domain cannot be empty")
        return v

    @field_validator("confidence_score")
    @classmethod
    def validate_confidence_score(cls, v: Optional[float]) -> Optional[float]:
        """Validate confidence score range."""
        if v is not None and (v < 0.0 or v > 1.0):
            raise ValueError("Confidence score must be between 0.0 and 1.0")
        return v

    @field_validator("discovery_method")
    @classmethod
    def validate_discovery_method(cls, v: str) -> str:
        """Validate discovery method."""
        valid_methods = ["snov_domain_search", "manual", "inferred", "companies_house"]
        if v not in valid_methods:
            raise ValueError(f"Invalid discovery method. Must be one of: {valid_methods}")
        return v

    @field_validator("status")
    @classmethod
    def validate_status(cls, v: str) -> str:
        """Validate domain status."""
        valid_statuses = ["active", "inactive", "unverified", "invalid"]
        if v not in valid_statuses:
            raise ValueError(f"Invalid status. Must be one of: {valid_statuses}")
        return v


class OfficerEmail(BaseModel):
    """Model for officer_emails table.

    Represents email addresses associated with company officers
    discovered through Snov.io email finder or other methods.
    """

    id: Optional[int] = Field(None, description="Primary key")
    officer_id: str = Field(..., description="Officer ID from Companies House")
    email: str = Field(..., description="Email address")
    email_type: Optional[str] = Field(None, description="Type of email")
    verification_status: str = Field(default="unknown", description="Email verification status")
    confidence_score: Optional[float] = Field(None, description="Confidence score (0.0-1.0)")
    domain: Optional[str] = Field(None, description="Email domain")
    discovery_method: str = Field(..., description="How email was discovered")
    discovered_at: datetime = Field(
        default_factory=datetime.utcnow, description="Discovery timestamp"
    )
    last_verified: Optional[datetime] = Field(None, description="Last verification timestamp")
    snov_request_id: Optional[str] = Field(None, description="Snov.io request ID for tracking")
    created_at: datetime = Field(
        default_factory=datetime.utcnow, description="Record creation timestamp"
    )
    updated_at: datetime = Field(
        default_factory=datetime.utcnow, description="Last update timestamp"
    )

    @field_validator("email")
    @classmethod
    def validate_email(cls, v: str) -> str:
        """Validate email format."""
        v = v.strip().lower()
        if not v or "@" not in v or "." not in v:
            raise ValueError("Invalid email format")
        return v

    @field_validator("email_type")
    @classmethod
    def validate_email_type(cls, v: Optional[str]) -> Optional[str]:
        """Validate email type."""
        if v is None:
            return v
        valid_types = ["work", "personal", "generic", "catch_all"]
        if v not in valid_types:
            raise ValueError(f"Invalid email type. Must be one of: {valid_types}")
        return v

    @field_validator("verification_status")
    @classmethod
    def validate_verification_status(cls, v: str) -> str:
        """Validate verification status."""
        valid_statuses = ["valid", "invalid", "catch_all", "unknown", "disposable", "risky"]
        if v not in valid_statuses:
            raise ValueError(f"Invalid verification status. Must be one of: {valid_statuses}")
        return v

    @field_validator("discovery_method")
    @classmethod
    def validate_discovery_method(cls, v: str) -> str:
        """Validate discovery method."""
        valid_methods = ["snov_email_finder", "snov_bulk_search", "manual", "inferred"]
        if v not in valid_methods:
            raise ValueError(f"Invalid discovery method. Must be one of: {valid_methods}")
        return v

    @field_validator("confidence_score")
    @classmethod
    def validate_confidence_score(cls, v: Optional[float]) -> Optional[float]:
        """Validate confidence score range."""
        if v is not None and (v < 0.0 or v > 1.0):
            raise ValueError("Confidence score must be between 0.0 and 1.0")
        return v


class SnovCreditUsage(BaseModel):
    """Model for snov_credit_usage table.

    Tracks credit consumption for operational monitoring and budget management.
    """

    id: Optional[int] = Field(None, description="Primary key")
    operation_type: str = Field(..., description="Type of Snov.io operation")
    credits_consumed: int = Field(..., description="Number of credits used")
    success: bool = Field(..., description="Whether operation was successful")
    request_id: Optional[str] = Field(None, description="Snov.io request ID")
    company_id: Optional[str] = Field(None, description="Associated company ID")
    officer_id: Optional[str] = Field(None, description="Associated officer ID")
    response_data: Optional[str] = Field(None, description="JSON response for audit trail")
    timestamp: datetime = Field(default_factory=datetime.utcnow, description="Usage timestamp")

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

    @field_validator("credits_consumed")
    @classmethod
    def validate_credits_consumed(cls, v: int) -> int:
        """Validate credits consumed."""
        if v < 0:
            raise ValueError("Credits consumed cannot be negative")
        return v


class SnovWebhook(BaseModel):
    """Model for snov_webhooks table.

    Manages webhook events from Snov.io for async operation tracking.
    """

    id: Optional[int] = Field(None, description="Primary key")
    webhook_id: str = Field(..., description="Unique webhook identifier")
    event_type: str = Field(..., description="Type of webhook event")
    status: str = Field(default="received", description="Processing status")
    request_id: Optional[str] = Field(None, description="Original Snov.io request ID")
    payload: str = Field(..., description="Full webhook payload JSON")
    processed_at: Optional[datetime] = Field(None, description="Processing completion timestamp")
    error_message: Optional[str] = Field(None, description="Error message if processing failed")
    created_at: datetime = Field(default_factory=datetime.utcnow, description="Receipt timestamp")

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

    @field_validator("status")
    @classmethod
    def validate_status(cls, v: str) -> str:
        """Validate processing status."""
        valid_statuses = ["received", "processed", "error", "ignored"]
        if v not in valid_statuses:
            raise ValueError(f"Invalid status. Must be one of: {valid_statuses}")
        return v


@dataclass
class DatabaseConnection:
    """Database connection management for Snov.io models.

    Provides SQLite-specific operations for the Snov.io integration models.
    """

    db_path: str
    connection: Optional[sqlite3.Connection] = field(default=None, init=False)

    def connect(self) -> sqlite3.Connection:
        """Establish database connection with proper configuration."""
        if self.connection is None:
            self.connection = sqlite3.connect(self.db_path, check_same_thread=False, timeout=30.0)
            self.connection.row_factory = sqlite3.Row
            # Enable foreign key constraints
            self.connection.execute("PRAGMA foreign_keys = ON")
            self.connection.execute("PRAGMA journal_mode = WAL")
        return self.connection

    def close(self) -> None:
        """Close database connection."""
        if self.connection:
            self.connection.close()
            self.connection = None

    def insert_company_domain(self, domain: CompanyDomain) -> int:
        """Insert a new company domain record.

        Args:
            domain: CompanyDomain model instance

        Returns:
            ID of inserted record
        """
        conn = self.connect()
        cursor = conn.cursor()

        cursor.execute(
            """
            INSERT INTO company_domains (
                company_id, domain, confidence_score, discovery_method,
                discovered_at, last_verified, is_primary, status,
                created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
            (
                domain.company_id,
                domain.domain,
                domain.confidence_score,
                domain.discovery_method,
                domain.discovered_at,
                domain.last_verified,
                domain.is_primary,
                domain.status,
                domain.created_at,
                domain.updated_at,
            ),
        )

        conn.commit()
        rowid = cursor.lastrowid
        if rowid is None:
            raise RuntimeError("Failed to insert company domain record")
        return rowid

    def insert_officer_email(self, email: OfficerEmail) -> int:
        """Insert a new officer email record.

        Args:
            email: OfficerEmail model instance

        Returns:
            ID of inserted record
        """
        conn = self.connect()
        cursor = conn.cursor()

        cursor.execute(
            """
            INSERT INTO officer_emails (
                officer_id, email, email_type, verification_status,
                confidence_score, domain, discovery_method, discovered_at,
                last_verified, snov_request_id, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
            (
                email.officer_id,
                email.email,
                email.email_type,
                email.verification_status,
                email.confidence_score,
                email.domain,
                email.discovery_method,
                email.discovered_at,
                email.last_verified,
                email.snov_request_id,
                email.created_at,
                email.updated_at,
            ),
        )

        conn.commit()
        rowid = cursor.lastrowid
        if rowid is None:
            raise RuntimeError("Failed to insert officer email record")
        return rowid

    def insert_credit_usage(self, usage: SnovCreditUsage) -> int:
        """Insert credit usage record.

        Args:
            usage: SnovCreditUsage model instance

        Returns:
            ID of inserted record
        """
        conn = self.connect()
        cursor = conn.cursor()

        cursor.execute(
            """
            INSERT INTO snov_credit_usage (
                operation_type, credits_consumed, success, request_id,
                company_id, officer_id, response_data, timestamp
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
            (
                usage.operation_type,
                usage.credits_consumed,
                usage.success,
                usage.request_id,
                usage.company_id,
                usage.officer_id,
                usage.response_data,
                usage.timestamp,
            ),
        )

        conn.commit()
        rowid = cursor.lastrowid
        if rowid is None:
            raise RuntimeError("Failed to insert credit usage record")
        return rowid

    def insert_webhook(self, webhook: SnovWebhook) -> int:
        """Insert webhook record.

        Args:
            webhook: SnovWebhook model instance

        Returns:
            ID of inserted record
        """
        conn = self.connect()
        cursor = conn.cursor()

        cursor.execute(
            """
            INSERT INTO snov_webhooks (
                webhook_id, event_type, status, request_id,
                payload, processed_at, error_message, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
            (
                webhook.webhook_id,
                webhook.event_type,
                webhook.status,
                webhook.request_id,
                webhook.payload,
                webhook.processed_at,
                webhook.error_message,
                webhook.created_at,
            ),
        )

        conn.commit()
        rowid = cursor.lastrowid
        if rowid is None:
            raise RuntimeError("Failed to insert webhook record")
        return rowid

    def get_company_domains(self, company_id: str) -> List[CompanyDomain]:
        """Get all domains for a company.

        Args:
            company_id: Company number

        Returns:
            List of CompanyDomain instances
        """
        conn = self.connect()
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT * FROM company_domains
            WHERE company_id = ?
            ORDER BY is_primary DESC, confidence_score DESC
        """,
            (company_id,),
        )

        rows = cursor.fetchall()
        return [CompanyDomain(**dict(row)) for row in rows]

    def get_officer_emails(self, officer_id: str) -> List[OfficerEmail]:
        """Get all emails for an officer.

        Args:
            officer_id: Officer ID

        Returns:
            List of OfficerEmail instances
        """
        conn = self.connect()
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT * FROM officer_emails
            WHERE officer_id = ?
            ORDER BY confidence_score DESC, discovered_at DESC
        """,
            (officer_id,),
        )

        rows = cursor.fetchall()
        return [OfficerEmail(**dict(row)) for row in rows]

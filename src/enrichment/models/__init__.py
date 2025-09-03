"""Model relationships and exports for Snov.io integration.

This module provides centralized access to all Snov.io integration models,
relationships, and validation utilities with comprehensive type hints.
"""

from typing import TYPE_CHECKING, List, Optional, Union

# API Response Models
from .api_responses import (
    BulkOperationResponse,
    CreditUsageResponse,
    DomainSearchResponse,
    EmailData,
    EmailFinderResponse,
    ErrorResponse,
    SnovioAPIResponse,
    WebhookEventData,
    WebhookEventPayload,
)

# Configuration Models
from .config import (
    IntegrationConfig,
    SnovioConfig,
    WebhookConfig,
    load_all_configs,
    load_integration_config,
    load_snov_config,
    load_webhook_config,
)

# Database Models
from .database import (
    CompanyDomain,
    DatabaseConnection,
    OfficerEmail,
    SnovCreditUsage,
    SnovWebhook,
)

# Validation Utilities
from .validators import (
    CreditValidator,
    DomainValidator,
    EmailValidator,
    WebhookValidator,
    sanitize_string_field,
    validate_confidence_score,
)

# Type checking imports
if TYPE_CHECKING:
    from datetime import datetime
    from typing import Any, Dict


# Model Collections for Type Hints
DatabaseModels = Union[CompanyDomain, OfficerEmail, SnovCreditUsage, SnovWebhook]
ConfigModels = Union[SnovioConfig, WebhookConfig, IntegrationConfig]
ValidationModels = Union[EmailValidator, DomainValidator, CreditValidator, WebhookValidator]


class ModelRelationships:
    """Manages relationships and foreign key constraints between models."""

    @staticmethod
    def get_company_domains(
        db_connection: DatabaseConnection, company_id: str
    ) -> List[CompanyDomain]:
        """Get all domains associated with a company.

        Args:
            db_connection: Database connection instance
            company_id: Companies House company number

        Returns:
            List of CompanyDomain instances
        """
        return db_connection.get_company_domains(company_id)

    @staticmethod
    def get_officer_emails(
        db_connection: DatabaseConnection, officer_id: str
    ) -> List[OfficerEmail]:
        """Get all emails associated with an officer.

        Args:
            db_connection: Database connection instance
            officer_id: Officer ID from Companies House

        Returns:
            List of OfficerEmail instances
        """
        return db_connection.get_officer_emails(officer_id)

    @staticmethod
    def get_primary_domain(
        db_connection: DatabaseConnection, company_id: str
    ) -> Optional[CompanyDomain]:
        """Get the primary domain for a company.

        Args:
            db_connection: Database connection instance
            company_id: Companies House company number

        Returns:
            Primary CompanyDomain or None if not found
        """
        domains = db_connection.get_company_domains(company_id)
        for domain in domains:
            if domain.is_primary:
                return domain
        # Return highest confidence domain if no primary set
        return domains[0] if domains else None

    @staticmethod
    def get_verified_emails(
        db_connection: DatabaseConnection, officer_id: str
    ) -> List[OfficerEmail]:
        """Get verified emails for an officer.

        Args:
            db_connection: Database connection instance
            officer_id: Officer ID from Companies House

        Returns:
            List of verified OfficerEmail instances
        """
        all_emails = db_connection.get_officer_emails(officer_id)
        return [email for email in all_emails if email.verification_status == "valid"]

    @staticmethod
    def link_domain_to_emails(
        domain: CompanyDomain, emails: List[OfficerEmail]
    ) -> List[OfficerEmail]:
        """Link domain information to officer emails.

        Args:
            domain: Company domain to link
            emails: List of officer emails to update

        Returns:
            Updated list of OfficerEmail instances
        """
        for email in emails:
            if email.domain is None and "@" in email.email:
                email_domain = email.email.split("@")[1]
                if email_domain == domain.domain:
                    email.domain = domain.domain
                    # Boost confidence if domain matches
                    if email.confidence_score and domain.confidence_score:
                        email.confidence_score = min(1.0, email.confidence_score + 0.1)
        return emails


class ModelValidation:
    """Centralized model validation utilities."""

    @staticmethod
    def validate_company_domain(domain: CompanyDomain) -> tuple[bool, List[str]]:
        """Validate CompanyDomain instance.

        Args:
            domain: CompanyDomain instance to validate

        Returns:
            Tuple of (is_valid, error_list)
        """
        errors = []

        # Validate domain format
        is_valid, error_msg = DomainValidator.validate_domain_format(domain.domain)
        if not is_valid:
            errors.append(f"Invalid domain format: {error_msg}")

        # Validate confidence score
        if domain.confidence_score is not None:
            if domain.confidence_score < 0.0 or domain.confidence_score > 1.0:
                errors.append("Confidence score must be between 0.0 and 1.0")

        return len(errors) == 0, errors

    @staticmethod
    def validate_officer_email(email: OfficerEmail) -> tuple[bool, List[str]]:
        """Validate OfficerEmail instance.

        Args:
            email: OfficerEmail instance to validate

        Returns:
            Tuple of (is_valid, error_list)
        """
        errors = []

        # Validate email format
        is_valid, error_msg = EmailValidator.validate_email_format(email.email)
        if not is_valid:
            errors.append(f"Invalid email format: {error_msg}")

        # Check for disposable emails
        if EmailValidator.is_disposable_email(email.email):
            errors.append("Disposable email addresses are not allowed")

        # Validate confidence score
        if email.confidence_score is not None:
            if email.confidence_score < 0.0 or email.confidence_score > 1.0:
                errors.append("Confidence score must be between 0.0 and 1.0")

        return len(errors) == 0, errors

    @staticmethod
    def validate_credit_operation(
        usage: SnovCreditUsage, available_credits: int
    ) -> tuple[bool, List[str]]:
        """Validate credit usage operation.

        Args:
            usage: SnovCreditUsage instance
            available_credits: Currently available credits

        Returns:
            Tuple of (is_valid, error_list)
        """
        errors = []

        # Validate credit availability
        can_proceed, error_msg = CreditValidator.validate_credit_operation(
            usage.operation_type, available_credits, usage.credits_consumed
        )
        if not can_proceed:
            errors.append(error_msg)

        # Validate operation type
        if usage.credits_consumed < 0:
            errors.append("Credits consumed cannot be negative")

        return len(errors) == 0, errors


# Export all models and utilities
__all__ = [
    # API Response Models
    "DomainSearchResponse",
    "EmailFinderResponse",
    "WebhookEventPayload",
    "CreditUsageResponse",
    "BulkOperationResponse",
    "ErrorResponse",
    "EmailData",
    "WebhookEventData",
    "SnovioAPIResponse",
    # Database Models
    "CompanyDomain",
    "OfficerEmail",
    "SnovCreditUsage",
    "SnovWebhook",
    "DatabaseConnection",
    # Configuration Models
    "SnovioConfig",
    "WebhookConfig",
    "IntegrationConfig",
    "load_snov_config",
    "load_webhook_config",
    "load_integration_config",
    "load_all_configs",
    # Validators
    "EmailValidator",
    "DomainValidator",
    "CreditValidator",
    "WebhookValidator",
    "validate_confidence_score",
    "sanitize_string_field",
    # Relationship Management
    "ModelRelationships",
    "ModelValidation",
    # Type Definitions
    "DatabaseModels",
    "ConfigModels",
    "ValidationModels",
]

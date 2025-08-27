"""Data validation and serialization utilities for Snov.io integration.

This module provides comprehensive validation utilities for email addresses,
domains, credit operations, and webhook signatures to ensure data integrity
and security throughout the Snov.io integration.
"""

import hashlib
import hmac
import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse


class EmailValidator:
    """Comprehensive email validation utilities."""

    # RFC 5322 compliant email regex (simplified for practical use)
    EMAIL_REGEX = re.compile(
        r"^[a-zA-Z0-9.!#$%&'*+/=?^_`{|}~-]+@[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}"
        r"[a-zA-Z0-9])?(?:\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$"
    )

    # Common disposable email domains (sample - would be expanded in production)
    DISPOSABLE_DOMAINS = {
        "10minutemail.com",
        "guerrillamail.com",
        "mailinator.com",
        "tempmail.org",
        "throwaway.email",
        "temp-mail.org",
    }

    # Common generic/role-based email prefixes
    GENERIC_PREFIXES = {
        "admin",
        "administrator",
        "billing",
        "contact",
        "help",
        "info",
        "marketing",
        "noreply",
        "no-reply",
        "postmaster",
        "sales",
        "support",
        "webmaster",
    }

    @classmethod
    def validate_email_format(cls, email: str) -> Tuple[bool, str]:
        """Validate email format using RFC 5322 regex.

        Args:
            email: Email address to validate

        Returns:
            Tuple of (is_valid, error_message)
        """
        if not email or not isinstance(email, str):
            return False, "Email cannot be empty"

        email = email.strip().lower()

        if len(email) > 254:  # RFC 5321 limit
            return False, "Email exceeds maximum length (254 characters)"

        if not cls.EMAIL_REGEX.match(email):
            return False, "Invalid email format"

        # Check local part length (before @)
        local_part = email.split("@")[0]
        if len(local_part) > 64:  # RFC 5321 limit
            return False, "Email local part exceeds maximum length (64 characters)"

        return True, ""

    @classmethod
    def categorize_email_type(cls, email: str) -> str:
        """Categorize email as work, personal, generic, or catch_all.

        Args:
            email: Email address to categorize

        Returns:
            Email type category
        """
        if not email:
            return "unknown"

        email = email.lower()
        local_part = email.split("@")[0]
        domain = email.split("@")[1] if "@" in email else ""

        # Check for generic/role-based emails
        if local_part in cls.GENERIC_PREFIXES:
            return "generic"

        # Check for catch-all patterns
        if local_part in ["catchall", "catch-all", "*"]:
            return "catch_all"

        # Check for common personal email providers
        personal_domains = {
            "gmail.com",
            "yahoo.com",
            "hotmail.com",
            "outlook.com",
            "icloud.com",
            "aol.com",
            "protonmail.com",
        }
        if domain in personal_domains:
            return "personal"

        # Default to work email for custom domains
        return "work"

    @classmethod
    def is_disposable_email(cls, email: str) -> bool:
        """Check if email is from a disposable email service.

        Args:
            email: Email address to check

        Returns:
            True if email is disposable
        """
        if not email or "@" not in email:
            return False

        domain = email.split("@")[1].lower()
        return domain in cls.DISPOSABLE_DOMAINS

    @classmethod
    def calculate_email_confidence(
        cls, email: str, context: Optional[Dict[str, Any]] = None
    ) -> float:
        """Calculate confidence score for email validity.

        Args:
            email: Email address to score
            context: Additional context (domain match, source, etc.)

        Returns:
            Confidence score between 0.0 and 1.0
        """
        if not email:
            return 0.0

        is_valid, _ = cls.validate_email_format(email)
        if not is_valid:
            return 0.0

        confidence = 0.5  # Base confidence for valid format

        # Penalty for disposable emails
        if cls.is_disposable_email(email):
            confidence -= 0.3

        # Bonus for work emails
        email_type = cls.categorize_email_type(email)
        if email_type == "work":
            confidence += 0.2
        elif email_type == "generic":
            confidence -= 0.1

        # Context-based adjustments
        if context:
            # Domain matching bonus
            if context.get("domain_match", False):
                confidence += 0.2

            # Source reliability bonus
            source_reliability = context.get("source_reliability", 0.0)
            confidence += source_reliability * 0.1

            # Name matching bonus
            if context.get("name_match", False):
                confidence += 0.1

        return max(0.0, min(1.0, confidence))


class DomainValidator:
    """Comprehensive domain validation utilities."""

    # Domain regex pattern
    DOMAIN_REGEX = re.compile(
        r"^(?:[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?\.)+"
        r"[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?$"
    )

    # Common TLDs for validation
    COMMON_TLDS = {
        ".com",
        ".org",
        ".net",
        ".edu",
        ".gov",
        ".mil",
        ".int",
        ".co.uk",
        ".uk",
        ".de",
        ".fr",
        ".au",
        ".ca",
        ".jp",
    }

    @classmethod
    def validate_domain_format(cls, domain: str) -> Tuple[bool, str]:
        """Validate domain format.

        Args:
            domain: Domain to validate

        Returns:
            Tuple of (is_valid, error_message)
        """
        if not domain or not isinstance(domain, str):
            return False, "Domain cannot be empty"

        domain = domain.strip().lower()

        # Remove protocol if present
        if domain.startswith(("http://", "https://")):
            domain = urlparse(f"http://{domain}").netloc

        # Remove www prefix
        if domain.startswith("www."):
            domain = domain[4:]

        if len(domain) > 253:  # RFC 1035 limit
            return False, "Domain exceeds maximum length"

        if not cls.DOMAIN_REGEX.match(domain):
            return False, "Invalid domain format"

        # Check for valid TLD
        parts = domain.split(".")
        if len(parts) < 2:
            return False, "Domain must have at least one dot"

        return True, ""

    @classmethod
    def normalize_domain(cls, domain: str) -> str:
        """Normalize domain format for consistency.

        Args:
            domain: Domain to normalize

        Returns:
            Normalized domain
        """
        if not domain:
            return ""

        domain = domain.strip().lower()

        # Remove protocol
        if domain.startswith(("http://", "https://")):
            parsed = urlparse(f"http://{domain}")
            domain = parsed.netloc

        # Remove www prefix
        if domain.startswith("www."):
            domain = domain[4:]

        # Remove trailing slash
        domain = domain.rstrip("/")

        return domain

    @classmethod
    def calculate_domain_confidence(
        cls, domain: str, context: Optional[Dict[str, Any]] = None
    ) -> float:
        """Calculate confidence score for domain validity.

        Args:
            domain: Domain to score
            context: Additional context

        Returns:
            Confidence score between 0.0 and 1.0
        """
        if not domain:
            return 0.0

        is_valid, _ = cls.validate_domain_format(domain)
        if not is_valid:
            return 0.0

        confidence = 0.6  # Base confidence for valid format

        # TLD bonus
        domain_lower = domain.lower()
        for tld in cls.COMMON_TLDS:
            if domain_lower.endswith(tld):
                confidence += 0.1
                break

        # Context-based adjustments
        if context:
            # Company name matching
            if context.get("company_name_match", False):
                confidence += 0.2

            # Source reliability
            source_reliability = context.get("source_reliability", 0.0)
            confidence += source_reliability * 0.1

        return max(0.0, min(1.0, confidence))


class CreditValidator:
    """Credit operation validation utilities."""

    @classmethod
    def validate_credit_operation(
        cls, operation_type: str, credits_available: int, estimated_credits: int
    ) -> Tuple[bool, str]:
        """Validate if credit operation can proceed.

        Args:
            operation_type: Type of operation
            credits_available: Currently available credits
            estimated_credits: Credits needed for operation

        Returns:
            Tuple of (can_proceed, message)
        """
        valid_operations = {
            "domain_search",
            "email_finder",
            "email_verifier",
            "bulk_email_finder",
            "bulk_domain_search",
        }

        if operation_type not in valid_operations:
            return False, f"Invalid operation type: {operation_type}"

        if estimated_credits < 0:
            return False, "Estimated credits cannot be negative"

        if credits_available < estimated_credits:
            return (
                False,
                f"Insufficient credits: {credits_available} available, {estimated_credits} needed",
            )

        # Buffer check - keep 10% buffer for critical operations
        buffer_threshold = max(10, int(credits_available * 0.1))
        if credits_available - estimated_credits < buffer_threshold:
            return False, "Operation would leave insufficient credit buffer"

        return True, "Operation validated"

    @classmethod
    def calculate_operation_priority(
        cls, operation_type: str, credits_available: int, queue_size: int
    ) -> int:
        """Calculate operation priority based on credit constraints.

        Args:
            operation_type: Type of operation
            credits_available: Available credits
            queue_size: Current queue size

        Returns:
            Priority score (higher = more priority)
        """
        base_priorities = {
            "email_verifier": 100,  # Highest - validates existing data
            "email_finder": 80,  # High - targeted search
            "domain_search": 60,  # Medium - broader search
            "bulk_email_finder": 40,  # Lower - batch operation
            "bulk_domain_search": 20,  # Lowest - large batch
        }

        priority = base_priorities.get(operation_type, 50)

        # Credit constraint adjustments
        if credits_available < 100:
            # Prioritize high-value, low-cost operations when credits are low
            if operation_type in ["email_verifier", "email_finder"]:
                priority += 20
            else:
                priority -= 30

        # Queue pressure adjustments
        if queue_size > 1000:
            # Deprioritize bulk operations under high load
            if "bulk" in operation_type:
                priority -= 20

        return max(0, priority)


class WebhookValidator:
    """Webhook signature validation and payload processing utilities."""

    @classmethod
    def verify_webhook_signature(cls, payload: str, signature: str, webhook_secret: str) -> bool:
        """Verify webhook signature using HMAC-SHA256.

        Args:
            payload: Raw webhook payload
            signature: Provided signature
            webhook_secret: Configured webhook secret

        Returns:
            True if signature is valid
        """
        if not all([payload, signature, webhook_secret]):
            return False

        # Generate expected signature
        expected_signature = hmac.new(
            webhook_secret.encode("utf-8"), payload.encode("utf-8"), hashlib.sha256
        ).hexdigest()

        # Remove 'sha256=' prefix if present
        if signature.startswith("sha256="):
            signature = signature[7:]

        # Constant-time comparison to prevent timing attacks
        return hmac.compare_digest(expected_signature, signature)

    @classmethod
    def validate_webhook_payload(cls, payload: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """Validate webhook payload structure and required fields.

        Args:
            payload: Webhook payload dictionary

        Returns:
            Tuple of (is_valid, list_of_errors)
        """
        errors = []

        # Required fields
        required_fields = ["event_type", "event_id", "timestamp", "data"]
        for field in required_fields:
            if field not in payload:
                errors.append(f"Missing required field: {field}")

        # Validate event type
        if "event_type" in payload:
            valid_events = {
                "bulk_email_finder_completed",
                "bulk_domain_search_completed",
                "email_verification_completed",
                "operation_failed",
            }
            if payload["event_type"] not in valid_events:
                errors.append(f"Invalid event type: {payload['event_type']}")

        # Validate timestamp
        if "timestamp" in payload:
            try:
                datetime.fromisoformat(payload["timestamp"].replace("Z", "+00:00"))
            except (ValueError, AttributeError):
                errors.append("Invalid timestamp format")

        # Validate data structure
        if "data" in payload:
            data = payload["data"]
            if not isinstance(data, dict):
                errors.append("Data field must be an object")
            else:
                # Check required data fields
                data_required = ["request_id", "status"]
                for field in data_required:
                    if field not in data:
                        errors.append(f"Missing required data field: {field}")

        return len(errors) == 0, errors

    @classmethod
    def extract_webhook_metadata(cls, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Extract metadata from webhook payload for processing.

        Args:
            payload: Webhook payload dictionary

        Returns:
            Extracted metadata dictionary
        """
        metadata = {
            "event_type": payload.get("event_type"),
            "event_id": payload.get("event_id"),
            "timestamp": payload.get("timestamp"),
            "request_id": payload.get("data", {}).get("request_id"),
            "status": payload.get("data", {}).get("status"),
            "results_count": payload.get("data", {}).get("results_count", 0),
            "credits_used": payload.get("data", {}).get("credits_used", 0),
            "error_message": payload.get("data", {}).get("error_message"),
            "download_url": payload.get("data", {}).get("download_url"),
        }

        return metadata


# Utility functions for model validation
def validate_confidence_score(score: Optional[float]) -> Optional[float]:
    """Validate and normalize confidence score.

    Args:
        score: Confidence score to validate

    Returns:
        Normalized confidence score or None
    """
    if score is None:
        return None

    if not isinstance(score, (int, float)):
        return None

    # Normalize to 0.0-1.0 range
    if score > 1.0:
        score = score / 100.0  # Assume percentage

    return max(0.0, min(1.0, float(score)))


def sanitize_string_field(value: Optional[str], max_length: int = 255) -> Optional[str]:
    """Sanitize string field for database storage.

    Args:
        value: String value to sanitize
        max_length: Maximum allowed length

    Returns:
        Sanitized string or None
    """
    if value is None:
        return None

    if not isinstance(value, str):
        value = str(value)

    # Strip whitespace and truncate
    value = value.strip()[:max_length]

    return value if value else None

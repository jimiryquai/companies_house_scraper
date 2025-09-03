"""Webhook endpoint handler for receiving Snov.io results.

This module provides webhook endpoint functionality for processing Snov.io API results
with signature verification, payload validation, and integration with existing
health monitoring systems.
"""

import hashlib
import hmac
import json
import logging
from datetime import datetime
from typing import Any, Optional

from aiohttp import web
from aiohttp.web import Request, Response

from ..streaming.health_endpoints import HealthStatus

logger = logging.getLogger(__name__)


class WebhookValidationError(Exception):
    """Raised when webhook validation fails."""

    pass


class WebhookProcessingError(Exception):
    """Raised when webhook processing fails."""

    pass


class WebhookHandler:
    """Handles Snov.io webhook endpoints with signature verification and processing.

    This class provides secure webhook handling following existing patterns from
    health endpoints and streaming modules, with proper error handling and
    integration with the existing monitoring system.
    """

    def __init__(
        self,
        webhook_secret: str,
        database_path: str = "companies.db",
        enable_signature_verification: bool = True,
    ):
        """Initialize webhook handler.

        Args:
            webhook_secret: Secret key for webhook signature verification
            database_path: Path to SQLite database
            enable_signature_verification: Whether to verify webhook signatures

        Raises:
            ValueError: If webhook_secret is empty when signature verification enabled
        """
        if enable_signature_verification and not webhook_secret:
            raise ValueError("webhook_secret is required when signature verification is enabled")

        self.webhook_secret = webhook_secret
        self.database_path = database_path
        self.enable_signature_verification = enable_signature_verification

        # Statistics tracking
        self.webhook_count = 0
        self.processed_count = 0
        self.failed_count = 0
        self.start_time = datetime.now()

    async def handle_webhook(self, request: Request) -> Response:
        """Handle incoming webhook request.

        Supports both POST (for actual webhook data) and GET (for URL validation).

        Args:
            request: Aiohttp request object

        Returns:
            HTTP response with processing status
        """
        try:
            self.webhook_count += 1

            # Handle GET requests for webhook URL validation
            if request.method == "GET":
                return await self._handle_webhook_validation(request)

            # Handle POST requests with webhook data
            if request.method != "POST":
                return web.Response(
                    status=405,
                    text=json.dumps({"error": "Method not allowed"}),
                    content_type="application/json",
                )

            # Extract payload
            payload_bytes = await request.read()
            if not payload_bytes:
                raise WebhookValidationError("Empty webhook payload")

            # Verify signature if enabled
            if self.enable_signature_verification:
                signature = request.headers.get("X-Snov-Signature")
                if not signature:
                    raise WebhookValidationError("Missing webhook signature")

                if not self._verify_signature(payload_bytes, signature):
                    raise WebhookValidationError("Invalid webhook signature")

            # Parse JSON payload
            try:
                payload_str = payload_bytes.decode("utf-8")
                payload_data = json.loads(payload_str)
            except (UnicodeDecodeError, json.JSONDecodeError) as e:
                raise WebhookValidationError(f"Invalid JSON payload: {e}") from e

            # Process webhook
            result = await self._process_webhook(payload_data)
            self.processed_count += 1

            return web.json_response(
                {
                    "status": "success",
                    "message": "Webhook processed successfully",
                    "result": result,
                    "timestamp": datetime.now().isoformat(),
                }
            )

        except WebhookValidationError as e:
            self.failed_count += 1
            logger.warning(f"Webhook validation failed: {e}")
            return web.json_response(
                {
                    "status": "error",
                    "message": f"Validation failed: {str(e)}",
                    "timestamp": datetime.now().isoformat(),
                },
                status=400,
            )

        except WebhookProcessingError as e:
            self.failed_count += 1
            logger.error(f"Webhook processing failed: {e}")
            return web.json_response(
                {
                    "status": "error",
                    "message": f"Processing failed: {str(e)}",
                    "timestamp": datetime.now().isoformat(),
                },
                status=500,
            )

        except Exception as e:
            self.failed_count += 1
            logger.error(f"Unexpected webhook error: {e}", exc_info=True)
            return web.json_response(
                {
                    "status": "error",
                    "message": "Internal server error",
                    "timestamp": datetime.now().isoformat(),
                },
                status=500,
            )

    def _verify_signature(self, payload: bytes, signature: str) -> bool:
        """Verify webhook signature using HMAC-SHA256.

        Args:
            payload: Raw webhook payload bytes
            signature: Signature from request headers

        Returns:
            True if signature is valid
        """
        try:
            # Extract signature from header (format: sha256=hash)
            if signature.startswith("sha256="):
                signature = signature[7:]

            # Calculate expected signature
            expected_signature = hmac.new(
                self.webhook_secret.encode("utf-8"),
                payload,
                hashlib.sha256,
            ).hexdigest()

            # Use constant-time comparison for security
            return hmac.compare_digest(signature, expected_signature)

        except Exception as e:
            logger.error(f"Signature verification error: {e}")
            return False

    async def _process_webhook(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Process webhook payload based on event type.

        Args:
            payload: Parsed webhook payload

        Returns:
            Processing result

        Raises:
            WebhookProcessingError: If processing fails
        """
        event_type = payload.get("event_type")
        if not event_type:
            raise WebhookProcessingError("Missing event_type in payload")

        logger.info(f"Processing webhook event: {event_type}")

        try:
            if event_type == "domain_search_completed":
                return await self._handle_domain_search_webhook(payload)
            if event_type == "email_finder_completed":
                return await self._handle_email_finder_webhook(payload)
            if event_type == "credits_updated":
                return await self._handle_credits_webhook(payload)
            logger.warning(f"Unknown webhook event type: {event_type}")
            return {"status": "ignored", "reason": f"Unknown event type: {event_type}"}

        except Exception as e:
            raise WebhookProcessingError(f"Error processing {event_type}: {e}") from e

    async def _handle_domain_search_webhook(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Handle domain search completion webhook.

        Args:
            payload: Webhook payload data

        Returns:
            Processing result
        """
        company_name = payload.get("company_name")
        domains = payload.get("domains", [])
        request_id = payload.get("request_id")

        logger.info(f"Domain search completed for {company_name}: {len(domains)} domains found")

        # Store results in snov_webhooks table for processing by enrichment state manager
        await self._store_webhook_event(request_id, "domain_search_completed", payload)

        return {
            "event": "domain_search_completed",
            "company_name": company_name,
            "domains_count": len(domains),
            "request_id": request_id,
        }

    async def _handle_email_finder_webhook(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Handle email finder completion webhook.

        Args:
            payload: Webhook payload data

        Returns:
            Processing result
        """
        officer_name = payload.get("officer_name")
        domain = payload.get("domain")
        emails = payload.get("emails", [])
        request_id = payload.get("request_id")

        logger.info(
            f"Email finder completed for {officer_name}@{domain}: {len(emails)} emails found"
        )

        # Store results in snov_webhooks table for processing by enrichment state manager
        await self._store_webhook_event(request_id, "email_finder_completed", payload)

        return {
            "event": "email_finder_completed",
            "officer_name": officer_name,
            "domain": domain,
            "emails_count": len(emails),
            "request_id": request_id,
        }

    async def _handle_credits_webhook(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Handle credits update webhook.

        Args:
            payload: Webhook payload data

        Returns:
            Processing result
        """
        credits_remaining = payload.get("credits_remaining", 0)
        credits_consumed = payload.get("credits_consumed", 0)

        logger.info(f"Credits updated: {credits_remaining} remaining, {credits_consumed} consumed")

        # Store credits update for credit manager processing
        await self._store_webhook_event(None, "credits_updated", payload)

        return {
            "event": "credits_updated",
            "credits_remaining": credits_remaining,
            "credits_consumed": credits_consumed,
        }

    async def _store_webhook_event(
        self,
        request_id: Optional[str],
        event_type: str,
        payload: dict[str, Any],
    ) -> None:
        """Store webhook event in database for processing.

        Args:
            request_id: Request ID associated with webhook
            event_type: Type of webhook event
            payload: Full webhook payload
        """
        # This is a simplified implementation - in production would use proper database connection
        # Following the pattern from existing modules for database operations
        logger.debug(f"Storing webhook event: {event_type}, request_id: {request_id}")

        # Note: Database storage implementation would go here
        # Following the pattern from company_state_manager.py for database operations

    def get_health_status(self) -> dict[str, Any]:
        """Get webhook handler health status.

        Returns:
            Health status information following existing patterns
        """
        uptime_seconds = (datetime.now() - self.start_time).total_seconds()
        success_rate = (
            (self.processed_count / max(self.webhook_count, 1) * 100)
            if self.webhook_count > 0
            else 100.0
        )

        return {
            "component": "webhook_handler",
            "status": HealthStatus.UP.value,
            "message": "Webhook handler operational",
            "timestamp": datetime.now().isoformat(),
            "details": {
                "webhooks_received": self.webhook_count,
                "webhooks_processed": self.processed_count,
                "webhooks_failed": self.failed_count,
                "success_rate": round(success_rate, 2),
                "uptime_seconds": round(uptime_seconds, 2),
                "signature_verification_enabled": self.enable_signature_verification,
            },
        }

    async def _handle_webhook_validation(self, request: Request) -> Response:
        """Handle GET request for webhook URL validation.

        Snov.io sends GET requests to validate webhook URLs during setup.
        This endpoint responds with a simple confirmation.

        Args:
            request: HTTP GET request

        Returns:
            Simple validation response
        """
        logger.info(f"Webhook validation request from {request.remote}")

        # Simple validation response
        response_data = {
            "status": "ok",
            "message": "Webhook endpoint is active",
            "timestamp": datetime.now().isoformat(),
        }

        return web.Response(
            status=200,
            text=json.dumps(response_data),
            content_type="application/json",
        )

    def get_metrics(self) -> dict[str, Any]:
        """Get webhook processing metrics.

        Returns:
            Metrics data for monitoring
        """
        uptime_seconds = (datetime.now() - self.start_time).total_seconds()

        return {
            "webhook_total_received": self.webhook_count,
            "webhook_processed_count": self.processed_count,
            "webhook_failed_count": self.failed_count,
            "webhook_success_rate": (self.processed_count / max(self.webhook_count, 1) * 100)
            if self.webhook_count > 0
            else 100.0,
            "webhook_uptime_seconds": uptime_seconds,
        }

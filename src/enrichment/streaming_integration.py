"""Integration bridge between streaming service and enrichment components.

This module provides the integration bridge that connects the streaming service
with enrichment components, integrating with the existing _handle_strike_off_company
workflow and queue manager for seamless operation.
"""

import logging
from datetime import datetime
from typing import Any, Optional

from ..streaming.company_state_manager import CompanyStateManager
from ..streaming.queue_manager import PriorityQueueManager
from .credit_manager import CreditManager
from .dependency_manager import DependencyManager
from .enrichment_state_manager import EnrichmentState, EnrichmentStateManager
from .webhook_handler import WebhookHandler

logger = logging.getLogger(__name__)


class EnrichmentIntegrationError(Exception):
    """Raised when enrichment integration operations fail."""

    pass


class StreamingIntegration:
    """Bridge between streaming service and enrichment components.

    This class provides seamless integration with the existing streaming service,
    connecting all enrichment components and managing the workflow from
    strike-off detection through domain discovery and email finding.
    """

    def __init__(
        self,
        company_state_manager: CompanyStateManager,
        queue_manager: PriorityQueueManager,
        credit_manager: Optional[CreditManager] = None,
        webhook_handler: Optional[WebhookHandler] = None,
        database_path: str = "companies.db",
    ):
        """Initialize streaming integration bridge.

        Args:
            company_state_manager: Existing company state manager
            queue_manager: Existing queue manager
            credit_manager: Credit management component
            webhook_handler: Webhook handling component
            database_path: Path to SQLite database

        Raises:
            TypeError: If required components are None
        """
        if not company_state_manager:
            raise TypeError("company_state_manager is required")
        if not queue_manager:
            raise TypeError("queue_manager is required")

        self.company_state_manager = company_state_manager
        self.queue_manager = queue_manager
        self.credit_manager = credit_manager
        self.webhook_handler = webhook_handler
        self.database_path = database_path

        # Initialize enrichment components
        self.enrichment_state_manager = EnrichmentStateManager(database_path)
        self.dependency_manager = DependencyManager(
            company_state_manager,
            queue_manager,
            database_path,
        )

        # Statistics tracking
        self.companies_processed = 0
        self.enrichment_initiated = 0
        self.enrichment_completed = 0
        self.enrichment_failed = 0

    async def initialize(self) -> None:
        """Initialize all enrichment components."""
        await self.enrichment_state_manager.initialize()
        logger.info("Streaming integration bridge initialized")

    async def handle_strike_off_company_enrichment(
        self,
        company_number: str,
        company_name: str,
    ) -> bool:
        """Handle enrichment for newly detected strike-off company.

        This method integrates with the existing _handle_strike_off_company workflow
        to initiate enrichment for companies that have completed basic processing.

        Args:
            company_number: Companies House company number
            company_name: Company name for domain search

        Returns:
            True if enrichment was initiated successfully
        """
        try:
            self.companies_processed += 1

            # Check if enrichment is eligible
            if not await self._should_enrich_company(company_number):
                logger.debug(f"Company {company_number} not eligible for enrichment")
                return False

            # Check credit availability
            if self.credit_manager and not await self._check_credits_available():
                logger.warning("Enrichment skipped due to insufficient credits")
                await self._mark_enrichment_skipped(company_number, "insufficient_credits")
                return False

            # Initiate domain search first (no officers needed upfront)
            # Officers will be queued AFTER domain search succeeds
            success = await self.dependency_manager.initiate_domain_search_first(
                company_number,
                company_name,
            )

            if success:
                self.enrichment_initiated += 1
                await self.enrichment_state_manager.update_enrichment_state(
                    company_number,
                    EnrichmentState.DOMAIN_QUEUED.value,
                    # officers_count will be updated when officers are actually fetched
                )
                logger.info(f"Domain search initiated for company {company_number}")

            return success

        except Exception as e:
            logger.error(f"Error handling enrichment for {company_number}: {e}")
            await self._mark_enrichment_failed(company_number, str(e))
            return False

    async def process_webhook_results(
        self,
        webhook_payload: dict[str, Any],
    ) -> None:
        """Process webhook results from Snov.io API.

        Args:
            webhook_payload: Parsed webhook payload
        """
        try:
            event_type = webhook_payload.get("event_type")

            if event_type == "domain_search_completed":
                await self._handle_domain_results(webhook_payload)
            elif event_type == "email_finder_completed":
                await self._handle_email_results(webhook_payload)
            elif event_type == "credits_updated":
                await self._handle_credits_update(webhook_payload)
            else:
                logger.warning(f"Unknown webhook event type: {event_type}")

        except Exception as e:
            logger.error(f"Error processing webhook results: {e}")

    async def _handle_domain_results(self, payload: dict[str, Any]) -> None:
        """Handle domain search completion results.

        Args:
            payload: Domain search webhook payload
        """
        company_number = payload.get("company_number")
        domains = payload.get("domains", [])
        success = payload.get("success", False)
        request_id = payload.get("request_id")

        if not company_number:
            logger.error("Missing company_number in domain results")
            return

        try:
            # Record credit consumption if available
            if self.credit_manager:
                credits_used = payload.get("credits_consumed", 1)
                await self.credit_manager.record_credit_consumption(
                    "domain_search",
                    credits_used,
                    success,
                    request_id,
                    company_number,
                )

            # Update enrichment state
            if success and domains:
                await self.enrichment_state_manager.update_enrichment_state(
                    company_number,
                    EnrichmentState.DOMAIN_COMPLETED.value,
                    domains_found=len(domains),
                )

                # Continue dependency chain
                await self.dependency_manager.handle_domain_completion(
                    company_number,
                    domains,
                    success,
                )
            else:
                await self.enrichment_state_manager.update_enrichment_state(
                    company_number,
                    EnrichmentState.DOMAIN_FAILED.value,
                    last_error="Domain search failed or no domains found",
                )
                await self._mark_enrichment_failed(company_number, "domain_search_failed")

        except Exception as e:
            logger.error(f"Error handling domain results for {company_number}: {e}")

    async def _handle_email_results(self, payload: dict[str, Any]) -> None:
        """Handle email finder completion results.

        Args:
            payload: Email finder webhook payload
        """
        company_number = payload.get("company_number")
        officer_id = payload.get("officer_id")
        emails = payload.get("emails", [])
        success = payload.get("success", False)
        request_id = payload.get("request_id")

        if not company_number or not officer_id:
            logger.error("Missing company_number or officer_id in email results")
            return

        try:
            # Record credit consumption
            if self.credit_manager:
                credits_used = payload.get("credits_consumed", 1)
                await self.credit_manager.record_credit_consumption(
                    "email_finder",
                    credits_used,
                    success,
                    request_id,
                    company_number,
                    officer_id,
                )

            # Continue dependency chain processing
            await self.dependency_manager.handle_officer_email_completion(
                company_number,
                officer_id,
                emails,
                success,
            )

            # Update enrichment state (simplified - would track individual officers)
            if success:
                current_state = await self.enrichment_state_manager.get_enrichment_state(
                    company_number
                )
                if current_state:
                    emails_found = current_state.get("emails_found", 0) + len(emails)
                    officers_processed = current_state.get("officers_processed", 0) + 1

                    await self.enrichment_state_manager.update_enrichment_state(
                        company_number,
                        EnrichmentState.OFFICERS_COMPLETED.value,
                        emails_found=emails_found,
                        officers_processed=officers_processed,
                    )

                    self.enrichment_completed += 1

        except Exception as e:
            logger.error(f"Error handling email results for {company_number}: {e}")

    async def _handle_credits_update(self, payload: dict[str, Any]) -> None:
        """Handle credits update from webhook.

        Args:
            payload: Credits update webhook payload
        """
        try:
            if self.credit_manager:
                credits_remaining = payload.get("credits_remaining")
                if credits_remaining is not None:
                    await self.credit_manager.update_balance_from_api(credits_remaining)
                    logger.info(f"Updated credit balance: {credits_remaining}")

        except Exception as e:
            logger.error(f"Error handling credits update: {e}")

    async def _should_enrich_company(self, company_number: str) -> bool:
        """Check if company should be enriched.

        Args:
            company_number: Company number to check

        Returns:
            True if company should be enriched
        """
        try:
            # Check if company processing is complete
            company_state = await self.company_state_manager.get_state(company_number)
            if not company_state or company_state["processing_state"] != "completed":
                return False

            # Check enrichment eligibility
            return await self.enrichment_state_manager.is_eligible_for_enrichment(company_number)

        except Exception as e:
            logger.error(f"Error checking enrichment eligibility for {company_number}: {e}")
            return False

    async def _check_credits_available(self) -> bool:
        """Check if sufficient credits are available.

        Returns:
            True if credits are available for operations
        """
        if not self.credit_manager:
            return True  # No credit manager - assume available

        try:
            # Estimate credits needed for domain search
            estimated_credits = 5  # Conservative estimate
            return await self.credit_manager.can_consume_credits(estimated_credits)

        except Exception as e:
            logger.error(f"Error checking credit availability: {e}")
            return False

    async def _queue_officers_fetch_if_needed(self, company_number: str) -> bool:
        """Queue officers fetch from CH REST API if not already in progress.

        This method follows the proper workflow: only fetch officers AFTER
        domain search succeeds, as per the linear diagram.

        Args:
            company_number: Company number to fetch officers for

        Returns:
            True if officers fetch was queued successfully

        Raises:
            Exception: If queue operation fails
        """
        try:
            # Queue officers fetch via CH REST API
            request_id = await self.company_state_manager.queue_officers_fetch(company_number)
            logger.info(
                f"Queued officers fetch for company {company_number}, request_id: {request_id}"
            )
            return True

        except Exception as e:
            logger.error(f"Failed to queue officers fetch for company {company_number}: {e}")
            raise

    async def _mark_enrichment_skipped(self, company_number: str, reason: str) -> None:
        """Mark enrichment as skipped for a company.

        Args:
            company_number: Company number
            reason: Reason for skipping
        """
        try:
            await self.enrichment_state_manager.update_enrichment_state(
                company_number,
                EnrichmentState.ENRICHMENT_SKIPPED.value,
                last_error=f"Skipped: {reason}",
            )

        except Exception as e:
            logger.error(f"Error marking enrichment skipped for {company_number}: {e}")

    async def _mark_enrichment_failed(self, company_number: str, reason: str) -> None:
        """Mark enrichment as failed for a company.

        Args:
            company_number: Company number
            reason: Failure reason
        """
        try:
            self.enrichment_failed += 1

            await self.enrichment_state_manager.update_enrichment_state(
                company_number,
                EnrichmentState.ENRICHMENT_FAILED.value,
                last_error=reason,
            )

        except Exception as e:
            logger.error(f"Error marking enrichment failed for {company_number}: {e}")

    def get_integration_statistics(self) -> dict[str, Any]:
        """Get integration statistics.

        Returns:
            Integration processing statistics
        """
        return {
            "companies_processed": self.companies_processed,
            "enrichment_initiated": self.enrichment_initiated,
            "enrichment_completed": self.enrichment_completed,
            "enrichment_failed": self.enrichment_failed,
            "success_rate": (self.enrichment_completed / max(self.enrichment_initiated, 1) * 100)
            if self.enrichment_initiated > 0
            else 0,
        }

    def get_health_status(self) -> dict[str, Any]:
        """Get integration bridge health status.

        Returns:
            Health status information
        """
        stats = self.get_integration_statistics()

        return {
            "component": "streaming_integration",
            "status": "UP",
            "message": "Streaming integration bridge operational",
            "timestamp": datetime.now().isoformat(),
            "details": {
                **stats,
                "credit_manager_enabled": self.credit_manager is not None,
                "webhook_handler_enabled": self.webhook_handler is not None,
            },
        }

    def get_metrics(self) -> dict[str, Any]:
        """Get integration metrics.

        Returns:
            Metrics data for monitoring
        """
        stats = self.get_integration_statistics()
        return {
            "integration_companies_processed": stats["companies_processed"],
            "integration_enrichment_initiated": stats["enrichment_initiated"],
            "integration_enrichment_completed": stats["enrichment_completed"],
            "integration_enrichment_failed": stats["enrichment_failed"],
            "integration_success_rate": stats["success_rate"],
        }

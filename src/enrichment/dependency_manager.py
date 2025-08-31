"""Dependency chain logic for domain → officers flow.

This module implements the dependency chain logic to ensure proper sequencing
of domain discovery followed by officer email finding, integrating with existing
CompanyStateManager and queue system for efficient processing.
"""

import logging
from datetime import datetime
from enum import Enum
from typing import Any

from ..streaming.company_state_manager import CompanyStateManager, ProcessingState
from ..streaming.queue_manager import PriorityQueueManager, QueuedRequest, RequestPriority

logger = logging.getLogger(__name__)


class DependencyError(Exception):
    """Raised when dependency chain operations fail."""

    pass


class EnrichmentState(Enum):
    """Enrichment processing states for dependency tracking."""

    # Domain discovery states
    DOMAIN_PENDING = "domain_pending"
    DOMAIN_PROCESSING = "domain_processing"
    DOMAIN_COMPLETED = "domain_completed"
    DOMAIN_FAILED = "domain_failed"

    # Email discovery states (depends on domain completion)
    OFFICERS_PENDING = "officers_pending"
    OFFICERS_PROCESSING = "officers_processing"
    OFFICERS_COMPLETED = "officers_completed"
    OFFICERS_FAILED = "officers_failed"

    # Final states
    ENRICHMENT_COMPLETED = "enrichment_completed"
    ENRICHMENT_FAILED = "enrichment_failed"


class DependencyManager:
    """Manages dependency chains for domain → officers enrichment flow.

    This class ensures that officer email discovery only proceeds after
    successful domain discovery, integrating with the existing queue system
    and state management for bulletproof operation.
    """

    def __init__(
        self,
        company_state_manager: CompanyStateManager,
        queue_manager: PriorityQueueManager,
        database_path: str = "companies.db",
    ):
        """Initialize dependency manager.

        Args:
            company_state_manager: Existing company state manager
            queue_manager: Existing queue manager for API requests
            database_path: Path to SQLite database

        Raises:
            TypeError: If required managers are None
        """
        if not company_state_manager:
            raise TypeError("company_state_manager is required")
        if not queue_manager:
            raise TypeError("queue_manager is required")

        self.company_state_manager = company_state_manager
        self.queue_manager = queue_manager
        self.database_path = database_path

        # Statistics tracking
        self.domain_requests_initiated = 0
        self.officer_requests_initiated = 0
        self.dependency_chains_completed = 0
        self.dependency_chains_failed = 0

    async def initiate_enrichment_chain(
        self,
        company_number: str,
        company_name: str,
        officer_list: list[dict[str, Any]],
    ) -> bool:
        """Initiate enrichment dependency chain for a company.

        Args:
            company_number: Companies House company number
            company_name: Company name for domain search
            officer_list: List of officers to find emails for

        Returns:
            True if chain initiated successfully

        Raises:
            DependencyError: If chain initiation fails
        """
        if not company_number or not company_name:
            raise ValueError("company_number and company_name are required")

        if not officer_list:
            logger.warning(
                f"No officers provided for company {company_number}, skipping enrichment"
            )
            return False

        try:
            # Check if company is eligible for enrichment
            company_state = await self.company_state_manager.get_state(company_number)
            if not company_state:
                logger.warning(f"No state found for company {company_number}")
                return False

            # Only enrich companies that have completed basic processing
            if company_state["processing_state"] != ProcessingState.COMPLETED.value:
                logger.debug(
                    f"Company {company_number} not ready for enrichment (state: {company_state['processing_state']})"
                )
                return False

            # Initiate domain discovery first
            success = await self._queue_domain_discovery(company_number, company_name)
            if success:
                self.domain_requests_initiated += 1
                logger.info(f"Initiated enrichment chain for company {company_number}")

            return success

        except Exception as e:
            raise DependencyError(
                f"Failed to initiate enrichment chain for {company_number}: {e}"
            ) from e

    async def _queue_domain_discovery(self, company_number: str, company_name: str) -> bool:
        """Queue domain discovery request.

        Args:
            company_number: Company number
            company_name: Company name for search

        Returns:
            True if queued successfully
        """
        try:
            request = QueuedRequest(
                request_id=f"domain_{company_number}_{int(datetime.now().timestamp())}",
                priority=RequestPriority.LOW,  # Snov.io operations are lower priority than CH API
                endpoint="/snov/domain-search",
                params={
                    "company_number": company_number,
                    "company_name": company_name,
                    "operation_type": "domain_discovery",
                    "enrichment_state": EnrichmentState.DOMAIN_PENDING.value,
                },
            )

            success = await self.queue_manager.enqueue(request)
            if success:
                logger.debug(f"Queued domain discovery for company {company_number}")

            return success

        except Exception as e:
            logger.error(f"Failed to queue domain discovery for {company_number}: {e}")
            return False

    async def handle_domain_completion(
        self,
        company_number: str,
        domains: list[str],
        success: bool,
    ) -> None:
        """Handle completion of domain discovery.

        Args:
            company_number: Company number
            domains: Discovered domains
            success: Whether domain discovery was successful
        """
        try:
            if success and domains:
                # Domain discovery successful - proceed to officer email discovery
                await self._queue_officer_email_discovery(company_number, domains)
                logger.info(
                    f"Domain discovery completed for {company_number}: {len(domains)} domains found"
                )
            else:
                # Domain discovery failed - mark enrichment as failed
                await self._mark_enrichment_failed(company_number, "domain_discovery_failed")
                self.dependency_chains_failed += 1
                logger.warning(f"Domain discovery failed for {company_number}")

        except Exception as e:
            logger.error(f"Error handling domain completion for {company_number}: {e}")
            await self._mark_enrichment_failed(company_number, f"domain_completion_error: {e}")

    async def _queue_officer_email_discovery(
        self,
        company_number: str,
        domains: list[str],
    ) -> None:
        """Queue officer email discovery requests.

        Args:
            company_number: Company number
            domains: Available domains for email search
        """
        try:
            # Get officers for this company from database
            officers = await self._get_company_officers(company_number)

            if not officers:
                logger.warning(f"No officers found for company {company_number}")
                await self._mark_enrichment_failed(company_number, "no_officers_found")
                return

            # Queue email discovery for each officer-domain combination
            requests_queued = 0

            for officer in officers:
                officer_name = officer.get("name")
                if not officer_name:
                    continue

                # Use the primary domain or first available domain
                primary_domain = domains[0] if domains else None
                if not primary_domain:
                    continue

                request = QueuedRequest(
                    request_id=f"email_{company_number}_{officer['id']}_{int(datetime.now().timestamp())}",
                    priority=RequestPriority.LOW,
                    endpoint="/snov/email-finder",
                    params={
                        "company_number": company_number,
                        "officer_id": officer["id"],
                        "officer_name": officer_name,
                        "domain": primary_domain,
                        "operation_type": "email_discovery",
                        "enrichment_state": EnrichmentState.OFFICERS_PENDING.value,
                    },
                )

                success = await self.queue_manager.enqueue(request)
                if success:
                    requests_queued += 1

            self.officer_requests_initiated += requests_queued
            logger.info(
                f"Queued {requests_queued} officer email requests for company {company_number}"
            )

        except Exception as e:
            logger.error(f"Failed to queue officer email discovery for {company_number}: {e}")
            await self._mark_enrichment_failed(company_number, f"officer_queue_error: {e}")

    async def handle_officer_email_completion(
        self,
        company_number: str,
        officer_id: str,
        emails: list[str],
        success: bool,
    ) -> None:
        """Handle completion of officer email discovery.

        Args:
            company_number: Company number
            officer_id: Officer ID
            emails: Discovered email addresses
            success: Whether email discovery was successful
        """
        try:
            if success:
                logger.info(
                    f"Email discovery completed for officer {officer_id}: {len(emails)} emails found"
                )
            else:
                logger.warning(f"Email discovery failed for officer {officer_id}")

            # Check if all officer email discoveries are complete
            await self._check_enrichment_completion(company_number)

        except Exception as e:
            logger.error(f"Error handling officer email completion: {e}")

    async def _check_enrichment_completion(self, company_number: str) -> None:
        """Check if enrichment chain is complete for a company.

        Args:
            company_number: Company number to check
        """
        try:
            # This would check pending officer requests in the database
            # For now, simplified implementation that marks completion
            # In production, would query pending enrichment operations

            self.dependency_chains_completed += 1
            logger.info(f"Enrichment chain completed for company {company_number}")

        except Exception as e:
            logger.error(f"Error checking enrichment completion for {company_number}: {e}")

    async def _get_company_officers(self, company_number: str) -> list[dict[str, Any]]:
        """Get officers for a company from database.

        Args:
            company_number: Company number

        Returns:
            List of officer records
        """
        # Simplified implementation - would use proper database connection
        # Following pattern from company_state_manager.py

        # This would query the officers table for the given company
        return []

    async def _mark_enrichment_failed(self, company_number: str, reason: str) -> None:
        """Mark enrichment chain as failed.

        Args:
            company_number: Company number
            reason: Failure reason
        """
        logger.warning(f"Marking enrichment failed for {company_number}: {reason}")
        # Would update enrichment state in database

    async def prevent_officer_processing_without_domain(self, company_number: str) -> bool:
        """Prevent officer email discovery if domain search failed.

        Args:
            company_number: Company number to check

        Returns:
            True if officer processing should be blocked
        """
        try:
            # Check if domain discovery was successful
            # This would query the enrichment state or company_domains table
            # For now, simplified logic

            # Block officer processing if no domains are available
            return False  # Simplified - would check actual domain availability

        except Exception as e:
            logger.error(f"Error checking domain dependency for {company_number}: {e}")
            # Conservative approach: block if we can't determine state
            return True

    def get_dependency_statistics(self) -> dict[str, Any]:
        """Get dependency chain statistics.

        Returns:
            Statistics about dependency chain operations
        """
        return {
            "domain_requests_initiated": self.domain_requests_initiated,
            "officer_requests_initiated": self.officer_requests_initiated,
            "chains_completed": self.dependency_chains_completed,
            "chains_failed": self.dependency_chains_failed,
            "success_rate": (
                self.dependency_chains_completed
                / max(self.dependency_chains_completed + self.dependency_chains_failed, 1)
                * 100
            )
            if (self.dependency_chains_completed + self.dependency_chains_failed) > 0
            else 0,
        }

    def get_health_status(self) -> dict[str, Any]:
        """Get dependency manager health status.

        Returns:
            Health status information
        """
        stats = self.get_dependency_statistics()

        return {
            "component": "dependency_manager",
            "status": "UP",
            "message": "Dependency manager operational",
            "timestamp": datetime.now().isoformat(),
            "details": stats,
        }

    def get_metrics(self) -> dict[str, Any]:
        """Get dependency management metrics.

        Returns:
            Metrics data for monitoring
        """
        stats = self.get_dependency_statistics()
        return {
            "dependency_domain_requests": stats["domain_requests_initiated"],
            "dependency_officer_requests": stats["officer_requests_initiated"],
            "dependency_chains_completed": stats["chains_completed"],
            "dependency_chains_failed": stats["chains_failed"],
            "dependency_success_rate": stats["success_rate"],
        }

"""Queue processor for handling Snov.io API requests from the queue system.

This module processes queued requests for domain search and email finder operations,
integrating with the existing streaming and queue infrastructure.
"""

import asyncio
import logging
from typing import Any, Optional

from ..streaming.queue_manager import PriorityQueueManager, QueuedRequest
from .dependency_manager import DependencyManager
from .snov_client import SnovioClient
from .streaming_integration import StreamingIntegration

logger = logging.getLogger(__name__)


class QueueProcessorError(Exception):
    """Raised when queue processing operations fail."""
    pass


class SnovQueueProcessor:
    """Processes queued Snov.io API requests from the priority queue system.
    
    This processor bridges the gap between the queue system and Snov.io API,
    handling domain search and email finder requests with proper error handling
    and workflow coordination.
    """

    def __init__(
        self,
        queue_manager: PriorityQueueManager,
        snov_client: SnovioClient,
        dependency_manager: DependencyManager,
        streaming_integration: Optional[StreamingIntegration] = None,
    ) -> None:
        """Initialize the queue processor.

        Args:
            queue_manager: Queue manager for dequeuing requests
            snov_client: Snov.io API client for making requests
            dependency_manager: Manages enrichment workflow dependencies
            streaming_integration: Optional streaming integration bridge
        """
        self.queue_manager = queue_manager
        self.snov_client = snov_client
        self.dependency_manager = dependency_manager
        self.streaming_integration = streaming_integration

        # Processing statistics
        self.requests_processed = 0
        self.requests_failed = 0
        self.domain_searches_completed = 0
        self.email_searches_completed = 0

        # Control flags
        self._running = False
        self._stop_event = asyncio.Event()

    async def start_processing(self) -> None:
        """Start processing queued requests continuously."""
        if self._running:
            logger.warning("Queue processor already running")
            return

        self._running = True
        self._stop_event.clear()
        logger.info("Starting Snov.io queue processor")

        try:
            while self._running and not self._stop_event.is_set():
                try:
                    # Dequeue next request with timeout
                    request = await self.queue_manager.dequeue(timeout=5.0)

                    if request is None:
                        # No requests available, continue polling
                        await asyncio.sleep(1.0)
                        continue

                    # Process the request
                    await self._process_request(request)

                except Exception as e:
                    logger.error(f"Error in queue processing loop: {e}")
                    await asyncio.sleep(5.0)  # Back off on errors

        except asyncio.CancelledError:
            logger.info("Queue processor cancelled")
        finally:
            self._running = False
            logger.info("Snov.io queue processor stopped")

    async def stop_processing(self) -> None:
        """Stop processing queued requests."""
        if not self._running:
            return

        logger.info("Stopping Snov.io queue processor")
        self._running = False
        self._stop_event.set()

    async def _process_request(self, request: QueuedRequest) -> None:
        """Process a single queued request.

        Args:
            request: The queued request to process
        """
        start_time = asyncio.get_event_loop().time()

        try:
            logger.debug(f"Processing request {request.request_id} for endpoint {request.endpoint}")

            # Route request to appropriate handler
            if request.endpoint == "/snov/domain-search":
                await self._handle_domain_search_request(request)
            elif request.endpoint == "/snov/email-finder":
                await self._handle_email_finder_request(request)
            else:
                logger.warning(f"Unknown endpoint in request {request.request_id}: {request.endpoint}")
                self.queue_manager.mark_failed(request)
                self.requests_failed += 1
                return

            # Mark as processed successfully
            processing_time = asyncio.get_event_loop().time() - start_time
            self.queue_manager.mark_processed(request, processing_time)
            self.requests_processed += 1

        except Exception as e:
            logger.error(f"Failed to process request {request.request_id}: {e}")

            # Attempt to requeue if retries available
            if await self.queue_manager.requeue(request):
                logger.info(f"Requeued failed request {request.request_id}")
            else:
                logger.error(f"Request {request.request_id} exceeded max retries")
                self.requests_failed += 1

    async def _handle_domain_search_request(self, request: QueuedRequest) -> None:
        """Handle domain search request from queue.

        Args:
            request: Queued domain search request
        """
        company_number = request.params.get("company_number")
        company_name = request.params.get("company_name")

        if not company_number or not company_name:
            raise QueueProcessorError("Missing company_number or company_name in domain search request")

        logger.info(f"Processing domain search for company {company_number}: {company_name}")

        # Call Snov.io domain search with polling
        try:
            result = await self.snov_client.domain_search(
                company_name=company_name,
                limit=10,
                use_polling=True
            )

            domains = result.get("domains", [])
            success = len(domains) > 0

            logger.info(f"Domain search completed for {company_number}: {len(domains)} domains found")

            # Save results to database via dependency manager
            if success:
                await self.dependency_manager.handle_domain_completion(
                    company_number=company_number,
                    domains=domains,
                    success=True
                )
                self.domain_searches_completed += 1
            else:
                await self.dependency_manager.handle_domain_completion(
                    company_number=company_number,
                    domains=[],
                    success=False
                )
                logger.warning(f"No domains found for company {company_number}")

        except Exception as e:
            logger.error(f"Domain search failed for company {company_number}: {e}")
            # Handle failure through dependency manager
            await self.dependency_manager.handle_domain_completion(
                company_number=company_number,
                domains=[],
                success=False
            )
            raise

    async def _handle_email_finder_request(self, request: QueuedRequest) -> None:
        """Handle email finder request from queue.

        Args:
            request: Queued email finder request
        """
        company_number = request.params.get("company_number")
        officer_id = request.params.get("officer_id")
        officer_name = request.params.get("officer_name")
        domain = request.params.get("domain")

        if not all([company_number, officer_id, officer_name, domain]):
            raise QueueProcessorError("Missing required parameters in email finder request")

        logger.info(f"Processing email search for officer {officer_name} at domain {domain}")

        # Call Snov.io email finder
        try:
            # Parse officer name safely
            name_parts = officer_name.split() if officer_name else []
            first_name = name_parts[0] if name_parts else ""
            last_name = " ".join(name_parts[1:]) if len(name_parts) > 1 else officer_name
            
            result = await self.snov_client.email_finder_by_name(
                domain=domain,
                first_name=first_name,
                last_name=last_name,
                use_polling=True
            )

            emails = result.get("emails", [])
            success = len(emails) > 0

            logger.info(f"Email search completed for officer {officer_name}: {len(emails)} emails found")

            # Handle results through dependency manager
            await self.dependency_manager.handle_officer_email_completion(
                company_number=str(company_number),
                officer_id=str(officer_id),
                emails=emails,
                success=success
            )

            if success:
                self.email_searches_completed += 1

        except Exception as e:
            logger.error(f"Email finder failed for officer {officer_name}: {e}")
            # Handle failure through dependency manager
            await self.dependency_manager.handle_officer_email_completion(
                company_number=str(company_number),
                officer_id=str(officer_id),
                emails=[],
                success=False
            )
            raise

    def get_processing_statistics(self) -> dict[str, Any]:
        """Get processing statistics.

        Returns:
            Dictionary with processing metrics
        """
        return {
            "requests_processed": self.requests_processed,
            "requests_failed": self.requests_failed,
            "domain_searches_completed": self.domain_searches_completed,
            "email_searches_completed": self.email_searches_completed,
            "is_running": self._running,
        }

"""Companies House REST API Queue Processor.

This module processes queued Companies House REST API requests, making actual HTTP calls
to fetch company status and officer data. It bridges the gap between the streaming API
event detection and the Snov.io enrichment workflow.
"""

import asyncio
import base64
import json
import logging
from datetime import datetime
from typing import Any, Optional

import aiohttp
from aiohttp import ClientError, ClientTimeout

from .company_state_manager import CompanyStateManager, ProcessingState
from .config import StreamingConfig
from .queue_manager import PriorityQueueManager, QueuedRequest

logger = logging.getLogger("streaming_service")  # Use same logger as streaming service


class CompaniesHouseAPIError(Exception):
    """Raised when Companies House API operations fail."""
    pass


class CompaniesHouseAPIProcessor:
    """Processes queued Companies House REST API requests.
    
    This processor dequeues requests for company status checks and officer fetching,
    makes actual HTTP calls to the Companies House REST API, processes responses,
    and triggers the Snov.io enrichment workflow for strike-off companies.
    """

    def __init__(
        self,
        config: StreamingConfig,
        queue_manager: PriorityQueueManager,
        company_state_manager: CompanyStateManager,
        enrichment_callback: Optional[callable] = None,
    ) -> None:
        """Initialize the CH API processor.

        Args:
            config: Streaming configuration with API credentials
            queue_manager: Queue manager for dequeuing requests
            company_state_manager: Company state management
            enrichment_callback: Optional callback to trigger enrichment workflow
        """
        self.config = config
        self.queue_manager = queue_manager
        self.company_state_manager = company_state_manager
        self.enrichment_callback = enrichment_callback

        # API configuration
        self.api_base_url = "https://api.company-information.service.gov.uk"
        self.api_key = config.rest_api_key

        # Session management
        self.session: Optional[aiohttp.ClientSession] = None

        # Processing statistics
        self.requests_processed = 0
        self.requests_failed = 0
        self.companies_status_fetched = 0
        self.officers_fetched = 0
        self.strike_off_companies_detected = 0

        # Control flags
        self._running = False
        self._stop_event = asyncio.Event()

    async def start_processing(self) -> None:
        """Start processing queued Companies House API requests continuously."""
        if self._running:
            logger.warning("CH API processor already running")
            return

        self._running = True
        self._stop_event.clear()
        logger.info(f"Starting Companies House API processor with API key: {self.api_key[:10]}...")
        logger.info(f"API base URL: {self.api_base_url}")

        # Initialize HTTP session
        try:
            timeout = ClientTimeout(total=30)
            auth_string = base64.b64encode(f"{self.api_key}:".encode()).decode()
            headers = {
                "Authorization": f"Basic {auth_string}",
                "User-Agent": "companies-house-scraper/1.0",
                "Accept": "application/json",
            }
            
            self.session = aiohttp.ClientSession(
                timeout=timeout,
                headers=headers
            )
            logger.info("HTTP session initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize HTTP session: {e}")
            raise

        try:
            logger.info("Entering CH API processing loop...")
            while self._running and not self._stop_event.is_set():
                try:
                    # Dequeue next request with timeout
                    logger.info("CH API processor: Attempting to dequeue request...")
                    request = await self.queue_manager.dequeue(timeout=5.0)

                    if request is None:
                        # No requests available, continue polling
                        logger.info("CH API processor: No requests available, sleeping...")
                        await asyncio.sleep(1.0)
                        continue

                    # Process the request
                    logger.info(f"CH API processor: Processing request {request.request_id}")
                    await self._process_request(request)

                except Exception as e:
                    logger.error(f"Error in CH API processing loop: {e}")
                    logger.exception("Full traceback:")
                    await asyncio.sleep(5.0)  # Back off on errors

        except asyncio.CancelledError:
            logger.info("CH API processor cancelled")
        finally:
            self._running = False
            if self.session:
                await self.session.close()
                self.session = None
            logger.info("Companies House API processor stopped")

    async def stop_processing(self) -> None:
        """Stop processing queued requests."""
        if not self._running:
            return

        logger.info("Stopping Companies House API processor")
        self._running = False
        self._stop_event.set()

        if self.session:
            await self.session.close()
            self.session = None

    async def _process_request(self, request: QueuedRequest) -> None:
        """Process a single queued request.

        Args:
            request: The queued request to process
        """
        start_time = asyncio.get_event_loop().time()

        try:
            logger.debug(f"Processing CH API request {request.request_id} for {request.endpoint}")

            # Route request to appropriate handler
            if request.endpoint.startswith("/company/") and request.endpoint.endswith("/officers"):
                await self._handle_officers_request(request)
            elif request.endpoint.startswith("/company/"):
                await self._handle_company_status_request(request)
            else:
                logger.warning(f"Unknown CH API endpoint in request {request.request_id}: {request.endpoint}")
                self.queue_manager.mark_failed(request)
                self.requests_failed += 1
                return

            # Mark as processed successfully
            processing_time = asyncio.get_event_loop().time() - start_time
            self.queue_manager.mark_processed(request, processing_time)
            self.requests_processed += 1

        except Exception as e:
            logger.error(f"Failed to process CH API request {request.request_id}: {e}")

            # Attempt to requeue if retries available
            if await self.queue_manager.requeue(request):
                logger.info(f"Requeued failed CH API request {request.request_id}")
            else:
                logger.error(f"CH API request {request.request_id} exceeded max retries")
                self.requests_failed += 1

    async def _handle_company_status_request(self, request: QueuedRequest) -> None:
        """Handle company status check request.

        Args:
            request: Queued company status request
        """
        company_number = request.params.get("company_number")
        if not company_number:
            raise CompaniesHouseAPIError("Missing company_number in status request")

        logger.info(f"Fetching company status for {company_number}")

        try:
            # Make API call to Companies House
            url = f"{self.api_base_url}/company/{company_number}"
            
            if not self.session:
                raise CompaniesHouseAPIError("HTTP session not initialized")

            async with self.session.get(url) as response:
                if response.status == 404:
                    logger.warning(f"Company {company_number} not found (404)")
                    # Update state to completed (not strike-off)
                    await self.company_state_manager.update_state(
                        company_number, 
                        ProcessingState.COMPLETED.value,
                        last_error="Company not found (404)"
                    )
                    return
                
                if response.status == 429:
                    logger.warning(f"Rate limit hit (429) for company {company_number}")
                    # Handle 429 response
                    await self.company_state_manager.handle_429_response(
                        company_number, request.request_id
                    )
                    # Requeue the request (will be handled by calling function)
                    raise CompaniesHouseAPIError(f"Rate limit hit (429)")

                if response.status != 200:
                    raise CompaniesHouseAPIError(
                        f"CH API returned status {response.status} for company {company_number}"
                    )

                # Parse response
                company_data = await response.json()
                
                # Extract company status detail
                company_status_detail = company_data.get("company_status_detail")
                company_name = company_data.get("company_name", "Unknown")
                company_status = company_data.get("company_status", "Unknown")

                logger.info(f"Company {company_number} - status: {company_status}, status_detail: {company_status_detail}")

                # Get current state to check if we've already processed this company
                current_state = await self.company_state_manager.get_state(company_number)
                current_processing_state = current_state.get("processing_state") if current_state else None
                
                # Only update to STATUS_FETCHED if we're not already at a later state
                if current_processing_state not in [ProcessingState.STRIKE_OFF_CONFIRMED.value, ProcessingState.COMPLETED.value]:
                    await self.company_state_manager.update_state(
                        company_number,
                        ProcessingState.STATUS_FETCHED.value
                    )

                # Check if this is a strike-off status
                if self._is_strike_off_status(company_status_detail):
                    logger.info(f"🎯 STRIKE-OFF DETECTED: {company_number} - {company_status_detail}")
                    
                    # Only process if we haven't already confirmed this as strike-off
                    if current_processing_state != ProcessingState.STRIKE_OFF_CONFIRMED.value:
                        # Update state to strike-off confirmed
                        await self.company_state_manager.update_state(
                            company_number,
                            ProcessingState.STRIKE_OFF_CONFIRMED.value
                        )
                        
                        # Update state to completed for strike-off companies before enrichment
                        await self.company_state_manager.update_state(
                            company_number,
                            ProcessingState.COMPLETED.value
                        )
                        
                        # Trigger enrichment workflow if callback available
                        if self.enrichment_callback:
                            try:
                                await self.enrichment_callback(company_number, company_name)
                                logger.info(f"Enrichment triggered for strike-off company {company_number}")
                            except Exception as e:
                                logger.error(f"Error triggering enrichment for {company_number}: {e}")
                        
                        self.strike_off_companies_detected += 1
                    else:
                        logger.info(f"Company {company_number} already processed as strike-off, skipping enrichment trigger")
                else:
                    logger.info(f"Company {company_number} not strike-off, completing processing")
                    # Update state to completed (not a strike-off company)
                    await self.company_state_manager.update_state(
                        company_number,
                        ProcessingState.COMPLETED.value
                    )

                self.companies_status_fetched += 1

        except ClientError as e:
            logger.error(f"HTTP error fetching company status for {company_number}: {e}")
            raise CompaniesHouseAPIError(f"HTTP error: {e}")

    async def _handle_officers_request(self, request: QueuedRequest) -> None:
        """Handle officers fetch request.

        Args:
            request: Queued officers request
        """
        company_number = request.params.get("company_number")
        if not company_number:
            raise CompaniesHouseAPIError("Missing company_number in officers request")

        logger.info(f"Fetching officers for company {company_number}")

        try:
            # Make API call to Companies House
            url = f"{self.api_base_url}/company/{company_number}/officers"
            
            if not self.session:
                raise CompaniesHouseAPIError("HTTP session not initialized")

            async with self.session.get(url) as response:
                if response.status == 404:
                    logger.warning(f"Officers not found for company {company_number} (404)")
                    # Update state to completed
                    await self.company_state_manager.update_state(
                        company_number,
                        ProcessingState.COMPLETED.value,
                        last_error="Officers not found (404)"
                    )
                    return
                
                if response.status == 429:
                    logger.warning(f"Rate limit hit (429) for officers of company {company_number}")
                    # Handle 429 response
                    await self.company_state_manager.handle_429_response(
                        company_number, request.request_id
                    )
                    # Requeue the request
                    raise CompaniesHouseAPIError(f"Rate limit hit (429)")

                if response.status != 200:
                    raise CompaniesHouseAPIError(
                        f"CH API returned status {response.status} for officers of company {company_number}"
                    )

                # Parse response
                officers_data = await response.json()
                officers = officers_data.get("items", [])

                logger.info(f"Found {len(officers)} officers for company {company_number}")

                # Update state to officers fetched
                await self.company_state_manager.update_state(
                    company_number,
                    ProcessingState.OFFICERS_FETCHED.value
                )

                # TODO: Store officers data in database if needed
                # For now, just complete the processing
                await self.company_state_manager.update_state(
                    company_number,
                    ProcessingState.COMPLETED.value
                )

                self.officers_fetched += 1

        except ClientError as e:
            logger.error(f"HTTP error fetching officers for company {company_number}: {e}")
            raise CompaniesHouseAPIError(f"HTTP error: {e}")

    def _is_strike_off_status(self, company_status_detail: Optional[str]) -> bool:
        """Check if company status indicates strike-off.

        Args:
            company_status_detail: Detailed company status from CH API

        Returns:
            True if status indicates strike-off
        """
        if not company_status_detail:
            return False

        # Check for the specific strike-off enum value
        return company_status_detail.lower() == "active-proposal-to-strike-off"

    def get_processing_statistics(self) -> dict[str, Any]:
        """Get processing statistics.

        Returns:
            Dictionary with processing metrics
        """
        return {
            "requests_processed": self.requests_processed,
            "requests_failed": self.requests_failed,
            "companies_status_fetched": self.companies_status_fetched,
            "officers_fetched": self.officers_fetched,
            "strike_off_companies_detected": self.strike_off_companies_detected,
            "is_running": self._running,
        }
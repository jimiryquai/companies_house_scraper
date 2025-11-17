#!/usr/bin/env python3
"""Simplified Companies House Streaming Service.

This service listens to the Companies House streaming API and syncs
company and officer data directly to PostgreSQL.

Workflow:
1. Listen to streaming API for company change events
2. Fetch company data from CH REST API → Upsert to PostgreSQL
3. Fetch officers data from CH REST API → Upsert to PostgreSQL
"""

import asyncio
import logging
import os
import signal
import sys
import time
from collections import deque
from typing import Any, Optional

import requests

from src.database import CompaniesTable, Database, OfficersTable
from src.streaming import StreamingClient, StreamingConfig

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

logger = logging.getLogger(__name__)


class SimplifiedStreamingService:
    """Simplified streaming service that syncs directly to PostgreSQL.

    This service uses 3 separate Companies House Developer Hub applications:
    1. Streaming API - for real-time events
    2. Companies REST API - for fetching company data (600 req/5min)
    3. Officers REST API - for fetching officer data (600 req/5min)
    """

    def __init__(
        self,
        streaming_config: StreamingConfig,
        database_url: str,
        companies_api_key: str,
        officers_api_key: str,
    ) -> None:
        """Initialize the streaming service.

        Args:
            streaming_config: Configuration for streaming API client
            database_url: PostgreSQL database connection URL
            companies_api_key: API key for Companies REST API (App #2)
            officers_api_key: API key for Officers REST API (App #3)
        """
        self.streaming_config = streaming_config
        self.database_url = database_url
        self.companies_api_key = companies_api_key
        self.officers_api_key = officers_api_key

        # Initialize components
        self.streaming_client: Optional[StreamingClient] = None
        self.db = Database(database_url)
        self.companies_table = CompaniesTable(self.db)
        self.officers_table = OfficersTable(self.db)

        # In-memory async event queue
        self.event_queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()

        # Control flags
        self.is_running = False
        self.shutdown_event = asyncio.Event()

        # Statistics
        self.stats = {
            "events_received": 0,
            "companies_synced": 0,
            "officers_synced": 0,
            "errors": 0,
        }

        # Rate limiting (600 requests per 5 minutes = 300 seconds)
        self.companies_api_calls: deque[float] = deque()
        self.officers_api_calls: deque[float] = deque()
        self.rate_limit_window = 300  # 5 minutes in seconds
        self.rate_limit_max = 600  # Max requests per window

    async def start(self) -> None:
        """Start the streaming service."""
        logger.info("Starting simplified streaming service...")
        self.is_running = True

        # Initialize database schema
        logger.info("Initializing database schema...")
        self.db.init_schema()

        # Initialize streaming client
        self.streaming_client = StreamingClient(self.streaming_config)

        # Setup signal handlers for graceful shutdown
        loop = asyncio.get_event_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, lambda: asyncio.create_task(self.shutdown()))

        try:
            # Start streaming client and event processor in parallel
            await asyncio.gather(
                self._stream_events(),
                self._process_events(),
            )
        except Exception as e:
            logger.error(f"Service error: {e}", exc_info=True)
        finally:
            await self.cleanup()

    async def _stream_events(self) -> None:
        """Listen to streaming API and queue events with auto-reconnect."""
        if not self.streaming_client:
            raise RuntimeError("Streaming client not initialized")

        max_reconnect_attempts = 10
        reconnect_delay = 5

        while self.is_running:
            try:
                logger.info("Connecting to streaming API...")
                await self.streaming_client.connect()
                logger.info("Successfully connected to streaming API")

                # Reset reconnect delay on successful connection
                reconnect_delay = 5

                try:
                    async for event in self.streaming_client.stream_events():
                        if not self.is_running:
                            break

                        self.stats["events_received"] += 1
                        await self.event_queue.put(event)
                        logger.debug(f"Queued event for company {event.get('resource_id', 'unknown')}")

                except Exception as e:
                    logger.error(f"Error streaming events: {e}", exc_info=True)
                    # Will retry connection in outer loop

            except Exception as e:
                logger.error(f"Failed to connect to streaming API: {e}", exc_info=True)

                if not self.is_running:
                    break

                logger.info(f"Reconnecting in {reconnect_delay} seconds...")
                await asyncio.sleep(reconnect_delay)

                # Exponential backoff up to 60 seconds
                reconnect_delay = min(reconnect_delay * 2, 60)

    async def _process_events(self) -> None:
        """Process queued events and sync to PostgreSQL."""
        logger.info("Starting event processor...")

        while self.is_running or not self.event_queue.empty():
            try:
                # Get event from queue (non-blocking with timeout)
                try:
                    event = await asyncio.wait_for(self.event_queue.get(), timeout=1.0)
                except asyncio.TimeoutError:
                    continue

                # Process the event
                await self._process_event(event)

            except Exception as e:
                logger.error(f"Error processing event: {e}", exc_info=True)
                self.stats["errors"] += 1

    async def _process_event(self, event: dict[str, Any]) -> None:
        """Process a single company event.

        Args:
            event: Company event from streaming API
        """
        # Extract company number from event
        company_number = event.get("resource_id", "")
        if not company_number:
            logger.warning(f"Event missing company number: {event}")
            return

        logger.info(f"Processing event for company {company_number}")

        max_retries = 3
        retry_delay = 2

        for attempt in range(max_retries):
            try:
                # Step 1: Fetch company data using Companies API (App #2)
                company_data = await self._fetch_company_data(company_number)
                if not company_data:
                    logger.warning(f"No company data found for {company_number}")
                    return

                # Step 2: Upsert company to PostgreSQL with error handling
                try:
                    self.companies_table.upsert_company(company_data)
                    self.stats["companies_synced"] += 1
                    logger.info(f"Synced company {company_number} to PostgreSQL")
                except Exception as db_error:
                    logger.error(f"Database error upserting company {company_number}: {db_error}")
                    if attempt < max_retries - 1:
                        await asyncio.sleep(retry_delay)
                        continue
                    raise

                # Step 3: Fetch officers data using Officers API (App #3)
                officers_data = await self._fetch_officers_data(company_number)

                # Step 4: Upsert officers to PostgreSQL with error handling
                if officers_data:
                    try:
                        self.officers_table.upsert_officers(company_number, officers_data)
                        self.stats["officers_synced"] += len(officers_data)
                        logger.info(f"Synced {len(officers_data)} officers for {company_number}")
                    except Exception as db_error:
                        logger.error(f"Database error upserting officers for {company_number}: {db_error}")
                        if attempt < max_retries - 1:
                            await asyncio.sleep(retry_delay)
                            continue
                        raise

                # Success - break retry loop
                break

            except Exception as e:
                logger.error(f"Failed to process company {company_number} (attempt {attempt + 1}/{max_retries}): {e}", exc_info=True)
                self.stats["errors"] += 1

                if attempt < max_retries - 1:
                    logger.info(f"Retrying in {retry_delay} seconds...")
                    await asyncio.sleep(retry_delay)
                    retry_delay *= 2  # Exponential backoff
                else:
                    logger.error(f"Giving up on company {company_number} after {max_retries} attempts")

    async def _wait_for_rate_limit(self, api_calls: deque[float], api_name: str) -> None:
        """Wait if necessary to respect rate limits.

        Args:
            api_calls: Deque tracking API call timestamps
            api_name: Name of API for logging
        """
        now = time.time()

        # Remove calls outside the time window
        while api_calls and api_calls[0] < now - self.rate_limit_window:
            api_calls.popleft()

        # If at limit, wait until oldest call expires
        if len(api_calls) >= self.rate_limit_max:
            oldest_call = api_calls[0]
            wait_time = (oldest_call + self.rate_limit_window) - now
            if wait_time > 0:
                logger.warning(
                    f"{api_name} rate limit reached ({len(api_calls)}/{self.rate_limit_max}). "
                    f"Waiting {wait_time:.1f}s..."
                )
                await asyncio.sleep(wait_time)

        # Record this call
        api_calls.append(now)

    async def _fetch_company_data(self, company_number: str) -> Optional[dict[str, Any]]:
        """Fetch company data from Companies House REST API.

        Uses dedicated Companies API key (App #2 - 600 req/5min).

        Args:
            company_number: Company number to fetch

        Returns:
            Company data dictionary or None if not found
        """
        # Wait for rate limit before making request
        await self._wait_for_rate_limit(self.companies_api_calls, "Companies API")

        url = f"https://api.company-information.service.gov.uk/company/{company_number}"
        headers = {"Authorization": self.companies_api_key}

        try:
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            return response.json()  # type: ignore[no-any-return]
        except requests.RequestException as e:
            logger.error(f"Error fetching company {company_number}: {e}")
            return None

    async def _fetch_officers_data(self, company_number: str) -> list[dict[str, Any]]:
        """Fetch officers data from Companies House REST API.

        Uses dedicated Officers API key (App #3 - 600 req/5min).

        Args:
            company_number: Company number to fetch officers for

        Returns:
            List of officer data dictionaries
        """
        # Wait for rate limit before making request
        await self._wait_for_rate_limit(self.officers_api_calls, "Officers API")

        url = f"https://api.company-information.service.gov.uk/company/{company_number}/officers"
        headers = {"Authorization": self.officers_api_key}

        try:
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            data = response.json()
            return data.get("items", [])  # type: ignore[no-any-return]
        except requests.RequestException as e:
            logger.error(f"Error fetching officers for {company_number}: {e}")
            return []

    async def shutdown(self) -> None:
        """Gracefully shutdown the service."""
        if not self.is_running:
            return

        logger.info("Shutting down streaming service...")
        self.is_running = False
        self.shutdown_event.set()

        # Print final statistics
        logger.info("Final statistics:")
        logger.info(f"  Events received: {self.stats['events_received']}")
        logger.info(f"  Companies synced: {self.stats['companies_synced']}")
        logger.info(f"  Officers synced: {self.stats['officers_synced']}")
        logger.info(f"  Errors: {self.stats['errors']}")

    async def cleanup(self) -> None:
        """Clean up resources."""
        if self.streaming_client:
            await self.streaming_client.disconnect()
        self.db.close()
        logger.info("Cleanup complete")


async def main() -> None:
    """Main entry point for the streaming service."""
    # Load configuration from environment
    streaming_config = StreamingConfig(
        streaming_api_key=os.getenv("CH_STREAMING_API_KEY", ""),  # App #1
        rest_api_key=os.getenv("CH_COMPANIES_API_KEY", ""),  # App #2 (for compatibility)
    )

    # Get PostgreSQL database URL
    database_url = os.getenv("DATABASE_URL", "")
    if not database_url:
        logger.error("DATABASE_URL environment variable is required")
        sys.exit(1)

    # Get separate API keys for Companies and Officers APIs
    companies_api_key = os.getenv("CH_COMPANIES_API_KEY", "")
    officers_api_key = os.getenv("CH_OFFICERS_API_KEY", "")

    if not companies_api_key:
        logger.error("CH_COMPANIES_API_KEY environment variable is required")
        sys.exit(1)
    if not officers_api_key:
        logger.error("CH_OFFICERS_API_KEY environment variable is required")
        sys.exit(1)

    # Create and start service
    service = SimplifiedStreamingService(
        streaming_config=streaming_config,
        database_url=database_url,
        companies_api_key=companies_api_key,
        officers_api_key=officers_api_key,
    )

    await service.start()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Service interrupted by user")
    except Exception as e:
        logger.error(f"Service failed: {e}", exc_info=True)
        sys.exit(1)

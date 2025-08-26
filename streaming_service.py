#!/usr/bin/env python3
"""Main Companies House Streaming Service Runner.

This script integrates all streaming components to provide real-time monitoring
of company status changes, particularly for strike-off status detection.
Integrates with existing officer import workflow and database cleanup.
"""

import asyncio
import logging
import signal
import sys
from datetime import datetime
from typing import Any, Optional

# Import streaming components
from src.streaming import (
    CompanyEvent,
    ErrorAlertManager,
    EventProcessor,
    HealthMonitor,
    PriorityQueueManager,
    RequestPriority,
    StreamingClient,
    StreamingConfig,
    StreamingDatabase,
)


class StreamingService:
    """Main streaming service that coordinates all components."""

    def __init__(self, config: StreamingConfig) -> None:
        """Initialize the streaming service with configuration."""
        self.config = config
        self.is_running = False
        self.shutdown_event = asyncio.Event()

        # Initialize components
        self.client: Optional[StreamingClient] = None
        self.event_processor: Optional[EventProcessor] = None
        self.database: Optional[StreamingDatabase] = None
        self.health_monitor: Optional[HealthMonitor] = None
        self.error_alerting: Optional[ErrorAlertManager] = None
        self.queue_manager: Optional[PriorityQueueManager] = None

        # API keys
        self.api_key = config.rest_api_key  # For REST API calls

        # Setup logging
        self.logger = self._setup_logging()

        # Statistics
        self.stats: dict[str, Any] = {
            "start_time": datetime.now(),
            "companies_processed": 0,
            "errors": 0,
            "cleanup_operations": 0,
        }

    def _setup_logging(self) -> logging.Logger:
        """Configure structured logging for the service."""
        logger = logging.getLogger("streaming_service")
        logger.setLevel(getattr(logging, self.config.log_level.upper()))

        # Create handler if not exists
        if not logger.handlers:
            handler = logging.StreamHandler(sys.stdout)
            formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
            handler.setFormatter(formatter)
            logger.addHandler(handler)

            # Add file handler if configured
            if self.config.log_file:
                file_handler = logging.FileHandler(self.config.log_file)
                file_handler.setFormatter(formatter)
                logger.addHandler(file_handler)

        return logger

    async def initialize_components(self) -> None:
        """Initialize all streaming components."""
        try:
            self.logger.info("Initializing streaming service components...")

            # Initialize database
            self.database = StreamingDatabase(self.config)
            await self.database.connect()

            # Initialize streaming client
            self.client = StreamingClient(self.config)

            # Initialize event processor
            self.event_processor = EventProcessor(self.config)

            # Register event handlers
            self.event_processor.register_event_handler(
                "company-profile", self._handle_company_event
            )

            # Initialize queue manager for rate-limited API calls
            self.queue_manager = PriorityQueueManager()

            # Initialize health monitor (will be set up after client is ready)
            self.health_monitor = None

            # Initialize error alerting (simplified without requiring config for now)
            self.error_alerting = None  # Will implement later if needed

            self.logger.info("All components initialized successfully")

        except Exception as e:
            self.logger.error(f"Failed to initialize components: {e}")
            raise

    async def _handle_company_event(self, event_data: dict[str, Any]) -> None:
        """Handle company profile events from the stream.

        Args:
            event_data: Raw event data from Companies House stream
        """
        try:
            # Parse the event
            company_event = CompanyEvent.from_dict(event_data)

            self.logger.debug(f"Processing company event for {company_event.company_number}")

            # NOTE: Streaming API only provides basic company_status ("active", "dissolved", etc.)
            # To get detailed status like "Active - Proposal to Strike Off", we need to
            # hit the REST API for each company that has changed

            # Fetch detailed company info from REST API
            await self._process_company_with_rest_api(company_event.company_number)

            self.stats["companies_processed"] += 1

        except Exception as e:
            self.logger.error(f"Error handling company event: {e}")
            self.stats["errors"] += 1

    async def _process_company_with_rest_api(self, company_number: str) -> None:
        """Fetch detailed company info from REST API to get accurate status.

        Args:
            company_number: Company number to fetch detailed info for
        """
        try:
            self.logger.debug(f"Fetching detailed company info for {company_number}")

            # TIME-BASED RATE LIMITING: Adjust delays based on UK business hours
            delay = self._get_adaptive_delay()
            await asyncio.sleep(delay)

            # Use existing REST API to get detailed company info
            import aiohttp

            if not self.api_key:
                self.logger.error("API key not available for REST API calls")
                return

            # Create Basic auth header for REST API
            import base64

            api_key_with_colon = f"{self.api_key}:"
            encoded_credentials = base64.b64encode(api_key_with_colon.encode()).decode()

            headers = {
                "Authorization": f"Basic {encoded_credentials}",
                "Accept": "application/json",
            }

            # Fetch company profile from REST API
            base_url = "https://api.company-information.service.gov.uk"
            url = f"{base_url}/company/{company_number}"

            async with (
                aiohttp.ClientSession() as session,
                session.get(url, headers=headers) as response,
            ):
                if response.status == 200:
                    company_data = await response.json()
                    detailed_status = company_data.get("company_status_detail")

                    self.logger.debug(
                        f"Company {company_number} detailed status: {detailed_status}"
                    )

                    # Check if this is a strike-off status
                    if detailed_status and self._is_strike_off_status(detailed_status):
                        await self._handle_strike_off_company(
                            company_number, detailed_status, company_data
                        )
                    else:
                        # No detailed status or not strike-off status
                        status_display = (
                            detailed_status if detailed_status else "no detailed status"
                        )
                        await self._handle_non_strike_off_company(company_number, status_display)

                elif response.status == 404:
                    self.logger.debug(f"Company {company_number} not found (404)")
                elif response.status == 429:
                    self.logger.warning(f"Rate limited while fetching company {company_number}")
                else:
                    self.logger.warning(
                        f"Failed to fetch company {company_number}: HTTP {response.status}"
                    )

        except Exception as e:
            self.logger.error(f"Error fetching detailed company info for {company_number}: {e}")
            self.stats["errors"] += 1

    def _get_adaptive_delay(self) -> float:
        """Get adaptive delay based on UK business hours and officer import status.

        Returns:
            Delay in seconds between REST API calls
        """
        import datetime

        # Get current UK time
        uk_time = datetime.datetime.now()  # Assuming server runs in UK timezone
        hour = uk_time.hour

        # UK Business Hours: 9 AM - 5 PM (high competition with officer import)
        if 9 <= hour < 17:
            # BUSINESS HOURS: Much longer delay to leave capacity for officer import
            # 2 seconds = 30 calls/min = 150 calls/5min (leaves 450 calls for officer import)
            delay = 2.0
            self.logger.debug(f"Business hours (hour {hour}): Using {delay}s delay")
        else:
            # OFF-HOURS: Shorter delay since officer import may be paused
            # 1 second = 60 calls/min = 300 calls/5min (moderate usage)
            delay = 1.0
            self.logger.debug(f"Off hours (hour {hour}): Using {delay}s delay")

        return delay

    def _is_strike_off_status(self, status: Optional[str]) -> bool:
        """Check if a status indicates strike-off.

        Args:
            status: Company status to check

        Returns:
            True if status indicates strike-off
        """
        if not status:
            return False

        status_lower = status.lower()
        return (
            "proposal to strike off" in status_lower
            or "struck off" in status_lower
            or "strike off" in status_lower
        )

    async def _handle_strike_off_company(
        self,
        company_number: str,
        status: str,
        company_data: dict[str, Any],  # noqa: ARG002
    ) -> None:
        """Handle a company entering strike-off status.

        Args:
            company_number: Company number
            status: Detailed company status
            company_data: Full company data from REST API
        """
        self.logger.info(f"Strike-off status detected for company {company_number}: {status}")

        try:
            # Check if company already exists in our database
            existing_company = await self._check_company_in_database(company_number)

            if existing_company:
                self.logger.info(f"Company {company_number} already in database")
            else:
                self.logger.info(f"New strike-off company {company_number}")
                # Fetch officers for this company using existing API
                await self._fetch_and_store_officers(company_number)

        except Exception as e:
            self.logger.error(f"Error processing strike-off company {company_number}: {e}")
            self.stats["errors"] += 1

    async def _handle_non_strike_off_company(self, company_number: str, status: str) -> None:
        """Handle a company that is not in strike-off status.

        Args:
            company_number: Company number
            status: Detailed company status
        """
        try:
            # Check if this company exists in our database
            # (our database only has strike-off companies)
            existing_company = await self._check_company_in_database(company_number)

            if existing_company:
                # Company exists in our database, which means it was previously in strike-off status
                # Now it has a different status, so we should clean it up
                self.logger.info(
                    f"Company {company_number} left strike-off status: "
                    f"was '{existing_company['company_status_detail']}' now '{status}'"
                )

                await self._cleanup_company_records(company_number)
                self.stats["cleanup_operations"] += 1
            else:
                # Company not in our database - just a regular company, ignore it
                self.logger.debug(f"Company {company_number} not in our database - ignoring")

        except Exception as e:
            self.logger.error(f"Error processing non-strike-off company {company_number}: {e}")
            self.stats["errors"] += 1

    async def _fetch_and_store_officers(self, company_number: str) -> None:
        """Queue officers request using the new queue-based architecture.

        Args:
            company_number: Company number to fetch officers for
        """
        if not self.queue_manager:
            self.logger.error("Queue manager not initialized")
            return

        try:
            self.logger.info(f"Queuing officers request for company {company_number}")

            # Create company processing state record
            if self.database:
                async with self.database.manager.get_connection() as conn:
                    # Insert or update company processing state
                    await conn.execute(
                        """
                        INSERT OR REPLACE INTO company_processing_state
                        (company_number, processing_state, created_at, updated_at, officers_queued_at)
                        VALUES (?, ?, ?, ?, ?)
                    """,
                        (
                            company_number,
                            "officers_queued",
                            datetime.now().isoformat(),
                            datetime.now().isoformat(),
                            datetime.now().isoformat(),
                        ),
                    )
                    await conn.commit()

            # Queue the officers request with HIGH priority (strike-off detection)
            from src.streaming.queue_manager import QueuedRequest

            request = QueuedRequest(
                request_id=f"officers_{company_number}_{int(datetime.now().timestamp())}",
                priority=RequestPriority.HIGH,
                endpoint=f"/company/{company_number}/officers",
                params={
                    "company_number": company_number,
                    "reason": "strike_off_detection",
                    "queued_at": datetime.now().isoformat(),
                },
            )
            success = await self.queue_manager.enqueue(request)

            if success:
                self.logger.info(
                    f"Successfully queued officers request for company {company_number}"
                )
                self.stats["companies_processed"] += 1
            else:
                self.logger.error(f"Failed to queue officers request for company {company_number}")
                self.stats["errors"] += 1

        except Exception as e:
            self.logger.error(f"Error queuing officers request for {company_number}: {e}")
            self.stats["errors"] += 1

    async def _cleanup_company_records(self, company_number: str) -> None:
        """Clean up records for a company no longer in strike-off status.

        Args:
            company_number: Company number to clean up
        """
        try:
            self.logger.info(f"Cleaning up records for company {company_number}")

            # For audit purposes, we'll mark the company as no longer tracked rather than delete
            if self.database:
                async with self.database.manager.get_connection() as conn:
                    # Update the company status to indicate it's no longer in strike-off
                    await conn.execute(
                        "UPDATE companies SET company_status_detail = ?, "
                        "stream_last_updated = ? WHERE company_number = ?",
                        ("Left strike-off status", datetime.now().isoformat(), company_number),
                    )
                    await conn.commit()

                    self.logger.info(
                        f"Updated database: company {company_number} marked as left strike-off"
                    )

            self.logger.info(f"Cleanup completed for company {company_number}")

        except Exception as e:
            self.logger.error(f"Error cleaning up company {company_number}: {e}")

    async def _check_company_in_database(self, company_number: str) -> Optional[dict[str, Any]]:
        """Check if company exists in our database and return its current status.

        Args:
            company_number: Company number to check

        Returns:
            Dictionary with company data if found, None if not found
        """
        try:
            if not self.database:
                return None

            # Use the database manager to get connection
            async with self.database.manager.get_connection() as conn:
                cursor = await conn.execute(
                    "SELECT company_number, company_status_detail FROM companies "
                    "WHERE company_number = ?",
                    (company_number,),
                )
                row = await cursor.fetchone()

                if row:
                    return {"company_number": row[0], "company_status_detail": row[1]}
                return None

        except Exception as e:
            self.logger.error(f"Error checking company {company_number} in database: {e}")
            return None

    async def start_streaming(self) -> None:
        """Start the streaming service and process events."""
        if not self.client or not self.event_processor:
            raise RuntimeError("Components not initialized")

        self.logger.info("Starting Companies House streaming service...")
        self.is_running = True

        try:
            # Connect to streaming API
            await self.client.connect()

            # Initialize health monitor now that client is ready
            self.health_monitor = HealthMonitor(self.client)

            # Start health monitoring
            if self.health_monitor:
                asyncio.create_task(self.health_monitor.start_monitoring())

            # Start processing events
            self.logger.info("🔄 Starting event processing loop...")
            async for event in self.client.stream_events():
                if self.shutdown_event.is_set():
                    break

                self.logger.info(
                    f"📨 Service received event: {event.get('resource_id', 'unknown')}"
                )

                # Process event
                await self.event_processor.process_event(event)

                # Log statistics periodically
                if self.stats["companies_processed"] % 100 == 0:
                    self._log_statistics()

        except Exception as e:
            self.logger.error(f"Error in streaming service: {e}")
            if self.error_alerting:
                # Would send alert in full implementation
                self.logger.error(f"Would send alert: Streaming service error: {e}")
            raise
        finally:
            self.is_running = False
            if self.health_monitor:
                await self.health_monitor.stop_monitoring()

    def _log_statistics(self) -> None:
        """Log current service statistics."""
        uptime = datetime.now() - self.stats["start_time"]

        self.logger.info(
            f"Service statistics - Uptime: {uptime}, "
            f"Companies processed: {self.stats['companies_processed']}, "
            f"Officers imported: {self.stats.get('officers_imported', 0)}, "
            f"Cleanup operations: {self.stats['cleanup_operations']}, "
            f"Errors: {self.stats['errors']}"
        )

    async def shutdown(self) -> None:
        """Gracefully shutdown the streaming service."""
        self.logger.info("Shutting down streaming service...")
        self.shutdown_event.set()

        if self.client:
            await self.client.disconnect()

        if self.database:
            await self.database.disconnect()

        self._log_statistics()
        self.logger.info("Streaming service shutdown complete")

    def setup_signal_handlers(self) -> None:
        """Setup signal handlers for graceful shutdown."""

        def signal_handler(sig: int, _frame: Any) -> None:
            self.logger.info(f"Received signal {sig}, initiating shutdown...")
            asyncio.create_task(self.shutdown())

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)


async def main() -> None:
    """Main entry point for the streaming service."""
    try:
        # Load configuration from config.yaml (same as officer import)
        yaml_config = load_config()
        streaming_api_key = yaml_config.get("api", {}).get("streaming_key", "")

        if not streaming_api_key:
            raise ValueError("streaming_key not found in config.yaml")

        # Create streaming config with key from yaml
        config = StreamingConfig(
            streaming_api_key=streaming_api_key,
            api_base_url=yaml_config.get("api_endpoints", {}).get(
                "stream_base_url", "https://stream.companieshouse.gov.uk"
            ),
        )

        # Create and initialize service
        service = StreamingService(config)
        service.setup_signal_handlers()

        await service.initialize_components()

        # Start streaming
        await service.start_streaming()

    except KeyboardInterrupt:
        logger = logging.getLogger("streaming_service")
        logger.info("Received interrupt signal, shutting down...")
    except Exception as e:
        logger = logging.getLogger("streaming_service")
        logger.error(f"Fatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())

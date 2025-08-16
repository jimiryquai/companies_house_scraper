"""Event processing functionality for Companies House Streaming API.

Handles incoming company events, validates data, extracts company information,
and processes events according to business rules for strike-off detection.
"""

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable, Optional

from .config import StreamingConfig

logger = logging.getLogger(__name__)


class EventValidationError(Exception):
    """Raised when event validation fails."""

    pass


@dataclass
class CompanyEvent:
    """Represents a company event from the Companies House stream."""

    resource_kind: str
    resource_id: str
    company_number: str
    company_name: Optional[str]
    company_status: Optional[str]
    timepoint: int
    published_at: Optional[str] = None
    raw_data: Optional[dict[str, Any]] = None

    @classmethod
    def from_dict(cls, event_data: dict[str, Any]) -> "CompanyEvent":
        """Create CompanyEvent from raw event dictionary."""
        data = event_data.get("data", {})
        event_meta = event_data.get("event", {})

        # Only use company_status_detail - company_status is too generic
        status = data.get("company_status_detail")

        return cls(
            resource_kind=event_data.get("resource_kind", ""),
            resource_id=event_data.get("resource_id", ""),
            company_number=data.get("company_number", ""),
            company_name=data.get("company_name"),
            company_status=status,
            timepoint=event_meta.get("timepoint", 0),
            published_at=event_meta.get("published_at"),
            raw_data=event_data,
        )

    def is_strike_off(self) -> bool:
        """Check if this company event indicates strike-off status."""
        if not self.company_status:
            return False

        # Check for actual strike-off status patterns as they appear in the database
        status_lower = self.company_status.lower()

        return (
            "proposal to strike off" in status_lower
            or "struck off" in status_lower
            or "strike off" in status_lower
        )

    def to_dict(self) -> dict[str, Any]:
        """Convert CompanyEvent back to dictionary format."""
        return {
            "resource_kind": self.resource_kind,
            "resource_id": self.resource_id,
            "company_number": self.company_number,
            "company_name": self.company_name,
            "company_status": self.company_status,
            "timepoint": self.timepoint,
            "published_at": self.published_at,
        }


class EventProcessor:
    """Processes events from Companies House Streaming API.

    Validates incoming events, extracts company data, and routes events
    to appropriate handlers for database updates and business logic processing.
    """

    def __init__(self, config: StreamingConfig):
        """Initialize the event processor."""
        self.config = config
        self.batch_size = config.batch_size

        # Processing statistics
        self.processed_events = 0
        self.failed_events = 0
        self.start_time = datetime.now()

        # Event handling
        self.event_handlers: dict[str, Callable[..., Any]] = {}
        self._shutdown_event = asyncio.Event()

        # Setup logging
        self._setup_logging()

    def _setup_logging(self) -> None:
        """Configure logging for the event processor."""
        log_level = getattr(logging, self.config.log_level.upper())
        logger.setLevel(log_level)

    def register_event_handler(self, event_type: str, handler: Callable[..., Any]) -> None:
        """Register an event handler for specific event types.

        Args:
            event_type: Type of event to handle (e.g., 'company-profile', 'company-officers')
            handler: Async function to handle the event
        """
        self.event_handlers[event_type] = handler
        logger.info(f"Registered handler for event type: {event_type}")

    async def process_event(self, event_data: dict[str, Any], timeout: float = 30.0) -> bool:
        """Process a single event with timeout handling.

        Args:
            event_data: Raw event data from the stream
            timeout: Processing timeout in seconds

        Returns:
            True if processing succeeded, False otherwise
        """
        try:
            # Validate event structure
            self._validate_event(event_data)

            # Extract event type
            event_type = event_data.get("resource_kind", "unknown")

            # Route to appropriate handler with timeout
            if event_type in self.event_handlers:
                handler = self.event_handlers[event_type]
                try:
                    await asyncio.wait_for(handler(event_data), timeout=timeout)
                except asyncio.TimeoutError:
                    logger.error(
                        f"Event processing timeout after {timeout}s for event type: {event_type}"
                    )
                    self.failed_events += 1
                    return False
            else:
                # Default handling - log and continue
                logger.debug(f"No handler registered for event type: {event_type}")
                try:
                    await asyncio.wait_for(self._default_event_handler(event_data), timeout=timeout)
                except asyncio.TimeoutError:
                    logger.error(f"Default event processing timeout after {timeout}s")
                    self.failed_events += 1
                    return False

            self.processed_events += 1
            logger.debug(
                f"Successfully processed event: {event_data.get('resource_id', 'unknown')}"
            )
            return True

        except EventValidationError as e:
            logger.warning(f"Event validation failed: {e}")
            self.failed_events += 1
            return False
        except Exception as e:
            self.failed_events += 1
            logger.error(f"Failed to process event: {e}", exc_info=True)
            return False

    async def process_batch(self, events: list[dict[str, Any]]) -> list[bool]:
        """Process a batch of events.

        Args:
            events: List of event data dictionaries

        Returns:
            List of boolean results for each event
        """
        logger.info(f"Processing batch of {len(events)} events")

        # Process events concurrently
        tasks = [self.process_event(event) for event in events]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Convert exceptions to False
        processed_results = []
        for result in results:
            if isinstance(result, Exception):
                processed_results.append(False)
            else:
                processed_results.append(bool(result))

        success_count = sum(processed_results)
        logger.info(f"Batch processing completed: {success_count}/{len(events)} successful")

        return processed_results

    def _validate_event(self, event_data: dict[str, Any]) -> None:
        """Validate event data structure.

        Args:
            event_data: Raw event data to validate

        Raises:
            EventValidationError: If validation fails
        """
        # Check required top-level fields
        required_fields = ["resource_kind", "data"]
        for field in required_fields:
            if field not in event_data:
                raise EventValidationError(f"Missing required field: {field}")

        # Validate data section
        data = event_data["data"]
        if not isinstance(data, dict):
            raise EventValidationError("Event data must be a dictionary")

        if not data:
            raise EventValidationError("Event data is empty")

        # Validate company events have company_number
        resource_kind = event_data["resource_kind"]
        if resource_kind.startswith("company") and "company_number" not in data:
            raise EventValidationError("Missing company_number in company event")

    def _extract_company_data(self, event_data: dict[str, Any]) -> dict[str, Any]:
        """Extract company data from event for database storage.

        Args:
            event_data: Raw event data

        Returns:
            Dictionary of extracted company data
        """
        data = event_data.get("data", {})
        address = data.get("registered_office_address", {})

        # Handle SIC codes - can be a list or single value
        sic_codes = data.get("sic_codes")
        if isinstance(sic_codes, list):
            sic_codes_str = ",".join(sic_codes) if sic_codes else None
        else:
            sic_codes_str = sic_codes

        return {
            "company_number": data.get("company_number"),
            "company_name": data.get("company_name"),
            "company_status": data.get("company_status"),
            "company_status_detail": data.get("company_status_detail"),
            "incorporation_date": data.get("date_of_creation"),
            "sic_codes": sic_codes_str,
            "address_line_1": address.get("address_line_1"),
            "address_line_2": address.get("address_line_2"),
            "locality": address.get("locality"),
            "region": address.get("region"),
            "country": address.get("country"),
            "postal_code": address.get("postal_code"),
            "premises": address.get("premises"),
        }

    async def _default_event_handler(self, event_data: dict[str, Any]) -> None:
        """Default handler for events with no specific handler.

        Args:
            event_data: Raw event data
        """
        event_type = event_data.get("resource_kind", "unknown")
        resource_id = event_data.get("resource_id", "unknown")

        logger.debug(
            f"Processing {event_type} event for resource {resource_id} with default handler"
        )

        # For company events, we might want to log strike-off status changes
        if event_type == "company-profile":
            company_event = CompanyEvent.from_dict(event_data)
            if company_event.is_strike_off():
                logger.info(
                    f"Strike-off status detected for company {company_event.company_number}"
                )

    def get_processing_stats(self) -> dict[str, Any]:
        """Get processing statistics.

        Returns:
            Dictionary with processing metrics
        """
        total_events = self.processed_events + self.failed_events
        success_rate = (self.processed_events / total_events * 100) if total_events > 0 else 0
        uptime = (datetime.now() - self.start_time).total_seconds()

        return {
            "processed_events": self.processed_events,
            "failed_events": self.failed_events,
            "total_events": total_events,
            "success_rate": round(success_rate, 2),
            "uptime_seconds": round(uptime, 2),
        }

    def reset_stats(self) -> None:
        """Reset processing statistics."""
        self.processed_events = 0
        self.failed_events = 0
        self.start_time = datetime.now()
        logger.info("Processing statistics reset")

    async def shutdown(self) -> None:
        """Gracefully shutdown the event processor."""
        logger.info("Shutting down event processor...")
        self._shutdown_event.set()

    def is_shutdown(self) -> bool:
        """Check if the processor is shutdown."""
        return self._shutdown_event.is_set()

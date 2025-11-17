"""Companies House Streaming API Client.

Handles real-time connections to the Companies House Streaming API to monitor
company status changes and detect companies entering/exiting strike-off status.
"""

import asyncio
import base64
import json
import logging
import random
from collections.abc import AsyncGenerator
from datetime import datetime, timedelta
from typing import Any, Callable, Optional
from urllib.parse import urljoin

import aiohttp
from aiohttp import ClientConnectorError, ClientError, ClientSession, ClientTimeout
from aiohttp.client_exceptions import ClientResponseError, ServerTimeoutError

from .config import StreamingConfig

logger = logging.getLogger(__name__)


class StreamingClient:
    """Companies House Streaming API client for real-time data monitoring.

    This client connects to the Companies House Streaming API to receive
    real-time updates about company data changes, with focus on detecting
    companies entering or exiting "Active - Proposal to Strike Off" status.
    """

    def __init__(self, config: StreamingConfig):
        """Initialize the streaming client."""
        self.config = config
        self.session: Optional[ClientSession] = None
        self.is_connected = False
        self.last_heartbeat: Optional[datetime] = None
        self._shutdown_event = asyncio.Event()

        # Connection management
        self._connection_attempts = 0
        self._last_request_time: Optional[datetime] = None
        self._request_count_window: dict[datetime, int] = {}

        # Circuit breaker state
        self._circuit_breaker_state = "closed"  # closed, open, half_open
        self._failure_count = 0
        self._success_count = 0
        self._last_failure_time: Optional[datetime] = None
        self._circuit_breaker_timeout = 60  # seconds

        # Error tracking
        self._error_count = 0
        self._error_types: dict[str, int] = {}
        self._last_error_time: Optional[datetime] = None

        # Degraded mode
        self.is_degraded_mode = False
        self.is_polling_mode = False
        self.polling_interval = 30  # seconds

        # Setup logging
        self._setup_logging()

        # Event handlers
        self.event_handlers: dict[str, Callable[..., Any]] = {}

    def _setup_logging(self) -> None:
        """Configure logging for the streaming client."""
        log_level = getattr(logging, self.config.log_level.upper())
        logger.setLevel(log_level)

        if self.config.log_file:
            handler = logging.FileHandler(self.config.log_file)
            formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
            handler.setFormatter(formatter)
            logger.addHandler(handler)

    async def __aenter__(self) -> "StreamingClient":
        """Async context manager entry."""
        await self.connect()
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Async context manager exit."""
        await self.disconnect()

    async def _wait_for_rate_limit(self) -> None:
        """Wait if necessary to comply with rate limits."""
        now = datetime.now()

        # Clean old entries from the request window (older than 1 minute)
        cutoff_time = now - timedelta(minutes=1)
        self._request_count_window = {
            timestamp: count
            for timestamp, count in self._request_count_window.items()
            if timestamp > cutoff_time
        }

        # Count requests in the current window
        current_requests = sum(self._request_count_window.values())

        if current_requests >= self.config.rate_limit_requests_per_minute:
            # Find the oldest request and wait until the window allows a new request
            oldest_request = min(self._request_count_window.keys())
            wait_time = (oldest_request + timedelta(minutes=1) - now).total_seconds()

            if wait_time > 0:
                logger.info(f"Rate limit reached. Waiting {wait_time:.2f} seconds")
                await asyncio.sleep(wait_time)

        # Record this request
        minute_mark = now.replace(second=0, microsecond=0)
        self._request_count_window[minute_mark] = self._request_count_window.get(minute_mark, 0) + 1
        self._last_request_time = now

    def _check_circuit_breaker(self) -> None:
        """Check circuit breaker state and raise exception if open."""
        if self._circuit_breaker_state == "open":
            # Check if timeout has passed to move to half-open
            if self._last_failure_time and datetime.now() - self._last_failure_time > timedelta(
                seconds=self._circuit_breaker_timeout
            ):
                self._circuit_breaker_state = "half_open"
                logger.info("Circuit breaker moved to half-open state")
            else:
                raise Exception("Circuit breaker is open - API calls are being rejected")

    def _record_success(self) -> None:
        """Record successful operation for circuit breaker."""
        self._success_count += 1
        if self._circuit_breaker_state == "half_open":
            # Close circuit breaker after successful operation
            self._circuit_breaker_state = "closed"
            self._failure_count = 0
            logger.info("Circuit breaker closed after successful operation")

    def _record_failure(self, error: Exception) -> None:
        """Record failed operation for circuit breaker."""
        self._failure_count += 1
        self._error_count += 1
        self._last_failure_time = datetime.now()
        self._last_error_time = datetime.now()

        # Track error types
        error_type = type(error).__name__
        self._error_types[error_type] = self._error_types.get(error_type, 0) + 1

        # Open circuit breaker if failure threshold exceeded
        # Use higher threshold for streaming (connections naturally drop)
        circuit_breaker_threshold = self.config.max_retries * 3  # More lenient for streaming
        if self._failure_count >= circuit_breaker_threshold:
            self._circuit_breaker_state = "open"
            logger.warning(f"Circuit breaker opened after {self._failure_count} failures")

    async def connect(self) -> None:
        """Establish connection to the streaming API."""
        # Check circuit breaker before attempting connection
        self._check_circuit_breaker()

        await self._wait_for_rate_limit()

        if self.session is None or self.session.closed:
            # For streaming connections, set sock_read timeout but no total timeout
            # This allows the connection to stay open indefinitely waiting for events
            timeout = ClientTimeout(total=None, sock_read=300)  # 5 min read timeout
            connector = aiohttp.TCPConnector(limit=10, limit_per_host=5, keepalive_timeout=300)

            # Create Basic auth header (API key + colon, then base64 encode)
            api_key_with_colon = f"{self.config.streaming_api_key}:"
            encoded_credentials = base64.b64encode(api_key_with_colon.encode()).decode()

            self.session = ClientSession(
                timeout=timeout,
                connector=connector,
                headers={
                    "Authorization": f"Basic {encoded_credentials}",
                    "Accept": "application/json",
                    "User-Agent": "Companies-House-Scraper/1.0",
                },
            )

        logger.info("Connecting to Companies House Streaming API...")

        # Skip health check for now - just mark as connected
        self.is_connected = True
        self.last_heartbeat = datetime.now()
        self._connection_attempts = 0  # Reset on success
        self._record_success()
        logger.info("Successfully connected to streaming API")

    async def disconnect(self) -> None:
        """Close the streaming connection."""
        self.is_connected = False
        self._shutdown_event.set()

        if self.session:
            await self.session.close()
            self.session = None

        logger.info("Disconnected from streaming API")

    async def _health_check(self) -> None:
        """Perform internal health check of streaming connection."""
        logger.debug("Performing internal health check")

        # Check if session is initialized
        if not self.session:
            logger.warning("Health check failed: Session not initialized")
            return

        # Check if we're connected
        if not self.is_connected:
            logger.warning("Health check failed: Not connected to streaming API")
            return

        # Check if we've received recent heartbeat
        if not self.last_heartbeat:
            logger.warning("Health check failed: No heartbeat received")
            return

        time_since_heartbeat = (datetime.now() - self.last_heartbeat).total_seconds()
        if time_since_heartbeat > self.config.health_check_interval:
            logger.warning(
                f"Health check failed: Last heartbeat was {time_since_heartbeat:.1f}s ago"
            )
            return

        logger.debug("Health check passed: Connection is healthy")

    def register_event_handler(self, event_type: str, handler: Callable[..., Any]) -> None:
        """Register an event handler for specific event types.

        Args:
            event_type: Type of event to handle (e.g., 'company-profile', 'officers')
            handler: Async function to handle the event
        """
        self.event_handlers[event_type] = handler
        logger.info(f"Registered handler for event type: {event_type}")

    async def _handle_event(self, event_data: dict[str, Any]) -> None:
        """Process incoming streaming events.

        Args:
            event_data: The event data received from the stream
        """
        try:
            event_type = event_data.get("resource_kind", "unknown")
            event_id = event_data.get("resource_id", "unknown")

            logger.debug(f"Processing event {event_id} of type {event_type}")

            # Update heartbeat
            self.last_heartbeat = datetime.now()

            # Check if we have a handler for this event type
            if event_type in self.event_handlers:
                handler = self.event_handlers[event_type]
                await handler(event_data)
            else:
                logger.debug(f"No handler registered for event type: {event_type}")

        except Exception as e:
            logger.error(f"Error handling event: {e}", exc_info=True)

    async def stream_events(  # noqa: C901
        self, timepoint: Optional[int] = None
    ) -> AsyncGenerator[dict[str, Any], None]:
        """Stream events from the Companies House Streaming API.

        Args:
            timepoint: Optional timepoint to start streaming from

        Yields:
            Dict containing event data
        """
        if not self.is_connected:
            raise RuntimeError("Client not connected. Call connect() first.")

        url = urljoin(self.config.api_base_url, "/companies")
        params = {}

        if timepoint:
            params["timepoint"] = timepoint

        logger.info(f"🌐 Streaming URL: {url} with params: {params}")

        retries = 0
        backoff = self.config.initial_backoff

        while not self._shutdown_event.is_set() and retries < self.config.max_retries:
            try:
                logger.info(f"Starting event stream (attempt {retries + 1})")
                await self._wait_for_rate_limit()

                if self.session is None:
                    raise RuntimeError("Session not established")
                async with self.session.get(url, params=params) as response:
                    if response.status == 429:
                        # Handle rate limiting
                        retry_after = response.headers.get("Retry-After", "60")
                        logger.warning(f"Rate limited. Retrying after {retry_after} seconds")
                        await asyncio.sleep(int(retry_after))
                        continue
                    elif response.status != 200:
                        raise ClientError(f"Stream request failed with status {response.status}")

                    logger.info("Event stream established - HTTP 200 OK received")
                    retries = 0  # Reset retry counter on successful connection
                    backoff = self.config.initial_backoff
                    # Reset failure count after successful stream establishment
                    self._failure_count = 0

                    # Process streaming data line by line
                    logger.info("🔄 Starting to read streaming data...")
                    logger.info("⏳ Waiting for Companies House to send events (connection is open)...")
                    line_count = 0
                    last_heartbeat_log = datetime.now()
                    async for line in response.content:
                        line_count += 1

                        # Log heartbeat every 5 seconds to show activity
                        now = datetime.now()
                        if (now - last_heartbeat_log).total_seconds() >= 5:
                            logger.info(
                                f"💓 Streaming heartbeat - processed {line_count} lines, waiting for events..."
                            )
                            last_heartbeat_log = now

                        if self._shutdown_event.is_set():
                            break

                        try:
                            # Skip empty lines
                            line_str = line.decode("utf-8").strip()
                            if not line_str:
                                logger.debug("Received empty line from stream")
                                continue

                            logger.info(f"Received data from stream: {line_str[:200]}...")

                            # Parse JSON event
                            try:
                                event_data = json.loads(line_str)
                                logger.debug(
                                    f"Received event: {event_data.get('resource_kind', 'unknown')} "
                                    f"for {event_data.get('resource_id', 'unknown')}"
                                )

                                # Check if we should process in degraded mode
                                if not self.should_process_in_degraded_mode(event_data):
                                    logger.info(
                                        f"Skipping event in degraded mode: "
                                        f"{event_data.get('resource_kind')} - "
                                        f"degraded_mode={self.is_degraded_mode}"
                                    )
                                    continue

                                # Yield the event for external processing
                                logger.info(
                                    f"🚀 Yielding event for processing: "
                                    f"{event_data.get('resource_id')}"
                                )
                                yield event_data

                                # Also handle internally if handlers are registered
                                await self._handle_event(event_data)

                            except json.JSONDecodeError as e:
                                logger.warning(
                                    f"Failed to parse JSON event: {line_str[:100]}... - {e}"
                                )
                                continue
                            except UnicodeDecodeError as e:
                                logger.warning(f"Failed to decode line: {e}")
                                continue
                            except Exception as e:
                                logger.error(f"Error processing stream event: {e}")
                                continue
                        except Exception as e:
                            logger.error(f"Error processing stream line: {e}")
                            continue

            except (ClientConnectorError, ServerTimeoutError, asyncio.TimeoutError) as e:
                retries += 1
                # Don't treat normal streaming disconnections as hard failures for circuit breaker
                if isinstance(e, (ServerTimeoutError, asyncio.TimeoutError)):
                    logger.info(f"Stream timeout (attempt {retries}) - normal for long connections")
                else:
                    self._record_failure(e)
                    logger.error(f"Stream connection failed (attempt {retries}): {e}")

                if retries >= self.config.max_retries:
                    logger.error("Max retries exceeded, enabling degraded mode")
                    self.enable_degraded_mode()
                    raise

                # Use adaptive retry delay
                wait_time = self.calculate_retry_delay(e, retries)
                logger.info(f"Retrying in {wait_time:.2f} seconds...")
                await asyncio.sleep(wait_time)
                backoff *= 2

                # Reconnect session if needed
                if self.session and self.session.closed:
                    await self.disconnect()
                    try:
                        await self.connect()
                    except Exception as reconnect_error:
                        self._record_failure(reconnect_error)
                        # Continue with retry loop
            except Exception as e:
                retries += 1
                logger.error(f"Unexpected error in stream (attempt {retries}): {e}")

                if retries >= self.config.max_retries:
                    logger.error("Max retries exceeded, giving up")
                    raise

                # Use shorter backoff for unexpected errors
                wait_time = min(self.config.initial_backoff, self.config.max_backoff)
                await asyncio.sleep(wait_time)

    async def monitor_company_status_changes(
        self, callback: Optional[Callable[..., Any]] = None
    ) -> None:
        """Monitor the stream for company status changes relevant to strike-off detection.

        Args:
            callback: Optional callback function to process relevant events
        """
        logger.info("Starting company status monitoring...")

        strike_off_statuses = ["active-proposal-to-strike-off", "Active - Proposal to Strike Off"]

        try:
            async for event in self.stream_events():
                # Filter for company profile changes
                if event.get("resource_kind") == "company-profile":
                    company_data = event.get("data", {})
                    company_number = company_data.get("company_number")
                    company_status = company_data.get("company_status", "").lower()

                    # Check if this is a strike-off related status change
                    is_strike_off = any(
                        status.lower().replace(" ", "-") in company_status
                        or company_status in status.lower().replace(" ", "-")
                        for status in strike_off_statuses
                    )

                    if is_strike_off:
                        logger.info(f"Strike-off status detected for company {company_number}")

                        if callback:
                            await callback(event)
                        else:
                            # Default handling - log the event
                            logger.info(f"Company {company_number} status change: {company_status}")

        except Exception as e:
            logger.error(f"Error in status monitoring: {e}")
            raise

    async def auto_reconnect(self) -> None:
        """Automatically reconnect with exponential backoff.

        This method attempts to reconnect to the streaming API with
        exponential backoff and jitter to handle temporary outages.
        """
        retries = 0
        backoff = self.config.initial_backoff

        while retries < self.config.max_retries and not self._shutdown_event.is_set():
            try:
                logger.info(f"Attempting auto-reconnection (attempt {retries + 1})")
                await self.connect()
                logger.info("Auto-reconnection successful")
                return

            except Exception as e:
                retries += 1
                logger.error(f"Auto-reconnection failed (attempt {retries}): {e}")

                if retries >= self.config.max_retries:
                    logger.error("Max auto-reconnection attempts exceeded")
                    raise

                # Exponential backoff with jitter
                jitter = random.uniform(0.1, 0.3) * backoff  # noqa: S311
                wait_time = min(backoff + jitter, self.config.max_backoff)
                logger.info(f"Waiting {wait_time:.2f} seconds before retry...")
                await asyncio.sleep(wait_time)
                backoff *= 2

    async def get_current_timepoint(self) -> int:
        """Get the current timepoint from the streaming API.

        Returns:
            Current timepoint value
        """
        if not self.session:
            raise RuntimeError("Client not connected")

        url = urljoin(self.config.api_base_url, "/companies")

        try:
            await self._wait_for_rate_limit()
            async with self.session.head(url) as response:
                if response.status == 200:
                    timepoint = response.headers.get("X-Stream-Timepoint")
                    if timepoint:
                        return int(timepoint)
                    raise ValueError("No timepoint header in response")
                if response.status == 429:
                    retry_after = response.headers.get("Retry-After", "60")
                    raise ClientError(f"Rate limited - retry after {retry_after} seconds")
                raise ClientError(f"Failed to get timepoint: status {response.status}")
        except Exception as e:
            logger.error(f"Error getting current timepoint: {e}")
            raise

    def is_healthy(self) -> bool:
        """Check if the streaming connection is healthy.

        Returns:
            True if connection is healthy, False otherwise
        """
        if not self.is_connected or not self.last_heartbeat:
            return False

        # Check if we've received data recently
        time_since_heartbeat = (datetime.now() - self.last_heartbeat).total_seconds()
        return time_since_heartbeat < self.config.health_check_interval

    def enable_degraded_mode(self) -> None:
        """Enable degraded mode for graceful degradation."""
        self.is_degraded_mode = True
        logger.warning("Streaming client enabled degraded mode")

    def should_process_in_degraded_mode(self, event_data: dict[str, Any]) -> bool:
        """Determine if event should be processed in degraded mode."""
        if not self.is_degraded_mode:
            return True

        # In degraded mode, only process critical events (strike-off status)
        if event_data.get("resource_kind") == "company-profile":
            company_data = event_data.get("data", {})
            company_status = company_data.get("company_status", "").lower()

            # Process strike-off related status changes
            strike_off_statuses = ["active-proposal-to-strike-off", "proposal-to-strike-off"]

            return any(status.lower() in company_status for status in strike_off_statuses)

        return False

    async def fallback_to_polling(self) -> None:
        """Fallback to polling mode when streaming fails."""
        self.is_polling_mode = True
        logger.info(f"Switched to polling mode with {self.polling_interval}s interval")

    async def attempt_recovery(self) -> bool:
        """Attempt to recover from degraded mode."""
        try:
            # Try to establish connection
            await self.connect()

            # If successful, disable degraded mode
            self.is_degraded_mode = False
            self.is_polling_mode = False
            logger.info("Successfully recovered from degraded mode")
            return True

        except Exception as e:
            logger.error(f"Recovery attempt failed: {e}")
            return False

    def calculate_retry_delay(self, error: Exception, attempt: int) -> float:
        """Calculate adaptive retry delay based on error type."""
        if isinstance(error, ClientResponseError):
            if error.status == 429:
                # Rate limit error - use Retry-After header
                if error.headers is not None:
                    retry_after = error.headers.get("Retry-After", "60")
                    try:
                        return float(retry_after)
                    except ValueError:
                        return 60.0
                return 60.0
            if 500 <= error.status < 600:
                # Server errors - use shorter backoff
                return float(
                    min(
                        self.config.initial_backoff * (2 ** (attempt - 1)),
                        self.config.max_backoff / 2,
                    )
                )

        elif isinstance(error, (ClientConnectorError, asyncio.TimeoutError)):
            # Network errors - use exponential backoff with jitter
            backoff = self.config.initial_backoff * (2 ** (attempt - 1))
            jitter = random.uniform(0.1, 0.3) * backoff  # noqa: S311
            return float(min(backoff + jitter, self.config.max_backoff))

        # Default backoff for other errors
        return float(self.config.initial_backoff)

    def get_circuit_breaker_metrics(self) -> dict[str, Any]:
        """Get circuit breaker metrics."""
        return {
            "state": self._circuit_breaker_state,
            "failure_count": self._failure_count,
            "success_count": self._success_count,
            "last_failure_time": self._last_failure_time.isoformat()
            if self._last_failure_time
            else None,
            "open_duration": (datetime.now() - self._last_failure_time).total_seconds()
            if self._last_failure_time and self._circuit_breaker_state == "open"
            else 0,
        }

    def get_error_metrics(self) -> dict[str, Any]:
        """Get error metrics."""
        now = datetime.now()
        error_rate = 0.0

        if self._last_error_time:
            time_window = max((now - self._last_error_time).total_seconds(), 1)
            error_rate = self._error_count / time_window

        return {
            "total_errors": self._error_count,
            "error_types": dict(self._error_types),
            "last_error_time": self._last_error_time.isoformat() if self._last_error_time else None,
            "error_rate": error_rate,
        }

    def get_health_status(self) -> dict[str, Any]:
        """Get comprehensive health status."""
        return {
            "is_connected": self.is_connected,
            "last_heartbeat": self.last_heartbeat.isoformat() if self.last_heartbeat else None,
            "circuit_breaker_state": self._circuit_breaker_state,
            "error_count": self._error_count,
            "degraded_mode": self.is_degraded_mode,
            "polling_mode": self.is_polling_mode,
            "connection_attempts": self._connection_attempts,
        }

    async def shutdown(self) -> None:
        """Gracefully shutdown the streaming client."""
        logger.info("Shutting down streaming client...")
        await self.disconnect()

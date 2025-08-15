"""
Companies House Streaming API Client

Handles real-time connections to the Companies House Streaming API to monitor
company status changes and detect companies entering/exiting strike-off status.
"""

import asyncio
import json
import logging
import random
import time
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, Callable, AsyncGenerator
from urllib.parse import urljoin

import aiohttp
from aiohttp import ClientSession, ClientTimeout, ClientError, ClientConnectorError
from aiohttp.client_exceptions import ServerTimeoutError, ClientResponseError

from .config import StreamingConfig


logger = logging.getLogger(__name__)


class StreamingClient:
    """
    Companies House Streaming API client for real-time data monitoring.
    
    This client connects to the Companies House Streaming API to receive
    real-time updates about company data changes, with focus on detecting
    companies entering or exiting "Active - Proposal to Strike Off" status.
    """
    
    def __init__(self, config: StreamingConfig):
        """Initialize the streaming client."""
        self.config = config
        self.session: Optional[ClientSession] = None
        self.is_connected = False
        self.last_heartbeat = None
        self._shutdown_event = asyncio.Event()
        
        # Connection management
        self._connection_attempts = 0
        self._last_request_time: Optional[datetime] = None
        self._request_count_window: Dict[datetime, int] = {}
        
        # Setup logging
        self._setup_logging()
        
        # Event handlers
        self.event_handlers: Dict[str, Callable] = {}
        
    def _setup_logging(self) -> None:
        """Configure logging for the streaming client."""
        log_level = getattr(logging, self.config.log_level.upper())
        logger.setLevel(log_level)
        
        if self.config.log_file:
            handler = logging.FileHandler(self.config.log_file)
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
    
    async def __aenter__(self):
        """Async context manager entry."""
        await self.connect()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.disconnect()
    
    async def _wait_for_rate_limit(self) -> None:
        """Wait if necessary to comply with rate limits."""
        now = datetime.now()
        
        # Clean old entries from the request window (older than 1 minute)
        cutoff_time = now - timedelta(minutes=1)
        self._request_count_window = {
            timestamp: count for timestamp, count in self._request_count_window.items()
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

    async def connect(self) -> None:
        """Establish connection to the streaming API."""
        await self._wait_for_rate_limit()
        
        if self.session is None or self.session.closed:
            timeout = ClientTimeout(total=self.config.connection_timeout)
            connector = aiohttp.TCPConnector(
                limit=10,
                limit_per_host=5,
                keepalive_timeout=30
            )
            
            self.session = ClientSession(
                timeout=timeout,
                connector=connector,
                headers={
                    'Authorization': f'Bearer {self.config.streaming_api_key}',
                    'Accept': 'application/json',
                    'User-Agent': 'Companies-House-Scraper/1.0'
                }
            )
            
        logger.info("Connecting to Companies House Streaming API...")
        
        # Test connection with health check
        try:
            await self._health_check()
            self.is_connected = True
            self.last_heartbeat = datetime.now()
            self._connection_attempts = 0  # Reset on success
            logger.info("Successfully connected to streaming API")
        except Exception as e:
            logger.error(f"Failed to connect to streaming API: {e}")
            await self.disconnect()
            raise
    
    async def disconnect(self) -> None:
        """Close the streaming connection."""
        self.is_connected = False
        self._shutdown_event.set()
        
        if self.session:
            await self.session.close()
            self.session = None
            
        logger.info("Disconnected from streaming API")
    
    async def _health_check(self) -> None:
        """Perform health check against the streaming API."""
        if not self.session:
            raise RuntimeError("Session not initialized")
            
        url = urljoin(self.config.api_base_url, '/healthcheck')
        
        try:
            await self._wait_for_rate_limit()
            async with self.session.get(url) as response:
                if response.status == 200:
                    logger.debug("Health check passed")
                elif response.status == 429:
                    # Rate limit exceeded
                    retry_after = response.headers.get('Retry-After', '60')
                    logger.warning(f"Rate limit exceeded. Retry after {retry_after} seconds")
                    raise ClientError(f"Health check failed with status {response.status}")
                elif response.status in [401, 403]:
                    # Authentication/Authorization errors
                    logger.error("Authentication failed - check API key")
                    raise ClientError(f"Health check failed with status {response.status}")
                else:
                    raise ClientError(f"Health check failed with status {response.status}")
        except (ClientConnectorError, ServerTimeoutError, asyncio.TimeoutError) as e:
            logger.error(f"Health check connection failed: {e}")
            raise
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            raise
    
    def register_event_handler(self, event_type: str, handler: Callable) -> None:
        """
        Register an event handler for specific event types.
        
        Args:
            event_type: Type of event to handle (e.g., 'company-profile', 'officers')
            handler: Async function to handle the event
        """
        self.event_handlers[event_type] = handler
        logger.info(f"Registered handler for event type: {event_type}")
    
    async def _handle_event(self, event_data: Dict[str, Any]) -> None:
        """
        Process incoming streaming events.
        
        Args:
            event_data: The event data received from the stream
        """
        try:
            event_type = event_data.get('resource_kind', 'unknown')
            event_id = event_data.get('resource_id', 'unknown')
            
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
    
    async def stream_events(self, 
                          timepoint: Optional[int] = None) -> AsyncGenerator[Dict[str, Any], None]:
        """
        Stream events from the Companies House Streaming API.
        
        Args:
            timepoint: Optional timepoint to start streaming from
            
        Yields:
            Dict containing event data
        """
        if not self.is_connected:
            raise RuntimeError("Client not connected. Call connect() first.")
            
        url = urljoin(self.config.api_base_url, '/firehose')
        params = {}
        
        if timepoint:
            params['timepoint'] = timepoint
            
        retries = 0
        backoff = self.config.initial_backoff
        
        while not self._shutdown_event.is_set() and retries < self.config.max_retries:
            try:
                logger.info(f"Starting event stream (attempt {retries + 1})")
                await self._wait_for_rate_limit()
                
                async with self.session.get(url, params=params) as response:
                    if response.status == 429:
                        # Handle rate limiting
                        retry_after = response.headers.get('Retry-After', '60')
                        logger.warning(f"Rate limited. Retrying after {retry_after} seconds")
                        await asyncio.sleep(int(retry_after))
                        continue
                    elif response.status != 200:
                        raise ClientError(f"Stream request failed with status {response.status}")
                    
                    logger.info("Event stream established")
                    retries = 0  # Reset retry counter on successful connection
                    backoff = self.config.initial_backoff
                    
                    # Process streaming data line by line
                    async for line in response.content:
                        if self._shutdown_event.is_set():
                            break
                            
                        try:
                            # Skip empty lines
                            line = line.decode('utf-8').strip()
                            if not line:
                                continue
                                
                            # Parse JSON event
                            event_data = json.loads(line)
                            
                            # Yield the event for external processing
                            yield event_data
                            
                            # Also handle internally if handlers are registered
                            await self._handle_event(event_data)
                            
                        except json.JSONDecodeError as e:
                            logger.warning(f"Failed to parse JSON event: {e}")
                            continue
                        except Exception as e:
                            logger.error(f"Error processing stream event: {e}")
                            continue
                            
            except (ClientConnectorError, ServerTimeoutError, asyncio.TimeoutError) as e:
                retries += 1
                logger.error(f"Stream connection failed (attempt {retries}): {e}")
                
                if retries >= self.config.max_retries:
                    logger.error("Max retries exceeded, giving up")
                    raise
                
                # Exponential backoff with jitter
                jitter = random.uniform(0.1, 0.3) * backoff
                wait_time = min(backoff + jitter, self.config.max_backoff)
                logger.info(f"Retrying in {wait_time:.2f} seconds...")
                await asyncio.sleep(wait_time)
                backoff *= 2
                
                # Reconnect session if needed
                if self.session and self.session.closed:
                    await self.disconnect()
                    await self.connect()
            except Exception as e:
                retries += 1
                logger.error(f"Unexpected error in stream (attempt {retries}): {e}")
                
                if retries >= self.config.max_retries:
                    logger.error("Max retries exceeded, giving up")
                    raise
                
                # Use shorter backoff for unexpected errors
                wait_time = min(self.config.initial_backoff, self.config.max_backoff)
                await asyncio.sleep(wait_time)
    
    async def monitor_company_status_changes(self, 
                                           callback: Optional[Callable] = None) -> None:
        """
        Monitor the stream for company status changes relevant to strike-off detection.
        
        Args:
            callback: Optional callback function to process relevant events
        """
        logger.info("Starting company status monitoring...")
        
        strike_off_statuses = [
            "active-proposal-to-strike-off",
            "Active - Proposal to Strike Off"
        ]
        
        try:
            async for event in self.stream_events():
                # Filter for company profile changes
                if event.get('resource_kind') == 'company-profile':
                    company_data = event.get('data', {})
                    company_number = company_data.get('company_number')
                    company_status = company_data.get('company_status', '').lower()
                    
                    # Check if this is a strike-off related status change
                    is_strike_off = any(
                        status.lower().replace(' ', '-') in company_status or 
                        company_status in status.lower().replace(' ', '-')
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
        """
        Automatically reconnect with exponential backoff.
        
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
                jitter = random.uniform(0.1, 0.3) * backoff
                wait_time = min(backoff + jitter, self.config.max_backoff)
                logger.info(f"Waiting {wait_time:.2f} seconds before retry...")
                await asyncio.sleep(wait_time)
                backoff *= 2

    async def get_current_timepoint(self) -> int:
        """
        Get the current timepoint from the streaming API.
        
        Returns:
            Current timepoint value
        """
        if not self.session:
            raise RuntimeError("Client not connected")
            
        url = urljoin(self.config.api_base_url, '/firehose')
        
        try:
            await self._wait_for_rate_limit()
            async with self.session.head(url) as response:
                if response.status == 200:
                    timepoint = response.headers.get('X-Stream-Timepoint')
                    if timepoint:
                        return int(timepoint)
                    else:
                        raise ValueError("No timepoint header in response")
                elif response.status == 429:
                    retry_after = response.headers.get('Retry-After', '60')
                    raise ClientError(f"Rate limited - retry after {retry_after} seconds")
                else:
                    raise ClientError(f"Failed to get timepoint: status {response.status}")
        except Exception as e:
            logger.error(f"Error getting current timepoint: {e}")
            raise
    
    def is_healthy(self) -> bool:
        """
        Check if the streaming connection is healthy.
        
        Returns:
            True if connection is healthy, False otherwise
        """
        if not self.is_connected or not self.last_heartbeat:
            return False
            
        # Check if we've received data recently
        time_since_heartbeat = (datetime.now() - self.last_heartbeat).total_seconds()
        return time_since_heartbeat < self.config.health_check_interval
    
    async def shutdown(self) -> None:
        """Gracefully shutdown the streaming client."""
        logger.info("Shutting down streaming client...")
        await self.disconnect()
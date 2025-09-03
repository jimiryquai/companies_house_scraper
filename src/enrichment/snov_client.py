"""Snov.io API client with OAuth authentication and comprehensive error handling.

This module provides a robust client for Snov.io's API services including:
- OAuth token acquisition and automatic refresh
- Rate limiting compliance with exponential backoff
- Comprehensive error handling and logging
- Async/await support for webhook operations
- Integration with existing queue system for batched operations
"""

import asyncio
import hashlib
import hmac
import json
import logging
import random
import time
from datetime import datetime, timedelta
from typing import Any, NoReturn, Optional, Union
from urllib.parse import urljoin

import aiohttp
import yaml
from aiohttp import ClientConnectorError, ClientError, ClientSession, ClientTimeout
from aiohttp.client_exceptions import ServerTimeoutError

from ..streaming.queue_manager import PriorityQueueManager, QueuedRequest, RequestPriority

logger = logging.getLogger(__name__)


class SnovioError(Exception):
    """Base exception for Snov.io API errors."""

    def __init__(
        self,
        message: str,
        status_code: Optional[int] = None,
        response_data: Optional[dict[str, Any]] = None,
    ):
        """Initialize Snov.io error.

        Args:
            message: Error description
            status_code: HTTP status code if available
            response_data: Response data if available
        """
        super().__init__(message)
        self.status_code = status_code
        self.response_data = response_data


class SnovioAuthError(SnovioError):
    """Authentication-specific error."""

    pass


class SnovioRateLimitError(SnovioError):
    """Rate limiting error."""

    def __init__(self, message: str, retry_after: int = 60, **kwargs: Any):
        """Initialize rate limit error.

        Args:
            message: Error description
            retry_after: Seconds to wait before retry
            **kwargs: Additional error information
        """
        super().__init__(message, **kwargs)
        self.retry_after = retry_after


class SnovioCreditsExhaustedError(SnovioError):
    """Credits exhausted error."""

    pass


class SnovioWebhookError(SnovioError):
    """Webhook-specific error."""

    pass


class SnovioClient:
    """Snov.io API client with OAuth authentication and advanced features.

    Features:
    - OAuth token acquisition and automatic refresh
    - Rate limiting compliance with exponential backoff
    - Request/response logging
    - Integration with queue system
    - Async/await support
    - Webhook processing and validation
    """

    # API Configuration
    BASE_URL = "https://api.snov.io/v1/"  # v1 for auth and basic operations
    BASE_URL_V2 = "https://api.snov.io/v2/"  # v2 for bulk operations
    TOKEN_ENDPOINT = "oauth/access_token"  # noqa: S105

    # Rate limiting (conservative defaults)
    REQUESTS_PER_MINUTE = 30
    REQUESTS_PER_HOUR = 1000

    # Retry configuration
    MAX_RETRIES = 3
    INITIAL_BACKOFF = 1.0
    MAX_BACKOFF = 60.0

    # Operation credit costs (for operation-based tracking)
    OPERATION_CREDITS = {
        "domain_search": 1,
        "email_finder": 1,
        "email_verifier": 1,
        "find_emails_by_domain": 1,
    }
    BACKOFF_MULTIPLIER = 2.0

    def __init__(
        self,
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
        config_file: str = "config.yaml",
        queue_manager: Optional[PriorityQueueManager] = None,
        enable_logging: bool = True,
        log_level: str = "INFO",
    ):
        """Initialize the Snov.io client.

        Args:
            client_id: OAuth client ID (if None, loaded from config)
            client_secret: OAuth client secret (if None, loaded from config)
            config_file: Path to configuration file
            queue_manager: Optional queue manager for batched operations
            enable_logging: Enable request/response logging
            log_level: Logging level
        """
        # Load configuration from file
        self.config = self._load_config(config_file)

        self.client_id = client_id or self.config.get("snov_io", {}).get("client_id")
        self.client_secret = client_secret or self.config.get("snov_io", {}).get("client_secret")
        self.queue_manager = queue_manager
        self.enable_logging = enable_logging

        if not self.client_id or not self.client_secret:
            raise SnovioAuthError("Client ID and client secret are required")

        # Webhook configuration
        self.webhook_config = self.config.get("webhooks", {})
        self.webhook_secret = self.webhook_config.get("secret")

        # Authentication state
        self.access_token: Optional[str] = None
        self.token_expires_at: Optional[datetime] = None
        self.token_refresh_threshold = timedelta(minutes=5)  # Refresh 5 min before expiry

        # HTTP session
        self.session: Optional[ClientSession] = None
        self._session_lock = asyncio.Lock()

        # Rate limiting
        self._request_history: list[float] = []
        self._rate_limit_lock = asyncio.Lock()

        # Request tracking
        self._total_requests = 0
        self._successful_requests = 0
        self._failed_requests = 0
        self._last_request_time: Optional[datetime] = None

        # Credit tracking
        self._credit_balance: Optional[int] = None
        self._last_credit_check: Optional[datetime] = None

        # Setup logging
        self._setup_logging(log_level)

    def _load_config(self, config_file: str) -> dict[str, Any]:
        """Load configuration from YAML file.

        Args:
            config_file: Path to configuration file

        Returns:
            Configuration dictionary

        Raises:
            SnovioError: If configuration cannot be loaded
        """
        try:
            with open(config_file, encoding="utf-8") as f:
                return yaml.safe_load(f) or {}
        except FileNotFoundError:
            logger.warning(f"Configuration file {config_file} not found, using defaults")
            return {}
        except yaml.YAMLError as e:
            raise SnovioError(f"Invalid YAML configuration: {e}") from e

    def _setup_logging(self, log_level: str) -> None:
        """Configure logging for the client."""
        if not self.enable_logging:
            return

        level = getattr(logging, log_level.upper(), logging.INFO)
        logger.setLevel(level)

    async def __aenter__(self) -> "SnovioClient":
        """Async context manager entry."""
        await self._ensure_session()
        await self._ensure_authenticated()
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Async context manager exit."""
        await self.close()

    async def _ensure_session(self) -> None:
        """Ensure HTTP session is initialized."""
        async with self._session_lock:
            if self.session is None or self.session.closed:
                timeout = ClientTimeout(total=30, connect=10)
                connector = aiohttp.TCPConnector(limit=100, limit_per_host=10, keepalive_timeout=30)

                self.session = ClientSession(
                    timeout=timeout,
                    connector=connector,
                    headers={
                        "User-Agent": "Companies-House-Scraper-SnovIO/1.0",
                        "Accept": "application/json",
                        "Content-Type": "application/json",
                    },
                )

    async def close(self) -> None:
        """Close the HTTP session."""
        if self.session and not self.session.closed:
            await self.session.close()
            self.session = None

    async def _ensure_authenticated(self) -> None:
        """Ensure we have a valid access token."""
        if self._needs_token_refresh():
            await self._refresh_token()

    def _needs_token_refresh(self) -> bool:
        """Check if token needs to be refreshed."""
        if not self.access_token or not self.token_expires_at:
            return True

        # Refresh if token expires within threshold
        return datetime.now() >= (self.token_expires_at - self.token_refresh_threshold)

    async def _refresh_token(self) -> None:
        """Refresh the OAuth access token."""
        logger.info("Refreshing OAuth access token...")

        await self._ensure_session()
        if not self.session:
            raise SnovioAuthError("Session not initialized")

        token_url = urljoin(self.BASE_URL, self.TOKEN_ENDPOINT)
        payload = {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
        }

        try:
            async with self.session.post(token_url, json=payload) as response:
                response_data = await response.json()

                if response.status != 200:
                    error_msg = response_data.get(
                        "message", f"Token refresh failed with status {response.status}"
                    )
                    raise SnovioAuthError(error_msg, response.status, response_data)

                self.access_token = response_data.get("access_token")
                expires_in = response_data.get("expires_in", 3600)  # Default 1 hour

                if not self.access_token:
                    raise SnovioAuthError(
                        "No access token in response", response.status, response_data
                    )

                self.token_expires_at = datetime.now() + timedelta(seconds=expires_in)

                logger.info(f"Token refreshed successfully, expires at {self.token_expires_at}")

        except aiohttp.ClientError as e:
            raise SnovioAuthError(f"Token refresh network error: {e}") from e

    async def _wait_for_rate_limit(self) -> None:
        """Enforce rate limiting compliance."""
        async with self._rate_limit_lock:
            now = time.time()

            # Clean old requests (older than 1 hour)
            cutoff_time = now - 3600
            self._request_history = [t for t in self._request_history if t > cutoff_time]

            # Check minute-based rate limit
            minute_cutoff = now - 60
            minute_requests = len([t for t in self._request_history if t > minute_cutoff])

            if minute_requests >= self.REQUESTS_PER_MINUTE:
                wait_time = 60.0
                logger.warning(f"Rate limit reached (per minute). Waiting {wait_time}s")
                await asyncio.sleep(wait_time)

            # Check hour-based rate limit
            hour_requests = len(self._request_history)
            if hour_requests >= self.REQUESTS_PER_HOUR:
                wait_time = 300.0  # Wait 5 minutes
                logger.warning(f"Rate limit reached (per hour). Waiting {wait_time}s")
                await asyncio.sleep(wait_time)

            # Record this request
            self._request_history.append(now)

    def _record_success(self) -> None:
        """Record successful request."""
        self._successful_requests += 1

    def _record_failure(self, _: Exception) -> None:
        """Record failed request."""
        self._failed_requests += 1

    async def _check_credits(self) -> bool:
        """Check if we have sufficient credits.

        Returns:
            True if credits are sufficient
        """
        # Check cache first
        if (
            self._credit_balance is not None
            and self._last_credit_check
            and datetime.now() - self._last_credit_check < timedelta(minutes=5)
        ):
            min_balance = (
                self.config.get("snov_io", {}).get("limits", {}).get("min_credit_balance", 100)
            )
            return bool(self._credit_balance is not None and self._credit_balance > min_balance)

        try:
            account_info = await self.get_account_info()
            self._credit_balance = account_info.get("credits", 0)
            self._last_credit_check = datetime.now()

            min_balance = (
                self.config.get("snov_io", {}).get("limits", {}).get("min_credit_balance", 100)
            )

            if self._credit_balance < min_balance:
                logger.warning(
                    f"Credit balance ({self._credit_balance}) below minimum ({min_balance})"
                )
                return False

            return True

        except Exception as e:
            logger.error(f"Failed to check credit balance: {e}")
            # Assume we have credits if check fails
            return True

    async def _make_request(
        self,
        method: str,
        endpoint: str,
        params: Optional[dict[str, Any]] = None,
        json_data: Optional[dict[str, Any]] = None,
        headers: Optional[dict[str, str]] = None,
    ) -> dict[str, Any]:
        """Make authenticated API request with comprehensive error handling.

        Args:
            method: HTTP method (GET, POST, etc.)
            endpoint: API endpoint (relative to base URL)
            params: Query parameters
            json_data: JSON request body
            headers: Additional headers

        Returns:
            Response data as dictionary

        Raises:
            Various SnovioError subclasses based on error type
        """
        # Check credits before making request
        if not await self._check_credits():
            raise SnovioCreditsExhaustedError("Insufficient credits for API request")

        await self._ensure_session()
        await self._ensure_authenticated()
        await self._wait_for_rate_limit()

        if not self.session:
            raise SnovioError("Session not initialized")

        return await self._execute_request(method, endpoint, params, json_data, headers)

    async def _make_request_v2(
        self,
        method: str,
        endpoint: str,
        params: Optional[dict[str, Any]] = None,
        json_data: Optional[dict[str, Any]] = None,
        headers: Optional[dict[str, str]] = None,
    ) -> dict[str, Any]:
        """Make authenticated v2 API request with comprehensive error handling.

        Args:
            method: HTTP method (GET, POST, etc.)
            endpoint: API endpoint (relative to v2 base URL)
            params: Query parameters
            json_data: JSON request body
            headers: Additional headers

        Returns:
            Response data as dictionary

        Raises:
            Various SnovioError subclasses based on error type
        """
        # Check credits before making request
        if not await self._check_credits():
            raise SnovioCreditsExhaustedError("Insufficient credits for API request")

        await self._ensure_session()
        await self._ensure_authenticated()
        await self._wait_for_rate_limit()

        if not self.session:
            raise SnovioError("Session not initialized")

        return await self._execute_request_v2(method, endpoint, params, json_data, headers)

    async def _execute_request(
        self,
        method: str,
        endpoint: str,
        params: Optional[dict[str, Any]],
        json_data: Optional[dict[str, Any]],
        headers: Optional[dict[str, str]],
    ) -> dict[str, Any]:
        """Execute HTTP request with retry logic."""
        url = urljoin(self.BASE_URL, endpoint.lstrip("/"))
        request_headers = self._prepare_headers(headers)

        self._total_requests += 1
        self._last_request_time = datetime.now()
        self._log_request(method, url, params, json_data)

        for attempt in range(self.MAX_RETRIES + 1):
            try:
                return await self._single_request_attempt(
                    method, url, params, json_data, request_headers, attempt
                )
            except SnovioRateLimitError as e:
                if not await self._handle_rate_limit_retry(e, attempt):
                    raise
            except (ClientConnectorError, ServerTimeoutError, asyncio.TimeoutError) as e:
                if not await self._handle_network_retry(e, attempt):
                    raise
            except ClientError as e:
                client_error = SnovioError(f"HTTP client error: {e}")
                self._record_failure(client_error)
                raise client_error from e

        # Should not reach here
        final_error = SnovioError(f"Max retries ({self.MAX_RETRIES}) exceeded")
        self._record_failure(final_error)
        raise final_error

    async def _execute_request_v2(
        self,
        method: str,
        endpoint: str,
        params: Optional[dict[str, Any]],
        json_data: Optional[dict[str, Any]],
        headers: Optional[dict[str, str]],
    ) -> dict[str, Any]:
        """Execute HTTP request to v2 API with retry logic."""
        url = urljoin(self.BASE_URL_V2, endpoint.lstrip("/"))
        request_headers = self._prepare_headers(headers)

        self._total_requests += 1
        self._last_request_time = datetime.now()
        self._log_request(method, url, params, json_data)

        for attempt in range(self.MAX_RETRIES + 1):
            try:
                return await self._single_request_attempt(
                    method, url, params, json_data, request_headers, attempt
                )
            except SnovioRateLimitError as e:
                if not await self._handle_rate_limit_retry(e, attempt):
                    raise
            except (ClientConnectorError, ServerTimeoutError, asyncio.TimeoutError) as e:
                if not await self._handle_network_retry(e, attempt):
                    raise
            except ClientError as e:
                client_error = SnovioError(f"HTTP client error: {e}")
                self._record_failure(client_error)
                raise client_error from e

        # Should not reach here
        final_error = SnovioError(f"Max retries ({self.MAX_RETRIES}) exceeded")
        self._record_failure(final_error)
        raise final_error

    def _prepare_headers(self, headers: Optional[dict[str, str]]) -> dict[str, str]:
        """Prepare request headers with authentication."""
        request_headers = {
            "Authorization": f"Bearer {self.access_token}",
        }
        if headers:
            request_headers.update(headers)
        return request_headers

    def _log_request(
        self,
        method: str,
        url: str,
        params: Optional[dict[str, Any]],
        json_data: Optional[dict[str, Any]],
    ) -> None:
        """Log request details if debugging is enabled."""
        if self.enable_logging and logger.isEnabledFor(logging.DEBUG):
            logger.debug(f"Making {method} request to {url}")
            if params:
                logger.debug(f"Query params: {params}")
            if json_data:
                # Don't log sensitive data in production
                logger.debug(f"Request body keys: {list(json_data.keys()) if json_data else None}")

    async def _handle_rate_limit_retry(self, error: SnovioRateLimitError, attempt: int) -> bool:
        """Handle rate limit retry logic."""
        if attempt < self.MAX_RETRIES:
            logger.warning(
                f"Rate limited, waiting {error.retry_after}s "
                f"(attempt {attempt + 1}/{self.MAX_RETRIES})"
            )
            await asyncio.sleep(error.retry_after)
            return True
        self._record_failure(error)
        return False

    async def _handle_network_retry(
        self,
        error: Exception,
        attempt: int,
    ) -> bool:
        """Handle network error retry logic."""
        if attempt < self.MAX_RETRIES:
            backoff = self._calculate_backoff(attempt)
            logger.warning(
                f"Network error, retrying in {backoff}s "
                f"(attempt {attempt + 1}/{self.MAX_RETRIES}): {error}"
            )
            await asyncio.sleep(backoff)
            return True

        network_error = SnovioError(f"Network error: {error}")
        self._record_failure(network_error)
        raise network_error from error

    async def _single_request_attempt(
        self,
        method: str,
        url: str,
        params: Optional[dict[str, Any]],
        json_data: Optional[dict[str, Any]],
        headers: dict[str, str],
        attempt: int,
    ) -> dict[str, Any]:
        """Execute single request attempt."""
        if not self.session:
            raise SnovioError("Session not initialized")

        async with self.session.request(
            method=method,
            url=url,
            params=params,
            json=json_data,
            headers=headers,
        ) as response:
            response_data = await response.json()

            # Log response if enabled
            if self.enable_logging and logger.isEnabledFor(logging.DEBUG):
                logger.debug(f"Response status: {response.status}")

            # Handle successful response
            if 200 <= response.status < 300:
                self._record_success()
                # API responses are dict[str, Any] by contract
                return dict(response_data)

            # Handle error responses - this will raise an exception
            await self._handle_error_response(response, response_data, attempt)

            # This line should never be reached due to exceptions above
            # But mypy needs it for type checking
            return response_data

    async def _handle_error_response(
        self, response: aiohttp.ClientResponse, response_data: dict[str, Any], attempt: int
    ) -> NoReturn:
        """Handle error responses with appropriate exceptions."""
        error_message = response_data.get(
            "message", f"Request failed with status {response.status}"
        )

        if response.status == 401:
            # Try token refresh once
            if attempt == 0:
                logger.warning("Authentication failed, refreshing token...")
                await self._refresh_token()
                # This will cause retry in parent loop
                raise SnovioAuthError("Token refresh required", response.status, response_data)
            raise SnovioAuthError(error_message, response.status, response_data)

        if response.status == 429:
            retry_after = int(response.headers.get("Retry-After", 60))
            raise SnovioRateLimitError(
                error_message,
                retry_after,
                status_code=response.status,
                response_data=response_data,
            )

        if response.status == 402:
            # Payment required - credits exhausted
            self._credit_balance = 0  # Update cache
            raise SnovioCreditsExhaustedError(error_message, response.status, response_data)

        if 400 <= response.status < 500:
            # Client error - don't retry
            self._record_failure(SnovioError(error_message))
            raise SnovioError(error_message, response.status, response_data)

        if 500 <= response.status < 600:
            # Server error - retry with backoff
            if attempt < self.MAX_RETRIES:
                backoff = self._calculate_backoff(attempt)
                logger.warning(
                    f"Server error {response.status}, retrying in {backoff}s "
                    f"(attempt {attempt + 1}/{self.MAX_RETRIES})"
                )
                await asyncio.sleep(backoff)
                # This will cause retry in parent loop
                raise SnovioError("Server error - retrying")

            self._record_failure(SnovioError(error_message))
            raise SnovioError(error_message, response.status, response_data)

        self._record_failure(SnovioError(error_message))
        raise SnovioError(error_message, response.status, response_data)

    def _calculate_backoff(self, attempt: int) -> float:
        """Calculate exponential backoff with jitter."""
        base_delay = self.INITIAL_BACKOFF * (self.BACKOFF_MULTIPLIER**attempt)
        max_delay = min(base_delay, self.MAX_BACKOFF)

        # Add jitter (±25%)
        jitter = random.uniform(0.75, 1.25)  # noqa: S311
        return max_delay * jitter

    # Queue Integration Methods

    async def enqueue_request(
        self,
        method: str,
        endpoint: str,
        params: Optional[dict[str, Any]] = None,
        json_data: Optional[dict[str, Any]] = None,
        priority: RequestPriority = RequestPriority.MEDIUM,
        request_id: Optional[str] = None,
    ) -> bool:
        """Enqueue a request for batch processing.

        Args:
            method: HTTP method
            endpoint: API endpoint
            params: Query parameters
            json_data: Request body
            priority: Request priority
            request_id: Unique request identifier

        Returns:
            True if enqueued successfully
        """
        if not self.queue_manager:
            raise SnovioError("Queue manager not configured")

        if not request_id:
            request_id = f"snov_{int(time.time() * 1000)}_{random.randint(1000, 9999)}"  # noqa: S311

        request = QueuedRequest(
            request_id=request_id,
            priority=priority,
            endpoint=endpoint,
            params={
                "method": method,
                "params": params or {},
                "json_data": json_data or {},
            },
        )

        return await self.queue_manager.enqueue(request)

    async def process_queued_requests(
        self, max_concurrent: int = 5
    ) -> dict[str, Union[int, list[str]]]:
        """Process queued requests with concurrency control.

        Args:
            max_concurrent: Maximum concurrent requests

        Returns:
            Processing statistics
        """
        if not self.queue_manager:
            raise SnovioError("Queue manager not configured")

        stats: dict[str, Union[int, list[str]]] = {
            "processed": 0,
            "failed": 0,
            "errors": [],
        }

        semaphore = asyncio.Semaphore(max_concurrent)

        async def process_single_request() -> None:
            async with semaphore:
                assert self.queue_manager is not None  # Already checked above
                request = await self.queue_manager.dequeue(timeout=1.0)
                if not request:
                    return

                start_time = time.time()
                try:
                    method = request.params.get("method", "GET")
                    params = request.params.get("params", {})
                    json_data = request.params.get("json_data", {})

                    await self._make_request(
                        method=method,
                        endpoint=request.endpoint,
                        params=params,
                        json_data=json_data if json_data else None,
                    )

                    processing_time = time.time() - start_time
                    assert self.queue_manager is not None  # Already checked above
                    self.queue_manager.mark_processed(request, processing_time)
                    processed_count = stats["processed"]
                    assert isinstance(processed_count, int)
                    stats["processed"] = processed_count + 1

                except Exception as e:
                    assert self.queue_manager is not None  # Already checked above
                    self.queue_manager.mark_failed(request)
                    failed_count = stats["failed"]
                    assert isinstance(failed_count, int)
                    stats["failed"] = failed_count + 1
                    error_list = stats["errors"]
                    assert isinstance(error_list, list)
                    error_list.append(str(e))
                    logger.error(f"Failed to process queued request {request.request_id}: {e}")

        # Process available requests
        while True:
            queue_status = self.queue_manager.get_queue_status()
            if queue_status["total_queued"] == 0:
                break

            tasks = []
            for _ in range(min(max_concurrent, queue_status["total_queued"])):
                tasks.append(asyncio.create_task(process_single_request()))

            if not tasks:
                break

            await asyncio.gather(*tasks, return_exceptions=True)

        return stats

    # API Methods (Snov.io specific endpoints)

    async def get_account_info(self) -> dict[str, Any]:
        """Get account information including credits and limits."""
        return await self._make_request("GET", "get-balance")

    def get_operation_credit_cost(self, operation_type: str) -> int:
        """Get the credit cost for a specific operation type.

        Args:
            operation_type: The type of operation (domain_search, email_finder, etc.)

        Returns:
            Number of credits consumed by the operation

        Note:
            Returns 1 credit as default for unknown operations.
            These costs are estimates based on Snov.io documentation.
        """
        return self.OPERATION_CREDITS.get(operation_type, 1)

    async def find_emails_by_domain(
        self,
        domain: str,
        limit: int = 100,
        offset: int = 0,
    ) -> dict[str, Any]:
        """Find emails by domain.

        Args:
            domain: Target domain
            limit: Maximum number of results
            offset: Pagination offset

        Returns:
            Email search results
        """
        params = {
            "domain": domain,
            "limit": limit,
            "offset": offset,
        }

        return await self._make_request("GET", "domain-emails-count", params=params)

    async def verify_email(self, email: str) -> dict[str, Any]:
        """Verify email address.

        Args:
            email: Email address to verify

        Returns:
            Email verification results
        """
        params = {"email": email}
        return await self._make_request("GET", "email-verifier", params=params)

    async def domain_search(
        self,
        company_name: str,
        limit: int = 10,
        use_polling: bool = True,
        use_v2: bool = True,
    ) -> dict[str, Any]:
        """Search for company domains by company name.

        Args:
            company_name: Name of the company to search domains for
            limit: Maximum number of results to return
            use_polling: Use polling instead of webhook (default True due to API limitations)
            use_v2: Use v2 API (default True for better reliability)

        Returns:
            Domain search results

        Raises:
            SnovioError: If the request fails
            SnovioCreditsExhaustedError: If insufficient credits
            SnovioRateLimitError: If rate limited
        """
        if not company_name or not company_name.strip():
            raise SnovioError("Company name cannot be empty")

        if use_v2 and use_polling:
            # Use v2 API with polling (recommended approach)
            return await self._domain_search_with_polling_v2(company_name, limit)
        if use_polling:
            # Use v1 polling approach
            return await self._domain_search_with_polling(company_name, limit)
        # Legacy direct approach (may fail with current API)
        params = {
            "company": company_name.strip(),
            "limit": limit,
        }
        return await self._make_request("GET", "domain-search", params=params)

    async def find_email_by_name(
        self,
        first_name: str,
        last_name: str,
        domain: str,
        webhook_url: Optional[str] = None,
    ) -> dict[str, Any]:
        """Find email by name and domain.

        Args:
            first_name: Person's first name
            last_name: Person's last name
            domain: Target domain
            webhook_url: Optional webhook URL for async result delivery

        Returns:
            Email search results or task information if webhook is used

        Raises:
            SnovioError: If the request fails
            SnovioCreditsExhaustedError: If insufficient credits
            SnovioRateLimitError: If rate limited
        """
        if not first_name or not first_name.strip():
            raise SnovioError("First name cannot be empty")
        if not last_name or not last_name.strip():
            raise SnovioError("Last name cannot be empty")
        if not domain or not domain.strip():
            raise SnovioError("Domain cannot be empty")

        params = {
            "firstName": first_name.strip(),
            "lastName": last_name.strip(),
            "domain": domain.strip(),
        }

        # Add webhook URL if provided for async processing
        if webhook_url:
            params["webhook_url"] = webhook_url

        return await self._make_request("GET", "email-finder", params=params)

    async def find_email_by_domain_and_name(
        self,
        domain: str,
        first_name: str,
        last_name: str,
        use_v2: bool = True,
        use_polling: bool = True,
    ) -> dict[str, Any]:
        """Find email by domain and name using v1 or v2 API.

        Args:
            domain: Target domain
            first_name: Person's first name
            last_name: Person's last name
            use_v2: Use v2 API (default: True)
            use_polling: Use polling instead of webhook (default: True)

        Returns:
            Email search results

        Raises:
            SnovioError: If the request fails
        """
        if use_v2 and use_polling:
            return await self._email_finder_with_polling_v2(domain, first_name, last_name)
        return await self.find_email_by_name(first_name, last_name, domain)

    async def bulk_email_finder(
        self,
        domains: list[str],
        limit_per_domain: int = 10,
    ) -> dict[str, Any]:
        """Bulk email finder for multiple domains.

        Args:
            domains: List of domains to search
            limit_per_domain: Maximum emails per domain

        Returns:
            Bulk search results
        """
        json_data = {
            "domains": domains,
            "limit": limit_per_domain,
        }

        return await self._make_request("POST", "bulk-email-finder", json_data=json_data)

    async def start_emails_by_domain_by_name(
        self,
        domain: str,
        first_name: str,
        last_name: str,
        webhook_url: Optional[str] = None,
    ) -> dict[str, Any]:
        """Start v2 email search by domain and name.

        Args:
            domain: Target domain
            first_name: Person's first name
            last_name: Person's last name
            webhook_url: Optional webhook URL for async result delivery

        Returns:
            Task hash for polling results
        """
        json_data = {
            "domain": domain.strip(),
            "firstName": first_name.strip(),
            "lastName": last_name.strip(),
        }

        if webhook_url:
            json_data["webhook_url"] = webhook_url

        return await self._make_request_v2(
            "POST", "emails-by-domain-by-name/start", json_data=json_data
        )

    async def start_company_domain_by_name(
        self,
        company_name: str,
        limit: int = 10,
        webhook_url: Optional[str] = None,
    ) -> dict[str, Any]:
        """Start v2 domain search by company name.

        Args:
            company_name: Name of the company to search domains for
            limit: Maximum number of results to return
            webhook_url: Optional webhook URL for async result delivery

        Returns:
            Task hash for polling results
        """
        json_data = {
            "company": company_name.strip(),
            "limit": limit,
        }

        if webhook_url:
            json_data["webhook_url"] = webhook_url

        return await self._make_request_v2(
            "POST", "company-domain-by-name/start", json_data=json_data
        )

    async def get_bulk_task_result(self, task_hash: str) -> dict[str, Any]:
        """Get v2 bulk task result by hash.

        Args:
            task_hash: Task hash from start operation

        Returns:
            Task result data
        """
        params = {"task_hash": task_hash}
        return await self._make_request_v2("GET", "bulk-task-result", params=params)

    # Webhook Processing Methods

    def validate_webhook_signature(
        self,
        payload: bytes,
        signature: str,
        secret: Optional[str] = None,
    ) -> bool:
        """Validate webhook signature.

        Args:
            payload: Raw webhook payload
            signature: Webhook signature header
            secret: Webhook secret (if None, uses config)

        Returns:
            True if signature is valid

        Raises:
            SnovioWebhookError: If signature validation fails
        """
        webhook_secret = secret or self.webhook_secret
        if not webhook_secret:
            raise SnovioWebhookError("Webhook secret not configured")

        try:
            # Extract signature from header (format: sha256=hash)
            if signature.startswith("sha256="):
                signature = signature[7:]

            # Calculate expected signature
            expected_signature = hmac.new(
                webhook_secret.encode("utf-8"),
                payload,
                hashlib.sha256,
            ).hexdigest()

            # Use constant-time comparison
            return hmac.compare_digest(signature, expected_signature)

        except Exception as e:
            raise SnovioWebhookError(f"Signature validation error: {e}") from e

    def parse_webhook_payload(self, payload: str) -> dict[str, Any]:
        """Parse webhook JSON payload.

        Args:
            payload: JSON payload string

        Returns:
            Parsed payload data

        Raises:
            SnovioWebhookError: If payload parsing fails
        """
        try:
            # JSON payload is expected to be a dict
            result = json.loads(payload)
            return dict(result)
        except json.JSONDecodeError as e:
            raise SnovioWebhookError(f"Invalid JSON payload: {e}") from e

    async def process_webhook_event(
        self,
        event_type: str,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        """Process webhook event.

        Args:
            event_type: Type of webhook event
            payload: Event payload data

        Returns:
            Processing result

        Raises:
            SnovioWebhookError: If event processing fails
        """
        logger.info(f"Processing webhook event: {event_type}")

        try:
            if event_type == "email_verification_completed":
                return await self._handle_email_verification_webhook(payload)
            if event_type == "bulk_search_completed":
                return await self._handle_bulk_search_webhook(payload)
            if event_type == "credits_updated":
                return await self._handle_credits_webhook(payload)
            logger.warning(f"Unknown webhook event type: {event_type}")
            return {"status": "ignored", "reason": f"Unknown event type: {event_type}"}

        except Exception as e:
            raise SnovioWebhookError(f"Error processing webhook event {event_type}: {e}") from e

    async def _handle_email_verification_webhook(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Handle email verification completion webhook."""
        email = payload.get("email")
        result = payload.get("result")

        logger.info(f"Email verification completed for {email}: {result}")

        # Here you could update your database, trigger notifications, etc.
        return {
            "status": "processed",
            "event": "email_verification_completed",
            "email": email,
            "result": result,
        }

    async def _handle_bulk_search_webhook(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Handle bulk search completion webhook."""
        search_id = payload.get("search_id")
        domain = payload.get("domain")
        results_count = payload.get("results_count", 0)

        logger.info(f"Bulk search completed for {domain}: {results_count} results")

        # Here you could fetch results and store them
        return {
            "status": "processed",
            "event": "bulk_search_completed",
            "search_id": search_id,
            "domain": domain,
            "results_count": results_count,
        }

    async def _handle_credits_webhook(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Handle credits update webhook."""
        credits_remaining = payload.get("credits_remaining", 0)

        logger.info(f"Credits updated: {credits_remaining} remaining")

        # Update cached credit balance
        self._credit_balance = credits_remaining
        self._last_credit_check = datetime.now()

        return {
            "status": "processed",
            "event": "credits_updated",
            "credits_remaining": credits_remaining,
        }

    # Health and Monitoring Methods

    def get_health_status(self) -> dict[str, Any]:
        """Get client health status."""
        return {
            "authenticated": self.access_token is not None,
            "token_expires_at": self.token_expires_at.isoformat()
            if self.token_expires_at
            else None,
            "needs_refresh": self._needs_token_refresh(),
            "total_requests": self._total_requests,
            "successful_requests": self._successful_requests,
            "failed_requests": self._failed_requests,
            "success_rate": self._successful_requests / max(self._total_requests, 1),
            "last_request_time": self._last_request_time.isoformat()
            if self._last_request_time
            else None,
            "credit_balance": self._credit_balance,
            "last_credit_check": self._last_credit_check.isoformat()
            if self._last_credit_check
            else None,
        }

    def get_rate_limit_status(self) -> dict[str, Any]:
        """Get rate limiting status."""
        now = time.time()
        minute_cutoff = now - 60
        hour_cutoff = now - 3600

        minute_requests = len([t for t in self._request_history if t > minute_cutoff])
        hour_requests = len([t for t in self._request_history if t > hour_cutoff])

        return {
            "requests_last_minute": minute_requests,
            "requests_last_hour": hour_requests,
            "minute_limit": self.REQUESTS_PER_MINUTE,
            "hour_limit": self.REQUESTS_PER_HOUR,
            "minute_remaining": max(0, self.REQUESTS_PER_MINUTE - minute_requests),
            "hour_remaining": max(0, self.REQUESTS_PER_HOUR - hour_requests),
        }

    def get_usage_statistics(self) -> dict[str, Any]:
        """Get comprehensive usage statistics."""
        token_expires = None
        if self.token_expires_at:
            token_expires = self.token_expires_at.isoformat()

        last_request = None
        if self._last_request_time:
            last_request = self._last_request_time.isoformat()

        # Removed unused last_failure variable

        last_credit_check = None
        if self._last_credit_check:
            last_credit_check = self._last_credit_check.isoformat()

        return {
            "authentication": {
                "authenticated": self.access_token is not None,
                "token_expires_at": token_expires,
                "needs_refresh": self._needs_token_refresh(),
            },
            "requests": {
                "total": self._total_requests,
                "successful": self._successful_requests,
                "failed": self._failed_requests,
                "success_rate": self._successful_requests / max(self._total_requests, 1),
                "last_request_time": last_request,
            },
            "rate_limiting": self.get_rate_limit_status(),
            "credits": {
                "balance": self._credit_balance,
                "last_check": last_credit_check,
            },
        }

    async def _domain_search_with_polling_v2(
        self,
        company_name: str,
        limit: int = 10,
        max_attempts: int = 30,
        delay_seconds: int = 10,
    ) -> dict[str, Any]:
        """Search for company domains using v2 API with polling.

        Args:
            company_name: Name of the company to search domains for
            limit: Maximum number of results to return
            max_attempts: Maximum polling attempts (default: 30)
            delay_seconds: Delay between polling attempts (default: 10)

        Returns:
            Domain search results

        Raises:
            SnovioError: If the request fails or times out
        """
        logger.info(f"Starting v2 domain search for: {company_name}")

        # Step 1: Start the domain search task
        start_response = await self.start_company_domain_by_name(company_name, limit)
        task_hash = start_response.get("task_hash")

        if not task_hash:
            # If no task_hash, this might be a direct response
            if "domains" in start_response or "status" in start_response:
                return start_response
            raise SnovioError("No task_hash received from v2 domain search start API")

        logger.info(f"Received v2 task_hash: {task_hash}, starting polling")

        # Step 2: Poll for results using v2 endpoint
        return await self._poll_task_result_v2(task_hash, max_attempts, delay_seconds)

    async def _email_finder_with_polling_v2(
        self,
        domain: str,
        first_name: str,
        last_name: str,
        max_attempts: int = 30,
        delay_seconds: int = 10,
    ) -> dict[str, Any]:
        """Find email using v2 API with polling.

        Args:
            domain: Target domain
            first_name: Person's first name
            last_name: Person's last name
            max_attempts: Maximum polling attempts (default: 30)
            delay_seconds: Delay between polling attempts (default: 10)

        Returns:
            Email search results

        Raises:
            SnovioError: If the request fails or times out
        """
        logger.info(f"Starting v2 email search for: {first_name} {last_name} @ {domain}")

        # Step 1: Start the email search task
        start_response = await self.start_emails_by_domain_by_name(domain, first_name, last_name)
        task_hash = start_response.get("task_hash")

        if not task_hash:
            # If no task_hash, this might be a direct response
            if "emails" in start_response or "status" in start_response:
                return start_response
            raise SnovioError("No task_hash received from v2 email search start API")

        logger.info(f"Received v2 email task_hash: {task_hash}, starting polling")

        # Step 2: Poll for results using v2 endpoint
        return await self._poll_task_result_v2(task_hash, max_attempts, delay_seconds)

    async def _poll_task_result_v2(
        self,
        task_hash: str,
        max_attempts: int = 30,
        delay_seconds: int = 10,
    ) -> dict[str, Any]:
        """Poll for v2 task results using task_hash.

        Args:
            task_hash: Task hash from initial v2 API request
            max_attempts: Maximum polling attempts
            delay_seconds: Delay between attempts

        Returns:
            Task results when completed

        Raises:
            SnovioError: If polling times out or fails
        """
        for attempt in range(max_attempts):
            try:
                # Poll the v2 task result endpoint
                result = await self.get_bulk_task_result(task_hash)

                status = result.get("status")

                if status == "completed":
                    logger.info(f"V2 task {task_hash} completed after {attempt + 1} attempts")
                    # Return data dict or result dict as dict[str, Any]
                    data = result.get("data", result)
                    return dict(data)
                if status == "failed":
                    error_message = result.get("error", "Task failed")
                    raise SnovioError(f"V2 task failed: {error_message}")
                if status in ["pending", "processing", "running"]:
                    logger.debug(f"V2 task {task_hash} still {status}, attempt {attempt + 1}")
                else:
                    logger.warning(f"Unknown v2 task status: {status}")

                # Wait before next attempt
                if attempt < max_attempts - 1:
                    await asyncio.sleep(delay_seconds)

            except SnovioError:
                # Re-raise Snov.io specific errors
                raise
            except Exception as e:
                logger.error(f"Error polling v2 task {task_hash}: {e}")
                if attempt == max_attempts - 1:
                    raise SnovioError(
                        f"V2 polling failed after {max_attempts} attempts: {e}"
                    ) from e
                await asyncio.sleep(delay_seconds)

        raise SnovioError(
            f"V2 task {task_hash} did not complete within {max_attempts * delay_seconds} seconds"
        )

    async def _domain_search_with_polling(
        self,
        company_name: str,
        limit: int = 10,
        max_attempts: int = 30,
        delay_seconds: int = 10,
    ) -> dict[str, Any]:
        """Search for company domains using polling instead of webhook.

        This method works around the Snov.io API limitation where webhook URLs
        cannot be included in POST requests by using a two-step process:
        1. POST request without webhook to get task_hash
        2. Poll GET endpoint to retrieve results

        Args:
            company_name: Name of the company to search domains for
            limit: Maximum number of results to return
            max_attempts: Maximum polling attempts (default: 30)
            delay_seconds: Delay between polling attempts (default: 10)

        Returns:
            Domain search results

        Raises:
            SnovioError: If the request fails or times out
        """
        # Step 1: Submit domain search request without webhook
        params = {
            "company": company_name.strip(),
            "limit": limit,
        }

        logger.info(f"Starting domain search for: {company_name}")

        # Make the initial request to get task_hash
        response = await self._make_request("GET", "domain-search", params=params)

        # Extract task_hash from response
        task_hash = response.get("task_hash")
        if not task_hash:
            # If no task_hash, this might be a direct response
            if "domains" in response or "status" in response:
                return response
            raise SnovioError("No task_hash received from domain search API")

        logger.info(f"Received task_hash: {task_hash}, starting polling")

        # Step 2: Poll for results
        return await self._poll_task_result(task_hash, max_attempts, delay_seconds)

    async def _poll_task_result(
        self,
        task_hash: str,
        max_attempts: int = 30,
        delay_seconds: int = 10,
    ) -> dict[str, Any]:
        """Poll for task results using task_hash.

        Args:
            task_hash: Task hash from initial API request
            max_attempts: Maximum polling attempts
            delay_seconds: Delay between attempts

        Returns:
            Task results when completed

        Raises:
            SnovioError: If polling times out or fails
        """
        for attempt in range(max_attempts):
            try:
                # Poll the task result endpoint
                result = await self._make_request("GET", f"get-task-result/{task_hash}")

                status = result.get("status")

                if status == "completed":
                    logger.info(f"Task {task_hash} completed after {attempt + 1} attempts")
                    # Return data dict or result dict as dict[str, Any]
                    data = result.get("data", result)
                    return dict(data)
                if status == "failed":
                    error_message = result.get("error", "Task failed")
                    raise SnovioError(f"Task failed: {error_message}")
                if status in ["pending", "processing", "running"]:
                    logger.debug(f"Task {task_hash} still {status}, attempt {attempt + 1}")
                else:
                    logger.warning(f"Unknown task status: {status}")

                # Wait before next attempt
                if attempt < max_attempts - 1:
                    await asyncio.sleep(delay_seconds)

            except SnovioError:
                # Re-raise Snov.io specific errors
                raise
            except Exception as e:
                logger.error(f"Error polling task {task_hash}: {e}")
                if attempt == max_attempts - 1:
                    raise SnovioError(
                        f"Polling failed after {max_attempts} attempts: {e}"
                    ) from e
                await asyncio.sleep(delay_seconds)

        raise SnovioError(
            f"Task {task_hash} did not complete within {max_attempts * delay_seconds} seconds"
        )


# Factory function for easy instantiation
async def create_snov_client(
    config_file: str = "config.yaml",
    queue_manager: Optional[PriorityQueueManager] = None,
    **kwargs: Any,
) -> SnovioClient:
    """Create and initialize a Snov.io client.

    Args:
        config_file: Path to configuration file
        queue_manager: Optional queue manager
        **kwargs: Additional client options

    Returns:
        Initialized SnovioClient instance
    """
    client = SnovioClient(
        config_file=config_file,
        queue_manager=queue_manager,
        **kwargs,
    )

    # Initialize session and authenticate
    await client._ensure_session()
    await client._ensure_authenticated()

    return client

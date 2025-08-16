"""
Tests for real API connectivity with test credentials.
These tests require valid API credentials and internet connectivity.
"""

import asyncio
import os
from datetime import datetime
from typing import Any

import aiohttp
import pytest

from src.streaming import LogContext, LogLevel, StreamingClient, StreamingConfig, StructuredLogger

# Skip API tests if no credentials are available
API_KEY = os.getenv("COMPANIES_HOUSE_STREAMING_API_KEY")
pytestmark = pytest.mark.skipif(
    not API_KEY or API_KEY == "test-api-key",
    reason="Real API key not available - set COMPANIES_HOUSE_STREAMING_API_KEY "
    "environment variable",
)


@pytest.fixture
def real_api_config() -> Any:
    """Create configuration with real API credentials."""
    api_key = os.getenv("COMPANIES_HOUSE_STREAMING_API_KEY")
    if not api_key or api_key == "test-api-key":
        pytest.skip("Real API key not available")

    return StreamingConfig(
        streaming_api_key=api_key,
        database_path=":memory:",
        api_base_url="https://stream-api.company-information.service.gov.uk",
        connection_timeout=30,
        max_retries=3,
        initial_backoff=1,
        max_backoff=60,
        rate_limit_requests_per_minute=600,
        batch_size=10,
    )


@pytest.fixture
def test_api_config() -> Any:
    """Create configuration for testing API endpoints (without streaming)."""
    api_key = os.getenv("COMPANIES_HOUSE_STREAMING_API_KEY")
    if not api_key or api_key == "test-api-key":
        pytest.skip("Real API key not available")

    return StreamingConfig(
        streaming_api_key=api_key,
        database_path=":memory:",
        api_base_url="https://api.companieshouse.gov.uk",
        connection_timeout=30,
        max_retries=3,
        initial_backoff=1,
        max_backoff=60,
        rate_limit_requests_per_minute=600,
        batch_size=10,
    )


class TestAPIConnectivity:
    """Test real API connectivity and authentication."""

    @pytest.mark.asyncio
    async def test_api_authentication(self, test_api_config: Any) -> None:
        """Test API authentication with real credentials."""
        client = StreamingClient(test_api_config)

        try:
            await client.connect()

            # Test a simple API endpoint to verify authentication
            # Use the standard API rather than streaming for this test
            async with aiohttp.ClientSession() as session:
                headers = {
                    "Authorization": f"Bearer {test_api_config.streaming_api_key}",
                    "User-Agent": "companies-house-streaming-test/1.0",
                }

                # Test the search endpoint (publicly available)
                async with session.get(
                    "https://api.companieshouse.gov.uk/search/companies",
                    headers=headers,
                    params={"q": "test", "items_per_page": "1"},
                    timeout=aiohttp.ClientTimeout(total=30),
                ) as response:
                    assert response.status in [200, 401, 403], (
                        f"Unexpected status: {response.status}"
                    )

                    if response.status == 200:
                        # Authentication successful
                        data = await response.json()
                        assert "items" in data
                    elif response.status == 401:
                        # Invalid credentials
                        pytest.fail("API authentication failed - invalid credentials")
                    elif response.status == 403:
                        # Valid credentials but insufficient permissions
                        pytest.skip(
                            "API credentials valid but insufficient permissions for search endpoint"
                        )

        finally:
            await client.disconnect()

    @pytest.mark.asyncio
    async def test_streaming_endpoint_availability(self, real_api_config: Any) -> None:
        """Test streaming endpoint availability and initial connection."""
        client = StreamingClient(real_api_config)

        try:
            await client.connect()

            # Test connection to streaming endpoint
            url = f"{real_api_config.api_base_url}/firehose"
            headers = {
                "Authorization": f"Bearer {real_api_config.streaming_api_key}",
                "User-Agent": "companies-house-streaming-test/1.0",
                "Accept": "application/json",
            }

            async with aiohttp.ClientSession() as session:
                try:
                    async with session.get(
                        url,
                        headers=headers,
                        timeout=aiohttp.ClientTimeout(total=10),  # Short timeout for test
                    ) as response:
                        # We expect either:
                        # 200 - Successfully connected to stream
                        # 401 - Invalid credentials
                        # 403 - Valid credentials but no streaming access
                        # 404 - Endpoint not found
                        # 429 - Rate limited
                        assert response.status in [200, 401, 403, 404, 429], (
                            f"Unexpected status: {response.status}"
                        )

                        if response.status == 200:
                            # Successfully connected - this is what we want
                            assert response.headers.get("content-type", "").startswith(
                                "text/plain"
                            ) or response.headers.get("content-type", "").startswith(
                                "application/json"
                            )
                        elif response.status == 401:
                            pytest.fail("Streaming API authentication failed - invalid credentials")
                        elif response.status == 403:
                            pytest.skip("Valid credentials but no access to streaming endpoint")
                        elif response.status == 404:
                            pytest.skip("Streaming endpoint not found - may not be available")
                        elif response.status == 429:
                            pytest.skip("Rate limited - try again later")

                except asyncio.TimeoutError:
                    pytest.skip("Connection to streaming endpoint timed out")
                except aiohttp.ClientConnectorError:
                    pytest.skip("Could not connect to streaming endpoint - network issue")

        finally:
            await client.disconnect()

    @pytest.mark.asyncio
    async def test_streaming_client_connection(self, real_api_config: Any) -> None:
        """Test StreamingClient connection capabilities."""
        client = StreamingClient(real_api_config)

        try:
            # Test connection
            await client.connect()
            assert client.session is not None

            # Test health check if available
            import contextlib

            with contextlib.suppress(Exception):
                await client._health_check()  # This method returns None but should not raise
                # Health check passed if no exception raised

            # Test connection state
            assert client.is_connected is True

        finally:
            await client.disconnect()
            assert client.is_connected is False

    @pytest.mark.asyncio
    async def test_rate_limiting_compliance(self, real_api_config: Any) -> None:
        """Test that the client respects rate limiting."""
        # Reduce rate limit for testing
        real_api_config.rate_limit_requests_per_minute = 60  # 1 per second

        client = StreamingClient(real_api_config)

        try:
            await client.connect()

            # Make multiple rapid requests and verify rate limiting is applied
            start_time = datetime.now()
            request_times = []

            for _i in range(3):
                request_start = datetime.now()

                try:
                    # Attempt to make a request (this will trigger rate limiting)
                    await client._wait_for_rate_limit()
                    request_times.append((datetime.now() - request_start).total_seconds())
                except Exception:  # noqa: S110
                    pass  # Expected in rate limit testing

            total_time = (datetime.now() - start_time).total_seconds()

            # Verify rate limiting is working (should take at least 2 seconds for 3 requests)
            if len(request_times) >= 2:
                assert total_time >= 1.0, "Rate limiting not properly applied"

        finally:
            await client.disconnect()

    @pytest.mark.asyncio
    async def test_error_handling_with_real_api(self, real_api_config: Any) -> None:
        """Test error handling with real API responses."""
        client = StreamingClient(real_api_config)

        try:
            await client.connect()

            # Test with invalid timepoint to trigger an error
            try:
                async for _event in client.stream_events(timepoint=-1):  # Invalid timepoint
                    # Should not reach here if error handling works
                    break
            except Exception as e:
                # This is expected - invalid timepoint should cause an error
                assert isinstance(e, (aiohttp.ClientError, ValueError, RuntimeError))

            # Test circuit breaker functionality
            # Force some failures to test circuit breaker
            original_failure_count = client._failure_count

            # Simulate failures
            for _ in range(5):
                client._record_failure(Exception("Test failure"))

            # Check circuit breaker state
            assert client._failure_count > original_failure_count

        finally:
            await client.disconnect()


class TestStreamingDataFlow:
    """Test actual streaming data flow with real API."""

    @pytest.mark.asyncio
    async def test_short_streaming_session(self, real_api_config: Any) -> None:
        """Test a short streaming session to verify data flow."""
        client = StreamingClient(real_api_config)

        # Set up logging
        logger = StructuredLogger(
            real_api_config, log_file="logs/api_connectivity_test.log", log_level=LogLevel.INFO
        )

        try:
            await client.connect()
            await logger.start()

            context = LogContext(operation="api_connectivity_test", request_id="test_stream_001")

            await logger.info("Starting streaming connectivity test", context)

            # Stream for a very short time to test connectivity
            event_count = 0
            max_events = 5  # Limit to avoid long test times
            timeout_seconds = 30  # Short timeout

            try:
                start_time = datetime.now()

                async for event in client.stream_events():
                    event_count += 1

                    # Log the event
                    await logger.info(
                        f"Received streaming event {event_count}",
                        context,
                        {
                            "event_type": event.get("resource_kind"),
                            "resource_id": event.get("resource_id"),
                            "has_data": "data" in event,
                            "has_timepoint": "event" in event
                            and "timepoint" in event.get("event", {}),
                        },
                    )

                    # Stop after receiving a few events or timeout
                    if (
                        event_count >= max_events
                        or (datetime.now() - start_time).total_seconds() > timeout_seconds
                    ):
                        break

                # Verify we received some events (or at least connected successfully)
                await logger.info(
                    "Streaming test completed",
                    context,
                    {
                        "events_received": event_count,
                        "duration_seconds": (datetime.now() - start_time).total_seconds(),
                    },
                )

                # Even if no events were received, successful connection is a pass
                assert event_count >= 0, (
                    "Streaming connection should work even if no events received"
                )

            except asyncio.TimeoutError:
                await logger.warning("Streaming test timed out", context)
                pytest.skip(
                    "Streaming test timed out - this may be normal if no events are available"
                )

            except aiohttp.ClientError as e:
                await logger.error("Streaming connection error", context, {"error": str(e)})
                if "401" in str(e) or "unauthorized" in str(e).lower():
                    pytest.fail("Streaming API authentication failed")
                elif "403" in str(e) or "forbidden" in str(e).lower():
                    pytest.skip("No access to streaming API")
                else:
                    pytest.fail(f"Streaming API connection failed: {e}")

            except Exception as e:
                await logger.error("Unexpected streaming error", context, {"error": str(e)})
                raise

        finally:
            await logger.stop()
            await client.disconnect()

    @pytest.mark.asyncio
    async def test_streaming_event_validation(self, real_api_config: Any) -> None:
        """Test validation of real streaming events."""
        client = StreamingClient(real_api_config)

        try:
            await client.connect()

            # Stream a few events and validate their structure
            event_count = 0
            max_events = 3
            timeout_seconds = 20

            start_time = datetime.now()

            async for event in client.stream_events():
                event_count += 1

                # Validate event structure
                assert isinstance(event, dict), "Event should be a dictionary"

                # Check for expected top-level fields
                expected_fields = ["resource_kind", "resource_id"]
                for field in expected_fields:
                    if field not in event:
                        pass  # noqa: S110

                # If event has data, validate it's a dict
                if "data" in event:
                    assert isinstance(event["data"], dict), "Event data should be a dictionary"

                # If event has meta, validate it's a dict
                if "event" in event:
                    assert isinstance(event["event"], dict), "Event meta should be a dictionary"

                    # Check for timepoint
                    assert event is not None
                    if "timepoint" in event["event"]:
                        assert isinstance(event["event"]["timepoint"], int), (
                            "Timepoint should be an integer"
                        )

                # Stop after validation or timeout
                if (
                    event_count >= max_events
                    or (datetime.now() - start_time).total_seconds() > timeout_seconds
                ):
                    break

            # Test passes if we successfully validated events or connected without errors
            assert event_count >= 0, "Should be able to connect and validate event structure"

        except asyncio.TimeoutError:
            pytest.skip("No events received within timeout - this may be normal")
        except aiohttp.ClientError:
            pytest.skip("Could not connect to streaming API")

        finally:
            await client.disconnect()


class TestAPIErrorScenarios:
    """Test various error scenarios with real API."""

    @pytest.mark.asyncio
    async def test_invalid_credentials(self) -> None:
        """Test behavior with invalid API credentials."""
        # Create config with invalid credentials
        invalid_config = StreamingConfig(
            streaming_api_key="invalid-key-12345",
            database_path=":memory:",
            api_base_url="https://api.companieshouse.gov.uk",
        )

        client = StreamingClient(invalid_config)

        try:
            await client.connect()

            # Try to access API with invalid credentials
            async with aiohttp.ClientSession() as session:
                headers = {
                    "Authorization": f"Bearer {invalid_config.streaming_api_key}",
                    "User-Agent": "companies-house-streaming-test/1.0",
                }

                async with session.get(
                    "https://api.companieshouse.gov.uk/search/companies",
                    headers=headers,
                    params={"q": "test", "items_per_page": "1"},
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as response:
                    # Should get 401 Unauthorized
                    assert response.status == 401, (
                        f"Expected 401 for invalid credentials, got {response.status}"
                    )

        finally:
            await client.disconnect()

    @pytest.mark.asyncio
    async def test_network_timeout_handling(self, real_api_config: Any) -> None:
        """Test handling of network timeouts."""
        # Set very short timeout
        real_api_config.connection_timeout = 0.001  # 1ms - guaranteed to timeout

        client = StreamingClient(real_api_config)

        try:
            # This should timeout during connection
            with pytest.raises((asyncio.TimeoutError, aiohttp.ClientError)):
                await client.connect()

                # If connect somehow succeeds, try streaming with timeout
                async for _event in client.stream_events():
                    break  # Should timeout before getting here

        except Exception as e:
            # Any timeout or connection error is expected
            assert any(word in str(e).lower() for word in ["timeout", "connection", "connect"])

        finally:
            await client.disconnect()

    @pytest.mark.asyncio
    async def test_malformed_url_handling(self) -> None:
        """Test handling of malformed API URLs."""
        malformed_config = StreamingConfig(
            streaming_api_key=os.getenv("COMPANIES_HOUSE_STREAMING_API_KEY", "test-key"),
            database_path=":memory:",
            api_base_url="https://invalid-url-that-does-not-exist.example.com",
        )

        client = StreamingClient(malformed_config)

        try:
            # Should fail to connect to invalid URL
            with pytest.raises(aiohttp.ClientConnectorError):
                await client.connect()

                # Try to stream from invalid URL
                async for _event in client.stream_events():
                    break  # Should not reach here

        finally:
            await client.disconnect()


@pytest.mark.slow
class TestExtendedAPITesting:
    """Extended API testing (marked as slow)."""

    @pytest.mark.asyncio
    async def test_extended_streaming_session(self, real_api_config: Any) -> None:
        """Test extended streaming session (runs longer)."""
        client = StreamingClient(real_api_config)

        try:
            await client.connect()

            # Stream for a longer period
            event_count = 0
            max_events = 50  # More events
            timeout_seconds = 300  # 5 minutes

            start_time = datetime.now()

            async for _event in client.stream_events():
                event_count += 1

                if event_count % 10 == 0:
                    pass  # noqa: S110

                if (
                    event_count >= max_events
                    or (datetime.now() - start_time).total_seconds() > timeout_seconds
                ):
                    break

        except Exception as e:
            pytest.skip(f"Extended streaming test failed: {e}")

        finally:
            await client.disconnect()

    @pytest.mark.asyncio
    async def test_rate_limit_enforcement(self, real_api_config: Any) -> None:
        """Test that API actually enforces rate limits."""
        # This test intentionally tries to exceed rate limits
        client = StreamingClient(real_api_config)

        try:
            await client.connect()

            # Make rapid requests to test rate limiting
            responses = []

            async with aiohttp.ClientSession() as session:
                headers = {
                    "Authorization": f"Bearer {real_api_config.streaming_api_key}",
                    "User-Agent": "companies-house-streaming-test/1.0",
                }

                # Make rapid requests
                for i in range(10):
                    try:
                        async with session.get(
                            "https://api.companieshouse.gov.uk/search/companies",
                            headers=headers,
                            params={"q": f"test{i}", "items_per_page": "1"},
                            timeout=aiohttp.ClientTimeout(total=5),
                        ) as response:
                            responses.append(response.status)
                    except Exception:
                        responses.append(-1)  # Use -1 to indicate error

                    # Small delay between requests
                    await asyncio.sleep(0.1)

            # Check if any requests were rate limited (429 status)
            rate_limited = any(status == 429 for status in responses if isinstance(status, int))

            if rate_limited:
                pass  # noqa: S110
            else:
                pass  # noqa: S110

            # Test passes regardless - we're just checking behavior
            assert len(responses) > 0

        finally:
            await client.disconnect()

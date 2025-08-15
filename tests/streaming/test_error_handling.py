"""
Tests for streaming client error handling, circuit breaker patterns, and graceful degradation.

This module tests comprehensive error handling scenarios including circuit breaker
patterns for API failures and graceful degradation strategies.
"""

import pytest
import asyncio
import time
from unittest.mock import AsyncMock, Mock, patch
from datetime import datetime, timedelta
from aiohttp import ClientError, ClientConnectorError, ClientTimeout
from aiohttp.client_exceptions import ServerTimeoutError, ClientResponseError

from src.streaming.client import StreamingClient
from src.streaming.config import StreamingConfig
from src.streaming.event_processor import EventProcessor


# Fixtures
@pytest.fixture
def config():
    """Create test configuration for error handling."""
    return StreamingConfig(
        streaming_api_key="test-api-key-123456",
        database_path=":memory:",
        connection_timeout=5.0,
        initial_backoff=1.0,
        max_backoff=30.0,
        max_retries=3,
        rate_limit_requests_per_minute=60,
        health_check_interval=30
    )


@pytest.fixture
def client(config):
    """Create streaming client instance."""
    return StreamingClient(config)


@pytest.fixture
def processor(config):
    """Create event processor instance."""
    return EventProcessor(config)


# Circuit Breaker State Enum (to be implemented)
class CircuitBreakerState:
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class TestErrorScenarios:
    """Test various error scenarios and their handling."""
    
    @pytest.mark.asyncio
    async def test_json_parsing_errors(self, client, config):
        """Test handling of malformed JSON in streaming data."""
        with patch('aiohttp.ClientSession') as mock_session_class:
            mock_session = AsyncMock()
            mock_session_class.return_value = mock_session
            mock_session.closed = False
            
            # Mock successful connection
            health_response = AsyncMock()
            health_response.status = 200
            
            # Mock streaming response with malformed JSON
            stream_response = AsyncMock()
            stream_response.status = 200
            stream_response.content = AsyncMock()
            
            # Simulate malformed JSON data
            malformed_data = [
                b'{"valid": "json"}\n',
                b'{"malformed": "json",}\n',  # Trailing comma
                b'invalid json data\n',
                b'{"another": "valid"}\n'
            ]
            
            async def mock_stream():
                for line in malformed_data:
                    yield line
            
            stream_response.content.__aiter__ = mock_stream
            
            def mock_get(url, **kwargs):
                if 'healthcheck' in str(url):
                    return health_response
                else:
                    return stream_response
            
            mock_session.get.side_effect = mock_get
            
            await client.connect()
            
            events_processed = 0
            async for event in client.stream_events():
                events_processed += 1
                if events_processed >= 2:  # Only valid JSON should be processed
                    break
            
            # Should process 2 valid events and skip malformed ones
            assert events_processed == 2
    
    @pytest.mark.asyncio
    async def test_database_connection_errors(self, processor, config):
        """Test handling of database connection failures."""
        # Mock database connection failure
        with patch('aiosqlite.connect') as mock_connect:
            mock_connect.side_effect = Exception("Database connection failed")
            
            # Event processor should handle database errors gracefully
            event_data = {
                "resource_kind": "company-profile",
                "resource_id": "12345678",
                "data": {"company_number": "12345678", "company_name": "Test Company"}
            }
            
            # This should not raise an exception, but handle the error
            result = await processor.process_event(event_data)
            assert result is False  # Processing should fail gracefully
            assert processor.failed_events == 1
    
    @pytest.mark.asyncio
    async def test_memory_pressure_handling(self, client, config):
        """Test handling of memory pressure scenarios."""
        # Simulate memory pressure by creating many large events
        with patch('aiohttp.ClientSession') as mock_session_class:
            mock_session = AsyncMock()
            mock_session_class.return_value = mock_session
            mock_session.closed = False
            
            # Mock successful connection
            health_response = AsyncMock()
            health_response.status = 200
            
            # Mock streaming response with large events
            stream_response = AsyncMock()
            stream_response.status = 200
            stream_response.content = AsyncMock()
            
            # Create large event data
            large_event_data = {
                "resource_kind": "company-profile",
                "resource_id": "12345678",
                "data": {
                    "company_number": "12345678",
                    "large_field": "x" * 10000,  # Large data field
                }
            }
            
            async def mock_stream():
                # Yield many large events
                for i in range(100):
                    yield f'{{"resource_kind": "company-profile", "data": {{"large_field": "{"x" * 1000}"}}}}\n'.encode()
            
            stream_response.content.__aiter__ = mock_stream
            
            def mock_get(url, **kwargs):
                if 'healthcheck' in str(url):
                    return health_response
                else:
                    return stream_response
            
            mock_session.get.side_effect = mock_get
            
            await client.connect()
            
            # Should handle large events without running out of memory
            events_processed = 0
            try:
                async for event in client.stream_events():
                    events_processed += 1
                    if events_processed >= 10:  # Process limited number
                        break
            except MemoryError:
                pytest.fail("Client should handle memory pressure gracefully")
            
            assert events_processed > 0
    
    @pytest.mark.asyncio
    async def test_api_quota_exceeded_errors(self, client, config):
        """Test handling of API quota exceeded errors."""
        with patch('aiohttp.ClientSession') as mock_session_class:
            mock_session = AsyncMock()
            mock_session_class.return_value = mock_session
            mock_session.closed = False
            
            # Mock quota exceeded response
            quota_response = AsyncMock()
            quota_response.status = 403
            quota_response.text.return_value = "API quota exceeded"
            quota_response.headers = {'X-RateLimit-Reset': str(int(time.time()) + 3600)}
            
            mock_session.get.return_value = quota_response
            
            # Should handle quota exceeded gracefully
            with pytest.raises(ClientError, match="Health check failed with status 403"):
                await client.connect()
    
    @pytest.mark.asyncio
    async def test_partial_data_corruption(self, client, config):
        """Test handling of partially corrupted streaming data."""
        with patch('aiohttp.ClientSession') as mock_session_class:
            mock_session = AsyncMock()
            mock_session_class.return_value = mock_session
            mock_session.closed = False
            
            # Mock successful connection
            health_response = AsyncMock()
            health_response.status = 200
            
            # Mock streaming response with corrupted data
            stream_response = AsyncMock()
            stream_response.status = 200
            stream_response.content = AsyncMock()
            
            # Mix of valid and corrupted data
            mixed_data = [
                b'{"resource_kind": "company-profile", "data": {"company_number": "12345"}}\n',
                b'{"resource_kind": "company-pro\x00\x00file", "data":\n',  # Corrupted
                b'{"resource_kind": "company-profile", "data": {"company_number": "67890"}}\n',
                b'\xff\xfe{"invalid": "encoding"}\n',  # Invalid encoding
            ]
            
            async def mock_stream():
                for line in mixed_data:
                    yield line
            
            stream_response.content.__aiter__ = mock_stream
            
            def mock_get(url, **kwargs):
                if 'healthcheck' in str(url):
                    return health_response
                else:
                    return stream_response
            
            mock_session.get.side_effect = mock_get
            
            await client.connect()
            
            events_processed = 0
            async for event in client.stream_events():
                events_processed += 1
                if events_processed >= 2:  # Should process valid events
                    break
            
            assert events_processed == 2


class TestCircuitBreaker:
    """Test circuit breaker pattern for API failures."""
    
    @pytest.mark.asyncio
    async def test_circuit_breaker_initialization(self, client, config):
        """Test circuit breaker initializes in closed state."""
        # Circuit breaker should be implemented in client
        # Initial state should be CLOSED
        assert hasattr(client, '_circuit_breaker_state')
        assert client._circuit_breaker_state == CircuitBreakerState.CLOSED
        assert hasattr(client, '_failure_count')
        assert client._failure_count == 0
        assert hasattr(client, '_last_failure_time')
        assert client._last_failure_time is None
    
    @pytest.mark.asyncio
    async def test_circuit_breaker_failure_threshold(self, client, config):
        """Test circuit breaker opens after failure threshold."""
        with patch('aiohttp.ClientSession') as mock_session_class:
            mock_session = AsyncMock()
            mock_session_class.return_value = mock_session
            mock_session.closed = False
            
            # Mock continuous failures
            mock_session.get.side_effect = ClientConnectorError(
                connection_key=None,
                os_error=OSError("Connection failed")
            )
            
            # Try connecting multiple times to trigger circuit breaker
            for i in range(5):  # Should exceed failure threshold
                try:
                    await client.connect()
                except ClientConnectorError:
                    pass
            
            # Circuit breaker should be open after failures
            assert client._circuit_breaker_state == CircuitBreakerState.OPEN
            assert client._failure_count >= config.max_retries
    
    @pytest.mark.asyncio
    async def test_circuit_breaker_open_rejects_requests(self, client, config):
        """Test circuit breaker rejects requests when open."""
        # Force circuit breaker to open state
        client._circuit_breaker_state = CircuitBreakerState.OPEN
        client._failure_count = 10
        client._last_failure_time = datetime.now()
        
        # Should reject connection attempts immediately
        with pytest.raises(Exception, match="Circuit breaker is open"):
            await client.connect()
    
    @pytest.mark.asyncio
    async def test_circuit_breaker_half_open_recovery(self, client, config):
        """Test circuit breaker recovery through half-open state."""
        # Set circuit breaker to open state with old failure time
        client._circuit_breaker_state = CircuitBreakerState.OPEN
        client._failure_count = 5
        client._last_failure_time = datetime.now() - timedelta(minutes=5)  # Old failure
        
        with patch('aiohttp.ClientSession') as mock_session_class:
            mock_session = AsyncMock()
            mock_session_class.return_value = mock_session
            mock_session.closed = False
            
            # Mock successful connection for recovery
            health_response = AsyncMock()
            health_response.status = 200
            mock_session.get.return_value = health_response
            
            # Should move to half-open, then closed on success
            await client.connect()
            
            assert client._circuit_breaker_state == CircuitBreakerState.CLOSED
            assert client._failure_count == 0
    
    @pytest.mark.asyncio
    async def test_circuit_breaker_metrics(self, client, config):
        """Test circuit breaker provides metrics."""
        metrics = client.get_circuit_breaker_metrics()
        
        assert 'state' in metrics
        assert 'failure_count' in metrics
        assert 'last_failure_time' in metrics
        assert 'success_count' in metrics
        assert 'open_duration' in metrics


class TestGracefulDegradation:
    """Test graceful degradation strategies."""
    
    @pytest.mark.asyncio
    async def test_fallback_to_polling_mode(self, client, config):
        """Test fallback to polling when streaming fails."""
        # When streaming connection fails repeatedly, should fallback to polling
        client._circuit_breaker_state = CircuitBreakerState.OPEN
        
        # Should have a fallback polling mechanism
        assert hasattr(client, 'fallback_to_polling')
        
        # Test polling mode activation
        await client.fallback_to_polling()
        assert client.is_polling_mode is True
        assert client.polling_interval > 0
    
    @pytest.mark.asyncio
    async def test_reduced_functionality_mode(self, client, config):
        """Test operation with reduced functionality during degradation."""
        # Enable degraded mode
        client.enable_degraded_mode()
        
        assert client.is_degraded_mode is True
        
        # In degraded mode, should still process critical events
        event_data = {
            "resource_kind": "company-profile",
            "resource_id": "12345678",
            "data": {
                "company_number": "12345678",
                "company_status": "active-proposal-to-strike-off"
            }
        }
        
        # Should process critical strike-off events even in degraded mode
        should_process = client.should_process_in_degraded_mode(event_data)
        assert should_process is True
        
        # Should skip non-critical events in degraded mode
        non_critical_event = {
            "resource_kind": "company-profile",
            "resource_id": "87654321",
            "data": {
                "company_number": "87654321",
                "company_status": "active"  # Non-critical status
            }
        }
        
        should_process = client.should_process_in_degraded_mode(non_critical_event)
        assert should_process is False
    
    @pytest.mark.asyncio
    async def test_graceful_service_recovery(self, client, config):
        """Test graceful recovery from degraded mode."""
        # Start in degraded mode
        client.enable_degraded_mode()
        assert client.is_degraded_mode is True
        
        # Simulate service recovery
        with patch('aiohttp.ClientSession') as mock_session_class:
            mock_session = AsyncMock()
            mock_session_class.return_value = mock_session
            mock_session.closed = False
            
            # Mock successful health check
            health_response = AsyncMock()
            health_response.status = 200
            mock_session.get.return_value = health_response
            
            # Attempt recovery
            recovery_successful = await client.attempt_recovery()
            
            assert recovery_successful is True
            assert client.is_degraded_mode is False
            assert client._circuit_breaker_state == CircuitBreakerState.CLOSED
    
    @pytest.mark.asyncio
    async def test_adaptive_retry_strategies(self, client, config):
        """Test adaptive retry strategies based on error types."""
        # Different retry strategies for different error types
        
        # Network errors - use exponential backoff
        network_error = ClientConnectorError(connection_key=None, os_error=OSError("Network error"))
        retry_delay = client.calculate_retry_delay(network_error, attempt=1)
        assert retry_delay >= config.initial_backoff
        
        # Rate limit errors - use fixed delay based on Retry-After
        rate_limit_error = ClientResponseError(
            request_info=Mock(),
            history=(),
            status=429,
            headers={'Retry-After': '60'}
        )
        retry_delay = client.calculate_retry_delay(rate_limit_error, attempt=1)
        assert retry_delay == 60  # Should use Retry-After value
        
        # Server errors - use shorter backoff
        server_error = ClientResponseError(
            request_info=Mock(),
            history=(),
            status=500
        )
        retry_delay = client.calculate_retry_delay(server_error, attempt=1)
        assert retry_delay <= config.initial_backoff
    
    @pytest.mark.asyncio
    async def test_resource_cleanup_on_errors(self, client, config):
        """Test proper resource cleanup during error scenarios."""
        with patch('aiohttp.ClientSession') as mock_session_class:
            mock_session = AsyncMock()
            mock_session_class.return_value = mock_session
            mock_session.closed = False
            
            # Mock connection failure
            mock_session.get.side_effect = Exception("Connection failed")
            
            try:
                await client.connect()
            except Exception:
                pass
            
            # Resources should be cleaned up properly
            await client.shutdown()
            
            # Session should be closed and cleaned up
            mock_session.close.assert_called()
            assert client.session is None
            assert not client.is_connected


class TestErrorReporting:
    """Test error reporting and monitoring capabilities."""
    
    def test_error_metrics_collection(self, client, config):
        """Test collection of error metrics."""
        # Client should collect error metrics
        metrics = client.get_error_metrics()
        
        assert 'total_errors' in metrics
        assert 'error_types' in metrics
        assert 'last_error_time' in metrics
        assert 'error_rate' in metrics
    
    @pytest.mark.asyncio
    async def test_error_event_logging(self, client, config):
        """Test logging of error events for monitoring."""
        with patch('logging.Logger.error') as mock_logger:
            # Trigger an error
            try:
                await client._health_check()  # Should fail with no session
            except RuntimeError:
                pass
            
            # Should log the error with appropriate context
            mock_logger.assert_called()
            
            # Check that error context is included
            call_args = mock_logger.call_args[0][0]
            assert 'Session not initialized' in call_args
    
    def test_health_status_reporting(self, client, config):
        """Test health status reporting capabilities."""
        health_status = client.get_health_status()
        
        assert 'is_connected' in health_status
        assert 'last_heartbeat' in health_status
        assert 'circuit_breaker_state' in health_status
        assert 'error_count' in health_status
        assert 'degraded_mode' in health_status


class TestEventProcessingErrors:
    """Test error handling in event processing pipeline."""
    
    @pytest.mark.asyncio
    async def test_event_validation_errors(self, processor, config):
        """Test handling of invalid event data."""
        # Missing required fields
        invalid_event = {
            "resource_kind": "company-profile"
            # Missing data field
        }
        
        result = await processor.process_event(invalid_event)
        assert result is False
        assert processor.failed_events == 1
    
    @pytest.mark.asyncio
    async def test_event_processing_timeout(self, processor, config):
        """Test handling of event processing timeouts."""
        # Mock slow event handler
        slow_handler = AsyncMock()
        
        async def slow_process(event):
            await asyncio.sleep(10)  # Simulate slow processing
        
        slow_handler.side_effect = slow_process
        processor.register_event_handler("company-profile", slow_handler)
        
        event_data = {
            "resource_kind": "company-profile",
            "data": {"company_number": "12345678"}
        }
        
        # Should timeout and handle gracefully
        with patch('asyncio.wait_for') as mock_wait_for:
            mock_wait_for.side_effect = asyncio.TimeoutError()
            
            result = await processor.process_event(event_data)
            assert result is False
    
    @pytest.mark.asyncio
    async def test_batch_processing_errors(self, processor, config):
        """Test error handling in batch event processing."""
        # Mix of valid and invalid events
        events = [
            {"resource_kind": "company-profile", "data": {"company_number": "12345"}},
            {"resource_kind": "invalid"},  # Invalid event
            {"resource_kind": "company-profile", "data": {"company_number": "67890"}},
        ]
        
        results = await processor.process_batch(events)
        
        # Should process valid events and handle invalid ones
        assert len(results) == 3
        assert results[0] is True   # Valid
        assert results[1] is False  # Invalid
        assert results[2] is True   # Valid
        
        assert processor.processed_events == 2
        assert processor.failed_events == 1
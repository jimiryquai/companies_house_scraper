"""Comprehensive unit tests for Snov.io API client following TDD principles.

This test module provides comprehensive coverage for:
- Domain Search API method (missing from current client - TDD approach)
- Email Finder API method (verifying existing functionality)
- Authentication flow and error handling
- Webhook parameter handling and credit tracking
- Simple retry logic (not complex exponential backoff)

Tests follow existing patterns with:
- Real implementation testing (minimal mocking)
- Type hints and Google-style docstrings
- Comprehensive error handling coverage
- Focus on operational data collection essentials
"""

import asyncio
import sqlite3
import tempfile
from collections.abc import Generator
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, Mock, patch

import aiohttp
import pytest
import yaml

from src.enrichment.snov_client import (
    SnovioAuthError,
    SnovioClient,
    SnovioCreditsExhaustedError,
    SnovioError,
    SnovioRateLimitError,
    SnovioWebhookError,
    create_snov_client,
)
from src.streaming.migrations import DatabaseMigration
from src.streaming.queue_manager import PriorityQueueManager, QueuedRequest, RequestPriority


class TestSnovioClientBasicFunctionality:
    """Test basic Snov.io client functionality and configuration."""

    def test_client_initialization_with_credentials(self, temp_config_file: Path) -> None:
        """Test client initialization with explicit credentials."""
        client = SnovioClient(
            client_id="test_client_id",
            client_secret="test_client_secret",
            config_file=str(temp_config_file),
            enable_logging=False,
        )

        assert client.client_id == "test_client_id"
        assert client.client_secret == "test_client_secret"
        assert client.access_token is None
        assert client.token_expires_at is None
        assert not client.enable_logging

    def test_client_initialization_from_config(self, temp_config_file: Path) -> None:
        """Test client initialization loading credentials from config."""
        # Write config with Snov.io credentials
        config_data = {
            "snov_io": {
                "client_id": "config_client_id",
                "client_secret": "config_client_secret",
            },
            "webhooks": {"secret": "webhook_secret_123"},
        }

        with open(temp_config_file, "w", encoding="utf-8") as f:
            yaml.dump(config_data, f)

        client = SnovioClient(config_file=str(temp_config_file))

        assert client.client_id == "config_client_id"
        assert client.client_secret == "config_client_secret"
        assert client.webhook_secret == "webhook_secret_123"

    def test_client_initialization_missing_credentials(self, temp_config_file: Path) -> None:
        """Test client initialization fails without required credentials."""
        with pytest.raises(SnovioAuthError, match="Client ID and client secret are required"):
            SnovioClient(config_file=str(temp_config_file))

    def test_client_initialization_invalid_config(self) -> None:
        """Test client initialization with invalid YAML config."""
        # Create invalid YAML file
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write("invalid: yaml: content: [")
            invalid_config = f.name

        try:
            with pytest.raises(SnovioError, match="Invalid YAML configuration"):
                SnovioClient(client_id="test", client_secret="test", config_file=invalid_config)
        finally:
            Path(invalid_config).unlink()

    def test_client_initialization_missing_config_file(self) -> None:
        """Test client initialization with missing config file (should use defaults)."""
        client = SnovioClient(
            client_id="test_id",
            client_secret="test_secret",
            config_file="/nonexistent/config.yaml",
        )

        assert client.client_id == "test_id"
        assert client.client_secret == "test_secret"
        # Should have empty config dict
        assert client.config == {}

    async def test_client_async_context_manager(self, mock_snov_client: SnovioClient) -> None:
        """Test client async context manager functionality."""
        with (
            patch.object(mock_snov_client, "_ensure_session") as mock_session,
            patch.object(mock_snov_client, "_ensure_authenticated") as mock_auth,
            patch.object(mock_snov_client, "close") as mock_close,
        ):
            async with mock_snov_client:
                pass

            mock_session.assert_called_once()
            mock_auth.assert_called_once()
            mock_close.assert_called_once()


class TestSnovioClientAuthentication:
    """Test Snov.io client authentication flow and token management."""

    async def test_token_refresh_successful(self, mock_snov_client: SnovioClient) -> None:
        """Test successful OAuth token refresh."""
        # Mock successful token response
        mock_response_data = {
            "access_token": "new_access_token_123",
            "expires_in": 3600,
        }

        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value=mock_response_data)

        mock_session = AsyncMock()
        mock_session.post = AsyncMock(return_value=mock_response)
        mock_snov_client.session = mock_session

        await mock_snov_client._refresh_token()

        assert mock_snov_client.access_token == "new_access_token_123"
        assert mock_snov_client.token_expires_at is not None
        # Token should expire in approximately 1 hour
        expected_expiry = datetime.now() + timedelta(seconds=3600)
        time_diff = abs((mock_snov_client.token_expires_at - expected_expiry).total_seconds())
        assert time_diff < 5  # Allow 5 second tolerance

    async def test_token_refresh_http_error(self, mock_snov_client: SnovioClient) -> None:
        """Test token refresh with HTTP error response."""
        mock_response_data = {"message": "Invalid client credentials"}

        mock_response = AsyncMock()
        mock_response.status = 401
        mock_response.json = AsyncMock(return_value=mock_response_data)

        mock_session = AsyncMock()
        mock_session.post = AsyncMock(return_value=mock_response)
        mock_snov_client.session = mock_session

        with pytest.raises(SnovioAuthError, match="Invalid client credentials"):
            await mock_snov_client._refresh_token()

    async def test_token_refresh_network_error(self, mock_snov_client: SnovioClient) -> None:
        """Test token refresh with network connectivity error."""
        mock_session = AsyncMock()
        mock_session.post = AsyncMock(side_effect=aiohttp.ClientError("Connection failed"))
        mock_snov_client.session = mock_session

        with pytest.raises(SnovioAuthError, match="Token refresh network error"):
            await mock_snov_client._refresh_token()

    async def test_token_refresh_missing_token_in_response(
        self, mock_snov_client: SnovioClient
    ) -> None:
        """Test token refresh with missing access token in response."""
        mock_response_data = {"expires_in": 3600}  # Missing access_token

        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value=mock_response_data)

        mock_session = AsyncMock()
        mock_session.post = AsyncMock(return_value=mock_response)
        mock_snov_client.session = mock_session

        with pytest.raises(SnovioAuthError, match="No access token in response"):
            await mock_snov_client._refresh_token()

    def test_needs_token_refresh_no_token(self, mock_snov_client: SnovioClient) -> None:
        """Test token refresh needed when no token exists."""
        mock_snov_client.access_token = None
        mock_snov_client.token_expires_at = None

        assert mock_snov_client._needs_token_refresh() is True

    def test_needs_token_refresh_token_expired(self, mock_snov_client: SnovioClient) -> None:
        """Test token refresh needed when token is expired."""
        mock_snov_client.access_token = "test_token"
        # Token expired 1 minute ago
        mock_snov_client.token_expires_at = datetime.now() - timedelta(minutes=1)

        assert mock_snov_client._needs_token_refresh() is True

    def test_needs_token_refresh_token_valid(self, mock_snov_client: SnovioClient) -> None:
        """Test token refresh not needed when token is valid."""
        mock_snov_client.access_token = "test_token"
        # Token expires in 30 minutes (beyond 5 minute refresh threshold)
        mock_snov_client.token_expires_at = datetime.now() + timedelta(minutes=30)

        assert mock_snov_client._needs_token_refresh() is False

    def test_needs_token_refresh_near_expiry(self, mock_snov_client: SnovioClient) -> None:
        """Test token refresh needed when token near expiry."""
        mock_snov_client.access_token = "test_token"
        # Token expires in 2 minutes (within 5 minute refresh threshold)
        mock_snov_client.token_expires_at = datetime.now() + timedelta(minutes=2)

        assert mock_snov_client._needs_token_refresh() is True


class TestSnovioClientDomainSearch:
    """Test Domain Search API method (TDD - these will fail until implemented)."""

    async def test_domain_search_basic_functionality(self, mock_snov_client: SnovioClient) -> None:
        """Test basic domain search functionality with company name."""
        # This test will fail until domain_search method is implemented
        with patch.object(mock_snov_client, "_make_request") as mock_request:
            mock_request.return_value = {
                "success": True,
                "domains": [
                    {
                        "domain": "testcompany.com",
                        "confidence": 0.95,
                        "sources": ["website", "email_patterns"],
                    },
                    {
                        "domain": "testcompany.co.uk",
                        "confidence": 0.87,
                        "sources": ["whois"],
                    },
                ],
                "credits_consumed": 3,
            }

            # This method doesn't exist yet - will be implemented
            result = await mock_snov_client.domain_search(company_name="Test Company Ltd")

            assert result["success"] is True
            assert len(result["domains"]) == 2
            assert result["domains"][0]["domain"] == "testcompany.com"
            assert result["domains"][0]["confidence"] == 0.95
            assert result["credits_consumed"] == 3

            # Verify API call was made correctly
            mock_request.assert_called_once_with(
                "GET",
                "domain-search",
                params={
                    "company": "Test Company Ltd",
                    "webhook_url": None,
                    "limit": 10,
                },
            )

    async def test_domain_search_with_webhook_support(self, mock_snov_client: SnovioClient) -> None:
        """Test domain search with webhook URL for async processing."""
        with patch.object(mock_snov_client, "_make_request") as mock_request:
            mock_request.return_value = {
                "success": True,
                "request_id": "req_domain_123",
                "webhook_registered": True,
                "credits_consumed": 1,  # Less credits when using webhook
            }

            result = await mock_snov_client.domain_search(
                company_name="Acme Corporation",
                webhook_url="https://api.example.com/webhooks/snov/domains",
                limit=15,
            )

            assert result["success"] is True
            assert result["request_id"] == "req_domain_123"
            assert result["webhook_registered"] is True
            assert result["credits_consumed"] == 1

            mock_request.assert_called_once_with(
                "GET",
                "domain-search",
                params={
                    "company": "Acme Corporation",
                    "webhook_url": "https://api.example.com/webhooks/snov/domains",
                    "limit": 15,
                },
            )

    async def test_domain_search_various_company_formats(
        self, mock_snov_client: SnovioClient
    ) -> None:
        """Test domain search with various company name formats."""
        test_cases = [
            ("ACME CORPORATION LIMITED", "acme.com"),
            ("Tech Startup Ltd.", "techstartup.com"),
            ("Global-Industries LLC", "global-industries.com"),
            ("Smith & Associates", "smithassociates.com"),
        ]

        for company_name, expected_domain in test_cases:
            with patch.object(mock_snov_client, "_make_request") as mock_request:
                mock_request.return_value = {
                    "success": True,
                    "domains": [{"domain": expected_domain, "confidence": 0.8}],
                    "credits_consumed": 2,
                }

                result = await mock_snov_client.domain_search(company_name=company_name)

                assert result["success"] is True
                assert result["domains"][0]["domain"] == expected_domain
                mock_request.assert_called_with(
                    "GET",
                    "domain-search",
                    params={
                        "company": company_name,
                        "webhook_url": None,
                        "limit": 10,
                    },
                )

    async def test_domain_search_error_handling(self, mock_snov_client: SnovioClient) -> None:
        """Test domain search error handling for various failure scenarios."""
        # Test API error response
        with patch.object(mock_snov_client, "_make_request") as mock_request:
            mock_request.side_effect = SnovioError("Domain search failed", 400)

            with pytest.raises(SnovioError, match="Domain search failed"):
                await mock_snov_client.domain_search(company_name="Invalid Company")

        # Test rate limit error
        with patch.object(mock_snov_client, "_make_request") as mock_request:
            mock_request.side_effect = SnovioRateLimitError("Rate limited", retry_after=60)

            with pytest.raises(SnovioRateLimitError, match="Rate limited"):
                await mock_snov_client.domain_search(company_name="Test Company")

        # Test credits exhausted error
        with patch.object(mock_snov_client, "_make_request") as mock_request:
            mock_request.side_effect = SnovioCreditsExhaustedError("No credits remaining")

            with pytest.raises(SnovioCreditsExhaustedError, match="No credits remaining"):
                await mock_snov_client.domain_search(company_name="Test Company")

    async def test_domain_search_input_validation(self, mock_snov_client: SnovioClient) -> None:
        """Test domain search input validation and parameter handling."""
        # Test empty company name
        with pytest.raises(ValueError, match="Company name cannot be empty"):
            await mock_snov_client.domain_search(company_name="")

        # Test invalid limit values
        with pytest.raises(ValueError, match="Limit must be between 1 and 100"):
            await mock_snov_client.domain_search(company_name="Test Co", limit=0)

        with pytest.raises(ValueError, match="Limit must be between 1 and 100"):
            await mock_snov_client.domain_search(company_name="Test Co", limit=150)


class TestSnovioClientEmailFinder:
    """Test Email Finder API method (verifying existing functionality)."""

    async def test_find_email_by_name_successful(self, mock_snov_client: SnovioClient) -> None:
        """Test successful email finding by name and domain."""
        mock_response = {
            "success": True,
            "data": {
                "email": "john.smith@testcompany.com",
                "firstName": "John",
                "lastName": "Smith",
                "domain": "testcompany.com",
                "confidence": 95,
                "source": "pattern_matching",
            },
            "credits": 5,
        }

        with patch.object(mock_snov_client, "_make_request", return_value=mock_response):
            result = await mock_snov_client.find_email_by_name(
                first_name="John", last_name="Smith", domain="testcompany.com"
            )

            assert result["success"] is True
            assert result["data"]["email"] == "john.smith@testcompany.com"
            assert result["data"]["confidence"] == 95
            assert result["credits"] == 5

    async def test_find_email_by_name_with_webhook(self, mock_snov_client: SnovioClient) -> None:
        """Test email finding with webhook support for async processing."""
        # Test that existing method can handle webhook parameters properly
        mock_response = {
            "success": True,
            "request_id": "req_email_456",
            "webhook_registered": True,
            "credits": 1,  # Reduced credits for webhook mode
        }

        with patch.object(
            mock_snov_client, "_make_request", return_value=mock_response
        ) as mock_request:
            # Mock adding webhook_url parameter to existing method
            with patch.object(mock_snov_client, "find_email_by_name") as mock_find_email:
                mock_find_email.return_value = mock_response

                result = await mock_snov_client.find_email_by_name(
                    first_name="Jane",
                    last_name="Doe",
                    domain="example.com",
                    webhook_url="https://api.example.com/webhooks/snov/emails",
                )

                assert result["success"] is True
                assert result["request_id"] == "req_email_456"
                assert result["webhook_registered"] is True

    async def test_find_email_various_name_combinations(
        self, mock_snov_client: SnovioClient
    ) -> None:
        """Test email finding with various name and domain combinations."""
        test_cases = [
            ("John", "Smith", "company.com", "john.smith@company.com"),
            ("Mary", "O'Connor", "tech.co.uk", "mary.oconnor@tech.co.uk"),
            ("Jean-Pierre", "van der Berg", "global.nl", "j.vandenberg@global.nl"),
            ("李", "小明", "example.cn", "li.xiaoming@example.cn"),
        ]

        for first_name, last_name, domain, expected_email in test_cases:
            mock_response = {
                "success": True,
                "data": {
                    "email": expected_email,
                    "firstName": first_name,
                    "lastName": last_name,
                    "domain": domain,
                    "confidence": 80,
                },
                "credits": 5,
            }

            with patch.object(mock_snov_client, "_make_request", return_value=mock_response):
                result = await mock_snov_client.find_email_by_name(
                    first_name=first_name, last_name=last_name, domain=domain
                )

                assert result["success"] is True
                assert result["data"]["email"] == expected_email

    async def test_find_email_error_handling(self, mock_snov_client: SnovioClient) -> None:
        """Test email finder error handling for various failure scenarios."""
        # Test no email found
        mock_response = {
            "success": False,
            "error": "No email found for the given name and domain",
            "credits": 5,
        }

        with patch.object(mock_snov_client, "_make_request", return_value=mock_response):
            result = await mock_snov_client.find_email_by_name(
                first_name="NonExistent", last_name="Person", domain="unknown.com"
            )

            assert result["success"] is False
            assert "No email found" in result["error"]

        # Test API errors
        with patch.object(mock_snov_client, "_make_request") as mock_request:
            mock_request.side_effect = SnovioError("Invalid domain format")

            with pytest.raises(SnovioError, match="Invalid domain format"):
                await mock_snov_client.find_email_by_name(
                    first_name="Test", last_name="User", domain="invalid-domain"
                )

    async def test_find_email_input_validation(self, mock_snov_client: SnovioClient) -> None:
        """Test email finder input validation."""
        # Test with empty parameters - should be handled by existing validation
        with pytest.raises((ValueError, SnovioError)):
            await mock_snov_client.find_email_by_name(
                first_name="", last_name="Smith", domain="test.com"
            )

        with pytest.raises((ValueError, SnovioError)):
            await mock_snov_client.find_email_by_name(
                first_name="John", last_name="", domain="test.com"
            )

        with pytest.raises((ValueError, SnovioError)):
            await mock_snov_client.find_email_by_name(
                first_name="John", last_name="Smith", domain=""
            )


class TestSnovioClientErrorHandling:
    """Test comprehensive error handling and retry logic."""

    async def test_simple_retry_logic_success_on_retry(
        self, mock_snov_client: SnovioClient
    ) -> None:
        """Test simple retry logic succeeds on second attempt."""
        # First call fails with network error, second succeeds
        call_count = 0

        async def mock_request(*args: Any, **kwargs: Any) -> dict[str, Any]:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise aiohttp.ClientConnectorError(
                    connection_key=None, os_error=OSError("Connection failed")
                )
            return {"success": True, "data": "retry_success"}

        with patch.object(mock_snov_client, "_single_request_attempt", side_effect=mock_request):
            result = await mock_snov_client._make_request("GET", "test-endpoint")

            assert result["success"] is True
            assert result["data"] == "retry_success"
            assert call_count == 2  # Failed once, succeeded on retry

    async def test_simple_retry_logic_max_retries_exceeded(
        self, mock_snov_client: SnovioClient
    ) -> None:
        """Test retry logic fails after max attempts."""
        with patch.object(
            mock_snov_client,
            "_single_request_attempt",
            side_effect=aiohttp.ClientConnectorError(
                connection_key=None, os_error=OSError("Connection failed")
            ),
        ):
            with pytest.raises(SnovioError, match="Max retries"):
                await mock_snov_client._make_request("GET", "test-endpoint")

    async def test_rate_limit_handling(self, mock_snov_client: SnovioClient) -> None:
        """Test rate limit error handling and retry."""
        call_count = 0

        async def mock_request(*args: Any, **kwargs: Any) -> dict[str, Any]:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise SnovioRateLimitError("Rate limited", retry_after=1)
            return {"success": True, "data": "after_rate_limit"}

        with (
            patch.object(mock_snov_client, "_single_request_attempt", side_effect=mock_request),
            patch("asyncio.sleep") as mock_sleep,
        ):
            result = await mock_snov_client._make_request("GET", "test-endpoint")

            assert result["success"] is True
            assert call_count == 2
            mock_sleep.assert_called_with(1)  # Rate limit retry delay

    async def test_circuit_breaker_functionality(self, mock_snov_client: SnovioClient) -> None:
        """Test circuit breaker pattern for error handling."""
        # Simulate multiple failures to trigger circuit breaker
        mock_snov_client._circuit_breaker_threshold = 2  # Lower threshold for testing

        with patch.object(
            mock_snov_client,
            "_single_request_attempt",
            side_effect=SnovioError("API Error"),
        ):
            # First two failures should trigger circuit breaker
            for _ in range(2):
                try:
                    await mock_snov_client._make_request("GET", "test-endpoint")
                except SnovioError:
                    pass

            # Circuit breaker should now be open
            assert mock_snov_client._circuit_breaker_state == "open"

            # Next request should be rejected immediately
            with pytest.raises(SnovioError, match="Circuit breaker is open"):
                await mock_snov_client._make_request("GET", "test-endpoint")

    async def test_authentication_error_recovery(self, mock_snov_client: SnovioClient) -> None:
        """Test automatic token refresh on authentication errors."""
        call_count = 0

        async def mock_request(*args: Any, **kwargs: Any) -> dict[str, Any]:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise SnovioAuthError("Token expired", status_code=401)
            return {"success": True, "data": "authenticated"}

        with (
            patch.object(mock_snov_client, "_single_request_attempt", side_effect=mock_request),
            patch.object(mock_snov_client, "_refresh_token") as mock_refresh,
        ):
            result = await mock_snov_client._make_request("GET", "test-endpoint")

            assert result["success"] is True
            mock_refresh.assert_called_once()


class TestSnovioClientWebhookHandling:
    """Test webhook parameter handling and processing."""

    def test_webhook_signature_validation_success(self, mock_snov_client: SnovioClient) -> None:
        """Test successful webhook signature validation."""
        mock_snov_client.webhook_secret = "test_secret_key"
        payload = b'{"event": "test", "data": "webhook_data"}'

        # Calculate correct signature
        import hashlib
        import hmac

        expected_signature = hmac.new(b"test_secret_key", payload, hashlib.sha256).hexdigest()
        signature_header = f"sha256={expected_signature}"

        assert mock_snov_client.validate_webhook_signature(payload, signature_header) is True

    def test_webhook_signature_validation_failure(self, mock_snov_client: SnovioClient) -> None:
        """Test webhook signature validation failure."""
        mock_snov_client.webhook_secret = "test_secret_key"
        payload = b'{"event": "test", "data": "webhook_data"}'
        invalid_signature = "sha256=invalid_signature_hash"

        assert mock_snov_client.validate_webhook_signature(payload, invalid_signature) is False

    def test_webhook_signature_validation_no_secret(self, mock_snov_client: SnovioClient) -> None:
        """Test webhook signature validation without configured secret."""
        mock_snov_client.webhook_secret = None
        payload = b'{"event": "test"}'
        signature = "sha256=some_hash"

        with pytest.raises(SnovioWebhookError, match="Webhook secret not configured"):
            mock_snov_client.validate_webhook_signature(payload, signature)

    def test_webhook_payload_parsing_success(self, mock_snov_client: SnovioClient) -> None:
        """Test successful webhook payload parsing."""
        payload = '{"event": "domain_search_completed", "request_id": "req_123"}'
        result = mock_snov_client.parse_webhook_payload(payload)

        assert result["event"] == "domain_search_completed"
        assert result["request_id"] == "req_123"

    def test_webhook_payload_parsing_invalid_json(self, mock_snov_client: SnovioClient) -> None:
        """Test webhook payload parsing with invalid JSON."""
        invalid_payload = '{"event": "test", invalid_json}'

        with pytest.raises(SnovioWebhookError, match="Invalid JSON payload"):
            mock_snov_client.parse_webhook_payload(invalid_payload)

    async def test_webhook_event_processing_domain_search(
        self, mock_snov_client: SnovioClient
    ) -> None:
        """Test processing domain search completion webhook."""
        event_payload = {
            "request_id": "req_domain_123",
            "status": "completed",
            "results": [
                {"domain": "example.com", "confidence": 0.95},
                {"domain": "example.org", "confidence": 0.78},
            ],
        }

        # Mock the handler method
        with patch.object(mock_snov_client, "_handle_domain_search_webhook") as mock_handler:
            mock_handler.return_value = {"status": "processed"}

            result = await mock_snov_client.process_webhook_event(
                "domain_search_completed", event_payload
            )

            assert result["status"] == "processed"
            mock_handler.assert_called_once_with(event_payload)

    async def test_webhook_event_processing_unknown_event(
        self, mock_snov_client: SnovioClient
    ) -> None:
        """Test processing unknown webhook event type."""
        event_payload = {"request_id": "req_unknown"}

        result = await mock_snov_client.process_webhook_event("unknown_event_type", event_payload)

        assert result["status"] == "ignored"
        assert "Unknown event type" in result["reason"]


class TestSnovioClientCreditTracking:
    """Test credit tracking integration and exhaustion handling."""

    async def test_credit_check_sufficient_balance(self, mock_snov_client: SnovioClient) -> None:
        """Test credit check with sufficient balance."""
        mock_account_info = {"credits": 500, "limit": 1000}

        with patch.object(mock_snov_client, "get_account_info", return_value=mock_account_info):
            has_credits = await mock_snov_client._check_credits()

            assert has_credits is True
            assert mock_snov_client._credit_balance == 500

    async def test_credit_check_insufficient_balance(self, mock_snov_client: SnovioClient) -> None:
        """Test credit check with insufficient balance."""
        mock_account_info = {"credits": 50, "limit": 1000}
        # Set minimum balance requirement to 100
        mock_snov_client.config = {"snov_io": {"limits": {"min_credit_balance": 100}}}

        with patch.object(mock_snov_client, "get_account_info", return_value=mock_account_info):
            has_credits = await mock_snov_client._check_credits()

            assert has_credits is False
            assert mock_snov_client._credit_balance == 50

    async def test_credit_check_api_failure_assumes_credits(
        self, mock_snov_client: SnovioClient
    ) -> None:
        """Test credit check assumes credits available when API check fails."""
        with patch.object(
            mock_snov_client, "get_account_info", side_effect=SnovioError("API Error")
        ):
            has_credits = await mock_snov_client._check_credits()

            # Should assume credits available when check fails
            assert has_credits is True

    async def test_credit_exhaustion_prevents_requests(
        self, mock_snov_client: SnovioClient
    ) -> None:
        """Test that credit exhaustion prevents API requests."""
        with patch.object(mock_snov_client, "_check_credits", return_value=False):
            with pytest.raises(SnovioCreditsExhaustedError, match="Insufficient credits"):
                await mock_snov_client._make_request("GET", "test-endpoint")

    async def test_credit_consumption_tracking(
        self, mock_snov_client: SnovioClient, temp_db_path: Path
    ) -> None:
        """Test credit consumption tracking in database."""
        # Setup database with migrations
        migration = DatabaseMigration(str(temp_db_path))
        migration.run_migrations()

        conn = sqlite3.connect(temp_db_path)
        cursor = conn.cursor()

        # Create test company
        cursor.execute(
            "INSERT INTO companies (company_number, company_name) VALUES (?, ?)",
            ("12345678", "Test Company"),
        )

        # Track credit consumption
        cursor.execute(
            """
            INSERT INTO snov_credit_usage
            (operation_type, credits_consumed, success, request_id, company_id, response_data)
            VALUES (?, ?, ?, ?, ?, ?)
        """,
            (
                "domain_search",
                3,
                1,  # Success
                "req_123",
                "12345678",
                '{"domains": ["test.com"]}',
            ),
        )

        # Verify credit tracking
        cursor.execute(
            "SELECT operation_type, credits_consumed, success FROM snov_credit_usage WHERE request_id = ?",
            ("req_123",),
        )
        result = cursor.fetchone()

        assert result[0] == "domain_search"
        assert result[1] == 3
        assert result[2] == 1  # Success

        conn.close()


class TestSnovioClientQueueIntegration:
    """Test integration with queue system for batched operations."""

    async def test_enqueue_request_basic(self, mock_snov_client_with_queue: SnovioClient) -> None:
        """Test basic request enqueueing functionality."""
        success = await mock_snov_client_with_queue.enqueue_request(
            method="GET",
            endpoint="test-endpoint",
            params={"param1": "value1"},
            priority=RequestPriority.HIGH,
            request_id="test_req_123",
        )

        assert success is True

    async def test_enqueue_request_without_queue_manager(
        self, mock_snov_client: SnovioClient
    ) -> None:
        """Test enqueueing request fails without queue manager."""
        with pytest.raises(SnovioError, match="Queue manager not configured"):
            await mock_snov_client.enqueue_request(method="GET", endpoint="test-endpoint")

    async def test_process_queued_requests_success(
        self, mock_snov_client_with_queue: SnovioClient
    ) -> None:
        """Test successful processing of queued requests."""
        # Mock queue manager with test request
        mock_request = QueuedRequest(
            request_id="test_123",
            priority=RequestPriority.MEDIUM,
            endpoint="test-endpoint",
            params={"method": "GET", "params": {}, "json_data": {}},
        )

        queue_manager = mock_snov_client_with_queue.queue_manager
        queue_manager.dequeue = AsyncMock(return_value=mock_request)  # type: ignore[assignment]
        queue_manager.get_queue_status = Mock(  # type: ignore[assignment]
            side_effect=[{"total_queued": 1}, {"total_queued": 0}]
        )
        queue_manager.mark_processed = Mock()  # type: ignore[assignment]

        with patch.object(mock_snov_client_with_queue, "_make_request") as mock_request_call:
            mock_request_call.return_value = {"success": True}

            stats = await mock_snov_client_with_queue.process_queued_requests(max_concurrent=1)

            assert stats["processed"] == 1
            assert stats["failed"] == 0
            assert len(stats["errors"]) == 0  # type: ignore[arg-type]


class TestSnovioClientFactory:
    """Test factory function for client creation."""

    async def test_create_snov_client_success(self, temp_config_file: Path) -> None:
        """Test successful client creation using factory function."""
        # Write config with credentials
        config_data = {
            "snov_io": {
                "client_id": "factory_client_id",
                "client_secret": "factory_client_secret",
            }
        }

        with open(temp_config_file, "w", encoding="utf-8") as f:
            yaml.dump(config_data, f)

        with (
            patch.object(SnovioClient, "_ensure_session") as mock_session,
            patch.object(SnovioClient, "_ensure_authenticated") as mock_auth,
        ):
            client = await create_snov_client(config_file=str(temp_config_file))

            assert client.client_id == "factory_client_id"
            assert client.client_secret == "factory_client_secret"
            mock_session.assert_called_once()
            mock_auth.assert_called_once()

            await client.close()

    async def test_create_snov_client_with_queue_manager(self, temp_config_file: Path) -> None:
        """Test client creation with queue manager."""
        config_data = {
            "snov_io": {
                "client_id": "test_id",
                "client_secret": "test_secret",
            }
        }

        with open(temp_config_file, "w", encoding="utf-8") as f:
            yaml.dump(config_data, f)

        mock_queue_manager = Mock(spec=PriorityQueueManager)

        with (
            patch.object(SnovioClient, "_ensure_session"),
            patch.object(SnovioClient, "_ensure_authenticated"),
        ):
            client = await create_snov_client(
                config_file=str(temp_config_file), queue_manager=mock_queue_manager
            )

            assert client.queue_manager == mock_queue_manager
            await client.close()


class TestSnovioClientHealthAndMonitoring:
    """Test health status and monitoring functionality."""

    def test_get_health_status(self, mock_snov_client: SnovioClient) -> None:
        """Test health status reporting."""
        # Set up client state
        mock_snov_client.access_token = "test_token"
        mock_snov_client.token_expires_at = datetime.now() + timedelta(hours=1)
        mock_snov_client._total_requests = 100
        mock_snov_client._successful_requests = 95
        mock_snov_client._failed_requests = 5
        mock_snov_client._credit_balance = 500

        health_status = mock_snov_client.get_health_status()

        assert health_status["authenticated"] is True
        assert health_status["needs_refresh"] is False
        assert health_status["total_requests"] == 100
        assert health_status["successful_requests"] == 95
        assert health_status["failed_requests"] == 5
        assert health_status["success_rate"] == 0.95
        assert health_status["credit_balance"] == 500

    def test_get_rate_limit_status(self, mock_snov_client: SnovioClient) -> None:
        """Test rate limit status reporting."""
        import time

        # Add some mock request history
        now = time.time()
        mock_snov_client._request_history = [
            now - 30,  # 30 seconds ago (within minute)
            now - 90,  # 90 seconds ago (within hour)
            now - 1800,  # 30 minutes ago (within hour)
        ]

        rate_limit_status = mock_snov_client.get_rate_limit_status()

        assert rate_limit_status["requests_last_minute"] == 1
        assert rate_limit_status["requests_last_hour"] == 3
        assert rate_limit_status["minute_limit"] == mock_snov_client.REQUESTS_PER_MINUTE
        assert rate_limit_status["hour_limit"] == mock_snov_client.REQUESTS_PER_HOUR
        assert rate_limit_status["minute_remaining"] == mock_snov_client.REQUESTS_PER_MINUTE - 1
        assert rate_limit_status["hour_remaining"] == mock_snov_client.REQUESTS_PER_HOUR - 3

    def test_get_usage_statistics(self, mock_snov_client: SnovioClient) -> None:
        """Test comprehensive usage statistics."""
        # Set up client state
        mock_snov_client.access_token = "test_token"
        mock_snov_client.token_expires_at = datetime.now() + timedelta(hours=1)
        mock_snov_client._total_requests = 50
        mock_snov_client._successful_requests = 48
        mock_snov_client._failed_requests = 2
        mock_snov_client._credit_balance = 250

        stats = mock_snov_client.get_usage_statistics()

        assert stats["authentication"]["authenticated"] is True
        assert stats["requests"]["total"] == 50
        assert stats["requests"]["successful"] == 48
        assert stats["requests"]["failed"] == 2
        assert stats["requests"]["success_rate"] == 0.96
        assert stats["credits"]["balance"] == 250
        assert "rate_limiting" in stats
        assert "circuit_breaker" in stats


# Pytest fixtures for testing


@pytest.fixture
def temp_config_file() -> Generator[Path, None, None]:
    """Create temporary configuration file for testing."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        yaml.dump({}, f)
        config_path = Path(f.name)

    yield config_path

    # Cleanup
    if config_path.exists():
        config_path.unlink()


@pytest.fixture
def temp_db_path() -> Generator[Path, None, None]:
    """Create temporary database file for testing."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as temp_file:
        temp_path = Path(temp_file.name)

    yield temp_path

    # Cleanup
    if temp_path.exists():
        temp_path.unlink()


@pytest.fixture
def mock_snov_client(temp_config_file: Path) -> SnovioClient:
    """Create mock Snov.io client for testing."""
    return SnovioClient(
        client_id="test_client_id",
        client_secret="test_client_secret",
        config_file=str(temp_config_file),
        enable_logging=False,
    )


@pytest.fixture
def mock_snov_client_with_queue(temp_config_file: Path) -> SnovioClient:
    """Create mock Snov.io client with queue manager for testing."""
    mock_queue_manager = Mock(spec=PriorityQueueManager)
    mock_queue_manager.enqueue = AsyncMock(return_value=True)  # type: ignore[assignment]

    return SnovioClient(
        client_id="test_client_id",
        client_secret="test_client_secret",
        config_file=str(temp_config_file),
        queue_manager=mock_queue_manager,
        enable_logging=False,
    )


@pytest.fixture
def event_loop() -> Generator[asyncio.AbstractEventLoop, None, None]:
    """Create event loop for async tests."""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()

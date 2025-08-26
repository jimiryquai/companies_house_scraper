"""Test emergency safeguards for API call protection."""

import os
from unittest.mock import patch

import pytest

from src.streaming.safeguards import (
    DirectAPICallError,
    api_health_check,
    check_emergency_shutdown,
    emergency_shutdown,
    get_company_officers,
    get_company_profile,
    initialize_safeguards,
    require_queue_system,
)


class TestEmergencySafeguards:
    """Test emergency safeguards functionality."""

    def setup_method(self) -> None:
        """Set up test environment."""
        # Clear any existing emergency shutdown state
        if "COMPANIES_HOUSE_API_EMERGENCY_SHUTDOWN" in os.environ:
            del os.environ["COMPANIES_HOUSE_API_EMERGENCY_SHUTDOWN"]

        # Reset safeguards to default state
        initialize_safeguards(queue_active=True)

    def test_direct_api_call_blocked_when_queue_active(self) -> None:
        """Test that direct API calls are blocked when queue system is active."""
        initialize_safeguards(queue_active=True)

        # Direct calls should be blocked
        with pytest.raises(DirectAPICallError) as exc_info:
            get_company_profile("12345678")

        assert "Direct API call to get_company_profile is forbidden" in str(exc_info.value)
        assert "PriorityQueueManager" in str(exc_info.value)

    def test_direct_api_call_allowed_when_queue_inactive(self) -> None:
        """Test that direct API calls work when queue system is disabled."""
        initialize_safeguards(queue_active=False)

        # Direct calls should work
        result = get_company_profile("12345678")
        assert result["company_number"] == "12345678"

    def test_queue_system_calls_allowed(self) -> None:
        """Test that calls from queue system are allowed even when safeguards active."""
        initialize_safeguards(queue_active=True)

        # Mock being called from queue manager
        with patch("inspect.currentframe") as mock_frame:
            # Create mock frame structure
            mock_queue_frame = type(
                "Frame",
                (),
                {
                    "f_back": None,
                    "f_code": type(
                        "Code",
                        (),
                        {"co_filename": "/path/to/queue_manager.py", "co_name": "_execute_request"},
                    )(),
                },
            )()

            mock_current_frame = type(
                "Frame",
                (),
                {
                    "f_back": mock_queue_frame,
                    "f_code": type(
                        "Code",
                        (),
                        {"co_filename": "/path/to/current.py", "co_name": "current_function"},
                    )(),
                },
            )()

            mock_frame.return_value = mock_current_frame

            # This should work because it's called from queue manager
            result = get_company_profile("12345678")
            assert result["company_number"] == "12345678"

    def test_test_calls_allowed(self) -> None:
        """Test that calls from test files are allowed."""
        initialize_safeguards(queue_active=True)

        # Mock being called from test file
        with patch("inspect.currentframe") as mock_frame:
            mock_test_frame = type(
                "Frame",
                (),
                {
                    "f_back": None,
                    "f_code": type(
                        "Code",
                        (),
                        {"co_filename": "/path/to/test_something.py", "co_name": "test_function"},
                    )(),
                },
            )()

            mock_current_frame = type(
                "Frame",
                (),
                {
                    "f_back": mock_test_frame,
                    "f_code": type(
                        "Code",
                        (),
                        {"co_filename": "/path/to/current.py", "co_name": "current_function"},
                    )(),
                },
            )()

            mock_frame.return_value = mock_current_frame

            # This should work because it's called from test
            result = get_company_officers("12345678")
            assert len(result) == 1
            assert result[0]["name"] == "Test Officer"

    def test_emergency_shutdown(self) -> None:
        """Test emergency shutdown functionality."""
        initialize_safeguards(queue_active=True)

        # Initially should not be in emergency state
        assert not check_emergency_shutdown()

        # Trigger emergency shutdown
        emergency_shutdown()

        # Should now be in emergency state
        assert check_emergency_shutdown()
        assert os.environ.get("COMPANIES_HOUSE_API_EMERGENCY_SHUTDOWN") == "true"

        # API calls should now work (safeguards disabled)
        result = get_company_profile("12345678")
        assert result["company_number"] == "12345678"

    def test_api_health_check(self) -> None:
        """Test API health check functionality."""
        initialize_safeguards(queue_active=True)

        health = api_health_check()

        assert health["queue_system_active"] is True
        assert health["emergency_shutdown"] is False
        assert health["safeguards_enabled"] is True
        assert "timestamp" in health

    def test_api_health_check_emergency_state(self) -> None:
        """Test API health check during emergency shutdown."""
        emergency_shutdown()

        health = api_health_check()

        assert health["queue_system_active"] is False
        assert health["emergency_shutdown"] is True
        assert health["safeguards_enabled"] is True

    def test_require_queue_system_decorator_preserves_function_metadata(self) -> None:
        """Test that the decorator preserves function metadata."""

        @require_queue_system
        def test_function(param: str) -> str:
            """Test docstring."""
            return f"result: {param}"

        assert test_function.__name__ == "test_function"
        assert "Test docstring" in test_function.__doc__

    def test_multiple_api_calls_blocked(self) -> None:
        """Test that multiple different API calls are blocked."""
        initialize_safeguards(queue_active=True)

        # Both API functions should be blocked
        with pytest.raises(DirectAPICallError):
            get_company_profile("12345678")

        with pytest.raises(DirectAPICallError):
            get_company_officers("87654321")

    def teardown_method(self) -> None:
        """Clean up after each test."""
        # Clear emergency shutdown state
        if "COMPANIES_HOUSE_API_EMERGENCY_SHUTDOWN" in os.environ:
            del os.environ["COMPANIES_HOUSE_API_EMERGENCY_SHUTDOWN"]

        # Reset to default state
        initialize_safeguards(queue_active=True)

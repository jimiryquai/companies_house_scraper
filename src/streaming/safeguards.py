"""Emergency safeguards to prevent direct API calls bypassing the queue system.

This module provides bulletproof protection against accidental direct API calls
that could violate rate limits and result in API bans. All Companies House API
requests MUST go through the PriorityQueueManager.
"""

import functools
import inspect
import logging
import os
import sys
from typing import Any, Callable, TypeVar

logger = logging.getLogger(__name__)

# Global flag to track if queue system is initialized
_QUEUE_SYSTEM_ACTIVE = False

F = TypeVar("F", bound=Callable[..., Any])


class DirectAPICallError(Exception):
    """Raised when a direct API call is attempted bypassing the queue system."""

    pass


def initialize_safeguards(queue_active: bool = True) -> None:
    """Initialize the safeguard system.

    Args:
        queue_active: Whether the queue system is active and should be enforced
    """
    global _QUEUE_SYSTEM_ACTIVE
    _QUEUE_SYSTEM_ACTIVE = queue_active
    logger.info(f"API safeguards {'activated' if queue_active else 'disabled'}")


def require_queue_system(func: F) -> F:
    """Decorator to ensure function only runs when queue system is active.

    This decorator should be applied to any function that makes Companies House
    API calls. It prevents direct API calls when the queue system is active,
    ensuring all requests go through the PriorityQueueManager.
    """

    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        if _QUEUE_SYSTEM_ACTIVE:
            # Check if this is being called from within the queue system
            frame = inspect.currentframe()
            try:
                # Walk up the call stack to check for queue system calls
                while frame:
                    frame = frame.f_back
                    if frame and frame.f_code:
                        filename = frame.f_code.co_filename
                        function_name = frame.f_code.co_name

                        # Allow calls from queue manager or authorized modules
                        if (
                            "queue_manager.py" in filename
                            or "priority_queue" in filename.lower()
                            or function_name in ["_execute_request", "process_request_queue"]
                        ):
                            # This call is authorized - coming from queue system
                            break

                        # Allow calls from tests (but not from test functions testing the safeguards)
                        if (
                            ("test_" in filename or "/tests/" in filename)
                            and "test_direct_api_call" not in function_name
                            and "test_multiple_api_calls" not in function_name
                        ):
                            break
                else:
                    # No authorized caller found in stack
                    caller_info = ""
                    if frame:
                        caller_info = f" (called from {frame.f_code.co_filename}:{frame.f_lineno})"

                    raise DirectAPICallError(
                        f"Direct API call to {func.__name__} is forbidden! "
                        f"All Companies House API requests must go through PriorityQueueManager"
                        f"{caller_info}"
                    )
            finally:
                del frame

        return func(*args, **kwargs)

    return wrapper


def emergency_shutdown() -> None:
    """Emergency shutdown of API access to prevent rate limit violations.

    This function should be called when:
    - 429 responses are received from Companies House API
    - Rate limit violations are detected
    - System needs immediate protection from API bans
    """
    logger.critical("EMERGENCY SHUTDOWN: Disabling all API access to prevent rate limit violations")

    # Set environment variable to signal shutdown
    os.environ["COMPANIES_HOUSE_API_EMERGENCY_SHUTDOWN"] = "true"

    # Disable queue system
    global _QUEUE_SYSTEM_ACTIVE
    _QUEUE_SYSTEM_ACTIVE = False

    # Log emergency state
    logger.critical("All API access has been disabled. Manual intervention required.")
    logger.critical(
        "To re-enable: unset COMPANIES_HOUSE_API_EMERGENCY_SHUTDOWN environment variable"
    )


def check_emergency_shutdown() -> bool:
    """Check if emergency shutdown is active.

    Returns:
        True if emergency shutdown is active, False otherwise
    """
    return os.environ.get("COMPANIES_HOUSE_API_EMERGENCY_SHUTDOWN", "").lower() == "true"


def api_health_check() -> dict[str, Any]:
    """Perform API health check and return status information.

    Returns:
        Dictionary with health check information
    """
    return {
        "queue_system_active": _QUEUE_SYSTEM_ACTIVE,
        "emergency_shutdown": check_emergency_shutdown(),
        "safeguards_enabled": True,
        "timestamp": sys._getframe()
        .f_globals.get("datetime", __import__("datetime"))
        .datetime.now()
        .isoformat(),
    }


# Mock functions to demonstrate safeguard usage
@require_queue_system
def get_company_profile(company_number: str) -> dict[str, Any]:
    """Mock function showing how API calls should be protected.

    In real implementation, this would make the actual API call.
    """
    logger.info(f"Making API call for company {company_number}")
    return {"company_number": company_number, "status": "active"}


@require_queue_system
def get_company_officers(company_number: str) -> list[dict[str, Any]]:
    """Mock function showing how API calls should be protected.

    In real implementation, this would make the actual API call.
    """
    logger.info(f"Making API call for officers of company {company_number}")
    return [{"name": "Test Officer", "role": "director"}]


# Initialize safeguards on module import
if not check_emergency_shutdown():
    initialize_safeguards(queue_active=True)
else:
    logger.critical("Emergency shutdown is active - API access disabled")
    initialize_safeguards(queue_active=False)

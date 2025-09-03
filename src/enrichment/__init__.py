"""Data enrichment module for Snov.io integration.

This module provides domain discovery and email finding capabilities
for companies and officers using Snov.io's API services.
"""

# Import basic modules that don't have cross-dependencies
from .snov_client import SnovioClient, SnovioError

# Lazy import for modules with dependencies to avoid circular imports during testing
__all__ = [
    "SnovioClient", 
    "SnovioError",
    "DomainManager",
]

def __getattr__(name):
    """Lazy import for modules with dependencies."""
    if name == "DomainManager":
        from .domain_manager import DomainManager
        return DomainManager
    raise AttributeError(f"module '{__name__}' has no attribute '{name}'")

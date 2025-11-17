"""Airtable integration module for Companies House data.

This module provides classes and utilities for storing Companies House
data in Airtable, including companies and officers.
"""

from .client import AirtableClient
from .companies import CompaniesTable
from .config import AirtableConfig
from .officers import OfficersTable

__all__ = [
    "AirtableClient",
    "AirtableConfig",
    "CompaniesTable",
    "OfficersTable",
]

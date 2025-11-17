"""PostgreSQL database module for Companies House data."""

from .client import Database
from .companies import CompaniesTable
from .officers import OfficersTable

__all__ = [
    "Database",
    "CompaniesTable",
    "OfficersTable",
]

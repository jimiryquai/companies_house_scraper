"""Airtable configuration management.

This module manages Airtable API credentials and table configuration
for the Companies House integration.
"""

import os
from dataclasses import dataclass


@dataclass
class AirtableConfig:
    """Configuration for Airtable API access.

    Attributes:
        api_key: Airtable personal access token
        base_id: Airtable base identifier
        companies_table: Name or ID of companies table
        officers_table: Name or ID of officers table
    """

    api_key: str
    base_id: str
    companies_table: str
    officers_table: str

    @classmethod
    def from_env(cls) -> "AirtableConfig":
        """Load configuration from environment variables.

        Returns:
            AirtableConfig: Configuration loaded from environment

        Raises:
            ValueError: If required environment variables are missing
        """
        api_key = os.getenv("AIRTABLE_API_KEY")
        base_id = os.getenv("AIRTABLE_BASE_ID")
        companies_table = os.getenv("AIRTABLE_COMPANIES_TABLE", "Companies")
        officers_table = os.getenv("AIRTABLE_OFFICERS_TABLE", "Officers")

        if not api_key:
            raise ValueError("AIRTABLE_API_KEY environment variable is required")
        if not base_id:
            raise ValueError("AIRTABLE_BASE_ID environment variable is required")

        return cls(
            api_key=api_key,
            base_id=base_id,
            companies_table=companies_table,
            officers_table=officers_table,
        )

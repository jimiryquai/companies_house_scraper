"""Airtable API client with rate limiting.

This module provides a robust client for interacting with the Airtable API,
including automatic rate limiting and error handling.
"""

import logging
import time
from typing import Any

from pyairtable import Api, Table
from pyairtable.formulas import match

from .config import AirtableConfig

logger = logging.getLogger(__name__)


class AirtableClient:
    """Airtable API client with rate limiting and error handling.

    This client manages connections to Airtable and provides methods
    for upserting records with automatic rate limiting.

    Attributes:
        config: Airtable configuration
        api: Airtable API instance
        base: Airtable base instance
    """

    # Airtable rate limit: 5 requests per second per base
    RATE_LIMIT_DELAY = 0.2  # 200ms between requests

    def __init__(self, config: AirtableConfig) -> None:
        """Initialize Airtable client.

        Args:
            config: Airtable configuration with credentials
        """
        self.config = config
        self.api = Api(config.api_key)
        self.base = self.api.base(config.base_id)
        self._last_request_time = 0.0

    def _rate_limit(self) -> None:
        """Enforce rate limiting between requests.

        Ensures minimum delay between API calls to respect Airtable's
        rate limits (5 requests per second per base).
        """
        elapsed = time.time() - self._last_request_time
        if elapsed < self.RATE_LIMIT_DELAY:
            time.sleep(self.RATE_LIMIT_DELAY - elapsed)
        self._last_request_time = time.time()

    def get_table(self, table_name: str) -> Table:
        """Get a table instance from the base.

        Args:
            table_name: Name or ID of the table

        Returns:
            Table: Airtable table instance
        """
        return self.base.table(table_name)

    def upsert_record(
        self,
        table_name: str,
        key_field: str,
        key_value: str,
        fields: dict[str, Any],
    ) -> dict[str, Any]:
        """Create or update a record in Airtable.

        Uses the key field to find existing records. If found, updates
        the record; otherwise creates a new one.

        Args:
            table_name: Name of the table to upsert into
            key_field: Field name to use as unique identifier
            key_value: Value of the key field
            fields: Dictionary of field values to set

        Returns:
            Dict containing the created/updated record

        Raises:
            Exception: If Airtable API returns an error
        """
        self._rate_limit()
        table = self.get_table(table_name)

        try:
            # Search for existing record
            formula = match({key_field: key_value})
            existing = table.first(formula=formula)

            if existing:
                # Update existing record
                logger.debug(f"Updating {table_name} record: {key_value}")
                record = table.update(existing["id"], fields)
            else:
                # Create new record
                logger.debug(f"Creating {table_name} record: {key_value}")
                # Ensure key field is in the fields dict
                fields[key_field] = key_value
                record = table.create(fields)

            return dict(record)

        except Exception as e:
            logger.error(f"Error upserting {table_name} record {key_value}: {e}")
            raise

    def batch_upsert(
        self,
        table_name: str,
        key_field: str,
        records: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Upsert multiple records in batch.

        Args:
            table_name: Name of the table
            key_field: Field name to use as unique identifier
            records: List of record field dictionaries

        Returns:
            List of created/updated records
        """
        results = []
        for record_fields in records:
            key_value = record_fields.get(key_field)
            if not key_value:
                logger.warning(f"Skipping record without {key_field}: {record_fields}")
                continue

            result = self.upsert_record(table_name, key_field, key_value, record_fields)
            results.append(result)

        return results

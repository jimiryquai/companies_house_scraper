"""Officers table operations for Airtable.

This module handles creation and updates of officer records in Airtable
from Companies House API data.
"""

import logging
from typing import Any

from .client import AirtableClient

logger = logging.getLogger(__name__)


class OfficersTable:
    """Manages officer records in Airtable.

    Provides methods to create and update officer records from
    Companies House API responses.

    Attributes:
        client: Airtable API client
        table_name: Name of the officers table
    """

    def __init__(self, client: AirtableClient) -> None:
        """Initialize officers table manager.

        Args:
            client: Configured Airtable client
        """
        self.client = client
        self.table_name = client.config.officers_table

    def upsert_officers(
        self, company_number: str, officers_data: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """Create or update multiple officer records for a company.

        Args:
            company_number: Company number the officers belong to
            officers_data: List of officer data from CH API

        Returns:
            List of created/updated Airtable records
        """
        logger.info(f"Upserting {len(officers_data)} officers for company {company_number}")

        results = []
        for officer in officers_data:
            try:
                result = self.upsert_officer(company_number, officer)
                results.append(result)
            except Exception as e:
                logger.error(f"Failed to upsert officer for {company_number}: {e}")
                # Continue with other officers even if one fails

        return results

    def upsert_officer(self, company_number: str, officer_data: dict[str, Any]) -> dict[str, Any]:
        """Create or update a single officer record.

        Args:
            company_number: Company number the officer belongs to
            officer_data: Officer data from CH API

        Returns:
            Dict containing the created/updated Airtable record

        Raises:
            ValueError: If required fields are missing
            Exception: If Airtable API returns an error
        """
        # Generate unique key from company number and officer links
        if "links" not in officer_data or "officer" not in officer_data["links"]:
            raise ValueError("Officer links data is required")

        officer_id = officer_data["links"]["officer"].get("appointments", "")
        if not officer_id:
            raise ValueError("Officer appointment link is required")

        # Use officer appointment link as unique identifier
        unique_key = f"{company_number}_{officer_id}"

        # Transform officer data to Airtable fields
        fields = self._transform_officer_data(company_number, officer_data)

        return self.client.upsert_record(
            table_name=self.table_name,
            key_field="unique_key",
            key_value=unique_key,
            fields=fields,
        )

    def _transform_officer_data(
        self, company_number: str, officer_data: dict[str, Any]
    ) -> dict[str, Any]:
        """Transform Companies House API officer data to Airtable format.

        Args:
            company_number: Company number for linking
            officer_data: Raw officer data from CH API

        Returns:
            Dict of Airtable field names to values
        """
        fields: dict[str, Any] = {}

        # Link to company
        fields["company_number"] = company_number

        # Officer identification
        fields["name"] = officer_data.get("name", "")
        officer_id = officer_data.get("links", {}).get("officer", {}).get("appointments", "")
        fields["unique_key"] = f"{company_number}_{officer_id}"

        # Officer role
        fields["officer_role"] = officer_data.get("officer_role", "")

        # Appointment dates
        if "appointed_on" in officer_data:
            fields["appointed_on"] = officer_data["appointed_on"]
        if "resigned_on" in officer_data:
            fields["resigned_on"] = officer_data["resigned_on"]

        # Officer details
        if "nationality" in officer_data:
            fields["nationality"] = officer_data["nationality"]
        if "occupation" in officer_data:
            fields["occupation"] = officer_data["occupation"]
        if "country_of_residence" in officer_data:
            fields["country_of_residence"] = officer_data["country_of_residence"]

        # Date of birth (partial)
        if "date_of_birth" in officer_data:
            dob = officer_data["date_of_birth"]
            if "month" in dob and "year" in dob:
                fields["date_of_birth"] = f"{dob['year']}-{dob['month']:02d}"

        # Address
        if "address" in officer_data:
            address = officer_data["address"]
            fields["address"] = self._format_address(address)
            if "postal_code" in address:
                fields["postal_code"] = address["postal_code"]

        return fields

    def _format_address(self, address: dict[str, Any]) -> str:
        """Format address dictionary as single line string.

        Args:
            address: Address dictionary from CH API

        Returns:
            Formatted address string
        """
        parts = []
        for key in [
            "premises",
            "address_line_1",
            "address_line_2",
            "locality",
            "region",
            "postal_code",
            "country",
        ]:
            if key in address and address[key]:
                parts.append(address[key])
        return ", ".join(parts)

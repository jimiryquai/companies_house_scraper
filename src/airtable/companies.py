"""Companies table operations for Airtable.

This module handles creation and updates of company records in Airtable
from Companies House API data.
"""

import logging
from typing import Any

from .client import AirtableClient

logger = logging.getLogger(__name__)


class CompaniesTable:
    """Manages company records in Airtable.

    Provides methods to create and update company records from
    Companies House API responses.

    Attributes:
        client: Airtable API client
        table_name: Name of the companies table
    """

    def __init__(self, client: AirtableClient) -> None:
        """Initialize companies table manager.

        Args:
            client: Configured Airtable client
        """
        self.client = client
        self.table_name = client.config.companies_table

    def upsert_company(self, company_data: dict[str, Any]) -> dict[str, Any]:
        """Create or update a company record in Airtable.

        Args:
            company_data: Company data from Companies House API

        Returns:
            Dict containing the created/updated Airtable record

        Raises:
            ValueError: If company_number is missing from data
            Exception: If Airtable API returns an error
        """
        company_number = company_data.get("company_number")
        if not company_number:
            raise ValueError("company_number is required")

        # Transform Companies House API data to Airtable fields
        fields = self._transform_company_data(company_data)

        logger.info(f"Upserting company: {company_number}")
        return self.client.upsert_record(
            table_name=self.table_name,
            key_field="company_number",
            key_value=company_number,
            fields=fields,
        )

    def _transform_company_data(self, company_data: dict[str, Any]) -> dict[str, Any]:
        """Transform Companies House API data to Airtable field format.

        Maps Companies House API response fields to Airtable table schema.

        Args:
            company_data: Raw company data from CH API

        Returns:
            Dict of Airtable field names to values
        """
        fields: dict[str, Any] = {}

        # Core company identifiers
        fields["company_number"] = company_data.get("company_number", "")
        fields["company_name"] = company_data.get("company_name", "")
        fields["company_status"] = company_data.get("company_status", "")

        # Company type and jurisdiction
        if "type" in company_data:
            fields["company_type"] = company_data["type"]
        if "jurisdiction" in company_data:
            fields["jurisdiction"] = company_data["jurisdiction"]

        # Dates
        if "date_of_creation" in company_data:
            fields["date_of_creation"] = company_data["date_of_creation"]
        if "date_of_cessation" in company_data:
            fields["date_of_cessation"] = company_data["date_of_cessation"]

        # Registered office address
        if "registered_office_address" in company_data:
            address = company_data["registered_office_address"]
            fields["registered_office_address"] = self._format_address(address)
            if "postal_code" in address:
                fields["postal_code"] = address["postal_code"]
            if "locality" in address:
                fields["locality"] = address["locality"]

        # SIC codes
        if "sic_codes" in company_data:
            fields["sic_codes"] = ", ".join(company_data["sic_codes"])

        # Company links
        if "links" in company_data and "self" in company_data["links"]:
            fields["ch_url"] = f"https://beta.companieshouse.gov.uk{company_data['links']['self']}"

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

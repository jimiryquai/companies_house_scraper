"""Officers table operations for PostgreSQL.

This module handles creation and updates of officer records in PostgreSQL
from Companies House API data.
"""

import logging
from typing import Any

from .client import Database

logger = logging.getLogger(__name__)


class OfficersTable:
    """Manages officer records in PostgreSQL.

    Provides methods to create and update officer records from
    Companies House API responses using PostgreSQL upsert operations.

    Attributes:
        db: PostgreSQL database client
    """

    def __init__(self, db: Database) -> None:
        """Initialize officers table manager.

        Args:
            db: Configured PostgreSQL database client
        """
        self.db = db

    def upsert_officers(
        self, company_number: str, officers_data: list[dict[str, Any]]
    ) -> list[str]:
        """Create or update multiple officer records for a company.

        Args:
            company_number: Company number the officers belong to
            officers_data: List of officer data from CH API

        Returns:
            List of unique_keys for created/updated records
        """
        logger.info(f"Upserting {len(officers_data)} officers for company {company_number}")

        results = []
        for officer in officers_data:
            try:
                unique_key = self.upsert_officer(company_number, officer)
                results.append(unique_key)
            except Exception as e:
                logger.error(f"Failed to upsert officer for {company_number}: {e}")
                # Continue with other officers even if one fails

        return results

    def upsert_officer(self, company_number: str, officer_data: dict[str, Any]) -> str:
        """Create or update a single officer record.

        Uses INSERT...ON CONFLICT DO UPDATE to handle upserts efficiently.

        Args:
            company_number: Company number the officer belongs to
            officer_data: Officer data from CH API

        Returns:
            Unique key of the upserted officer record

        Raises:
            ValueError: If required fields are missing
            Exception: If database operation fails
        """
        # Generate unique key from company number and officer links
        if "links" not in officer_data or "officer" not in officer_data["links"]:
            raise ValueError("Officer links data is required")

        officer_id = officer_data["links"]["officer"].get("appointments", "")
        if not officer_id:
            raise ValueError("Officer appointment link is required")

        # Use officer appointment link as unique identifier
        unique_key = f"{company_number}_{officer_id}"

        # Transform officer data to database fields
        fields = self._transform_officer_data(company_number, officer_data)

        with self.db.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO officers (
                        unique_key, company_number, name, officer_role,
                        appointed_on, resigned_on, nationality, occupation,
                        country_of_residence, date_of_birth, address, postal_code
                    ) VALUES (
                        %(unique_key)s, %(company_number)s, %(name)s, %(officer_role)s,
                        %(appointed_on)s, %(resigned_on)s, %(nationality)s, %(occupation)s,
                        %(country_of_residence)s, %(date_of_birth)s, %(address)s, %(postal_code)s
                    )
                    ON CONFLICT (unique_key) DO UPDATE SET
                        name = EXCLUDED.name,
                        officer_role = EXCLUDED.officer_role,
                        appointed_on = EXCLUDED.appointed_on,
                        resigned_on = EXCLUDED.resigned_on,
                        nationality = EXCLUDED.nationality,
                        occupation = EXCLUDED.occupation,
                        country_of_residence = EXCLUDED.country_of_residence,
                        date_of_birth = EXCLUDED.date_of_birth,
                        address = EXCLUDED.address,
                        postal_code = EXCLUDED.postal_code
                    """,
                    fields,
                )

        return unique_key

    def _transform_officer_data(
        self, company_number: str, officer_data: dict[str, Any]
    ) -> dict[str, Any]:
        """Transform Companies House API officer data to database format.

        Args:
            company_number: Company number for linking
            officer_data: Raw officer data from CH API

        Returns:
            Dict of database field names to values
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
        fields["appointed_on"] = officer_data.get("appointed_on")
        fields["resigned_on"] = officer_data.get("resigned_on")

        # Officer details
        fields["nationality"] = officer_data.get("nationality")
        fields["occupation"] = officer_data.get("occupation")
        fields["country_of_residence"] = officer_data.get("country_of_residence")

        # Date of birth (partial)
        if "date_of_birth" in officer_data:
            dob = officer_data["date_of_birth"]
            if "month" in dob and "year" in dob:
                fields["date_of_birth"] = f"{dob['year']}-{dob['month']:02d}"
            else:
                fields["date_of_birth"] = None
        else:
            fields["date_of_birth"] = None

        # Address
        if "address" in officer_data:
            address = officer_data["address"]
            fields["address"] = self._format_address(address)
            fields["postal_code"] = address.get("postal_code")
        else:
            fields["address"] = None
            fields["postal_code"] = None

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

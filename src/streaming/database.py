"""Database integration for Companies House Streaming API.

Provides async database operations for storing and retrieving company data,
stream events, and metadata from SQLite database.
"""

import json
import logging
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional

import aiosqlite

from .config import StreamingConfig

logger = logging.getLogger(__name__)


class DatabaseError(Exception):
    """Raised when database operations fail."""

    pass


@dataclass
class CompanyRecord:
    """Represents a company record in the database."""

    company_number: str
    company_name: Optional[str] = None
    company_status: Optional[str] = None
    company_status_detail: Optional[str] = None
    incorporation_date: Optional[str] = None
    sic_codes: Optional[str] = None
    address_line_1: Optional[str] = None
    address_line_2: Optional[str] = None
    locality: Optional[str] = None
    region: Optional[str] = None
    country: Optional[str] = None
    postal_code: Optional[str] = None
    premises: Optional[str] = None
    stream_last_updated: Optional[str] = None
    stream_status: str = "unknown"
    data_source: str = "bulk"
    last_stream_event_id: Optional[str] = None
    stream_metadata: Optional[dict[str, Any]] = None

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "CompanyRecord":
        """Create CompanyRecord from dictionary."""
        # Handle stream_metadata JSON
        metadata = data.get("stream_metadata")
        if isinstance(metadata, str):
            try:
                metadata = json.loads(metadata)
            except json.JSONDecodeError:
                metadata = None

        company_number = data.get("company_number")
        if not company_number:
            raise ValueError("company_number is required")

        return cls(
            company_number=company_number,
            company_name=data.get("company_name"),
            company_status=data.get("company_status"),
            company_status_detail=data.get("company_status_detail"),
            incorporation_date=data.get("incorporation_date"),
            sic_codes=data.get("sic_codes"),
            address_line_1=data.get("address_line_1"),
            address_line_2=data.get("address_line_2"),
            locality=data.get("locality"),
            region=data.get("region"),
            country=data.get("country"),
            postal_code=data.get("postal_code"),
            premises=data.get("premises"),
            stream_last_updated=data.get("stream_last_updated"),
            stream_status=data.get("stream_status", "unknown"),
            data_source=data.get("data_source", "bulk"),
            last_stream_event_id=data.get("last_stream_event_id"),
            stream_metadata=metadata,
        )

    def to_dict(self) -> dict[str, Any]:
        """Convert CompanyRecord to dictionary for database storage."""
        return {
            "company_number": self.company_number,
            "company_name": self.company_name,
            "company_status": self.company_status,
            "company_status_detail": self.company_status_detail,
            "incorporation_date": self.incorporation_date,
            "sic_codes": self.sic_codes,
            "address_line_1": self.address_line_1,
            "address_line_2": self.address_line_2,
            "locality": self.locality,
            "region": self.region,
            "country": self.country,
            "postal_code": self.postal_code,
            "premises": self.premises,
            "stream_last_updated": self.stream_last_updated,
            "stream_status": self.stream_status,
            "data_source": self.data_source,
            "last_stream_event_id": self.last_stream_event_id,
            "stream_metadata": json.dumps(self.stream_metadata) if self.stream_metadata else None,
        }

    def is_strike_off(self) -> bool:
        """Check if company has strike-off status."""
        if not self.company_status:
            return False

        strike_off_statuses = [
            "active-proposal-to-strike-off",
            "proposal-to-strike-off",
            "struck-off",
        ]

        status_lower = self.company_status.lower().replace(" ", "-")
        return any(status in status_lower for status in strike_off_statuses)


@dataclass
class StreamEventRecord:
    """Represents a stream event record in the database."""

    event_id: str
    event_type: str
    company_number: Optional[str]
    event_data: dict[str, Any]
    processed_at: datetime
    id: Optional[int] = None
    created_at: Optional[datetime] = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for database storage."""
        return {
            "event_id": self.event_id,
            "event_type": self.event_type,
            "company_number": self.company_number,
            "event_data": json.dumps(self.event_data),
            "processed_at": self.processed_at.isoformat() if self.processed_at else None,
        }


class DatabaseManager:
    """Manages database connections and operations.

    Provides connection pooling and transaction management for async database operations.
    """

    def __init__(self, config: StreamingConfig):
        """Initialize database manager."""
        self.db_path = config.database_path
        self.pool_size = 5
        self._pool: list[aiosqlite.Connection] = []
        self._is_initialized = False

    async def connect(self) -> None:
        """Initialize database connection pool."""
        if self._is_initialized:
            return

        try:
            # Create connection pool
            for _ in range(self.pool_size):
                conn = await aiosqlite.connect(self.db_path)
                conn.row_factory = aiosqlite.Row
                self._pool.append(conn)

            self._is_initialized = True
            logger.info(f"Database connection pool initialized with {self.pool_size} connections")

        except Exception as e:
            logger.error(f"Failed to initialize database pool: {e}")
            raise DatabaseError(f"Database connection failed: {e}") from e

    async def disconnect(self) -> None:
        """Close all database connections."""
        for conn in self._pool:
            await conn.close()

        self._pool = []
        self._is_initialized = False
        logger.info("Database connections closed")

    async def __aenter__(self) -> "DatabaseManager":
        """Async context manager entry."""
        await self.connect()
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Async context manager exit."""
        await self.disconnect()

    @asynccontextmanager
    async def get_connection(self) -> Any:
        """Get a connection from the pool."""
        if not self._is_initialized:
            await self.connect()

        if not self._pool:
            raise DatabaseError("No database connections available")

        conn = self._pool.pop()
        try:
            yield conn
        finally:
            self._pool.append(conn)

    @asynccontextmanager
    async def transaction(self) -> Any:
        """Execute operations in a transaction."""
        async with self.get_connection() as conn:
            try:
                await conn.execute("BEGIN")
                yield conn
                await conn.commit()
            except Exception as e:
                await conn.rollback()
                logger.error(f"Transaction rolled back: {e}")
                raise

    async def execute(self, query: str, params: tuple[Any, ...] = ()) -> Any:
        """Execute a single query."""
        async with self.get_connection() as conn:
            cursor = await conn.execute(query, params)
            await conn.commit()
            return cursor

    async def execute_many(self, query: str, params_list: list[tuple[Any, ...]]) -> None:
        """Execute multiple queries with different parameters."""
        async with self.get_connection() as conn:
            await conn.executemany(query, params_list)
            await conn.commit()

    async def fetch_one(self, query: str, params: tuple[Any, ...] = ()) -> Optional[dict[str, Any]]:
        """Fetch a single row."""
        async with self.get_connection() as conn:
            cursor = await conn.execute(query, params)
            row = await cursor.fetchone()
            return dict(row) if row else None

    async def fetch_all(self, query: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
        """Fetch all rows."""
        async with self.get_connection() as conn:
            cursor = await conn.execute(query, params)
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]


class StreamingDatabase:
    """High-level database interface for streaming operations.

    Provides methods for managing company records, stream events, and metadata.
    """

    def __init__(self, config: StreamingConfig):
        """Initialize streaming database."""
        self.config = config
        self.manager = DatabaseManager(config)

    async def connect(self) -> None:
        """Connect to database."""
        await self.manager.connect()

    async def disconnect(self) -> None:
        """Disconnect from database."""
        await self.manager.disconnect()

    async def upsert_company(self, company_data: dict[str, Any]) -> None:
        """Insert or update a company record.

        Args:
            company_data: Company data dictionary
        """
        company_number = company_data.get("company_number")
        if not company_number:
            raise ValueError("Company number is required")

        # Check if company exists
        existing = await self.get_company(company_number)

        if existing:
            # Update existing record
            set_clause = ", ".join(f"{k} = ?" for k in company_data if k != "company_number")
            values = [v for k, v in company_data.items() if k != "company_number"]
            values.append(company_number)

            query = f"UPDATE companies SET {set_clause} WHERE company_number = ?"  # noqa: S608
            await self.manager.execute(query, tuple(values))

            logger.debug(f"Updated company {company_number}")
        else:
            # Insert new record
            columns = list(company_data.keys())
            placeholders = ", ".join("?" * len(columns))
            columns_str = ", ".join(columns)

            query = f"INSERT INTO companies ({columns_str}) VALUES ({placeholders})"  # noqa: S608
            await self.manager.execute(query, tuple(company_data.values()))

            logger.debug(f"Inserted new company {company_number}")

    async def batch_upsert_companies(self, companies: list[dict[str, Any]]) -> None:
        """Batch insert or update multiple companies.

        Args:
            companies: List of company data dictionaries
        """
        for company in companies:
            await self.upsert_company(company)

    async def get_company(self, company_number: str) -> Optional[dict[str, Any]]:
        """Get a company record by company number.

        Args:
            company_number: The company number to lookup

        Returns:
            Company data dictionary or None if not found
        """
        query = "SELECT * FROM companies WHERE company_number = ?"
        return await self.manager.fetch_one(query, (company_number,))

    async def get_companies_by_status(self, status: str) -> list[dict[str, Any]]:
        """Get all companies with a specific status.

        Args:
            status: Company status to filter by

        Returns:
            List of company data dictionaries
        """
        query = "SELECT * FROM companies WHERE company_status = ?"
        return await self.manager.fetch_all(query, (status,))

    async def update_stream_metadata(self, company_number: str, metadata: dict[str, Any]) -> None:
        """Update streaming metadata for a company.

        Args:
            company_number: Company to update
            metadata: Metadata dictionary containing stream information
        """
        query = """
            UPDATE companies
            SET stream_last_updated = ?,
                stream_status = ?,
                last_stream_event_id = ?,
                data_source = ?,
                stream_metadata = ?
            WHERE company_number = ?
        """

        params = (
            datetime.now().isoformat(),
            metadata.get("stream_status", "active"),
            metadata.get("last_event_id"),
            "stream" if metadata.get("data_source") != "both" else "both",
            json.dumps(metadata),
            company_number,
        )

        await self.manager.execute(query, params)
        logger.debug(f"Updated stream metadata for company {company_number}")

    async def log_stream_event(
        self,
        event_id: str,
        event_type: str,
        company_number: Optional[str],
        event_data: dict[str, Any],
    ) -> Optional[int]:
        """Log a stream event to the database.

        Args:
            event_id: Unique event identifier
            event_type: Type of event (e.g., 'company-profile')
            company_number: Related company number
            event_data: Full event data

        Returns:
            Event record ID or None if duplicate
        """
        try:
            query = """
                INSERT INTO stream_events
                (event_id, event_type, company_number, event_data, processed_at)
                VALUES (?, ?, ?, ?, ?)
            """

            params = (
                event_id,
                event_type,
                company_number,
                json.dumps(event_data),
                datetime.now().isoformat(),
            )

            cursor = await self.manager.execute(query, params)
            logger.debug(f"Logged stream event {event_id}")
            return int(cursor.lastrowid) if cursor.lastrowid else None

        except aiosqlite.IntegrityError:
            # Duplicate event ID
            logger.debug(f"Duplicate stream event {event_id}, skipping")
            return None
        except Exception as e:
            logger.error(f"Failed to log stream event: {e}")
            raise DatabaseError(f"Failed to log stream event: {e}") from e

    async def get_stream_event(self, event_id: str) -> Optional[dict[str, Any]]:
        """Get a stream event by ID.

        Args:
            event_id: Event identifier

        Returns:
            Event data dictionary or None if not found
        """
        query = "SELECT * FROM stream_events WHERE event_id = ?"
        return await self.manager.fetch_one(query, (event_id,))

    async def get_stream_stats(self) -> dict[str, Any]:
        """Get streaming statistics.

        Returns:
            Dictionary with streaming statistics
        """
        # Get company counts by data source
        company_stats_query = """
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN data_source = 'stream' THEN 1 ELSE 0 END) as stream_only,
                SUM(CASE WHEN data_source = 'bulk' THEN 1 ELSE 0 END) as bulk_only,
                SUM(CASE WHEN data_source = 'both' THEN 1 ELSE 0 END) as both_source
            FROM companies
        """

        company_stats = await self.manager.fetch_one(company_stats_query, ())

        # Get event statistics
        event_stats_query = """
            SELECT
                event_type,
                COUNT(*) as count
            FROM stream_events
            GROUP BY event_type
        """

        event_stats = await self.manager.fetch_all(event_stats_query, ())

        # Get total event count
        total_events_query = "SELECT COUNT(*) as total FROM stream_events"
        total_events = await self.manager.fetch_one(total_events_query, ())

        # Compile statistics
        event_types = {row["event_type"]: row["count"] for row in event_stats}

        return {
            "total_companies": company_stats["total"] if company_stats else 0,
            "stream_companies": company_stats["stream_only"] if company_stats else 0,
            "bulk_companies": company_stats["bulk_only"] if company_stats else 0,
            "both_source_companies": company_stats["both_source"] if company_stats else 0,
            "total_events": total_events["total"] if total_events else 0,
            "event_types": event_types,
        }

    # Data Consistency Methods
    def transaction(self) -> Any:
        """Get a database transaction context manager."""
        return self.manager.transaction()

    async def execute_query(
        self, query: str, params: tuple[Any, ...] = (), connection: Any = None
    ) -> list[dict[str, Any]]:
        """Execute a query, optionally within a specific connection/transaction."""
        if connection:
            cursor = await connection.execute(query, params)
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]
        return await self.manager.fetch_all(query, params)

    async def upsert_company_with_conflict_resolution(
        self, company: CompanyRecord
    ) -> dict[str, Any]:
        """Upsert company with conflict resolution between bulk and stream data.

        Args:
            company: CompanyRecord instance to upsert

        Returns:
            Dictionary with operation result and conflict resolution info
        """
        async with self.transaction() as tx:
            # Check for existing record
            existing_query = "SELECT * FROM companies WHERE company_number = ?"
            existing_rows = await self.execute_query(existing_query, (company.company_number,), tx)

            if not existing_rows:
                # No conflict - insert new record
                await self._insert_company_record(company, tx)
                return {
                    "action": "inserted",
                    "company_number": company.company_number,
                    "conflict_resolution": None,
                }

            existing = CompanyRecord.from_dict(existing_rows[0])

            # Determine conflict resolution strategy
            resolution_result = self._resolve_data_conflict(existing, company)

            if resolution_result["action"] == "skip":
                return {
                    "action": "skipped",
                    "company_number": company.company_number,
                    "conflict_resolution": resolution_result,
                }

            # Apply resolution
            merged_company = resolution_result["merged_company"]
            await self._update_company_record(merged_company, tx)

            return {
                "action": resolution_result["action"],
                "company_number": company.company_number,
                "conflict_resolution": {
                    "strategy": resolution_result["strategy"],
                    "previous_data_source": existing.data_source,
                    "new_data_source": company.data_source,
                    "merged_fields": resolution_result.get("merged_fields", []),
                },
            }

    def _resolve_data_conflict(self, existing: CompanyRecord, new: CompanyRecord) -> dict[str, Any]:
        """Resolve conflicts between existing and new company data.

        Args:
            existing: Existing company record
            new: New company record to merge

        Returns:
            Dictionary with resolution strategy and merged result
        """
        now = datetime.now()

        # Parse timestamps
        existing_time = None
        if existing.stream_last_updated:
            try:
                existing_time = datetime.fromisoformat(
                    existing.stream_last_updated.replace("Z", "+00:00")
                )
            except (ValueError, AttributeError):
                existing_time = None

        new_time = None
        if new.stream_last_updated:
            try:
                new_time = datetime.fromisoformat(new.stream_last_updated.replace("Z", "+00:00"))
            except (ValueError, AttributeError):
                new_time = None

        # Rule 1: Stream data always wins over bulk data (when stream data is recent)
        if new.data_source == "stream" and existing.data_source == "bulk":
            merged = self._merge_company_records(existing, new, prefer_new=True)
            return {
                "action": "updated",
                "strategy": "stream_over_bulk",
                "merged_company": merged,
                "merged_fields": self._get_changed_fields(existing, merged),
            }

        # Rule 2: Don't overwrite recent stream data with bulk data
        if (
            new.data_source == "bulk"
            and existing.data_source == "stream"
            and existing_time
            and (now - existing_time).total_seconds() < 3600  # 1 hour threshold
        ):
            return {
                "action": "skip",
                "strategy": "protect_recent_stream",
                "reason": "Recent stream data protected from bulk overwrite",
            }

        # Rule 3: Newer data wins (if both have timestamps)
        if existing_time and new_time:
            if new_time > existing_time:
                merged = self._merge_company_records(existing, new, prefer_new=True)
                return {
                    "action": "updated",
                    "strategy": "newer_wins",
                    "merged_company": merged,
                    "merged_fields": self._get_changed_fields(existing, merged),
                }
            return {"action": "skip", "strategy": "older_data", "reason": "Existing data is newer"}

        # Rule 4: Intelligent merge - combine best of both
        merged = self._merge_company_records(existing, new, prefer_new=False)
        return {
            "action": "merged",
            "strategy": "intelligent_merge",
            "merged_company": merged,
            "merged_fields": self._get_changed_fields(existing, merged),
        }

    def _merge_company_records(  # noqa: C901
        self, existing: CompanyRecord, new: CompanyRecord, prefer_new: bool = False
    ) -> CompanyRecord:
        """Merge two company records intelligently.

        Args:
            existing: Existing company record
            new: New company record
            prefer_new: Whether to prefer new values when both exist

        Returns:
            Merged CompanyRecord
        """
        # Start with existing record
        merged_data = existing.to_dict()

        # Update with new data, applying merge logic
        new_data = new.to_dict()

        for field, new_value in new_data.items():
            if field == "company_number":
                continue  # Don't change company number

            existing_value = merged_data.get(field)

            if new_value is not None:
                if existing_value is None:
                    # New field has value, existing doesn't - use new
                    merged_data[field] = new_value
                elif prefer_new:
                    # Prefer new value
                    merged_data[field] = new_value
                elif field in ["stream_last_updated", "last_stream_event_id", "stream_metadata"]:
                    # Always use stream-specific fields from newer data
                    if new.data_source == "stream":
                        merged_data[field] = new_value
                elif len(str(new_value)) > len(str(existing_value)):
                    # Use longer/more complete value
                    merged_data[field] = new_value

        # Update data source and metadata
        if new.data_source == "stream" or existing.data_source == "stream":
            merged_data["data_source"] = "both"

        # Add conflict history to metadata
        if merged_data.get("stream_metadata"):
            if isinstance(merged_data["stream_metadata"], str):
                try:
                    metadata = json.loads(merged_data["stream_metadata"])
                except json.JSONDecodeError:
                    metadata = {}
            else:
                metadata = merged_data["stream_metadata"] or {}
        else:
            metadata = {}

        if "conflict_history" not in metadata:
            metadata["conflict_history"] = []

        metadata["conflict_history"].append(
            {
                "timestamp": datetime.now().isoformat(),
                "previous_source": existing.data_source,
                "new_source": new.data_source,
                "merge_strategy": "intelligent" if not prefer_new else "prefer_new",
            }
        )

        merged_data["stream_metadata"] = metadata

        return CompanyRecord.from_dict(merged_data)

    def _get_changed_fields(self, existing: CompanyRecord, merged: CompanyRecord) -> list[str]:
        """Get list of fields that changed during merge."""
        changed = []
        existing_dict = existing.to_dict()
        merged_dict = merged.to_dict()

        for field, merged_value in merged_dict.items():
            existing_value = existing_dict.get(field)
            if existing_value != merged_value:
                changed.append(field)

        return changed

    async def _insert_company_record(self, company: CompanyRecord, connection: Any) -> None:
        """Insert a company record within a transaction."""
        company_dict = company.to_dict()

        # Convert stream_metadata to JSON string if needed
        if isinstance(company_dict.get("stream_metadata"), dict):
            company_dict["stream_metadata"] = json.dumps(company_dict["stream_metadata"])

        columns = list(company_dict.keys())
        placeholders = ", ".join("?" * len(columns))
        columns_str = ", ".join(columns)

        query = f"INSERT INTO companies ({columns_str}) VALUES ({placeholders})"  # noqa: S608
        await connection.execute(query, tuple(company_dict.values()))

    async def _update_company_record(self, company: CompanyRecord, connection: Any) -> None:
        """Update a company record within a transaction."""
        company_dict = company.to_dict()

        # Convert stream_metadata to JSON string if needed
        if isinstance(company_dict.get("stream_metadata"), dict):
            company_dict["stream_metadata"] = json.dumps(company_dict["stream_metadata"])

        set_clause = ", ".join(f"{k} = ?" for k in company_dict if k != "company_number")
        values = [v for k, v in company_dict.items() if k != "company_number"]
        values.append(company.company_number)

        query = f"UPDATE companies SET {set_clause} WHERE company_number = ?"  # noqa: S608
        await connection.execute(query, tuple(values))

    async def upsert_company_with_validation(self, company: CompanyRecord) -> None:
        """Upsert company with data validation.

        Args:
            company: CompanyRecord to validate and upsert

        Raises:
            DatabaseError: If validation fails
        """
        # Validate company data
        self._validate_company_record(company)

        # Proceed with upsert
        await self.upsert_company_with_conflict_resolution(company)

    def _validate_company_record(self, company: CompanyRecord) -> None:  # noqa: C901
        """Validate a company record for data integrity.

        Args:
            company: CompanyRecord to validate

        Raises:
            DatabaseError: If validation fails
        """
        # Validate company number
        if not company.company_number:
            raise DatabaseError("Invalid company number: empty")

        if len(company.company_number) < 4 or len(company.company_number) > 12:
            raise DatabaseError("Invalid company number: length must be 4-12 characters")

        if not company.company_number.isalnum():
            raise DatabaseError("Invalid company number: must be alphanumeric")

        # Validate required fields for new records
        if not company.company_name:
            raise DatabaseError("Missing required field: company_name")

        # Validate date formats
        if company.incorporation_date:
            try:
                datetime.strptime(company.incorporation_date, "%Y-%m-%d")
            except ValueError:
                raise DatabaseError(
                    "Invalid date format: incorporation_date must be YYYY-MM-DD"
                ) from None

        # Validate SIC codes
        if company.sic_codes:
            sic_codes = company.sic_codes.split(",")
            for sic_code in sic_codes:
                sic_code = sic_code.strip()
                if not sic_code.isdigit() or len(sic_code) != 5:
                    raise DatabaseError(f"Invalid SIC code: {sic_code}")

        # Validate address fields length
        address_fields = [
            company.address_line_1,
            company.address_line_2,
            company.locality,
            company.region,
            company.country,
        ]
        for field in address_fields:
            if field and len(field) > 200:
                raise DatabaseError("Address field too long: maximum 200 characters")

        # Validate company status
        if company.company_status:
            valid_statuses = [
                "active",
                "dissolved",
                "liquidation",
                "receivership",
                "administration",
                "voluntary-arrangement",
                "converted-closed",
                "active-proposal-to-strike-off",
                "struck-off",
            ]
            if company.company_status not in valid_statuses:
                raise DatabaseError(f"Invalid company status: {company.company_status}")

        # Validate status consistency
        if (
            company.company_status == "active"
            and company.company_status_detail
            and "dissolved" in company.company_status_detail.lower()
        ):
            raise DatabaseError("Inconsistent status: active status with dissolved detail")

        # Validate stream metadata
        if company.stream_metadata:
            try:
                if isinstance(company.stream_metadata, dict):
                    json.dumps(company.stream_metadata)  # Test JSON serialization
                elif isinstance(company.stream_metadata, str):
                    json.loads(company.stream_metadata)  # Test JSON parsing
            except (TypeError, json.JSONDecodeError):
                raise DatabaseError("Invalid stream metadata: must be valid JSON") from None

    async def get_consistency_metrics(self) -> dict[str, Any]:
        """Get data consistency metrics."""
        # Count conflicts by type
        conflict_query = """
            SELECT
                COUNT(*) as total_conflicts,
                data_source,
                COUNT(*) as count
            FROM companies
            WHERE stream_metadata LIKE '%conflict_history%'
            GROUP BY data_source
        """

        conflict_stats = await self.manager.fetch_all(conflict_query, ())

        total_conflicts = sum(row["count"] for row in conflict_stats)

        return {
            "total_conflicts": total_conflicts,
            "conflicts_by_type": {row["data_source"]: row["count"] for row in conflict_stats},
            "resolution_strategies": {
                "stream_over_bulk": 0,  # Would need more complex query to get actual counts
                "intelligent_merge": 0,
                "newer_wins": 0,
            },
        }

    async def get_data_quality_metrics(self) -> dict[str, Any]:
        """Get data quality metrics."""
        # Count total companies
        total_query = "SELECT COUNT(*) as total FROM companies"
        total_result = await self.manager.fetch_one(total_query, ())
        total_companies = total_result["total"] if total_result else 0

        # Count field completeness
        completeness_query = """
            SELECT
                COUNT(CASE WHEN company_name IS NOT NULL THEN 1 END) as name_count,
                COUNT(CASE WHEN company_status IS NOT NULL THEN 1 END) as status_count,
                COUNT(CASE WHEN incorporation_date IS NOT NULL THEN 1 END) as date_count,
                COUNT(CASE WHEN address_line_1 IS NOT NULL THEN 1 END) as address_count,
                COUNT(CASE WHEN sic_codes IS NOT NULL THEN 1 END) as sic_count
            FROM companies
        """

        completeness_result = await self.manager.fetch_one(completeness_query, ())

        # Calculate completeness scores
        field_coverage = {}
        completeness_score = 0.0
        if total_companies > 0 and completeness_result:
            for field in ["name_count", "status_count", "date_count", "address_count", "sic_count"]:
                coverage = (completeness_result[field] / total_companies) * 100
                field_coverage[field.replace("_count", "")] = coverage
                completeness_score += coverage

            completeness_score = completeness_score / 5.0  # Average across fields

        return {
            "total_companies": total_companies,
            "completeness_score": completeness_score,
            "field_coverage": field_coverage,
        }

    async def get_consistency_health_status(self) -> dict[str, Any]:
        """Get overall data consistency health status."""
        metrics = await self.get_consistency_metrics()
        quality_metrics = await self.get_data_quality_metrics()

        # Determine health status
        issues = []
        recommendations = []

        if metrics["total_conflicts"] > 100:
            issues.append("High number of data conflicts detected")
            recommendations.append("Review conflict resolution strategies")

        if quality_metrics["completeness_score"] < 70:
            issues.append("Low data completeness score")
            recommendations.append("Improve data collection processes")

        # Determine overall status
        if len(issues) == 0:
            status = "healthy"
        elif len(issues) <= 2:
            status = "warning"
        else:
            status = "critical"

        return {
            "status": status,
            "issues": issues,
            "recommendations": recommendations,
            "last_check": datetime.now().isoformat(),
            "metrics": {
                "conflicts": metrics["total_conflicts"],
                "completeness": quality_metrics["completeness_score"],
            },
        }

"""
Database integration for Companies House Streaming API.

Provides async database operations for storing and retrieving company data,
stream events, and metadata from SQLite database.
"""

import asyncio
import aiosqlite
import json
import logging
from datetime import datetime
from typing import Dict, Any, List, Optional, AsyncGenerator
from dataclasses import dataclass, field
from contextlib import asynccontextmanager

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
    stream_metadata: Optional[Dict[str, Any]] = None
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "CompanyRecord":
        """Create CompanyRecord from dictionary."""
        # Handle stream_metadata JSON
        metadata = data.get("stream_metadata")
        if isinstance(metadata, str):
            try:
                metadata = json.loads(metadata)
            except json.JSONDecodeError:
                metadata = None
        
        return cls(
            company_number=data.get("company_number"),
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
            stream_metadata=metadata
        )
    
    def to_dict(self) -> Dict[str, Any]:
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
            "stream_metadata": json.dumps(self.stream_metadata) if self.stream_metadata else None
        }
    
    def is_strike_off(self) -> bool:
        """Check if company has strike-off status."""
        if not self.company_status:
            return False
        
        strike_off_statuses = [
            "active-proposal-to-strike-off",
            "proposal-to-strike-off",
            "struck-off"
        ]
        
        status_lower = self.company_status.lower().replace(" ", "-")
        return any(status in status_lower for status in strike_off_statuses)


@dataclass
class StreamEventRecord:
    """Represents a stream event record in the database."""
    event_id: str
    event_type: str
    company_number: Optional[str]
    event_data: Dict[str, Any]
    processed_at: datetime
    id: Optional[int] = None
    created_at: Optional[datetime] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for database storage."""
        return {
            "event_id": self.event_id,
            "event_type": self.event_type,
            "company_number": self.company_number,
            "event_data": json.dumps(self.event_data),
            "processed_at": self.processed_at.isoformat() if self.processed_at else None
        }


class DatabaseManager:
    """
    Manages database connections and operations.
    
    Provides connection pooling and transaction management for async database operations.
    """
    
    def __init__(self, config: StreamingConfig):
        """Initialize database manager."""
        self.db_path = config.database_path
        self.pool_size = 5
        self._pool: List[aiosqlite.Connection] = []
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
            raise DatabaseError(f"Database connection failed: {e}")
    
    async def disconnect(self) -> None:
        """Close all database connections."""
        for conn in self._pool:
            await conn.close()
        
        self._pool = []
        self._is_initialized = False
        logger.info("Database connections closed")
    
    async def __aenter__(self):
        """Async context manager entry."""
        await self.connect()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.disconnect()
    
    @asynccontextmanager
    async def get_connection(self):
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
    async def transaction(self):
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
    
    async def execute(self, query: str, params: tuple = ()) -> Any:
        """Execute a single query."""
        async with self.get_connection() as conn:
            cursor = await conn.execute(query, params)
            await conn.commit()
            return cursor
    
    async def execute_many(self, query: str, params_list: List[tuple]) -> None:
        """Execute multiple queries with different parameters."""
        async with self.get_connection() as conn:
            await conn.executemany(query, params_list)
            await conn.commit()
    
    async def fetch_one(self, query: str, params: tuple = ()) -> Optional[Dict[str, Any]]:
        """Fetch a single row."""
        async with self.get_connection() as conn:
            cursor = await conn.execute(query, params)
            row = await cursor.fetchone()
            return dict(row) if row else None
    
    async def fetch_all(self, query: str, params: tuple = ()) -> List[Dict[str, Any]]:
        """Fetch all rows."""
        async with self.get_connection() as conn:
            cursor = await conn.execute(query, params)
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]


class StreamingDatabase:
    """
    High-level database interface for streaming operations.
    
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
    
    async def upsert_company(self, company_data: Dict[str, Any]) -> None:
        """
        Insert or update a company record.
        
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
            set_clause = ", ".join(f"{k} = ?" for k in company_data.keys() if k != "company_number")
            values = [v for k, v in company_data.items() if k != "company_number"]
            values.append(company_number)
            
            query = f"UPDATE companies SET {set_clause} WHERE company_number = ?"
            await self.manager.execute(query, tuple(values))
            
            logger.debug(f"Updated company {company_number}")
        else:
            # Insert new record
            columns = list(company_data.keys())
            placeholders = ", ".join("?" * len(columns))
            columns_str = ", ".join(columns)
            
            query = f"INSERT INTO companies ({columns_str}) VALUES ({placeholders})"
            await self.manager.execute(query, tuple(company_data.values()))
            
            logger.debug(f"Inserted new company {company_number}")
    
    async def batch_upsert_companies(self, companies: List[Dict[str, Any]]) -> None:
        """
        Batch insert or update multiple companies.
        
        Args:
            companies: List of company data dictionaries
        """
        for company in companies:
            await self.upsert_company(company)
    
    async def get_company(self, company_number: str) -> Optional[Dict[str, Any]]:
        """
        Get a company record by company number.
        
        Args:
            company_number: The company number to lookup
            
        Returns:
            Company data dictionary or None if not found
        """
        query = "SELECT * FROM companies WHERE company_number = ?"
        return await self.manager.fetch_one(query, (company_number,))
    
    async def get_companies_by_status(self, status: str) -> List[Dict[str, Any]]:
        """
        Get all companies with a specific status.
        
        Args:
            status: Company status to filter by
            
        Returns:
            List of company data dictionaries
        """
        query = "SELECT * FROM companies WHERE company_status = ?"
        return await self.manager.fetch_all(query, (status,))
    
    async def update_stream_metadata(self, company_number: str, metadata: Dict[str, Any]) -> None:
        """
        Update streaming metadata for a company.
        
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
            company_number
        )
        
        await self.manager.execute(query, params)
        logger.debug(f"Updated stream metadata for company {company_number}")
    
    async def log_stream_event(self, 
                              event_id: str,
                              event_type: str,
                              company_number: Optional[str],
                              event_data: Dict[str, Any]) -> Optional[int]:
        """
        Log a stream event to the database.
        
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
                datetime.now().isoformat()
            )
            
            cursor = await self.manager.execute(query, params)
            logger.debug(f"Logged stream event {event_id}")
            return cursor.lastrowid
            
        except aiosqlite.IntegrityError:
            # Duplicate event ID
            logger.debug(f"Duplicate stream event {event_id}, skipping")
            return None
        except Exception as e:
            logger.error(f"Failed to log stream event: {e}")
            raise DatabaseError(f"Failed to log stream event: {e}")
    
    async def get_stream_event(self, event_id: str) -> Optional[Dict[str, Any]]:
        """
        Get a stream event by ID.
        
        Args:
            event_id: Event identifier
            
        Returns:
            Event data dictionary or None if not found
        """
        query = "SELECT * FROM stream_events WHERE event_id = ?"
        return await self.manager.fetch_one(query, (event_id,))
    
    async def get_stream_stats(self) -> Dict[str, Any]:
        """
        Get streaming statistics.
        
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
            "total_companies": company_stats["total"] or 0,
            "stream_companies": company_stats["stream_only"] or 0,
            "bulk_companies": company_stats["bulk_only"] or 0,
            "both_source_companies": company_stats["both_source"] or 0,
            "total_events": total_events["total"] or 0,
            "event_types": event_types
        }
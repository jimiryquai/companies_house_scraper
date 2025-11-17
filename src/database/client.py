"""PostgreSQL database client."""

import logging
import os
from contextlib import contextmanager
from typing import Generator

import psycopg2
from psycopg2 import pool
from psycopg2.extensions import connection as Connection

logger = logging.getLogger(__name__)


class Database:
    """PostgreSQL database client with connection pooling."""

    def __init__(self, database_url: str | None = None) -> None:
        """Initialize database client.

        Args:
            database_url: PostgreSQL connection URL. If None, reads from DATABASE_URL env var.
        """
        self.database_url = database_url or os.getenv("DATABASE_URL")
        if not self.database_url:
            raise ValueError("DATABASE_URL must be provided or set in environment")

        # Create connection pool (min 1, max 10 connections)
        self.pool = psycopg2.pool.SimpleConnectionPool(
            1,  # minconn
            10,  # maxconn
            self.database_url,
        )

        logger.info("PostgreSQL connection pool created")

    @contextmanager
    def get_connection(self) -> Generator[Connection, None, None]:
        """Get database connection from pool.

        Yields:
            psycopg2 connection object

        Example:
            with db.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT * FROM companies")
        """
        conn = self.pool.getconn()
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            self.pool.putconn(conn)

    def init_schema(self) -> None:
        """Initialize database schema (create tables if not exist)."""
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                # Create companies table
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS companies (
                        company_number VARCHAR(20) PRIMARY KEY,
                        company_name TEXT NOT NULL,
                        company_status VARCHAR(50),
                        company_type VARCHAR(50),
                        jurisdiction VARCHAR(50),
                        date_of_creation DATE,
                        date_of_cessation DATE,
                        registered_office_address TEXT,
                        postal_code VARCHAR(20),
                        locality VARCHAR(100),
                        sic_codes TEXT,
                        ch_url TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                    """
                )

                # Create officers table
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS officers (
                        unique_key VARCHAR(100) PRIMARY KEY,
                        company_number VARCHAR(20) REFERENCES companies(company_number),
                        name TEXT NOT NULL,
                        officer_role VARCHAR(50),
                        appointed_on DATE,
                        resigned_on DATE,
                        nationality VARCHAR(50),
                        occupation VARCHAR(100),
                        country_of_residence VARCHAR(50),
                        date_of_birth VARCHAR(10),
                        address TEXT,
                        postal_code VARCHAR(20),
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                    """
                )

                # Create index on company_number in officers table
                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_officers_company_number
                    ON officers(company_number)
                    """
                )

                # Create trigger to update updated_at on companies
                cur.execute(
                    """
                    CREATE OR REPLACE FUNCTION update_updated_at_column()
                    RETURNS TRIGGER AS $$
                    BEGIN
                        NEW.updated_at = CURRENT_TIMESTAMP;
                        RETURN NEW;
                    END;
                    $$ language 'plpgsql'
                    """
                )

                cur.execute(
                    """
                    DROP TRIGGER IF EXISTS update_companies_updated_at ON companies
                    """
                )

                cur.execute(
                    """
                    CREATE TRIGGER update_companies_updated_at
                    BEFORE UPDATE ON companies
                    FOR EACH ROW
                    EXECUTE FUNCTION update_updated_at_column()
                    """
                )

                # Create trigger to update updated_at on officers
                cur.execute(
                    """
                    DROP TRIGGER IF EXISTS update_officers_updated_at ON officers
                    """
                )

                cur.execute(
                    """
                    CREATE TRIGGER update_officers_updated_at
                    BEFORE UPDATE ON officers
                    FOR EACH ROW
                    EXECUTE FUNCTION update_updated_at_column()
                    """
                )

        logger.info("Database schema initialized successfully")

    def close(self) -> None:
        """Close all connections in the pool."""
        if self.pool:
            self.pool.closeall()
            logger.info("Database connection pool closed")

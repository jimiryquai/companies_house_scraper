"""
Load testing for streaming functionality with simulated high-volume events.
Tests system performance, memory usage, and throughput under stress.
"""

import pytest
import asyncio
import tempfile
import os
import sqlite3
import time
import psutil
import gc
from datetime import datetime, timedelta
from unittest.mock import Mock, patch, AsyncMock
from typing import Dict, Any, List, AsyncGenerator
import json
import random
import string

from src.streaming import (
    StreamingDatabase,
    StreamingConfig,
    EventProcessor,
    CompanyEvent,
    StructuredLogger,
    HealthMonitor,
    StreamingClient,
    LogLevel,
    LogContext
)


@pytest.fixture
def load_test_db():
    """Create a database optimized for load testing."""
    temp_file = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    temp_path = temp_file.name
    temp_file.close()

    # Initialize database with optimized settings
    with sqlite3.connect(temp_path) as conn:
        # Enable WAL mode for better concurrent access
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA cache_size=10000")
        conn.execute("PRAGMA temp_store=MEMORY")

        # Create tables
        conn.execute("""
            CREATE TABLE companies (
                company_number TEXT PRIMARY KEY,
                company_name TEXT,
                company_status TEXT,
                company_status_detail TEXT,
                incorporation_date TEXT,
                sic_codes TEXT,
                address_line_1 TEXT,
                address_line_2 TEXT,
                locality TEXT,
                region TEXT,
                country TEXT,
                postal_code TEXT,
                premises TEXT,
                stream_last_updated TEXT,
                stream_status TEXT DEFAULT 'unknown',
                data_source TEXT DEFAULT 'bulk',
                last_stream_event_id TEXT,
                stream_metadata TEXT
            )
        """)

        conn.execute("""
            CREATE TABLE stream_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id TEXT UNIQUE,
                event_type TEXT NOT NULL,
                company_number TEXT,
                event_data TEXT,
                processed_at TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (company_number) REFERENCES companies(company_number)
            )
        """)

        # Create indexes for better performance
        conn.execute("CREATE INDEX idx_companies_status ON companies(company_status)")
        conn.execute("CREATE INDEX idx_companies_data_source ON companies(data_source)")
        conn.execute("CREATE INDEX idx_companies_stream_updated ON companies(stream_last_updated)")
        conn.execute("CREATE INDEX idx_stream_events_type ON stream_events(event_type)")
        conn.execute("CREATE INDEX idx_stream_events_created ON stream_events(created_at)")
        conn.execute("CREATE INDEX idx_stream_events_company ON stream_events(company_number)")

        conn.commit()

    yield temp_path

    # Cleanup
    os.unlink(temp_path)


@pytest.fixture
def load_test_config(load_test_db):
    """Create configuration optimized for load testing."""
    return StreamingConfig(
        streaming_api_key="12345678-1234-1234-1234-123456789012",
        database_path=load_test_db,
        batch_size=50,  # Larger batch size for load testing
        api_base_url="https://api.companieshouse.gov.uk",
        connection_timeout=10,
        max_retries=2,
        initial_backoff=0.1,  # Faster backoff for load testing
        max_backoff=5,
        rate_limit_requests_per_minute=1200  # Higher rate limit
    )


class MockHighVolumeClient:
    """Mock client that simulates high-volume event streams."""

    def __init__(self, events_per_second: int = 100, total_events: int = 1000):
        self.events_per_second = events_per_second
        self.total_events = total_events
        self.events_generated = 0
        self.is_connected = True
        self.session = None

    async def connect(self):
        """Mock connection."""
        self.is_connected = True

    async def disconnect(self):
        """Mock disconnection."""
        self.is_connected = False

    async def stream_events(self) -> AsyncGenerator[Dict[str, Any], None]:
        """Generate high-volume stream of events."""
        interval = 1.0 / self.events_per_second

        company_statuses = ["active", "liquidation", "dissolved", "administration"]
        status_details = [None, "proposal-to-strike-off", "gazette-notice", "final-gazette"]

        for i in range(self.total_events):
            if not self.is_connected:
                break

            # Generate realistic company number
            company_number = f"LOAD{i:08d}"

            # Vary event types
            event_type = random.choice(["company-profile", "filing-history", "officers"])

            event_data = {
                "resource_kind": event_type,
                "resource_id": company_number,
                "data": {
                    "company_number": company_number,
                    "company_name": f"Load Test Company {i:08d} Ltd",
                    "company_status": random.choice(company_statuses),
                    "company_status_detail": random.choice(status_details),
                    "incorporation_date": f"20{random.randint(10, 23):02d}-{random.randint(1, 12):02d}-{random.randint(1, 28):02d}",
                    "address": {
                        "address_line_1": f"{random.randint(1, 999)} Test Street",
                        "locality": random.choice(["London", "Manchester", "Birmingham", "Leeds", "Liverpool"]),
                        "region": "England",
                        "country": "England",
                        "postal_code": f"L{random.randint(1, 9)}{random.randint(1, 9)} {random.randint(1, 9)}AA"
                    }
                },
                "event": {
                    "timepoint": 100000 + i,
                    "published_at": datetime.now().isoformat()
                }
            }

            self.events_generated += 1
            yield event_data

            # Rate limiting
            await asyncio.sleep(interval)


def generate_bulk_companies(count: int) -> List[Dict[str, Any]]:
    """Generate bulk test company data."""
    companies = []
    statuses = ["active", "liquidation", "dissolved", "administration"]
    postcodes = ["SW1A 1AA", "M1 1AA", "B1 1AA", "LS1 1AA", "L1 1AA"]

    for i in range(count):
        company = {
            "company_number": f"BULK{i:08d}",
            "company_name": f"Bulk Test Company {i:08d} Ltd",
            "company_status": random.choice(statuses),
            "company_status_detail": None,
            "incorporation_date": f"20{random.randint(10, 22):02d}-{random.randint(1, 12):02d}-{random.randint(1, 28):02d}",
            "postal_code": random.choice(postcodes),  # Fixed column name
            "sic_codes": str(random.randint(10000, 99999)),  # Fixed column name
            "data_source": "bulk"
        }
        companies.append(company)

    return companies


class TestHighVolumeEventProcessing:
    """Test processing of high-volume event streams."""

    @pytest.mark.slow
    @pytest.mark.asyncio
    async def test_sustained_event_processing_throughput(self, load_test_config):
        """Test sustained processing of high-volume events."""
        database = StreamingDatabase(load_test_config)
        processor = EventProcessor(load_test_config)

        await database.connect()

        # Create test scenario
        events_per_second = 50  # Reasonable rate for testing
        total_events = 500

        mock_client = MockHighVolumeClient(events_per_second, total_events)

        try:
            start_time = time.time()
            processed_count = 0
            failed_count = 0
            processing_times = []

            # Process events
            async for event_data in mock_client.stream_events():
                event_start = time.time()

                try:
                    # Process event
                    result = await processor.process_event(event_data)

                    if result:
                        # Update database
                        company_event = CompanyEvent.from_dict(event_data)
                        company_data = {
                            "company_number": company_event.company_number,
                            "company_name": company_event.company_name,
                            "company_status": company_event.company_status,
                            "data_source": "stream",
                            "stream_last_updated": datetime.now().isoformat()
                        }
                        await database.upsert_company(company_data)
                        processed_count += 1
                    else:
                        failed_count += 1

                except Exception as e:
                    failed_count += 1
                    print(f"Failed to process event: {e}")

                event_end = time.time()
                processing_times.append(event_end - event_start)

                # Log progress
                if processed_count % 100 == 0:
                    elapsed = time.time() - start_time
                    current_rate = processed_count / elapsed if elapsed > 0 else 0
                    print(f"Processed {processed_count} events, rate: {current_rate:.1f} events/sec")

            # Calculate metrics
            total_time = time.time() - start_time
            avg_throughput = processed_count / total_time if total_time > 0 else 0
            avg_processing_time = sum(processing_times) / len(processing_times) if processing_times else 0
            max_processing_time = max(processing_times) if processing_times else 0
            min_processing_time = min(processing_times) if processing_times else 0

            print(f"Load test results:")
            print(f"  Total events: {total_events}")
            print(f"  Processed: {processed_count}")
            print(f"  Failed: {failed_count}")
            print(f"  Total time: {total_time:.2f}s")
            print(f"  Average throughput: {avg_throughput:.1f} events/sec")
            print(f"  Processing time - avg: {avg_processing_time*1000:.1f}ms, min: {min_processing_time*1000:.1f}ms, max: {max_processing_time*1000:.1f}ms")

            # Assertions
            assert processed_count > 0, "Should process some events"
            assert processed_count >= total_events * 0.95, f"Should process at least 95% of events, got {processed_count}/{total_events}"
            assert avg_throughput >= 20, f"Should maintain at least 20 events/sec, got {avg_throughput:.1f}"
            assert avg_processing_time < 0.1, f"Average processing time should be under 100ms, got {avg_processing_time*1000:.1f}ms"

            # Verify database state
            final_count = await database.manager.fetch_one("SELECT COUNT(*) as count FROM companies", ())
            assert final_count["count"] >= processed_count * 0.9, "Most events should be in database"

        finally:
            await database.disconnect()

    @pytest.mark.slow
    @pytest.mark.asyncio
    async def test_memory_usage_under_load(self, load_test_config):
        """Test memory usage during high-volume processing."""
        database = StreamingDatabase(load_test_config)
        processor = EventProcessor(load_test_config)

        await database.connect()

        # Get initial memory usage
        process = psutil.Process()
        initial_memory = process.memory_info().rss / 1024 / 1024  # MB

        try:
            events_per_second = 30
            total_events = 300

            mock_client = MockHighVolumeClient(events_per_second, total_events)

            memory_samples = [initial_memory]
            processed_count = 0

            async for event_data in mock_client.stream_events():
                # Process event
                result = await processor.process_event(event_data)

                if result:
                    company_event = CompanyEvent.from_dict(event_data)
                    company_data = {
                        "company_number": company_event.company_number,
                        "company_name": company_event.company_name,
                        "company_status": company_event.company_status,
                        "data_source": "stream",
                        "stream_last_updated": datetime.now().isoformat()
                    }
                    await database.upsert_company(company_data)
                    processed_count += 1

                # Sample memory every 50 events
                if processed_count % 50 == 0:
                    current_memory = process.memory_info().rss / 1024 / 1024
                    memory_samples.append(current_memory)

                    # Force garbage collection periodically
                    if processed_count % 100 == 0:
                        gc.collect()

            # Final memory measurement
            final_memory = process.memory_info().rss / 1024 / 1024
            memory_samples.append(final_memory)

            # Calculate memory metrics
            max_memory = max(memory_samples)
            memory_growth = final_memory - initial_memory
            memory_efficiency = processed_count / memory_growth if memory_growth > 0 else float('inf')

            print(f"Memory usage results:")
            print(f"  Initial memory: {initial_memory:.1f}MB")
            print(f"  Final memory: {final_memory:.1f}MB")
            print(f"  Max memory: {max_memory:.1f}MB")
            print(f"  Memory growth: {memory_growth:.1f}MB")
            print(f"  Events processed: {processed_count}")
            print(f"  Memory efficiency: {memory_efficiency:.1f} events/MB")

            # Memory usage assertions
            assert memory_growth < 100, f"Memory growth should be under 100MB, got {memory_growth:.1f}MB"
            assert max_memory < initial_memory + 150, f"Max memory should not exceed initial + 150MB"
            assert memory_efficiency > 2, f"Should process at least 2 events per MB of memory growth"

        finally:
            await database.disconnect()

    @pytest.mark.slow
    @pytest.mark.asyncio
    async def test_concurrent_event_processing(self, load_test_config):
        """Test concurrent processing of multiple event streams."""
        database = StreamingDatabase(load_test_config)

        await database.connect()

        try:
            # Create multiple concurrent streams
            num_streams = 3
            events_per_stream = 100

            async def process_stream(stream_id: int) -> Dict[str, Any]:
                """Process a single stream of events."""
                processor = EventProcessor(load_test_config)
                mock_client = MockHighVolumeClient(20, events_per_stream)

                processed = 0
                failed = 0
                start_time = time.time()

                async for event_data in mock_client.stream_events():
                    try:
                        # Modify company number to be unique per stream
                        event_data["resource_id"] = f"STREAM{stream_id}_{event_data['resource_id']}"
                        event_data["data"]["company_number"] = event_data["resource_id"]

                        result = await processor.process_event(event_data)

                        if result:
                            company_event = CompanyEvent.from_dict(event_data)
                            company_data = {
                                "company_number": company_event.company_number,
                                "company_name": f"Stream {stream_id} - {company_event.company_name}",
                                "company_status": company_event.company_status,
                                "data_source": "stream",
                                "stream_last_updated": datetime.now().isoformat()
                            }
                            await database.upsert_company(company_data)
                            processed += 1
                        else:
                            failed += 1
                    except Exception as e:
                        failed += 1
                        print(f"Stream {stream_id} error: {e}")

                duration = time.time() - start_time
                return {
                    "stream_id": stream_id,
                    "processed": processed,
                    "failed": failed,
                    "duration": duration,
                    "rate": processed / duration if duration > 0 else 0
                }

            # Run streams concurrently
            start_time = time.time()
            tasks = [process_stream(i) for i in range(num_streams)]
            results = await asyncio.gather(*tasks)
            total_duration = time.time() - start_time

            # Analyze results
            total_processed = sum(r["processed"] for r in results)
            total_failed = sum(r["failed"] for r in results)
            average_rate = sum(r["rate"] for r in results) / len(results)
            overall_rate = total_processed / total_duration if total_duration > 0 else 0

            print(f"Concurrent processing results:")
            print(f"  Streams: {num_streams}")
            print(f"  Events per stream: {events_per_stream}")
            print(f"  Total processed: {total_processed}")
            print(f"  Total failed: {total_failed}")
            print(f"  Total duration: {total_duration:.2f}s")
            print(f"  Average stream rate: {average_rate:.1f} events/sec")
            print(f"  Overall rate: {overall_rate:.1f} events/sec")

            for result in results:
                print(f"    Stream {result['stream_id']}: {result['processed']} events, {result['rate']:.1f} events/sec")

            # Concurrent processing assertions
            assert total_processed >= num_streams * events_per_stream * 0.9, "Should process most events"
            assert total_failed < total_processed * 0.1, "Failure rate should be under 10%"
            assert overall_rate >= 30, f"Overall rate should be at least 30 events/sec, got {overall_rate:.1f}"

            # Verify database consistency
            final_count = await database.manager.fetch_one("SELECT COUNT(*) as count FROM companies", ())
            assert final_count["count"] >= total_processed * 0.9, "Most events should be in database"

            # Check for data integrity across streams
            stream_counts = await database.manager.fetch_all("""
                SELECT
                    SUBSTR(company_number, 1, 8) as stream_prefix,
                    COUNT(*) as count
                FROM companies
                WHERE company_number LIKE 'STREAM%'
                GROUP BY stream_prefix
            """, ())

            assert len(stream_counts) == num_streams, f"Should have data from all {num_streams} streams"

        finally:
            await database.disconnect()


class TestBulkDataScaling:
    """Test scaling with large amounts of bulk data."""

    @pytest.mark.slow
    @pytest.mark.asyncio
    async def test_streaming_performance_with_large_bulk_dataset(self, load_test_config):
        """Test streaming performance when database contains large amounts of bulk data."""
        database = StreamingDatabase(load_test_config)
        await database.connect()

        try:
            # Populate database with bulk data
            bulk_companies = generate_bulk_companies(5000)  # 5k bulk companies

            print("Inserting bulk data...")
            start_time = time.time()

            # Batch insert for performance
            batch_size = 100
            for i in range(0, len(bulk_companies), batch_size):
                batch = bulk_companies[i:i + batch_size]
                for company in batch:
                    await database.upsert_company(company)

            bulk_insert_time = time.time() - start_time
            print(f"Bulk insert completed in {bulk_insert_time:.2f}s")

            # Now test streaming performance
            print("Testing streaming performance...")
            processor = EventProcessor(load_test_config)
            mock_client = MockHighVolumeClient(30, 200)  # 200 streaming events

            start_time = time.time()
            processed_count = 0

            async for event_data in mock_client.stream_events():
                try:
                    result = await processor.process_event(event_data)

                    if result:
                        company_event = CompanyEvent.from_dict(event_data)
                        company_data = {
                            "company_number": company_event.company_number,
                            "company_name": company_event.company_name,
                            "company_status": company_event.company_status,
                            "data_source": "stream",
                            "stream_last_updated": datetime.now().isoformat()
                        }
                        await database.upsert_company(company_data)
                        processed_count += 1

                except Exception as e:
                    print(f"Streaming error: {e}")

            streaming_time = time.time() - start_time
            streaming_rate = processed_count / streaming_time if streaming_time > 0 else 0

            # Verify final state
            total_companies = await database.manager.fetch_one("SELECT COUNT(*) as count FROM companies", ())
            bulk_count = await database.manager.fetch_one(
                "SELECT COUNT(*) as count FROM companies WHERE data_source = 'bulk'", ()
            )
            stream_count = await database.manager.fetch_one(
                "SELECT COUNT(*) as count FROM companies WHERE data_source = 'stream'", ()
            )

            print(f"Performance with large dataset:")
            print(f"  Bulk companies: {bulk_count['count']}")
            print(f"  Stream companies: {stream_count['count']}")
            print(f"  Total companies: {total_companies['count']}")
            print(f"  Streaming time: {streaming_time:.2f}s")
            print(f"  Streaming rate: {streaming_rate:.1f} events/sec")

            # Performance assertions with large dataset
            assert total_companies["count"] >= 5000, "Should have bulk data"
            assert stream_count["count"] >= 190, "Should process most streaming events"
            assert streaming_rate >= 15, f"Should maintain at least 15 events/sec with large dataset, got {streaming_rate:.1f}"

            # Test query performance on large dataset
            query_start = time.time()
            recent_updates = await database.manager.fetch_all("""
                SELECT company_number, company_name, data_source, stream_last_updated
                FROM companies
                WHERE stream_last_updated IS NOT NULL
                ORDER BY stream_last_updated DESC
                LIMIT 50
            """, ())
            query_time = time.time() - query_start

            assert query_time < 1.0, f"Query should complete in under 1 second, took {query_time:.3f}s"
            assert len(recent_updates) > 0, "Should find recently updated companies"

        finally:
            await database.disconnect()

    @pytest.mark.slow
    @pytest.mark.asyncio
    async def test_database_optimization_under_load(self, load_test_config):
        """Test database performance optimizations under load."""
        database = StreamingDatabase(load_test_config)
        await database.connect()

        try:
            # Test 1: Batch operations vs individual operations
            test_companies = generate_bulk_companies(1000)

            # Individual inserts
            start_time = time.time()
            for i, company in enumerate(test_companies[:100]):
                await database.upsert_company(company)
            individual_time = time.time() - start_time

            # Batch operations (simulated by rapid sequential operations)
            start_time = time.time()
            for company in test_companies[100:200]:
                await database.upsert_company(company)
            batch_time = time.time() - start_time

            print(f"Database operation performance:")
            print(f"  Individual inserts (100): {individual_time:.2f}s ({100/individual_time:.1f} ops/sec)")
            print(f"  Sequential inserts (100): {batch_time:.2f}s ({100/batch_time:.1f} ops/sec)")

            # Test 2: Query performance with different indexes
            # Complex query that should use indexes
            query_start = time.time()
            complex_query_result = await database.manager.fetch_all("""
                SELECT c.company_number, c.company_name, c.data_source, COUNT(se.id) as event_count
                FROM companies c
                LEFT JOIN stream_events se ON c.company_number = se.company_number
                WHERE c.company_status = 'active'
                GROUP BY c.company_number, c.company_name, c.data_source
                HAVING event_count >= 0
                ORDER BY c.company_name
                LIMIT 100
            """, ())
            complex_query_time = time.time() - query_start

            print(f"  Complex query (100 results): {complex_query_time:.3f}s")

            # Test 3: Concurrent read/write performance
            async def write_worker():
                """Worker that performs writes."""
                write_count = 0
                for i in range(50):
                    company = {
                        "company_number": f"WRITE{i:04d}",
                        "company_name": f"Write Test Company {i}",
                        "company_status": "active",
                        "data_source": "stream"
                    }
                    await database.upsert_company(company)
                    write_count += 1
                return write_count

            async def read_worker():
                """Worker that performs reads."""
                read_count = 0
                for i in range(50):
                    result = await database.manager.fetch_all(
                        "SELECT COUNT(*) as count FROM companies WHERE company_status = ?", ("active",)
                    )
                    if result:
                        read_count += 1
                return read_count

            # Run concurrent read/write operations
            concurrent_start = time.time()
            write_task = asyncio.create_task(write_worker())
            read_task = asyncio.create_task(read_worker())

            write_result, read_result = await asyncio.gather(write_task, read_task)
            concurrent_time = time.time() - concurrent_start

            print(f"  Concurrent operations: {write_result} writes, {read_result} reads in {concurrent_time:.2f}s")

            # Performance assertions
            assert individual_time < 10, "Individual operations should complete in reasonable time"
            assert batch_time < individual_time, "Sequential operations should be faster than individual"
            assert complex_query_time < 5, "Complex queries should complete quickly"
            assert write_result == 50 and read_result == 50, "All concurrent operations should succeed"
            assert concurrent_time < 20, "Concurrent operations should not block excessively"

        finally:
            await database.disconnect()


class TestSystemResourceManagement:
    """Test system resource management under load."""

    @pytest.mark.slow
    @pytest.mark.asyncio
    async def test_connection_pool_under_stress(self, load_test_config):
        """Test database connection pool behavior under stress."""
        # Test multiple databases to stress connection pooling
        databases = []

        try:
            # Create multiple database connections
            for i in range(5):
                db = StreamingDatabase(load_test_config)
                await db.connect()
                databases.append(db)

            # Perform simultaneous operations
            async def stress_database(db: StreamingDatabase, db_id: int):
                """Stress test a single database connection."""
                operations = 0
                for i in range(100):
                    try:
                        # Mix of operations
                        if i % 3 == 0:
                            # Insert operation
                            company = {
                                "company_number": f"STRESS{db_id}_{i:04d}",
                                "company_name": f"Stress Test Company {db_id}-{i}",
                                "company_status": "active",
                                "data_source": "stream"
                            }
                            await db.upsert_company(company)
                        elif i % 3 == 1:
                            # Read operation
                            result = await db.manager.fetch_all(
                                "SELECT COUNT(*) as count FROM companies WHERE data_source = ?", ("stream",)
                            )
                            if result:
                                operations += 1
                        else:
                            # Update operation (simulate streaming update)
                            await db.manager.execute(
                                "UPDATE companies SET stream_last_updated = ? WHERE company_number LIKE ?",
                                (datetime.now().isoformat(), f"STRESS{db_id}%")
                            )

                        operations += 1

                    except Exception as e:
                        print(f"Database {db_id} operation {i} failed: {e}")

                return {"db_id": db_id, "operations": operations}

            # Run stress test on all databases concurrently
            start_time = time.time()
            tasks = [stress_database(db, i) for i, db in enumerate(databases)]
            results = await asyncio.gather(*tasks)
            stress_time = time.time() - start_time

            total_operations = sum(r["operations"] for r in results)
            ops_per_second = total_operations / stress_time if stress_time > 0 else 0

            print(f"Connection pool stress test:")
            print(f"  Databases: {len(databases)}")
            print(f"  Total operations: {total_operations}")
            print(f"  Duration: {stress_time:.2f}s")
            print(f"  Operations per second: {ops_per_second:.1f}")

            for result in results:
                print(f"    DB {result['db_id']}: {result['operations']} operations")

            # Resource management assertions
            assert total_operations >= len(databases) * 80, "Should complete most operations"
            assert ops_per_second >= 100, f"Should maintain at least 100 ops/sec, got {ops_per_second:.1f}"

            # Test connection pool health
            for db in databases:
                assert db.manager._is_initialized, "Database connections should remain healthy"

                # Test that we can still perform operations
                test_result = await db.manager.fetch_one("SELECT COUNT(*) as count FROM companies", ())
                assert test_result is not None, "Should be able to query after stress test"

        finally:
            # Clean up all database connections
            for db in databases:
                await db.disconnect()

    @pytest.mark.slow
    @pytest.mark.asyncio
    async def test_error_recovery_under_load(self, load_test_config):
        """Test error recovery mechanisms under high load."""
        database = StreamingDatabase(load_test_config)
        processor = EventProcessor(load_test_config)

        await database.connect()

        try:
            # Inject errors periodically
            error_injection_rate = 0.1  # 10% error rate
            total_events = 200
            processed_count = 0
            error_count = 0
            recovery_count = 0

            for i in range(total_events):
                # Create event
                event_data = {
                    "resource_kind": "company-profile",
                    "resource_id": f"ERROR{i:06d}",
                    "data": {
                        "company_number": f"ERROR{i:06d}",
                        "company_name": f"Error Test Company {i}",
                        "company_status": "active"
                    },
                    "event": {
                        "timepoint": 200000 + i,
                        "published_at": datetime.now().isoformat()
                    }
                }

                # Inject errors artificially
                if random.random() < error_injection_rate:
                    # Corrupt the event data
                    event_data["data"]["company_number"] = None  # Invalid data

                try:
                    # Process with error handling
                    result = await processor.process_event(event_data)

                    if result:
                        company_event = CompanyEvent.from_dict(event_data)
                        company_data = {
                            "company_number": company_event.company_number,
                            "company_name": company_event.company_name,
                            "company_status": company_event.company_status,
                            "data_source": "stream",
                            "stream_last_updated": datetime.now().isoformat()
                        }
                        await database.upsert_company(company_data)
                        processed_count += 1
                    else:
                        error_count += 1

                        # Attempt recovery by fixing the event
                        if event_data["data"]["company_number"] is None:
                            event_data["data"]["company_number"] = f"RECOVER{i:06d}"
                            event_data["resource_id"] = f"RECOVER{i:06d}"

                            # Retry processing
                            retry_result = await processor.process_event(event_data)
                            if retry_result:
                                company_event = CompanyEvent.from_dict(event_data)
                                company_data = {
                                    "company_number": company_event.company_number,
                                    "company_name": company_event.company_name,
                                    "company_status": company_event.company_status,
                                    "data_source": "stream",
                                    "stream_last_updated": datetime.now().isoformat()
                                }
                                await database.upsert_company(company_data)
                                recovery_count += 1

                except Exception as e:
                    error_count += 1
                    print(f"Event {i} failed: {e}")

            # Calculate recovery metrics
            success_rate = processed_count / total_events
            error_rate = error_count / total_events
            recovery_rate = recovery_count / error_count if error_count > 0 else 0

            print(f"Error recovery test results:")
            print(f"  Total events: {total_events}")
            print(f"  Processed: {processed_count}")
            print(f"  Errors: {error_count}")
            print(f"  Recoveries: {recovery_count}")
            print(f"  Success rate: {success_rate:.1%}")
            print(f"  Error rate: {error_rate:.1%}")
            print(f"  Recovery rate: {recovery_rate:.1%}")

            # Error recovery assertions
            assert success_rate >= 0.8, f"Should successfully process at least 80% of events, got {success_rate:.1%}"
            assert error_rate <= 0.3, f"Error rate should be under 30%, got {error_rate:.1%}"
            if error_count > 0:
                assert recovery_rate >= 0.5, f"Should recover from at least 50% of errors, got {recovery_rate:.1%}"

            # Verify system stability after errors
            final_count = await database.manager.fetch_one("SELECT COUNT(*) as count FROM companies", ())
            assert final_count["count"] >= processed_count + recovery_count, "All successful operations should be persisted"

        finally:
            await database.disconnect()


# Add performance markers for pytest
pytestmark = [
    pytest.mark.slow,
    pytest.mark.performance
]


class TestLoadTestSummary:
    """Summary test that combines multiple load test scenarios."""

    @pytest.mark.slow
    @pytest.mark.asyncio
    async def test_comprehensive_load_scenario(self, load_test_config):
        """Run a comprehensive load test combining multiple scenarios."""
        print("Starting comprehensive load test...")

        database = StreamingDatabase(load_test_config)
        await database.connect()

        try:
            start_time = time.time()

            # Phase 1: Populate with bulk data
            print("Phase 1: Bulk data population")
            bulk_companies = generate_bulk_companies(1000)
            for company in bulk_companies:
                await database.upsert_company(company)

            bulk_time = time.time() - start_time
            print(f"  Bulk population: {bulk_time:.2f}s")

            # Phase 2: Concurrent streaming
            print("Phase 2: Concurrent streaming")
            phase2_start = time.time()

            async def streaming_worker(worker_id: int):
                """Worker for concurrent streaming."""
                processor = EventProcessor(load_test_config)
                mock_client = MockHighVolumeClient(25, 100)

                processed = 0
                async for event_data in mock_client.stream_events():
                    event_data["resource_id"] = f"COMP{worker_id}_{event_data['resource_id']}"
                    event_data["data"]["company_number"] = event_data["resource_id"]

                    try:
                        result = await processor.process_event(event_data)
                        if result:
                            company_event = CompanyEvent.from_dict(event_data)
                            company_data = {
                                "company_number": company_event.company_number,
                                "company_name": company_event.company_name,
                                "company_status": company_event.company_status,
                                "data_source": "stream",
                                "stream_last_updated": datetime.now().isoformat()
                            }
                            await database.upsert_company(company_data)
                            processed += 1
                    except Exception as e:
                        print(f"Worker {worker_id} error: {e}")

                return processed

            # Run 3 concurrent workers
            workers = [streaming_worker(i) for i in range(3)]
            worker_results = await asyncio.gather(*workers)

            phase2_time = time.time() - phase2_start
            total_streamed = sum(worker_results)

            print(f"  Concurrent streaming: {phase2_time:.2f}s, {total_streamed} events")

            # Phase 3: Mixed operations
            print("Phase 3: Mixed operations")
            phase3_start = time.time()

            mixed_operations = 0
            for i in range(100):
                if i % 3 == 0:
                    # Query operation
                    result = await database.manager.fetch_all(
                        "SELECT company_number FROM companies WHERE data_source = ? LIMIT 10", ("stream",)
                    )
                    mixed_operations += len(result)
                elif i % 3 == 1:
                    # Update operation
                    await database.manager.execute(
                        "UPDATE companies SET stream_last_updated = ? WHERE company_number LIKE ?",
                        (datetime.now().isoformat(), "COMP%")
                    )
                    mixed_operations += 1
                else:
                    # Insert operation
                    company = {
                        "company_number": f"MIXED{i:04d}",
                        "company_name": f"Mixed Operation Company {i}",
                        "company_status": "active",
                        "data_source": "stream"
                    }
                    await database.upsert_company(company)
                    mixed_operations += 1

            phase3_time = time.time() - phase3_start

            print(f"  Mixed operations: {phase3_time:.2f}s, {mixed_operations} operations")

            # Final measurements
            total_time = time.time() - start_time

            final_stats = await database.manager.fetch_all("""
                SELECT
                    data_source,
                    COUNT(*) as count,
                    MIN(stream_last_updated) as first_update,
                    MAX(stream_last_updated) as last_update
                FROM companies
                GROUP BY data_source
            """, ())

            total_companies = await database.manager.fetch_one("SELECT COUNT(*) as count FROM companies", ())

            print(f"\nComprehensive load test results:")
            print(f"  Total duration: {total_time:.2f}s")
            print(f"  Total companies: {total_companies['count']}")

            for stat in final_stats:
                print(f"  {stat['data_source'].capitalize()} companies: {stat['count']}")

            # Comprehensive assertions
            assert total_companies["count"] >= 1200, "Should have substantial amount of data"
            assert total_time < 120, "Comprehensive test should complete in reasonable time"

            # Verify data integrity
            bulk_count = next(s["count"] for s in final_stats if s["data_source"] == "bulk")
            stream_count = next(s["count"] for s in final_stats if s["data_source"] == "stream")

            assert bulk_count >= 900, "Should preserve most bulk data"
            assert stream_count >= 250, "Should have substantial streaming data"

            print("Comprehensive load test completed successfully!")

        finally:
            await database.disconnect()

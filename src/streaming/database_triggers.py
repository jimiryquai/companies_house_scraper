"""Database triggers and constraints to prevent direct API calls.

This module creates database-level safeguards to ensure that all API operations
go through the proper state management and queue system, preventing any direct
insertions or updates that could lead to rate limit violations.
"""

import logging
import sqlite3

logger = logging.getLogger(__name__)


def create_emergency_safeguard_triggers(conn: sqlite3.Connection) -> None:
    """Create database triggers to prevent direct API calls and enforce state management.

    These triggers ensure that:
    1. Company processing state transitions follow the correct workflow
    2. API rate limit logs are properly tracked
    3. No direct insertions bypass the state management system

    Args:
        conn: SQLite database connection
    """
    try:
        # Create trigger to validate state transitions
        conn.execute("""
        CREATE TRIGGER IF NOT EXISTS validate_state_transition
        BEFORE UPDATE ON company_processing_state
        FOR EACH ROW
        WHEN NEW.processing_state != OLD.processing_state
        BEGIN
            SELECT CASE
                -- Detected can only go to status_queued or failed
                WHEN OLD.processing_state = 'detected'
                    AND NEW.processing_state NOT IN ('status_queued', 'failed')
                    THEN RAISE(ABORT, 'Invalid state transition from detected')

                -- Status_queued can only go to status_fetched or failed
                WHEN OLD.processing_state = 'status_queued'
                    AND NEW.processing_state NOT IN ('status_fetched', 'failed')
                    THEN RAISE(ABORT, 'Invalid state transition from status_queued')

                -- Status_fetched can only go to strike_off_confirmed, completed, or failed
                WHEN OLD.processing_state = 'status_fetched'
                    AND NEW.processing_state NOT IN ('strike_off_confirmed', 'completed', 'failed')
                    THEN RAISE(ABORT, 'Invalid state transition from status_fetched')

                -- Strike_off_confirmed can only go to officers_queued or failed
                WHEN OLD.processing_state = 'strike_off_confirmed'
                    AND NEW.processing_state NOT IN ('officers_queued', 'failed')
                    THEN RAISE(ABORT, 'Invalid state transition from strike_off_confirmed')

                -- Officers_queued can only go to officers_fetched or failed
                WHEN OLD.processing_state = 'officers_queued'
                    AND NEW.processing_state NOT IN ('officers_fetched', 'failed')
                    THEN RAISE(ABORT, 'Invalid state transition from officers_queued')

                -- Officers_fetched can only go to completed or failed
                WHEN OLD.processing_state = 'officers_fetched'
                    AND NEW.processing_state NOT IN ('completed', 'failed')
                    THEN RAISE(ABORT, 'Invalid state transition from officers_fetched')

                -- Completed and failed are terminal states
                WHEN OLD.processing_state IN ('completed', 'failed')
                    AND NEW.processing_state != OLD.processing_state
                    THEN RAISE(ABORT, 'Cannot transition from terminal state')
            END;
        END;
        """)

        # Create trigger to enforce queue request IDs
        conn.execute("""
        CREATE TRIGGER IF NOT EXISTS enforce_queue_request_ids
        BEFORE UPDATE ON company_processing_state
        FOR EACH ROW
        WHEN NEW.processing_state IN ('status_queued', 'officers_queued')
        BEGIN
            SELECT CASE
                WHEN NEW.processing_state = 'status_queued'
                    AND (NEW.status_request_id IS NULL OR NEW.status_request_id = '')
                    THEN RAISE(ABORT, 'status_request_id required when status_queued')

                WHEN NEW.processing_state = 'officers_queued'
                    AND (NEW.officers_request_id IS NULL OR NEW.officers_request_id = '')
                    THEN RAISE(ABORT, 'officers_request_id required when officers_queued')
            END;
        END;
        """)

        # Create trigger to update updated_at timestamp
        conn.execute("""
        CREATE TRIGGER IF NOT EXISTS update_updated_at_timestamp
        AFTER UPDATE ON company_processing_state
        FOR EACH ROW
        BEGIN
            UPDATE company_processing_state
            SET updated_at = CURRENT_TIMESTAMP
            WHERE id = NEW.id;
        END;
        """)

        # Create triggers to update state-specific timestamps
        conn.execute("""
        CREATE TRIGGER IF NOT EXISTS update_status_queued_timestamp
        AFTER UPDATE ON company_processing_state
        FOR EACH ROW
        WHEN NEW.processing_state = 'status_queued' AND OLD.status_queued_at IS NULL
        BEGIN
            UPDATE company_processing_state
            SET status_queued_at = CURRENT_TIMESTAMP
            WHERE id = NEW.id;
        END;
        """)

        conn.execute("""
        CREATE TRIGGER IF NOT EXISTS update_status_fetched_timestamp
        AFTER UPDATE ON company_processing_state
        FOR EACH ROW
        WHEN NEW.processing_state = 'status_fetched' AND OLD.status_fetched_at IS NULL
        BEGIN
            UPDATE company_processing_state
            SET status_fetched_at = CURRENT_TIMESTAMP
            WHERE id = NEW.id;
        END;
        """)

        conn.execute("""
        CREATE TRIGGER IF NOT EXISTS update_officers_queued_timestamp
        AFTER UPDATE ON company_processing_state
        FOR EACH ROW
        WHEN NEW.processing_state = 'officers_queued' AND OLD.officers_queued_at IS NULL
        BEGIN
            UPDATE company_processing_state
            SET officers_queued_at = CURRENT_TIMESTAMP
            WHERE id = NEW.id;
        END;
        """)

        conn.execute("""
        CREATE TRIGGER IF NOT EXISTS update_officers_fetched_timestamp
        AFTER UPDATE ON company_processing_state
        FOR EACH ROW
        WHEN NEW.processing_state = 'officers_fetched' AND OLD.officers_fetched_at IS NULL
        BEGIN
            UPDATE company_processing_state
            SET officers_fetched_at = CURRENT_TIMESTAMP
            WHERE id = NEW.id;
        END;
        """)

        conn.execute("""
        CREATE TRIGGER IF NOT EXISTS update_completed_timestamp
        AFTER UPDATE ON company_processing_state
        FOR EACH ROW
        WHEN NEW.processing_state = 'completed' AND OLD.completed_at IS NULL
        BEGIN
            UPDATE company_processing_state
            SET completed_at = CURRENT_TIMESTAMP
            WHERE id = NEW.id;
        END;
        """)

        # Create trigger to track 429 responses
        conn.execute("""
        CREATE TRIGGER IF NOT EXISTS track_rate_limit_violations
        AFTER INSERT ON api_rate_limit_log
        FOR EACH ROW
        WHEN NEW.response_status = 429
        BEGIN
            UPDATE company_processing_state
            SET rate_limit_violations = rate_limit_violations + 1,
                last_429_response_at = CURRENT_TIMESTAMP
            WHERE company_number = NEW.company_number;
        END;
        """)

        # Create trigger to prevent direct API log insertions without proper tracking
        conn.execute("""
        CREATE TRIGGER IF NOT EXISTS validate_api_log_insertion
        BEFORE INSERT ON api_rate_limit_log
        FOR EACH ROW
        WHEN NEW.request_id IS NULL OR NEW.request_id = ''
        BEGIN
            SELECT RAISE(ABORT, 'API calls must have a request_id from the queue system');
        END;
        """)

        conn.commit()
        logger.info("Emergency safeguard triggers created successfully")

    except sqlite3.Error as e:
        logger.error(f"Failed to create safeguard triggers: {e}")
        raise


def drop_emergency_safeguard_triggers(conn: sqlite3.Connection) -> None:
    """Drop all emergency safeguard triggers.

    This should only be used for testing or emergency situations.

    Args:
        conn: SQLite database connection
    """
    triggers = [
        "validate_state_transition",
        "enforce_queue_request_ids",
        "update_updated_at_timestamp",
        "update_status_queued_timestamp",
        "update_status_fetched_timestamp",
        "update_officers_queued_timestamp",
        "update_officers_fetched_timestamp",
        "update_completed_timestamp",
        "track_rate_limit_violations",
        "validate_api_log_insertion",
    ]

    for trigger in triggers:
        try:
            conn.execute(f"DROP TRIGGER IF EXISTS {trigger}")
        except sqlite3.Error as e:
            logger.error(f"Failed to drop trigger {trigger}: {e}")

    conn.commit()
    logger.info("Emergency safeguard triggers dropped")


def verify_safeguard_triggers(conn: sqlite3.Connection) -> dict[str, bool]:
    """Verify that all safeguard triggers are installed and active.

    Args:
        conn: SQLite database connection

    Returns:
        Dictionary mapping trigger names to their installation status
    """
    expected_triggers = [
        "validate_state_transition",
        "enforce_queue_request_ids",
        "update_updated_at_timestamp",
        "update_status_queued_timestamp",
        "update_status_fetched_timestamp",
        "update_officers_queued_timestamp",
        "update_officers_fetched_timestamp",
        "update_completed_timestamp",
        "track_rate_limit_violations",
        "validate_api_log_insertion",
    ]

    cursor = conn.cursor()
    cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='trigger' AND tbl_name IN ('company_processing_state', 'api_rate_limit_log')"
    )
    installed_triggers = {row[0] for row in cursor.fetchall()}

    return {trigger: trigger in installed_triggers for trigger in expected_triggers}


def apply_safeguard_triggers_to_database(db_path: str = "companies.db") -> None:
    """Apply emergency safeguard triggers to the main database.

    Args:
        db_path: Path to the database file
    """
    try:
        conn = sqlite3.connect(db_path)
        create_emergency_safeguard_triggers(conn)

        # Verify installation
        status = verify_safeguard_triggers(conn)
        all_installed = all(status.values())

        if all_installed:
            logger.info("All safeguard triggers successfully installed")
        else:
            missing = [name for name, installed in status.items() if not installed]
            logger.warning(f"Missing safeguard triggers: {missing}")

        conn.close()

    except Exception as e:
        logger.error(f"Failed to apply safeguard triggers: {e}")
        raise


if __name__ == "__main__":
    # Apply triggers when run directly
    logging.basicConfig(level=logging.INFO)
    apply_safeguard_triggers_to_database()

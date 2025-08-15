# Test file for strike_off_scraper.py

import pytest
import requests
import time
from unittest.mock import MagicMock, call
import sqlite3 # Import for mocking
import logging

# Import necessary modules/functions from strike_off_scraper
# Assuming strike_off_scraper.py is in the parent directory relative to tests/
# Adjust import if your structure is different or using src-layout
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from strike_off_scraper import (
    _make_request_with_retry,
    _fetch_search_page,
    _fetch_and_process_profile,
    # Import DB functions
    init_db,
    get_last_start_index,
    update_last_start_index,
    save_company_record,
    CompanyRecord,
    AppSettings, 
    # Import the main function to test
    get_companies_with_strike_off_status,
    # Import constants needed for test data
    TARGET_STRIKE_OFF_STATUS,
    DEFAULT_ITEMS_PER_PAGE,
    PROFILE_FETCH_DELAY,
    SEARCH_BATCH_DELAY,
    # Add the new helpers
    _extract_appointment_id,
    _fetch_and_save_officer_data, # Add function to test
    save_officer_record, # Import for mocking
    OfficerRecord, # Import for creating expected data
    TARGET_OFFICER_ROLES, # Import for filter test
)


# --- Tests for _make_request_with_retry ---

@pytest.fixture
def mock_response():
    """Fixture to create a mock requests.Response object."""
    def _create_mock(status_code=200, json_data=None, text_data="", headers=None):
        mock = MagicMock(spec=requests.Response)
        mock.status_code = status_code
        mock.headers = headers or {}
        mock.text = text_data
        if json_data is not None:
            mock.json = MagicMock(return_value=json_data)
        
        # Mock raise_for_status behavior
        if 400 <= status_code < 600:
            mock.raise_for_status = MagicMock(side_effect=requests.exceptions.HTTPError(response=mock))
        else:
            mock.raise_for_status = MagicMock()
        return mock
    return _create_mock


def test_make_request_success_first_try(mocker, mock_response):
    """Test successful request on the first attempt.""" 
    mock_get = mocker.patch('requests.get')
    expected_response = mock_response(status_code=200, json_data={"key": "value"})
    mock_get.return_value = expected_response
    
    response = _make_request_with_retry("http://test.com", "fake_key")
    
    assert response == expected_response
    mock_get.assert_called_once_with("http://test.com", params=None, auth=("fake_key", ''), timeout=30)


def test_make_request_retry_429_then_success(mocker, mock_response):
    """Test retry on HTTP 429 (rate limit) then success.""" 
    mock_sleep = mocker.patch('time.sleep', return_value=None) # Prevent actual sleep
    mock_get = mocker.patch('requests.get')
    response_429 = mock_response(status_code=429, headers={"Retry-After": "0.1"}) # Use header
    response_ok = mock_response(status_code=200, json_data={"key": "ok"})
    mock_get.side_effect = [response_429, response_ok]
    
    response = _make_request_with_retry("http://test.com", "fake_key", max_retries=3, initial_backoff=0.01)
    
    assert response == response_ok
    assert mock_get.call_count == 2
    mock_sleep.assert_called_once_with(pytest.approx(0.1)) # Check sleep uses header value


def test_make_request_retry_500_then_success(mocker, mock_response):
    """Test retry on HTTP 500 (server error) then success."""
    mock_sleep = mocker.patch('time.sleep', return_value=None)
    mock_get = mocker.patch('requests.get')
    response_500 = mock_response(status_code=500)
    response_ok = mock_response(status_code=200, json_data={"key": "ok"})
    mock_get.side_effect = [response_500, response_500, response_ok]
    
    response = _make_request_with_retry("http://test.com", "fake_key", max_retries=3, initial_backoff=0.01)
    
    assert response == response_ok
    assert mock_get.call_count == 3
    assert mock_sleep.call_count == 2
    # Check sleeps use exponential backoff (approx due to floating point)
    mock_sleep.assert_has_calls([call(pytest.approx(0.01)), call(pytest.approx(0.02))])


def test_make_request_fail_after_max_retries_503(mocker, mock_response):
    """Test request fails permanently after max retries on 503.""" 
    mock_sleep = mocker.patch('time.sleep', return_value=None)
    mock_get = mocker.patch('requests.get')
    response_503 = mock_response(status_code=503)
    mock_get.return_value = response_503 # Always return 503
    
    response = _make_request_with_retry("http://test.com", "fake_key", max_retries=3, initial_backoff=0.01)
    
    assert response is None
    assert mock_get.call_count == 3
    assert mock_sleep.call_count == 3 # Sleeps before each retry attempt

def test_make_request_fail_non_retryable_404(mocker, mock_response):
    """Test request fails immediately on a non-retryable 404 error."""
    mock_sleep = mocker.patch('time.sleep', return_value=None)
    mock_get = mocker.patch('requests.get')
    response_404 = mock_response(status_code=404)
    mock_get.return_value = response_404
    
    response = _make_request_with_retry("http://test.com", "fake_key", max_retries=3)
    
    assert response is None
    mock_get.assert_called_once() # Should not retry
    mock_sleep.assert_not_called()


def test_make_request_retry_network_error_then_success(mocker, mock_response):
    """Test retry on requests.exceptions.RequestException then success."""
    mock_sleep = mocker.patch('time.sleep', return_value=None)
    mock_get = mocker.patch('requests.get')
    network_error = requests.exceptions.Timeout("Connection timed out")
    response_ok = mock_response(status_code=200, json_data={"key": "ok"})
    mock_get.side_effect = [network_error, response_ok]
    
    response = _make_request_with_retry("http://test.com", "fake_key", max_retries=3, initial_backoff=0.01)
    
    assert response == response_ok
    assert mock_get.call_count == 2
    mock_sleep.assert_called_once_with(pytest.approx(0.01))


def test_make_request_fail_network_error_max_retries(mocker, mock_response):
    """Test request fails after max retries on network errors."""
    mock_sleep = mocker.patch('time.sleep', return_value=None)
    mock_get = mocker.patch('requests.get')
    network_error = requests.exceptions.ConnectionError("Connection failed")
    mock_get.side_effect = network_error # Always raise network error
    
    response = _make_request_with_retry("http://test.com", "fake_key", max_retries=2, initial_backoff=0.01)
    
    assert response is None
    assert mock_get.call_count == 2
    assert mock_sleep.call_count == 2 # Sleeps before each retry


# --- Tests for _fetch_search_page ---

@pytest.fixture
def mock_settings():
    """Fixture to provide default AppSettings for tests."""
    # Provide minimal valid settings, including retry config used by helpers
    return AppSettings(api_key="dummy-key", max_retries=1, initial_backoff_secs=0.01)

def test_fetch_search_page_success(mocker, mock_response, mock_settings):
    """Test successful fetching and parsing of a search page."""
    # Arrange
    mock_retry_helper = mocker.patch('strike_off_scraper._make_request_with_retry')
    search_params = {"start_index": 0, "items_per_page": 10}
    expected_items = [{"company_number": "123"}, {"company_number": "456"}]
    mock_api_response = mock_response(status_code=200, json_data={"items": expected_items})
    mock_retry_helper.return_value = mock_api_response
    
    # Act
    result = _fetch_search_page("fake_key", search_params, mock_settings)
    
    # Assert
    assert result == expected_items
    mock_retry_helper.assert_called_once_with(
        url=mocker.ANY, # Don't need to assert exact URL constant here
        api_key="fake_key",
        params=search_params,
        max_retries=mock_settings.max_retries,
        initial_backoff=mock_settings.initial_backoff_secs
    )

def test_fetch_search_page_success_no_items(mocker, mock_response, mock_settings):
    """Test successful fetch when the API returns an empty items list."""
    # Arrange
    mock_retry_helper = mocker.patch('strike_off_scraper._make_request_with_retry')
    search_params = {"start_index": 100}
    mock_api_response = mock_response(status_code=200, json_data={"items": []})
    mock_retry_helper.return_value = mock_api_response
    
    # Act
    result = _fetch_search_page("fake_key", search_params, mock_settings)
    
    # Assert
    assert result == []
    mock_retry_helper.assert_called_once()

def test_fetch_search_page_success_missing_items_key(mocker, mock_response, mock_settings):
    """Test successful fetch when the API response JSON lacks the 'items' key."""
    # Arrange
    mock_retry_helper = mocker.patch('strike_off_scraper._make_request_with_retry')
    search_params = {"start_index": 0}
    mock_api_response = mock_response(status_code=200, json_data={"total_results": 0}) # No 'items' key
    mock_retry_helper.return_value = mock_api_response
    
    # Act
    result = _fetch_search_page("fake_key", search_params, mock_settings)
    
    # Assert
    assert result == [] # Should default to empty list
    mock_retry_helper.assert_called_once()

def test_fetch_search_page_request_fails(mocker, mock_settings):
    """Test behavior when the underlying request fails after retries."""
    # Arrange
    mock_retry_helper = mocker.patch('strike_off_scraper._make_request_with_retry')
    search_params = {"start_index": 0}
    mock_retry_helper.return_value = None # Simulate request failure
    
    # Act
    result = _fetch_search_page("fake_key", search_params, mock_settings)
    
    # Assert
    assert result is None
    mock_retry_helper.assert_called_once()

def test_fetch_search_page_json_decode_error(mocker, mock_response, mock_settings):
    """Test behavior when the API returns invalid JSON."""
    # Arrange
    mock_retry_helper = mocker.patch('strike_off_scraper._make_request_with_retry')
    search_params = {"start_index": 0}
    mock_api_response = mock_response(status_code=200, text_data="invalid json")
    # Make the .json() method raise an error
    mock_api_response.json = MagicMock(side_effect=ValueError("Decode error")) 
    mock_retry_helper.return_value = mock_api_response
    
    # Act
    result = _fetch_search_page("fake_key", search_params, mock_settings)
    
    # Assert
    assert result is None
    mock_retry_helper.assert_called_once()


# --- Tests for _fetch_and_process_profile ---

def test_fetch_profile_success(mocker, mock_response, mock_settings):
    """Test successful fetching and processing of a company profile."""
    # Arrange
    mock_retry_helper = mocker.patch('strike_off_scraper._make_request_with_retry')
    company_num = "12345678"
    api_profile_data = {
        "company_number": company_num,
        "company_name": "Test Ltd",
        "company_status": "active",
        "company_status_detail": "active-proposal-to-strike-off",
        "date_of_creation": "2023-01-01",
        "registered_office_address": {
            "address_line_1": "1 Test Street",
            "locality": "Testville",
            "postal_code": "T1 1TT"
        },
        "sic_codes": ["12345", "67890"]
    }
    mock_api_response = mock_response(status_code=200, json_data=api_profile_data)
    mock_retry_helper.return_value = mock_api_response
    
    expected_record = CompanyRecord(
        company_number=company_num,
        company_name="Test Ltd",
        company_status="active",
        company_status_detail="active-proposal-to-strike-off",
        incorporation_date="2023-01-01",
        address="1 Test Street, Testville, T1 1TT",
        sic_codes="12345, 67890"
    )

    # Act
    result = _fetch_and_process_profile(company_num, "fake_key", mock_settings)
    
    # Assert
    assert result == expected_record
    mock_retry_helper.assert_called_once_with(
        url=mocker.ANY, # Don't need to check exact formatted URL
        api_key="fake_key",
        max_retries=mock_settings.max_retries,
        initial_backoff=mock_settings.initial_backoff_secs
    )

def test_fetch_profile_request_fails(mocker, mock_settings):
    """Test profile processing when the API request fails."""
    # Arrange
    mock_retry_helper = mocker.patch('strike_off_scraper._make_request_with_retry')
    company_num = "87654321"
    mock_retry_helper.return_value = None # Simulate request failure
    
    # Act
    result = _fetch_and_process_profile(company_num, "fake_key", mock_settings)
    
    # Assert
    assert result is None
    mock_retry_helper.assert_called_once()

def test_fetch_profile_json_decode_error(mocker, mock_response, mock_settings):
    """Test profile processing when the API returns invalid JSON."""
    # Arrange
    mock_retry_helper = mocker.patch('strike_off_scraper._make_request_with_retry')
    company_num = "11223344"
    mock_api_response = mock_response(status_code=200, text_data="not json")
    mock_api_response.json = MagicMock(side_effect=ValueError("Bad JSON"))
    mock_retry_helper.return_value = mock_api_response
    
    # Act
    result = _fetch_and_process_profile(company_num, "fake_key", mock_settings)
    
    # Assert
    assert result is None
    mock_retry_helper.assert_called_once()


# --- Tests for Database Functions ---

@pytest.fixture
def mock_db(mocker):
    """Fixture to mock sqlite3 connection and cursor."""
    mock_conn = MagicMock(spec=sqlite3.Connection)
    mock_cursor = MagicMock(spec=sqlite3.Cursor)
    mock_conn.cursor.return_value = mock_cursor
    # Mock connect globally and return the mock for assertion
    mock_connect = mocker.patch('sqlite3.connect', return_value=mock_conn)
    return mock_conn, mock_cursor, mock_connect # Return connect mock too

def test_init_db(mocker, mock_db):
    """Test database initialization logic."""
    # Unpack the connect mock from the fixture
    mock_conn, mock_cursor, mock_connect = mock_db 
    db_path = "test.db"
    
    conn = init_db(db_path)
    
    # Assert connection was attempted using the mock from the fixture
    mock_connect.assert_called_once_with(db_path, timeout=10)
    assert conn == mock_conn
    
    # Check key execute calls less strictly due to potential whitespace issues
    execute_calls = mock_cursor.execute.call_args_list
    assert any("CREATE TABLE IF NOT EXISTS companies" in str(c[0][0]) for c in execute_calls)
    assert any("CREATE TABLE IF NOT EXISTS officers" in str(c[0][0]) for c in execute_calls)
    assert any("CREATE TABLE IF NOT EXISTS metadata" in str(c[0][0]) for c in execute_calls)
    assert call("INSERT OR IGNORE INTO metadata (key, value) VALUES (?, ?);", ('last_start_index', 0)) in execute_calls
    # Check total number of execute calls expected
    assert len(execute_calls) == 4 # Create companies, officers, metadata, insert metadata

    mock_conn.commit.assert_called_once()

def test_get_last_start_index_success(mock_db):
    """Test retrieving the last start index successfully."""
    mock_conn, mock_cursor, _ = mock_db
    mock_cursor.fetchone.return_value = (100,) # Simulate finding the value
    
    index = get_last_start_index(mock_conn)
    
    assert index == 100
    mock_cursor.execute.assert_called_once_with(
        "SELECT value FROM metadata WHERE key = ?;", ('last_start_index',)
    )

def test_get_last_start_index_not_found(mock_db):
    """Test retrieving when the index key is somehow missing (should default to 0)."""
    mock_conn, mock_cursor, _ = mock_db
    mock_cursor.fetchone.return_value = None # Simulate not finding the value
    
    index = get_last_start_index(mock_conn)
    
    assert index == 0
    mock_cursor.execute.assert_called_once()

def test_get_last_start_index_db_error(mock_db):
    """Test retrieval when a database error occurs."""
    mock_conn, mock_cursor, _ = mock_db # Unpack fixture
    mock_cursor.execute.side_effect = sqlite3.DatabaseError("Test DB Error")
    
    index = get_last_start_index(mock_conn)
    
    assert index == 0

def test_update_last_start_index(mock_db):
    """Test updating the last start index."""
    mock_conn, mock_cursor, _ = mock_db
    new_index = 200
    
    update_last_start_index(mock_conn, new_index)
    
    mock_cursor.execute.assert_called_once_with(
        "INSERT OR REPLACE INTO metadata (key, value) VALUES (?, ?);",
        ('last_start_index', new_index)
    )
    mock_conn.commit.assert_not_called() # Commit should happen elsewhere

def test_save_company_record_success(mock_db):
    """Test saving a CompanyRecord successfully."""
    mock_conn, mock_cursor, _ = mock_db
    record = CompanyRecord(
        company_number="111",
        company_name="Save Test Ltd",
        company_status="active",
        company_status_detail="active",
        incorporation_date="2022-02-02",
        address="1 Save Street",
        sic_codes="10000"
    )
    
    save_company_record(mock_conn, record)
    
    expected_sql = 'INSERT OR IGNORE INTO companies ("company_number", "company_name", "company_status", "company_status_detail", "incorporation_date", "address", "sic_codes") VALUES (?, ?, ?, ?, ?, ?, ?);'
    expected_values = ("111", "Save Test Ltd", "active", "active", "2022-02-02", "1 Save Street", "10000")
    
    mock_cursor.execute.assert_called_once_with(expected_sql, expected_values)
    mock_conn.commit.assert_not_called() # Commit should happen elsewhere

def test_save_company_record_none(mock_db):
    """Test that saving None does nothing."""
    mock_conn, mock_cursor, _ = mock_db
    save_company_record(mock_conn, None)
    mock_cursor.execute.assert_not_called()
    mock_conn.commit.assert_not_called()


# --- Integration Tests for get_companies_with_strike_off_status ---

@pytest.fixture
def mock_helpers(mocker):
    """Fixture to mock all helper functions used by the main orchestrator."""
    mocks = {
        'get_last_start_index': mocker.patch('strike_off_scraper.get_last_start_index'),
        '_fetch_search_page': mocker.patch('strike_off_scraper._fetch_search_page'),
        '_fetch_and_process_profile': mocker.patch('strike_off_scraper._fetch_and_process_profile'),
        'save_company_record': mocker.patch('strike_off_scraper.save_company_record'),
        'update_last_start_index': mocker.patch('strike_off_scraper.update_last_start_index'),
        'sleep': mocker.patch('time.sleep'),
        # Add mock for the new officer helper
        '_fetch_and_save_officer_data': mocker.patch('strike_off_scraper._fetch_and_save_officer_data')
    }
    return mocks

def test_orchestrator_basic_run_one_page(mocker, mock_helpers, mock_settings, mock_db):
    """Test a basic run processing one page and stopping. 
       Verify officer fetch is called for strike-off company.
    """
    mock_conn, mock_cursor, _ = mock_db
    mock_settings.max_strike_off_results = 5 
    api_key = "test-key"
    mock_helpers['get_last_start_index'].return_value = 0
    page_0_summaries = [
        {'company_number': '101'}, {'company_number': '102'}, {'company_number': '103'}
    ]
    company_101 = CompanyRecord(company_number='101', company_name='Active Ltd', company_status_detail='active')
    company_102 = CompanyRecord(company_number='102', company_name='StrikeOff Ltd', company_status_detail=TARGET_STRIKE_OFF_STATUS)
    mock_helpers['_fetch_search_page'].side_effect = [page_0_summaries, []] 
    mock_helpers['_fetch_and_process_profile'].side_effect = [company_101, company_102, None] 
    
    # Act
    get_companies_with_strike_off_status(api_key, mock_settings, mock_conn)
    
    # Assertions
    assert mock_helpers['_fetch_search_page'].call_count == 2
    assert mock_helpers['_fetch_and_process_profile'].call_count == 3
    assert mock_helpers['save_company_record'].call_count == 2
    
    # *** Assert Officer fetch was called ONLY for the strike-off company (102) ***
    mock_helpers['_fetch_and_save_officer_data'].assert_called_once_with(
        company_102.company_number, api_key, mock_settings, mock_conn
    )

    # Check progress update
    mock_helpers['update_last_start_index'].assert_called_once_with(mock_conn, DEFAULT_ITEMS_PER_PAGE)
    mock_conn.commit.assert_called_once()
    assert mock_helpers['sleep'].call_count == 3 + 1 

def test_orchestrator_resume_run(mock_helpers, mock_settings, mock_db):
    """Test resuming from a non-zero start index."""
    mock_conn, mock_cursor, _ = mock_db
    api_key = "test-key"
    resume_index = 200
    
    # Configure Mocks
    mock_helpers['get_last_start_index'].return_value = resume_index
    page_resume_summaries = [{'company_number': '301'}]
    company_301 = CompanyRecord(company_number='301')
    mock_helpers['_fetch_search_page'].side_effect = [page_resume_summaries, []]
    mock_helpers['_fetch_and_process_profile'].return_value = company_301
    
    # Act
    get_companies_with_strike_off_status(api_key, mock_settings, mock_conn)
    
    # Assertions
    mock_helpers['get_last_start_index'].assert_called_once_with(mock_conn)
    
    # Check search page fetching starts at resume_index
    assert mock_helpers['_fetch_search_page'].call_count == 2
    assert mock_helpers['_fetch_search_page'].call_args_list[0][0][1]['start_index'] == resume_index
    assert mock_helpers['_fetch_search_page'].call_args_list[1][0][1]['start_index'] == resume_index + DEFAULT_ITEMS_PER_PAGE
    
    # Check progress update uses the next index
    next_index = resume_index + DEFAULT_ITEMS_PER_PAGE
    mock_helpers['update_last_start_index'].assert_called_once_with(mock_conn, next_index)
    mock_conn.commit.assert_called_once()

def test_orchestrator_search_fetch_fails(mock_helpers, mock_settings, mock_db):
    """Test orchestrator stops if fetching a search page fails."""
    mock_conn, mock_cursor, _ = mock_db
    api_key = "test-key"
    
    # Configure Mocks
    mock_helpers['get_last_start_index'].return_value = 0
    mock_helpers['_fetch_search_page'].return_value = None # Simulate failure
    
    # Act
    get_companies_with_strike_off_status(api_key, mock_settings, mock_conn)
    
    # Assertions
    mock_helpers['get_last_start_index'].assert_called_once()
    mock_helpers['_fetch_search_page'].assert_called_once() # Called once before failing
    # Ensure no processing or saving happened
    mock_helpers['_fetch_and_process_profile'].assert_not_called()
    mock_helpers['save_company_record'].assert_not_called()
    mock_helpers['update_last_start_index'].assert_not_called()
    mock_conn.commit.assert_not_called()
    mock_helpers['sleep'].assert_not_called()

def test_orchestrator_commit_fails(mock_helpers, mock_settings, mock_db):
    """Test orchestrator stops if committing progress fails."""
    mock_conn, mock_cursor, _ = mock_db
    api_key = "test-key"

    # Configure Mocks
    mock_helpers['get_last_start_index'].return_value = 0
    page_0_summaries = [{'company_number': '101'}]
    company_101 = CompanyRecord(company_number='101')
    mock_helpers['_fetch_search_page'].side_effect = [page_0_summaries, []] # Stop after one page
    mock_helpers['_fetch_and_process_profile'].return_value = company_101
    mock_conn.commit.side_effect = sqlite3.DatabaseError("Commit Failed") # Simulate commit error

    # Act
    get_companies_with_strike_off_status(api_key, mock_settings, mock_conn)

    # Assertions
    mock_helpers['get_last_start_index'].assert_called_once()
    mock_helpers['_fetch_search_page'].assert_called_once()
    mock_helpers['_fetch_and_process_profile'].assert_called_once()
    mock_helpers['save_company_record'].assert_called_once_with(mock_conn, company_101)
    # Update index IS called before commit fails
    mock_helpers['update_last_start_index'].assert_called_once_with(mock_conn, DEFAULT_ITEMS_PER_PAGE)
    # Commit IS called
    mock_conn.commit.assert_called_once()
    # Ensure sleep for next batch didn't happen because loop should break
    assert mock_helpers['sleep'].call_count == 1 # Only the profile fetch sleep
    mock_helpers['sleep'].assert_called_once_with(PROFILE_FETCH_DELAY)

# Remove final TODO
# --- TODO Tests --- 

# --- Tests for Officer Helper Functions ---

@pytest.mark.parametrize(
    "link, expected_id",
    [
        ("/company/123/appointments/abcDEF123_-", "abcDEF123_-"),
        ("/company/14721121/appointments/QgnmXzMRFsiDiGYHgrYtqx85DRM", "QgnmXzMRFsiDiGYHgrYtqx85DRM"),
        ("/company/SC123/appointments/AnotherID", "AnotherID"),
    ]
)
def test_extract_appointment_id_success(link, expected_id):
    """Test successful extraction of appointment ID from various links."""
    assert _extract_appointment_id(link) == expected_id

@pytest.mark.parametrize(
    "invalid_link",
    [
        "/company/123/officers/abc", # Wrong path segment
        "/company/123/appointments/", # Missing ID
        "invalid-string",
        "",
        None, # Add test case for None
    ]
)
def test_extract_appointment_id_failure(invalid_link):
    """Test failure cases for appointment ID extraction, including None."""
    assert _extract_appointment_id(invalid_link) is None


# --- Tests for _fetch_and_save_officer_data ---

# Sample officer data for tests
OFFICER_ITEM_DIRECTOR = {
    "name": "DOE, John",
    "officer_role": "director",
    "appointed_on": "2020-01-01",
    "links": {"self": "/company/123/appointments/AppointID1"}
    # Add other relevant fields used by OfficerRecord
}
OFFICER_ITEM_SECRETARY = {
    "name": "SMITH, Jane",
    "officer_role": "secretary",
    "appointed_on": "2021-02-02",
    "links": {"self": "/company/123/appointments/AppointID2"}
}
OFFICER_ITEM_MANAGING_DIRECTOR = {
    "name": "ROE, Richard",
    "officer_role": "managing-director",
    "appointed_on": "2019-03-03",
    "links": {"self": "/company/123/appointments/AppointID3"}
}


def test_fetch_officers_success_one_page_filtered(mocker, mock_response, mock_settings, mock_db):
    """Test fetching officers, filtering by role, and saving."""
    # Arrange
    mock_conn, mock_cursor, _ = mock_db
    mock_retry_helper = mocker.patch('strike_off_scraper._make_request_with_retry')
    mock_save_officer = mocker.patch('strike_off_scraper.save_officer_record')
    mock_extract_id = mocker.patch('strike_off_scraper._extract_appointment_id', side_effect=lambda x: x.split('/')[-1])
    
    company_num = "12345"
    api_response_data = {
        "items": [OFFICER_ITEM_DIRECTOR, OFFICER_ITEM_SECRETARY, OFFICER_ITEM_MANAGING_DIRECTOR],
        "total_results": 3,
        "items_per_page": 35,
        "start_index": 0
    }
    mock_api_response = mock_response(status_code=200, json_data=api_response_data)
    mock_retry_helper.return_value = mock_api_response
    
    # Act
    _fetch_and_save_officer_data(company_num, "fake_key", mock_settings, mock_conn)
    
    # Assert
    mock_retry_helper.assert_called_once()
    assert mock_extract_id.call_count == 2 # Only called for director and managing-director
    assert mock_save_officer.call_count == 2
    
    # Check that save was called with correctly constructed OfficerRecord objects
    saved_records = [call_args[0][1] for call_args in mock_save_officer.call_args_list]
    assert any(r.appointment_id == "AppointID1" and r.officer_role == "director" for r in saved_records)
    assert any(r.appointment_id == "AppointID3" and r.officer_role == "managing-director" for r in saved_records)
    assert not any(r.appointment_id == "AppointID2" for r in saved_records) # Secretary not saved

def test_fetch_officers_success_no_officers(mocker, mock_response, mock_settings, mock_db):
    """Test fetching when API reports zero officers."""
    # Arrange
    mock_conn, mock_cursor, _ = mock_db
    mock_retry_helper = mocker.patch('strike_off_scraper._make_request_with_retry')
    mock_save_officer = mocker.patch('strike_off_scraper.save_officer_record')
    mock_extract_id = mocker.patch('strike_off_scraper._extract_appointment_id')
    
    company_num = "67890"
    api_response_data = {"items": [], "total_results": 0}
    mock_api_response = mock_response(status_code=200, json_data=api_response_data)
    mock_retry_helper.return_value = mock_api_response
    
    # Act
    _fetch_and_save_officer_data(company_num, "fake_key", mock_settings, mock_conn)
    
    # Assert
    mock_retry_helper.assert_called_once() 
    mock_extract_id.assert_not_called()
    mock_save_officer.assert_not_called()

def test_fetch_officers_pagination(mocker, mock_response, mock_settings, mock_db):
    """Test that pagination is handled correctly for the officer endpoint."""
     # Arrange
    mock_conn, mock_cursor, _ = mock_db
    mock_retry_helper = mocker.patch('strike_off_scraper._make_request_with_retry')
    mock_save_officer = mocker.patch('strike_off_scraper.save_officer_record')
    mock_extract_id = mocker.patch('strike_off_scraper._extract_appointment_id', side_effect=lambda x: x.split('/')[-1])
    mock_sleep = mocker.patch('time.sleep')
    company_num = "PAGINATE"
    # Correct items_per_page to match hardcoded value in function
    items_per_page = 35 
    officer1 = {"name": "Dir 1", "officer_role": "director", "links": {"self": "/app/1"}}
    officer2 = {"name": "Sec 1", "officer_role": "secretary", "links": {"self": "/app/2"}}
    officer3 = {"name": "Dir 2", "officer_role": "director", "links": {"self": "/app/3"}}
    # Adjust mock API responses to simulate pagination with items_per_page = 35
    # Simulate needing only one call since total_results=3 < items_per_page=35
    page1_data = {"items": [officer1, officer2, officer3], "total_results": 3, "items_per_page": items_per_page, "start_index": 0}
    mock_api_response1 = mock_response(status_code=200, json_data=page1_data)
    mock_retry_helper.return_value = mock_api_response1 # Only one response needed
    
    # Act
    _fetch_and_save_officer_data(company_num, "fake_key", mock_settings, mock_conn)

    # Assert
    # Fix: Only one call should be made as total_results < items_per_page
    assert mock_retry_helper.call_count == 1 
    # Check params for pagination
    assert mock_retry_helper.call_args_list[0][1]['params'] == {"items_per_page": items_per_page, "start_index": 0}
    
    # Fix: Extract ID called only for items passing filter (officer1, officer3)
    assert mock_extract_id.call_count == 2 
    assert mock_save_officer.call_count == 2 # Dir 1, Dir 2 saved
    assert mock_sleep.call_count == 0 # No sleep between pages needed

def test_fetch_officers_request_fails(mocker, mock_settings, mock_db):
    """Test behaviour when fetching officer page fails."""
    # Arrange
    mock_conn, mock_cursor, _ = mock_db
    mock_retry_helper = mocker.patch('strike_off_scraper._make_request_with_retry', return_value=None)
    mock_save_officer = mocker.patch('strike_off_scraper.save_officer_record')

    # Act
    _fetch_and_save_officer_data("FAIL", "fake_key", mock_settings, mock_conn)

    # Assert
    mock_retry_helper.assert_called_once() 
    mock_save_officer.assert_not_called()

# Remove final TODO
# --- TODO Tests --- 
# TODO: Add tests for _fetch_and_save_officer_data
# TODO: Add tests for database helpers (init_db, get_last_start_index, etc. if not covered)
# TODO: Update integration tests for get_companies_with_strike_off_status 
#!/usr/bin/env python3
"""
Render service scheduler for Companies House streaming.
Controls starting/stopping the service during UK business hours.
"""

import os
import sys
import json
import requests
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Render API configuration
RENDER_API_KEY = os.environ.get('RENDER_API_KEY')
SERVICE_ID = os.environ.get('SERVICE_ID')
RENDER_API_URL = 'https://api.render.com/v1'

# UK bank holidays (update yearly or fetch from gov.uk API)
UK_BANK_HOLIDAYS_2025 = [
    '2025-01-01',  # New Year's Day
    '2025-04-18',  # Good Friday
    '2025-04-21',  # Easter Monday
    '2025-05-05',  # Early May bank holiday
    '2025-05-26',  # Spring bank holiday
    '2025-08-25',  # Summer bank holiday
    '2025-12-25',  # Christmas Day
    '2025-12-26',  # Boxing Day
]

def is_uk_bank_holiday(date=None):
    """Check if given date is a UK bank holiday."""
    if date is None:
        date = datetime.now().strftime('%Y-%m-%d')
    return date in UK_BANK_HOLIDAYS_2025

def get_service_status():
    """Get current status of the streaming service."""
    headers = {
        'Authorization': f'Bearer {RENDER_API_KEY}',
        'Accept': 'application/json'
    }

    try:
        response = requests.get(
            f'{RENDER_API_URL}/services/{SERVICE_ID}',
            headers=headers,
            timeout=30
        )
        response.raise_for_status()
        service = response.json()
        return service.get('service', {}).get('suspended', True)
    except Exception as e:
        logger.error(f"Failed to get service status: {e}")
        return None

def suspend_service():
    """Suspend (stop) the streaming service."""
    headers = {
        'Authorization': f'Bearer {RENDER_API_KEY}',
        'Accept': 'application/json',
        'Content-Type': 'application/json'
    }

    try:
        response = requests.post(
            f'{RENDER_API_URL}/services/{SERVICE_ID}/suspend',
            headers=headers,
            timeout=30
        )
        response.raise_for_status()
        logger.info(f"Service suspended successfully at {datetime.now()}")
        return True
    except Exception as e:
        logger.error(f"Failed to suspend service: {e}")
        return False

def resume_service():
    """Resume (start) the streaming service."""
    headers = {
        'Authorization': f'Bearer {RENDER_API_KEY}',
        'Accept': 'application/json',
        'Content-Type': 'application/json'
    }

    try:
        response = requests.post(
            f'{RENDER_API_URL}/services/{SERVICE_ID}/resume',
            headers=headers,
            timeout=30
        )
        response.raise_for_status()
        logger.info(f"Service resumed successfully at {datetime.now()}")
        return True
    except Exception as e:
        logger.error(f"Failed to resume service: {e}")
        return False

def should_be_running():
    """Determine if service should be running based on current UK time."""
    now = datetime.now()

    # Check if it's a weekend
    if now.weekday() >= 5:  # Saturday = 5, Sunday = 6
        return False

    # Check if it's a bank holiday
    if is_uk_bank_holiday():
        return False

    # Check if within business hours (8:30 AM - 5:30 PM)
    current_time = now.time()
    start_time = datetime.strptime('08:30', '%H:%M').time()
    end_time = datetime.strptime('17:30', '%H:%M').time()

    return start_time <= current_time <= end_time

def save_state(state):
    """Save service state to file for recovery."""
    state_file = '/opt/render/project/data/service_state.json'
    try:
        os.makedirs(os.path.dirname(state_file), exist_ok=True)
        with open(state_file, 'w') as f:
            json.dump({
                'timestamp': datetime.now().isoformat(),
                'state': state,
                'suspended': state == 'stopped'
            }, f)
    except Exception as e:
        logger.error(f"Failed to save state: {e}")

def main(action):
    """Main scheduler function."""
    if not RENDER_API_KEY or not SERVICE_ID:
        logger.error("Missing RENDER_API_KEY or SERVICE_ID environment variables")
        sys.exit(1)

    if action == 'start':
        # Check if it's actually time to start
        if not should_be_running():
            logger.info("Outside business hours or bank holiday - not starting service")
            return

        # Check if already running
        is_suspended = get_service_status()
        if is_suspended is False:
            logger.info("Service is already running")
            return

        # Start the service
        if resume_service():
            save_state('running')
            logger.info("Streaming service started for business hours")
        else:
            logger.error("Failed to start streaming service")
            sys.exit(1)

    elif action == 'stop':
        # Check if already stopped
        is_suspended = get_service_status()
        if is_suspended is True:
            logger.info("Service is already suspended")
            return

        # Stop the service
        if suspend_service():
            save_state('stopped')
            logger.info("Streaming service stopped after business hours")
        else:
            logger.error("Failed to stop streaming service")
            sys.exit(1)

    elif action == 'check':
        # Health check - ensure service state matches schedule
        should_run = should_be_running()
        is_suspended = get_service_status()

        if should_run and is_suspended:
            logger.warning("Service should be running but is suspended - starting")
            resume_service()
            save_state('running')
        elif not should_run and not is_suspended:
            logger.warning("Service should be suspended but is running - stopping")
            suspend_service()
            save_state('stopped')
        else:
            logger.info(f"Service state correct - Running: {not is_suspended}, Should run: {should_run}")

    else:
        logger.error(f"Unknown action: {action}")
        print("Usage: render_scheduler.py [start|stop|check]")
        sys.exit(1)

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: render_scheduler.py [start|stop|check]")
        sys.exit(1)

    main(sys.argv[1])

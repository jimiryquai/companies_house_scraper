#!/usr/bin/env python3
"""Test streaming API connection only."""
import asyncio
import os
import sys

from dotenv import load_dotenv

from src.streaming import StreamingClient, StreamingConfig

load_dotenv()


async def test_streaming():
    """Test streaming API connection."""
    streaming_api_key = os.getenv("CH_STREAMING_API_KEY", "")
    companies_api_key = os.getenv("CH_COMPANIES_API_KEY", "")

    if not streaming_api_key or not companies_api_key:
        print("ERROR: Missing API keys")
        sys.exit(1)

    print(f"Creating StreamingConfig...")
    config = StreamingConfig(
        streaming_api_key=streaming_api_key,
        rest_api_key=companies_api_key,
    )

    print(f"Creating StreamingClient...")
    client = StreamingClient(config)

    print(f"Connecting to streaming API...")
    await client.connect()
    print(f"✓ Connected successfully!")

    print(f"Streaming events for 10 seconds...")
    count = 0
    try:
        async for event in client.stream_events():
            count += 1
            company_id = event.get("resource_id", "unknown")
            event_type = event.get("event", {}).get("type", "unknown")
            print(f"  Event #{count}: {event_type} for company {company_id}")

            if count >= 5:  # Just get 5 events to verify it works
                print(f"✓ Received {count} events successfully!")
                break
    except asyncio.TimeoutError:
        print(f"Timeout - received {count} events")
    finally:
        await client.disconnect()
        print(f"✓ Disconnected")


if __name__ == "__main__":
    asyncio.run(test_streaming())

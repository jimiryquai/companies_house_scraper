#!/bin/bash
export CH_STREAMING_API_KEY=d0b7e04e-a90f-4a7b-b48b-83a0b0bf8eed
export CH_COMPANIES_API_KEY=fdb5bed5-a7f6-4547-8157-86b7e49c6095
export CH_OFFICERS_API_KEY=fdb5bed5-a7f6-4547-8157-86b7e49c6095
export DATABASE_URL="postgresql://testuser:testpass@localhost:5433/companies_test"

echo "Starting streaming service locally..."
uv run python main.py

# Claude Code Instructions

## Project Setup

This project uses `uv` for Python dependency management. 

### Important Commands

**Always use `uv` for Python operations:**

- Install dependencies: `uv sync`
- Add new dependencies: `uv add <package>`
- Add dev dependencies: `uv add --dev <package>`
- Run Python scripts: `uv run python <script.py>`
- Run tests: `uv run python -m pytest`
- Run specific test files: `uv run python -m pytest tests/streaming/test_event_processor.py -v`

### DO NOT USE
- `pip install` 
- `python -m pytest` (without uv run)
- Direct `python` commands without `uv run`

## Current Development Status

**Officer Import Running**: The officer_import.py script is running in the background, processing ~2100+ companies out of 395,412 total.

**Streaming API Implementation**: Working on Week 2 tasks for real-time Companies House monitoring.

## Test Commands

```bash
# Run all streaming tests
uv run python -m pytest tests/streaming/ -v

# Run specific test file
uv run python -m pytest tests/streaming/test_event_processor.py -v

# Run with coverage
uv run python -m pytest --cov=src/streaming tests/streaming/
```

## Database

- Main database: `companies.db`
- Schema version: 2 (includes streaming metadata)
- Officer table fixed: `role` column renamed to `officer_role`
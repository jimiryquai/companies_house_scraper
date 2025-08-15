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

## Coding Standards and Guidelines

**MANDATORY: Follow standards from `global-rules copy.md` for ALL development:**

### Code Quality Requirements
- **Type Hints**: Always use Python type hints for all variables, functions, and returns
- **PEP 8 Compliance**: Strict adherence to PEP 8 styling
- **Docstrings**: Use Google-style docstrings for all public functions/classes
- **Error Handling**: Use specific exception types, avoid bare `except:`
- **Single Responsibility**: Functions < 25 lines, focused on one task

### Testing Philosophy (Based on Google Testing Blog)
- **Real Implementation Testing**: Prefer real functionality tests over mocks
- **Test Distribution**: Focus on integration tests that validate actual behavior
- **Coverage**: Prioritize testing critical functionality over coverage percentage
- **No Heavy Mocking**: Use real database operations, real JSON parsing, real logic

### Architecture Standards
- **SOLID Principles**: Follow all SOLID principles in class design
- **Composition over Inheritance**: Favor object composition
- **Database Patterns**: Use optimistic locking, idempotency keys, transactions
- **Security**: Validate all inputs, use environment variables for secrets

### Development Workflow
- **TDD Adapted**: Write real functionality tests first, then implement
- **Type Check First**: Run `mypy` before and after changes
- **Guard Clauses**: Use early returns to avoid deep nesting
- **Immutability**: Prefer immutable data structures where possible

**CRITICAL: Reference `global-rules copy.md` for complete standards during all development work.**

## Pre-Code Checklist (MANDATORY for every code change)

**Before writing ANY code, verify:**
- [ ] Type hints planned for all parameters and return values
- [ ] Google-style docstrings planned for public functions/classes
- [ ] Error handling strategy with specific exception types
- [ ] Function design: single responsibility, < 25 lines
- [ ] Guard clauses planned to avoid deep nesting
- [ ] Real implementation tests planned (not mocks)
- [ ] Data validation strategy defined
- [ ] Security considerations reviewed (input validation, secrets handling)

## Post-Code Validation (MANDATORY)

**After writing code, run these commands in order:**
```bash
# 1. Format and lint code
uv run ruff check .
uv run ruff format .

# 2. Type checking
uv run mypy .

# 3. Run relevant tests
uv run python -m pytest [test_file_path] -v

# 4. Check all tests still pass
uv run python -m pytest tests/streaming/ -v
```

**Only proceed if ALL commands pass without errors.**

## Task Execution Rules

**CRITICAL: When using /execute-tasks command:**

**STEP 1: CODING STANDARDS COMPLIANCE (MANDATORY FIRST STEP)**
- Before ANY code changes, complete the Pre-Code Checklist above
- After ANY code changes, run the Post-Code Validation commands above
- All ruff, mypy, and tests must pass before proceeding to next task

**STEP 2: TASK EXECUTION**
1. **ALWAYS update the tasks.md file** after completing any task
2. Mark completed tasks with `[x]` in the appropriate tasks.md file
3. The tasks.md file is located at: `.agent-os/specs/2025-08-15-streaming-api-integration/tasks.md`
4. **This is MANDATORY** - do not forget to update the task status
5. Use the Edit tool to change `[ ]` to `[x]` for completed tasks

**STEP 3: VALIDATION**
- Run pre-commit hooks validation: `uv run pre-commit run --all-files`
- Ensure all git commits pass pre-commit hooks automatically
- Only bypass hooks with `--no-verify` in absolute emergencies

**Example:**
```
**Task 4.1: Health Monitoring (TDD)**
- [x] Write tests for health check functionality  ← Mark as completed
- [x] Implement stream health monitoring         ← Mark as completed
```

**FAILURE TO FOLLOW CODING STANDARDS OR UPDATE TASKS.MD WILL RESULT IN INCOMPLETE TASK EXECUTION.**

## Quick Validation Command

Run all checks at once:
```bash
uv run ruff check . && uv run ruff format . && uv run mypy . && uv run python -m pytest tests/streaming/ -v
```

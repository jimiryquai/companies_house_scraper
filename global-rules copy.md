---
description:
globs:
alwaysApply: false
---
---
description: These are the coding standards and guidelines for the Python project.
globs:
alwaysApply: false
---
# Cursor Rules: Python Edition

You are a senior Python programmer with experience in modern web frameworks (e.g., FastAPI, Flask, Django), backend systems, data modeling, and potentially interacting with frontend frameworks if applicable. You have a preference for clean code, robust design patterns, and maintainable systems.

Generate code, corrections, and refactorings that comply with the principles and nomenclature outlined below.

## Python General Guidelines

### Basic Principles

-   **Language:** Use English for all code, documentation, and comments to maintain consistency and enable global collaboration.
-   **Type Hinting:** Always use Python's type hints for variables, function parameters, and return values for improved type safety, code clarity, and tooling support (e.g., MyPy).
    -   Avoid using `typing.Any` whenever a more specific type is possible.
    -   Create necessary types using `dataclasses`, `typing.TypedDict`, `enum.Enum`, or libraries like `Pydantic` to model your domain accurately.
    -   Utilize modern type hinting features (e.g., `|` for Unions in Python 3.10+, `typing.Self`).
-   **Dependency Management:** Use `Poetry` or `PDM` for robust dependency management, environment isolation, and package publishing. Define dependencies clearly in `pyproject.toml`.
-   **Docstrings:** Use docstrings (following Google, NumPy, or reStructuredText style - **choose one and be consistent**) to document public modules, classes, functions, and methods. Include examples where helpful.
-   **Code Density:** Avoid excessive blank lines within functions or methods to maintain readability. Use blank lines purposefully to separate logical blocks.
-   **Module Structure:** Aim for one primary class or cohesive set of functions per file (`.py` module) to ensure clear boundaries and improve organization. Use packages (`directories` with `__init__.py`) to group related modules.
-   **Function Definitions:** Prefer standard `def` statements. Use `lambda` only for very simple, short, anonymous functions where readability is not sacrificed.
-   **Styling/Formatting:**
    -   Adhere strictly to **PEP 8**. Use tools like `Black` for automated code formatting and `Ruff` or `Flake8` (with plugins like `flake8-bugbear`) for linting. Use `isort` for import sorting. Configure these in `pyproject.toml`.
    -   *(If applicable, e.g., using Jinja2/HTML)*: Follow similar CSS principles (Flexbox/Grid over margins) if generating frontend code or templates.

### Nomenclature (PEP 8 Emphasis)

-   **Classes:** `PascalCase`.
-   **Variables, Functions, Methods, Modules, Packages:** `snake_case`.
-   **Constants:** `UPPER_SNAKE_CASE`. Define constants instead of using magic numbers or strings directly in code.
-   **Protected Members:** Single leading underscore (`_protected_member`).
-   **Private (Name-Mangling):** Double leading underscore (`__private_member`) - use sparingly.
-   **Environment Variables:** `UPPER_SNAKE_CASE` (usually accessed via settings/config modules).
-   **Function/Method Naming:** Start with a verb (e.g., `calculate_total`, `get_user_data`).
-   **Boolean Variables/Functions:** Use prefixes like `is_`, `has_`, `can_`, `should_` (e.g., `is_loading`, `has_permission`).
-   **Clarity:** Use complete words; avoid abbreviations except for universally understood ones (e.g., `i`, `j` in loops; `e` or `err` for exceptions; `ctx` for context objects; framework-specific like `request`, `response`).

### Functions and Methods

-   **Single Responsibility:** Write short functions/methods focused on a single task (aim for < 25 lines, adjust as needed but keep focus).
-   **Nesting:** Avoid deep nesting. Use guard clauses (early returns) and helper function extraction.
-   **Comprehensions/Generators:** Prefer list/dict/set comprehensions and generator expressions over `map` and `filter` where they improve readability.
-   **Parameters:**
    -   Use default parameter values where appropriate.
    -   For functions with many parameters (> 3-4), consider grouping them into a `dataclass` or `Pydantic` model for clarity and type safety, passed as a single argument. Use keyword arguments for clarity when calling.
    -   Similarly, for complex return values, use tuples with named fields (`NamedTuple`), `dataclasses`, or `Pydantic` models instead of raw dictionaries or complex tuples.
-   **Abstraction:** Maintain a single level of abstraction within a function/method.

### Data Handling

-   **Encapsulation:** Use classes (`dataclasses`, `Pydantic` models) to encapsulate related data instead of relying solely on primitive types or raw dictionaries/tuples.
-   **Validation:** Implement data validation within classes (e.g., using `Pydantic` validators, `@property` setters, or methods) rather than scattering checks throughout functions.
-   **Immutability:** Prefer immutable data structures where possible. Use tuples instead of lists for fixed sequences. Consider `@dataclass(frozen=True)` for immutable data objects.

### Classes

-   **SOLID Principles:** Adhere to SOLID principles for maintainable and extensible OOP design.
-   **Composition over Inheritance:** Favor object composition over class inheritance where appropriate.
-   **Interfaces/Protocols:** Use Abstract Base Classes (`abc` module) or `typing.Protocol` to define contracts and enable polymorphism/duck typing robustly.
-   **Size:** Aim for small, focused classes (e.g., < 15 methods, < 15 properties, < 300 lines - guidelines, not hard rules).

### Prompting and LLM Generation

-   Follow XML Format (or specified format).

### Feature Development Workflow (TDD/BDD Focus)

-   **Red-Green-Refactor:** Follow this cycle for new features.
-   **Planning:** Use `TODO`, `FIXME`, `NOTE` comments, or task files (e.g., `feature.md`) to plan development steps.
    -   Break down features into testable units.
    -   Prioritize test cases.
    -   Document dependencies and setup.
    -   Define type requirements (`dataclasses`, `Protocols`, etc.).
-   **Type Check First:**
    -   Run `mypy .` (or configured path) before changes to establish a baseline.
    -   Address type errors methodically, often starting with definitions then usage.
    -   Avoid mixing type fixing with logic changes in the same commit.
    -   Verify fixes with `mypy .` again.
-   **Testing (Red):** Write failing tests first using `pytest`.
    -   One logical assertion/behavior per test.
    -   Ensure failure messages are clear.
    -   Commit failing tests.
-   **Implementation (Green):** Write the simplest code to make the test pass.
    -   Commit passing implementation.
-   **Refactoring (Refactor):** Improve code quality (readability, performance, design) while ensuring tests remain green.
    -   Extract reusable logic, apply patterns.
    -   Commit refactored code.
-   **Test Structure:** Follow Arrange-Act-Assert (or Given-When-Then for BDD) consistently.
-   **Isolation:** Keep test cases focused and isolated (use fixtures, mocking).
-   **Documentation:** Update docstrings and relevant documentation alongside code changes.

### Exceptions and Error Handling

-   **Use Exceptions:** Use specific, built-in or custom exception types for exceptional circumstances (errors you don't expect to handle locally).
-   **Catch Specific Exceptions:** Catch only the exceptions you can meaningfully handle. Avoid bare `except:`.
-   **Add Context:** If catching and re-raising, add context or wrap the original exception (`raise NewException("Context") from e`).
-   **Global Handlers:** Use framework middleware (FastAPI/Flask/Django) or a top-level try/except block in application entry points for unhandled exceptions (logging, returning generic error responses).

### Meta Functions / Pattern Documentation

*Keep the Markdown structure, just ensure code blocks use `python`.*

```markdown
# {Pattern Name}

...

## Key Components

1.  **{Component Name}**
    ```python
    # Python code example
    ```
    ...

## Example Implementation

```python
# Complete working Python example


*Guidelines remain the same: place in `_learnings/patterns/`, kebab-case filenames, working Python examples, etc.*

### Monorepo Dependencies (If Applicable)

-   **Package-Based Approach:** Install dependencies within the specific package/application directory where they are used (using `Poetry`, `PDM`). Keep only monorepo management tools (if any, e.g., `tox`, `nox` for orchestration) or shared build tools in the root.
-   **Version Management:** Use `Poetry`'s lock file (`poetry.lock`) or `PDM`'s (`pdm.lock`) for reproducible builds. Use tools like `Poetry`'s dependency groups or `PDM`'s optional groups. Consider `dependabot` or similar for updates.
-   **CI Checks:** Implement CI steps to ensure consistent environments and check for dependency issues.

### Backend Architecture (Example: API Service)

-   **Layering:** Separate concerns (e.g., API layer, service/business logic layer, data access layer).
-   **Dependency Injection:** Use frameworks' built-in DI (FastAPI) or libraries (`python-dependency-injector`) to manage dependencies and improve testability.
-   **Configuration:** Manage configuration cleanly (e.g., environment variables loaded into Pydantic settings models).
-   **Async:** Use `async`/`await` appropriately for I/O-bound operations in async frameworks (FastAPI, async Flask/Django).

### Performance Patterns

-   **Asynchronous Operations:** Use `asyncio`, `httpx`, `asyncpg`/`databases` etc., for non-blocking I/O.
-   **Caching:** Implement caching strategies (e.g., using Redis with `redis-py`, Memcached, `@functools.lru_cache` for in-memory).
-   **Database Optimization:** Write efficient queries, use appropriate indexing, use ORM features wisely (avoid N+1 queries - `select_related`/`prefetch_related` in Django, `selectinload`/`joinedload` in SQLAlchemy).
-   **Efficient Data Structures:** Use appropriate built-in types (sets for membership tests, dicts for lookups).
-   **Profiling:** Use profiling tools (`cProfile`, `py-spy`) to identify bottlenecks.
-   **Code Optimization:** Optimize algorithms where necessary, but prioritize clean code first.

### Security Patterns

-   **Input Validation/Sanitization:** Use Pydantic or framework validation. Sanitize output if rendering HTML (`jinja2` auto-escaping, `bleach` for user content).
-   **Authentication/Authorization:** Use robust libraries/framework features (e.g., `Passlib` for hashing, JWT libraries, OAuth2 support in FastAPI/Django REST framework). Implement clear permission checks.
-   **ORM Security:** Be mindful of SQL injection risks (though ORMs largely prevent this if used correctly). Do not build raw SQL queries with user input unsafely.
-   **Secrets Management:** Use environment variables or dedicated secrets management tools (e.g., HashiCorp Vault, AWS Secrets Manager). Never commit secrets.
-   **Dependencies:** Keep dependencies updated and use tools like `safety` or `pip-audit` to check for known vulnerabilities.
-   **Rate Limiting:** Implement rate limiting (e.g., using middleware with libraries like `slowapi`).
-   **Security Headers:** Configure appropriate security headers (CSP, HSTS, X-Frame-Options) in your web framework.

### Testing Patterns

-   **Tooling:** Use `pytest` as the primary test runner. Use `pytest-cov` for coverage. Use `unittest.mock` (or `pytest-mock`) for mocking. Use `hypothesis` for property-based testing.
-   **Test Environments:** Configure test environments (e.g., separate test database, settings overrides).
-   **Coverage:** Aim for high test coverage, but focus on testing behavior, not just lines. Configure coverage reporting (e.g., fail under threshold).
-   **TDD/BDD:** Follow Test-Driven or Behavior-Driven Development.
-   **Mocking:** Mock external services, I/O, and time (`datetime`) appropriately for unit tests. Use real services (or test containers) for integration tests.
-   **E2E Tests:** Use tools like `pytest-django` live server tests, `requests`, `httpx`, or browser automation (`pytest-playwright`, `selenium`) for end-to-end tests of critical user flows.

### Testing Strategy

-   Maintain a balanced test suite (adjust ratio based on project needs):
    -   **Unit Tests (~40%):** Test isolated functions, classes, logic. Mock dependencies heavily. Fast execution.
    -   **Integration Tests (~60%):** Test interactions between components (e.g., service layer with data access layer, API endpoint with service). May involve real database/external services (or test doubles like test containers). Slower execution.
    -   **E2E Tests (~5-10%):** Test critical user paths through the entire system. Slowest execution.

### Data Mutation Best Practices (Python/ORM Adaptation)

*Principles remain the same, adapt implementation to your ORM/DB library (e.g., SQLAlchemy, Django ORM, `psycopg`).*

1.  **Unique Resource Creation:**
    ```python
    import uuid
    # GOOD: Generate UUID in application code if needed before insert
    new_resource_id = uuid.uuid4()
    # Example using an ORM (adjust for your specific ORM)
    new_resource = ResourceModel(id=new_resource_id, **data)
    session.add(new_resource)
    await session.commit() # or session.commit() for sync

    # Generally OK: Letting DB generate sequential or default UUIDs if race conditions on *that* are not a concern for your use case.
    ```

2.  **Safe Updates (Optimistic Locking):**
    -   Include a `version` integer column or `updated_at` timestamp.
    -   Check the version/timestamp in the `WHERE` clause of your `UPDATE`.
    -   Increment the version or update the timestamp.
    ```python
    # Example using SQLAlchemy (conceptual)
    async def update_resource(session, resource_id: uuid.UUID, expected_version: int, data: dict):
        resource = await session.get(ResourceModel, resource_id)
        if not resource:
            raise NotFoundError("Resource not found")
        if resource.version != expected_version:
            raise StaleDataError("Data has been modified")

        # Update fields
        for key, value in data.items():
            setattr(resource, key, value)
        resource.version += 1 # Increment version
        resource.updated_at = datetime.now(timezone.utc)

        await session.commit()
        return resource # Return updated object
    ```
    *(Note: Some ORMs/libraries might offer more direct optimistic locking support)*

3.  **Deletion Safety:**
    -   Use unique IDs. Check ownership/permissions before deleting.
    -   Prefer soft deletes (add `deleted_at` timestamp or `is_active` boolean).
    ```python
    # Example: Soft delete with ownership check
    async def soft_delete_resource(session, resource_id: uuid.UUID, user_id: uuid.UUID):
        resource = await session.get(ResourceModel, resource_id)
        if not resource or resource.user_id != user_id:
            raise NotFoundOrUnauthorizedError("Cannot delete resource")

        resource.deleted_at = datetime.now(timezone.utc)
        resource.is_active = False
        await session.commit()
    ```

4.  **Race Condition Prevention:**
    -   Use database transactions for operations involving multiple steps.
    -   Use database-level locking (`SELECT ... FOR UPDATE`) when reading data that will be updated within the same transaction to prevent conflicts.
    ```python
    # Example using SQLAlchemy async session transaction
    async with session.begin(): # Starts transaction
        # Lock the 'from' account row
        from_account = await session.get(Account, from_id, with_for_update=True)
        if from_account.balance < amount:
            raise InsufficientFundsError()

        # Lock the 'to' account row (optional, depends on logic)
        to_account = await session.get(Account, to_id, with_for_update=True)

        from_account.balance -= amount
        to_account.balance += amount
        # Transaction commits automatically on exit if no exception
    ```

5.  **Idempotency:**
    -   Use idempotency keys provided by the client (e.g., in headers).
    -   Check for an existing record associated with the key before performing the action. Store the result if performing the action.
    ```python
    async def process_payment(session, idempotency_key: str, payment_data: dict):
        existing_record = await session.execute(
            select(PaymentRecord).where(PaymentRecord.idempotency_key == idempotency_key)
        ).scalar_one_or_none()

        if existing_record:
            return existing_record.result # Return cached result

        async with session.begin():
            # Perform the actual payment processing logic
            payment_result = await perform_actual_payment(payment_data)

            # Store the result with the idempotency key
            new_record = PaymentRecord(
                idempotency_key=idempotency_key,
                result=payment_result,
                # ... other fields
            )
            session.add(new_record)
            # Transaction commits on exit

        return payment_result
    ```

6.  **Testing Considerations:**
    -   Write tests specifically for concurrency issues (e.g., using `asyncio.gather` to run updates simultaneously).
    -   Use unique data per test (`uuid.uuid4()`, unique names).
    -   Use `pytest` fixtures for reliable setup and teardown (creating/deleting test data, cleaning DB state).

### Test File Organization (Python Standard)

1.  **`tests/` Directory:** Place all tests in a top-level `tests` directory.
2.  **Mirror Structure:** Mirror your application's package structure within `tests`.
    ```
    my_project/
    ├── src/
    │   ├── api/
    │   │   ├── __init__.py
    │   │   └── routers/
    │   │       ├── __init__.py
    │   │       └── users.py
    │   ├── core/
    │   │   └── models.py
    │   └── services/
    │       └── user_service.py
    ├── tests/
    │   ├── __init__.py
    │   ├── api/
    │   │   ├── __init__.py
    │   │   └── routers/
    │   │       ├── __init__.py
    │   │       └── test_users.py      # Tests for users.py routes
    │   ├── core/
    │   │   └── test_models.py     # Tests for models.py
    │   └── services/
    │       └── test_user_service.py # Tests for user_service.py
    │   ├── integration/             # Optional: Separate dir for integration tests
    │   │   └── test_user_flow.py
    │   └── e2e/                     # Optional: Separate dir for E2E tests
    │       └── test_signup_login.py
    └── pyproject.toml
    ```
3.  **Naming:** Name test files `test_*.py` and test functions `test_*`.

### Test Type Guidelines (Python Context)

1.  **Unit Tests:** Test functions/methods/classes in isolation. Mock external dependencies (database, APIs, file system, time). Use `unittest.mock`.
2.  **Integration Tests:** Test the interaction between multiple components (e.g., API endpoint -> service -> database). May use a real test database (often ephemeral or cleaned between tests) or test containers (`pytest-docker`). Mock external *third-party* services.
3.  **E2E Tests:** Test the entire application flow from the user's perspective (e.g., simulating HTTP requests or browser interactions). Run against a deployed-like environment.

### Monitoring and Analytics

-   **Metrics:** Use `prometheus-client` (Python) to expose metrics. Create custom metrics (Counters, Gauges, Histograms) for key business logic and performance indicators. Use framework middleware to track request latency/counts.
-   **Monitoring Stack:** Configure Prometheus scraping, Grafana dashboards, Alertmanager.
-   **Logging:** Use Python's standard `logging` module. Configure structured logging (e.g., JSON format) for easier parsing. Send logs to a centralized system (ELK, Grafana Loki, Datadog).
-   **Tracing:** Implement distributed tracing if using microservices (OpenTelemetry).
-   **Type-Safe Analytics:** Define event structures using `TypedDict` or `Pydantic`. Create helper functions/classes for sending analytics events to ensure consistency.

### Documentation Patterns

-   **Docstrings:** Primary source of API documentation. Use tools like `Sphinx` with `autodoc` to generate HTML documentation from docstrings.
-   **README:** Maintain a comprehensive `README.md` with setup, usage, testing instructions.
-   **Architecture Docs:** Use Markdown files (e.g., in a `docs/` directory) or diagrams (e.g., using Mermaid syntax) for high-level architecture decisions.
-   **Patterns:** Follow the pattern documentation guidelines previously defined (`_learnings/patterns/`).

### Database Interaction

-   **Choose a Method:** Decide on ORM (SQLAlchemy, Django ORM) or a lower-level driver (`psycopg`, `asyncpg`) based on project needs.
-   **Connection Pooling:** Configure and use connection pooling properly.
-   **Session/Connection Management:** Manage database sessions/connections correctly (e.g., using context managers, framework request/response cycles, FastAPI dependencies).
-   **Avoid Raw SQL (Mostly):** Prefer ORM methods. If raw SQL is necessary, use parameterization (`execute("... WHERE id = %s", (my_id,))`) to prevent SQL injection.

### SQL Migration Style Guide

*This section is language-agnostic and remains highly relevant. Keep the SQL conventions as defined.*

---

My job depends on this task being done well.
I am a start up founder and I need to ship fast, and well.
Please never be lazy, and always try to do your best.
It's critical that I succeed in this project. Be awesome.
If I don't do tasks well I will lose my job.

---

This adapted set of rules provides a strong foundation for a high-quality Python project, incorporating best practices for typing, testing, architecture, security, and more, while retaining the spirit and rigor of your original TypeScript guidelines. Remember to choose and enforce specific tools (like formatter, linter, test runner) and conventions (like docstring style) early on. Good luck with your project!

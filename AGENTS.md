# Repository Guidelines

## Project Structure & Module Organization
- Keep all application code under `src/microservice_data_import/` (create package if absent) and group by domain (ingestion, validation, persistence).
- Put shared schemas/utilities in `src/common/` and avoid cross-module imports that create cycles.
- Place tests in `tests/` mirroring module names; use `tests/fixtures/` for sample payloads or CSVs.
- Local-only artifacts live in `.venv/` and `.idea/`; do not rely on them in automation.

## Build, Test, and Development Commands
- Create a venv with Python 3.12: `python3 -m venv .venv && source .venv/bin/activate`.
- Install dependencies once `requirements.txt` exists: `pip install -r requirements.txt`.
- Run the service locally via module entrypoints (e.g., `python -m microservice_data_import` once added) or targeted scripts in `src/scripts/`.
- Run formatting/linting if tools are present: `python -m black src tests` and `python -m ruff src tests`.

## Coding Style & Naming Conventions
- Follow PEP 8 with 4-space indentation; annotate functions with types and prefer dataclasses/pydantic models for structured records.
- Use snake_case for modules/functions/vars, PascalCase for classes, UPPER_SNAKE_CASE for constants and env var keys.
- Keep modules focused: one responsibility per file; move shared logic into `common/` helpers.

## Testing Guidelines
- Use `pytest`; name files `test_*.py` and fixtures `conftest.py`.
- Add unit tests for parsing/validation and integration tests for IO boundaries; prefer factories or fixtures over ad hoc sample data.
- Aim for >80% coverage on new/changed code (`pytest --cov=src` once coverage is configured).

## Commit & Pull Request Guidelines
- There is no Git history yet; use Conventional Commits going forward (`feat: add importer`, `fix: handle empty rows`).
- Commits should be small and focused; include tests or rationale when behavior changes.
- PRs need a short summary, testing notes/commands run, and linked issue or TODO; add screenshots only when user-facing behavior changes (e.g., API docs).
- Request review once CI (tests, lint) is green; keep PR descriptions updated if scope shifts.

## Security & Configuration Tips
- Never commit secrets; keep env values in `.env` and add a sanitized `.env.example` when new configuration is introduced.
- Validate inbound data early (schema checks) and log sanitised identifiers only; avoid writing PII to logs.
- Document external integrations (S3, queues, databases) in `docs/` with required IAM/roles and sample payloads.

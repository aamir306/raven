# Contributing to RAVEN

Thank you for your interest in contributing to RAVEN! This document provides guidelines and instructions for contributing.

## Getting Started

1. Fork the repository
2. Clone your fork: `git clone https://github.com/YOUR_USERNAME/raven.git`
3. Create a branch: `git checkout -b feature/your-feature-name`
4. Make your changes
5. Push and open a Pull Request

## Development Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pip install -r requirements-dev.txt
```

## Code Style

- Python code follows [PEP 8](https://peps.python.org/pep-0008/)
- Use type hints for all function signatures
- Use `structlog` for logging (not `print()`)
- Async functions where applicable (connectors, LLM calls)

## Project Structure

- `src/raven/` — Core pipeline code
- `preprocessing/` — Data ingestion and indexing scripts
- `prompts/` — LLM prompt templates (plain text with `{placeholders}`)
- `config/` — Configuration files (YAML, JSON)
- `web/` — FastAPI API + React UI
- `tests/` — Test suite and evaluation scripts

## Commit Messages

Use [Conventional Commits](https://www.conventionalcommits.org/):

```
feat: add LSH entity matching for fuzzy value resolution
fix: handle NULL partition columns in cost guard
docs: update semantic model YAML examples
refactor: extract common prompt loading into base class
test: add evaluation for multi-join queries
```

## Pull Request Process

1. Update documentation if you change behavior
2. Add tests for new functionality
3. Ensure all tests pass: `pytest tests/`
4. Update `config/` files if you add new configuration options
5. Keep PRs focused — one feature or fix per PR

## Reporting Issues

When reporting bugs, please include:
- Steps to reproduce
- Expected vs actual behavior
- Trino version and table count (if relevant)
- Error messages and logs

## Adding Prompt Templates

Prompt templates live in `prompts/` as plain text files. Follow these conventions:
- Use `{variable_name}` for placeholders
- Always include `{trino_dialect_rules}` in SQL generation prompts
- Enforce structured output (e.g., "Output EXACTLY one word: ...")
- Add the template to the stage mapping in the docs

## License

By contributing, you agree that your contributions will be licensed under the Apache License 2.0.

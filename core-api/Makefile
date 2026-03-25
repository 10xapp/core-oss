.PHONY: start install dev test test-openapi typecheck lint format check deploy clean help

help:
	@echo "Available commands:"
	@echo "  make start     - Start the API server (uses uv)"
	@echo "  make install   - Install dependencies"
	@echo "  make test      - Run tests"
	@echo "  make typecheck - Run mypy type checker (catches signature errors)"
	@echo "  make lint      - Run ruff linter"
	@echo "  make format    - Format code with ruff"
	@echo "  make check     - Run all checks (lint + typecheck)"
	@echo "  make deploy    - Deploy to Vercel"
	@echo "  make clean     - Clean up cache files"

start:
	@echo "🚀 Starting Core API..."
	@uv run python dev.py

install:
	@echo "📦 Installing dependencies..."
	@uv pip install -r requirements-dev.txt
	@uv pip install cryptography

dev: start

test:
	@echo "🧪 Running tests..."
	@uv run pytest

test-openapi:
	@echo "🔍 Validating OpenAPI schema..."
	@uv run pytest tests/test_openapi_schema.py -v

typecheck:
	@echo "🔍 Running type checker..."
	@uv run mypy api/
	@echo "✅ Type check complete!"

lint:
	@echo "🔍 Running linter..."
	@uv run ruff check api/ lib/
	@echo "✅ Lint complete!"

format:
	@echo "✨ Formatting code..."
	@uv run ruff format api/ lib/
	@uv run ruff check --fix api/ lib/
	@echo "✅ Format complete!"

check: lint typecheck
	@echo "✅ All checks passed!"

deploy:
	@echo "🚢 Deploying to Vercel..."
	@vercel --prod

clean:
	@echo "🧹 Cleaning up..."
	@find . -type d -name "__pycache__" -exec rm -rf {} +
	@find . -type d -name ".pytest_cache" -exec rm -rf {} +
	@find . -type f -name "*.pyc" -delete
	@echo "✨ Clean complete!"

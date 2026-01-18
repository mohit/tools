.PHONY: help install test test-verbose clean lint

help:
	@echo "Apple Health Export - Available Commands"
	@echo ""
	@echo "  make install       - Install package with uv"
	@echo "  make test          - Run tests"
	@echo "  make test-verbose  - Run tests with verbose output"
	@echo "  make test-cov      - Run tests with coverage report"
	@echo "  make clean         - Clean up temporary files"
	@echo "  make lint          - Run code linting (if configured)"
	@echo ""

install:
	@./install.sh

test:
	@echo "Running tests..."
	@python3 -m pytest tests/ -v

test-verbose:
	@echo "Running tests with verbose output..."
	@python3 -m pytest tests/ -vv

test-cov:
	@echo "Running tests with coverage..."
	@python3 -m pytest tests/ --cov=. --cov-report=term-missing

clean:
	@echo "Cleaning up..."
	@find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	@find . -type f -name "*.pyc" -delete 2>/dev/null || true
	@find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	@find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true
	@find . -type d -name ".coverage" -delete 2>/dev/null || true
	@echo "Clean complete!"

lint:
	@echo "Linting not configured. Install ruff or flake8 if needed."

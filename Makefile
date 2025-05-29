
all: format coverage
	uv run coverage report -m

format:
	uv run ruff format .
	uv run mypy .

test:
	uv run python -m pytest

coverage:
	uv run coverage run -m pytest
	rm assets/coverage.svg
	uv run coverage-badge -o assets/coverage.svg

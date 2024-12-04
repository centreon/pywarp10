
all: format coverage
	poetry run coverage report -m

format:
	poetry run ruff format .
	poetry run mypy .

test:
	poetry run python -m pytest

coverage:
	poetry run coverage run -m pytest
	rm assets/coverage.svg
	poetry run coverage-badge -o assets/coverage.svg

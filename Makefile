sort: 
	isort .

test: sort
	poetry run python -m pytest

coverage: test
	poetry run coverage run -m pytest
	rm assets/coverage.svg
	poetry run coverage-badge -o assets/coverage.svg

check: coverage
	poetry run coverage report -m

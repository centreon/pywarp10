sort: 
	isort .

update:
	poetry update
	poetry version patch

test: sort
	poetry run python -m pytest

coverage: sort
	poetry run coverage run -m pytest
	rm assets/coverage.svg
	poetry run coverage-badge -o assets/coverage.svg

check: update coverage
	poetry run coverage report -m

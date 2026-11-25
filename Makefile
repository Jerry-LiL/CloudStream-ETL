.PHONY: install test lint run

install:
	python -m pip install -r requirements.txt -r requirements-dev.txt

test:
	pytest -q

lint:
	ruff check src tests dags

run:
	python -m cloudstream.cli run --date 2026-03-01
	python -m cloudstream.cli run --date 2026-03-02

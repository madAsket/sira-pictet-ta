.PHONY: help api api-debug  ingest-equities ingest-pdfs clear-vector-db clear-vector-db-recreate

help:
	@echo "Available commands:"
	@echo "  make api              - Run API on localhost:8020"
	@echo "  make ingest-equities  - Ingest equities dataset into SQLite"
	@echo "  make ingest-pdfs      - Ingest PDF documents into vector DB"
	@echo "  make clear-vector-db  - Delete Qdrant collection"
	@echo "  make clear-vector-db-recreate - Delete and recreate Qdrant collection"

api:
	@pipenv run uvicorn app.web_api.main:app --host "$${API_HOST:-localhost}" --port "$${API_PORT:-8020}"

ingest-equities:
	@pipenv run python -m app.cli.ingest_equities

ingest-pdfs:
	@pipenv run python -m app.cli.ingest_pdfs

clear-vector-db:
	@pipenv run python -m app.cli.clear_vector_db

clear-vector-db-recreate:
	@pipenv run python -m app.cli.clear_vector_db --recreate --vector-size "$${VECTOR_SIZE:-3072}"
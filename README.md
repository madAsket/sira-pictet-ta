# SIRA (Stock Investment Research Assistant)

FastAPI service for answering questions about equities (data in SQLite) and macro/research (PDF RAG in Qdrant).

## Requirements

- Python (see `Pipfile`) + `pipenv`
- Docker (for Qdrant)
- `OPENAI_API_KEY` (required for /ask and PDF ingestion)

## Quick start

```bash
pipenv install --dev
cp .env.example .env
# set OPENAI_API_KEY in .env
docker compose up -d qdrant  # or: docker-compose up -d qdrant
make ingest-equities
make ingest-pdfs
make api-debug
```

By default the API runs on `http://localhost:8020`.

## Data initialization

### Equities → SQLite

- Input file (default): `data/equities.xlsx`
- Output DB file: `db/equities.db`

### PDFs → Qdrant (+ metadata in SQLite)

- PDF source (default): `data/PDF` (env var `PDF_INPUT_DIR`)
- Qdrant: `QDRANT_URL` (default `http://localhost:6333`)
- Collection: `QDRANT_COLLECTION` (default `pdf_chunks`)
- Document metadata is stored in `db/equities.db` (table `documents`)

## Swagger: how to test requests

After `make api-debug`:

- Swagger UI: `http://localhost:8020/docs`
- OpenAPI schema: `http://localhost:8020/openapi.json`

### POST /ask

1. Open `POST /ask` → **Try it out**
2. Enter JSON:

```json
{ "question": "What are key macro risks for equities this year?" }
```

3. Click **Execute**

Helpful:

- `make api-debug` in query parameters will return technical fields (`sql`, `sql_rows_preview`, `errors`, etc.).

### POST /upload/equities

- Format: multipart/form-data, field `file` (one `.xlsx`)
- Example file: `data/test/equities_to_upload.xlsx`

### POST /upload/pdfs

- Format: multipart/form-data, field `files` (multiple `.pdf`)
- Test files: `data/test/*.pdf`

## Configuration

The full list of variables is in `.env.example`.

## Example questions:

- "What is the target price and dividend yield for Apple?"
- "What is better to invest Ametek or Vivendi?"
- "What are key macro risks for equities this year?"
- "What is the target price for Tesla and how does inflation affect growth stocks?"
- "How is company ABCXYZ doing?"
- "Show me the top by european region"
- "What is the target price and recommendation for Tesla?"
- "Show me the top software companies"

## Assumptions and limitations
1. Intent routing, entity resolution, Text to SQL, and the final answer could send a query to the wrong branch or map it to the wrong company, so the prompts and pipelines logic need some minor tweaks:
    1. It's better to add a few-shot set of examples to the final answer composer, so responses stay consistent and more readable.
    2. The guidance is too loose for Text to SQL prompt: when and how each equities field can be used, which filters are required, and what SQL patterns are allowed.
    3. SIRA needs more precise heuristics to decide where to fetch data from: the equities table or the PDF. The current signals are a bit too broad and can misroute edge cases.
    4. When intent comes back as unknown, the pipeline runs both SQL and RAG to maximize recall. That improves coverage but increases latency and model spend.
    5. Text to SQL is limited to read-only SELECT queries against a single table (equities).
2. Scaling limitations:
    1. Caching, batching, and per-request budgets aren't implemented yet.
    2. SQLite is file-based - not ideal for concurrent writes or scaling.
    3. PDF ingestion is synchronous right now, which can be slow and expensive. At scale, this should move to background workers with retries and idempotent jobs.
    4. PDF ingestion relies on text extraction only (no OCR). Scanned or image-only PDFs will have poor recall.
    5. RAG retrieval is vector-only. There's no hybrid search or re-ranking, so some terms can be missed.
    6. The API is intentionally minimal (no auth, rate limiting, or metrics).
    7. Orchestration of pipelines needs to be brought into a common contract.
    8. Expand support for uploaded file types and their validation.
    9. The implementation is currently tied to OpenAI models only. Swapping providers would require some refactoring.
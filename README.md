# Context-Aware Analytics Agent (CA3) - v0.2

CA3 is a local-first analytics agent for BigQuery projects. It uses project metadata as context, streams agent responses into a SvelteKit UI, executes read-only SQL through a Python FastAPI sidecar, and includes an evaluation workflow for checking generated SQL and result rows.

## Key Features

- **Context-aware agent workflow**: The backend uses the Vercel AI SDK tool loop with table search, metadata reading, and SQL execution tools.
- **Project-configured models**: Chat and evals read `llm.provider` and `llm.annotation_model` from `ca3_config.yaml`; environment variables remain supported for API keys.
- **Filesystem metadata graph**: CA3 reads Hive-style metadata paths such as `type=bigquery/database=.../schema=.../table=...`.
- **Safe SQL execution**: SQL is limited to `SELECT` / `WITH`, placeholder IDs are blocked, and missing `LIMIT` clauses are injected before execution.
- **Evaluation v0.2**: Evals run one case at a time, verify SQL fragments and expected rows, save JSON history, and display generated SQL/check details in the UI.
- **Modern local UI**: SvelteKit 5 app with chat, table explorer, inspector, chat history, and eval views.

## Prerequisites

- Node.js 20+
- Python 3.12+ with `uv`
- BigQuery access configured for the project in `cli/redlake-ca3/ca3_config.yaml`
- At least one LLM API key matching your configured provider

## Configuration

Create a repo-root `.env` file:

```env
ANTHROPIC_API_KEY=sk-ant-...
# Optional when using other providers:
# OPENAI_API_KEY=...
# GOOGLE_GENERATIVE_AI_API_KEY=...

CA3_DEFAULT_PROJECT_PATH=/absolute/path/to/Context-Aware-Analytics-Agent/cli/redlake-ca3
```

Configure the active project in `cli/redlake-ca3/ca3_config.yaml`:

```yaml
project_name: redlake-ca3
llm:
  provider: anthropic
  annotation_model: claude-haiku-4-5-20251001
databases:
  - type: bigquery
    name: bigquery-redlake
    project_id: redlake-474918
    dataset_id: redlake_dw
    sso: true
```

API keys are safer in `.env`; `llm.api_key` in `ca3_config.yaml` is only a fallback.

## Installation

Install workspace dependencies:

```bash
npm install

cd cli
uv sync
uv pip install -e ".[bigquery]"
```

Generate or refresh metadata:

```bash
cd cli/redlake-ca3
uv run ca3 sync
```

## Running Locally

Start three services:

```bash
# Backend, port 5005
npm run dev --workspace=@ca3/backend

# Frontend, port 3000
npm run dev --workspace=@ca3/frontend

# FastAPI SQL sidecar, port 8005
npm run fastapi --workspace=@ca3/backend
```

Then open:

```text
http://localhost:3000
```

The SQL sidecar exposes:

```text
GET  /health
POST /execute_sql
```

The backend SQL tool calls `/execute_sql` with `sql` and `ca3_project_folder`.

## Evaluations

Eval cases live in `cli/redlake-ca3/tests/*.yml` or `*.yaml`:

```yaml
- id: tech_keywords_count
  question: "How many tech keywords are there in total?"
  expected_sql_contains: ["COUNT", "tech_keywords"]
  forbidden_sql_contains: ["DROP", "UPDATE"]
  expected_columns: ["total_tech_keywords"]
  expected_rows:
    - total_tech_keywords: 118
  threshold: 1.0
```

Evaluation behavior:

- `GET /api/core/evals` lists cases and their latest saved result.
- `POST /api/core/evals/run` runs one case by `id`.
- SQL checks are case-insensitive.
- Row checks ignore row order and allow small numeric tolerance.
- Any SQL execution error fails the eval, even if SQL text checks pass.
- Results are saved under `tests/outputs/results_*.json`.

Run from CLI:

```bash
cd cli/redlake-ca3
uv run ca3 test --select tech_keywords_count
uv run ca3 test -m anthropic:claude-haiku-4-5-20251001
```

When `-m` is omitted, the CLI uses the project model from `ca3_config.yaml`.

## Verification

Useful checks before release:

```bash
npm run lint --workspace=@ca3/backend
npm test --workspace=@ca3/backend
npm run check --workspace=@ca3/frontend
npm run build --workspace=@ca3/backend
npm run build --workspace=@ca3/frontend
```

Current backend test coverage includes:

- SQL tool safety and FastAPI sidecar contract
- Project model config resolution
- Agent stream handling for AI SDK 6
- Table metadata route security
- Eval case loading, verifier behavior, and native eval run API

## Notes

- Local database files such as `apps/backend/ca3_local.db` are ignored and should not be committed.
- macOS AppleDouble files (`._*`, `.!*`) are ignored and were removed from the repository history moving forward.
- The frontend proxies API calls to the backend during local development.

## License

MIT

# Context-Aware Analytics Agent (CA3) - v0.2

CA3 is a local-first analytics agent for data projects. It uses project metadata as context, streams agent responses into a SvelteKit UI, executes read-only SQL through a Python FastAPI sidecar, and includes an evaluation workflow for checking generated SQL and result rows.

## Key Features

- **Context-aware agent workflow**: The backend uses the Vercel AI SDK tool loop with table search, metadata reading, and SQL execution tools.
- **Project-configured models**: Chat and evals read `llm.provider` and `llm.annotation_model` from `ca3_config.yaml`; environment variables remain supported for API keys.
- **Filesystem metadata graph**: `ca3 sync` reads configured providers and writes context files such as `databases/type=bigquery/database=.../schema=.../table=...`.
- **Safe SQL execution**: SQL is limited to `SELECT` / `WITH`, placeholder IDs are blocked, and missing `LIMIT` clauses are injected before execution.
- **Evaluation v0.2**: Evals run one case at a time, verify SQL fragments and expected rows, save JSON history, and display generated SQL/check details in the UI.
- **Modern local UI**: SvelteKit 5 app with chat, table explorer, inspector, chat history, and eval views.

## Prerequisites

- Node.js 20+
- Python 3.12+ with `uv`
- Database access for the providers configured in your CA3 project
- At least one LLM API key matching your configured provider

## Installation

Install workspace dependencies:

```bash
npm install

cd cli
uv sync
uv pip install -e ".[bigquery]"
source .venv/bin/activate
```

The CLI is available as `ca3` inside the Python environment:

```bash
ca3 --help
```

## Project Configuration

The normal setup flow is to create or open a CA3 project folder, then configure databases and models there.

### Option A: Initialize a Project

Use `ca3 init` to create a project folder with `ca3_config.yaml`:

```bash
mkdir my-ca3-project
cd my-ca3-project
ca3 init
```

Then configure the generated `ca3_config.yaml` to point at your database and model provider. If you are not using an activated virtual environment, run the same CLI commands from `cli/` as `uv run ca3 ...`.

### Demo Project: Redlake

The demo for this repository uses the Redlake project:

```text
https://github.com/zyusong0614/redlake
```

The checked-in `cli/redlake-ca3` folder is the CA3 project configuration used by that demo. It points CA3 at the Redlake BigQuery dataset, stores the generated metadata context, and contains the demo eval cases. For your own data, create a separate CA3 project with `ca3 init` and update its `ca3_config.yaml`.

## Environment

Create a repo-root `.env` file for local backend/frontend development:

```env
ANTHROPIC_API_KEY=sk-ant-...
# Optional when using other providers:
# OPENAI_API_KEY=...
# GOOGLE_GENERATIVE_AI_API_KEY=...

CA3_DEFAULT_PROJECT_PATH=/absolute/path/to/my-ca3-project
```

For the Redlake demo, that path is:

```env
CA3_DEFAULT_PROJECT_PATH=/absolute/path/to/Context-Aware-Analytics-Agent/cli/redlake-ca3
```

API keys are safer in `.env`; `llm.api_key` in `ca3_config.yaml` is only a fallback.

## YAML Configuration

Each CA3 project is configured by its own `ca3_config.yaml`. A BigQuery project can look like this:

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

Validate the configured database and LLM connections:

```bash
cd /absolute/path/to/my-ca3-project
ca3 debug
```

Generate or refresh metadata context:

```bash
ca3 sync
```

For the Redlake demo:

```bash
cd cli/redlake-ca3
ca3 debug
ca3 sync
```

## Running Locally

You can launch the app through the CLI from a configured project:

```bash
cd /absolute/path/to/my-ca3-project
ca3 chat
```

For development on this repository, start three services:

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

Eval cases live in each CA3 project under `tests/*.yml` or `tests/*.yaml`. For the Redlake demo, that is `cli/redlake-ca3/tests/`.

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
ca3 test --select tech_keywords_count
ca3 test -m anthropic:claude-haiku-4-5-20251001
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

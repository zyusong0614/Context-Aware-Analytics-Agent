# ca3 CLI

Command-line interface for ca3 chat.

## Installation

Install the core package (lightweight, no database or LLM dependencies):

```bash
pip install ca3-core
```

Then add only the providers you need:

```bash
# Database backends
pip install 'ca3-core[postgres]'
pip install 'ca3-core[bigquery]'
pip install 'ca3-core[snowflake]'
pip install 'ca3-core[duckdb]'
pip install 'ca3-core[clickhouse]'
pip install 'ca3-core[databricks]'
pip install 'ca3-core[mysql]'
pip install 'ca3-core[mssql]'
pip install 'ca3-core[athena]'
pip install 'ca3-core[trino]'
pip install 'ca3-core[redshift]'
pip install 'ca3-core[fabric]'

# LLM providers
pip install 'ca3-core[openai]'
pip install 'ca3-core[anthropic]'
pip install 'ca3-core[mistral]'
pip install 'ca3-core[gemini]'
pip install 'ca3-core[ollama]'

# Integrations
pip install 'ca3-core[notion]'
```

Combine multiple extras in a single install:

```bash
pip install 'ca3-core[postgres,openai]'
pip install 'ca3-core[snowflake,bigquery,anthropic]'
```

Or install everything at once (equivalent to the previous default):

```bash
pip install 'ca3-core[all]'
```

Convenience groups are also available:

```bash
pip install 'ca3-core[all-databases]'  # all database backends
pip install 'ca3-core[all-llms]'       # all LLM providers
```

## Usage

```bash
ca3 --help
Usage: ca3 COMMAND

╭─ Commands ────────────────────────────────────────────────────────────────╮
│ chat         Start the ca3 chat UI.                                       │
│ debug        Test connectivity to configured resources.                   │
│ init         Initialize a new ca3 project.                                │
│ sync         Sync resources to local files.                               │
│ test         Run and explore ca3 tests.                                   │
│ --help (-h)  Display this message and exit.                               │
│ --version    Display application version.                                 │
╰───────────────────────────────────────────────────────────────────────────╯
```

### Initialize a new ca3 project

```bash
ca3 init
```

This will create a new ca3 project in the current directory. It will prompt you for a project name and ask you to configure:

- **Database connections** (BigQuery, DuckDB, Databricks, Snowflake, PostgreSQL, Redshift, MSSQL, Trino)
- **Git repositories** to sync
- **LLM provider** (OpenAI, Anthropic, Mistral, Gemini, OpenRouter, Ollama)
- **`ai_summary` template + model** (prompted only when you enable `ai_summary` for databases)
- **Slack integration**
- **Notion integration**

The resulting project structure looks like:

```
<project>/
├── ca3_config.yaml
├── .naoignore
├── RULES.md
├── databases/
├── queries/
├── docs/
├── semantics/
├── repos/
├── agent/
│   ├── tools/
│   └── mcps/
└── tests/
```

Options:

- `--force` / `-f`: Force re-initialization even if the project already exists

### Start the ca3 chat UI

```bash
ca3 chat
```

This will start the ca3 chat UI. It will open the chat interface in your browser at `http://localhost:5005`.

### Test connectivity

```bash
ca3 debug
```

Tests connectivity to all configured databases and LLM providers. Displays a summary table showing connection status and details for each resource.

### Sync resources

```bash
ca3 sync
```

Syncs configured resources to local files:

- **Databases** - generates markdown docs (`columns.md`, `preview.md`, `description.md`) for each table into `databases/`
- **Git repositories** — clones or pulls repos into `repos/`
- **Notion pages** — exports pages as markdown into `docs/notion/`

After syncing, any Jinja templates (`*.j2` files) in the project directory are rendered with the ca3 context.

Optional `ai_summary` generation:

- Add `ai_summary` to a database connection `templates` list to render `ai_summary.md`.
- Use `prompt("...")` inside Jinja templates to generate `ai_summary` content.
- `prompt(...)` requires `llm.provider`, `llm.annotation_model`, and `llm.api_key` (except for ollama).

### Run tests

```bash
ca3 test
```

Runs test cases defined as YAML files in `tests/`. Each test has a `name`, `prompt`, and expected `sql`. Results are saved to `tests/outputs/`.

Options:

- `--model` / `-m`: Models to test against (default: `openai:gpt-4.1`). Can be specified multiple times.
- `--threads` / `-t`: Number of parallel threads (default: `1`)

Examples:

```bash
ca3 test -m openai:gpt-4.1
ca3 test -m openai:gpt-4.1 -m anthropic:claude-sonnet-4-20250514
ca3 test --threads 4
```

### Explore test results

```bash
ca3 test server
```

Starts a local web server to explore test results in a browser UI showing pass/fail status, token usage, cost, and detailed data comparisons.

Options:

- `--port` / `-p`: Port to run the server on (default: `8765`)
- `--no-open`: Don't automatically open the browser

### BigQuery service account permissions

When you connect BigQuery during `ca3 init`, the service account used by `credentials_path`/ADC must be able to list datasets and run read-only queries to generate docs. Grant the account:

- Project: `roles/bigquery.jobUser` (or `roles/bigquery.user`) so the CLI can submit queries
- Each dataset you sync: `roles/bigquery.dataViewer` (or higher) to read tables

The combination above mirrors the typical "BigQuery User" setup and is sufficient for CA3 metadata and preview pulls.

### Snowflake authentication

Snowflake supports three authentication methods during `ca3 init`:

- **SSO**: Browser-based authentication (recommended for organizations with SSO policies)
- **Password**: Traditional username/password
- **Key-pair**: Private key file with optional passphrase

## Development

### Building the package

```bash
cd cli
python build.py --help
Usage: build.py [OPTIONS]

Build and package ca3-core CLI.

╭─ Parameters ──────────────────────────────────────────────────────────────────╮
│ --force -f --no-force              Force rebuild the server binary             │
│ --skip-server -s --no-skip-server  Skip server build, only build Python pkg   │
│ --bump                             Bump version (patch, minor, major)          │
╰───────────────────────────────────────────────────────────────────────────────╯
```

This will:
1. Build the frontend with Vite
2. Compile the backend with Bun into a standalone binary
3. Bundle everything into a Python wheel in `dist/`

### Installing for development

```bash
cd cli
pip install -e '.[all]'
```

### Publishing to PyPI

```bash
# Build first
python build.py

# Publish
uv publish dist/*
```

## Architecture

```
ca3 chat (CLI command)
    ↓ spawns
ca3-chat-server (Bun-compiled binary, port 5005)
  + FastAPI server (port 8005)
    ↓ serves
Backend API + Frontend Static Files
    ↓
Browser at http://localhost:5005
```

# Context-Aware Analytics Agent (CA3) - v0.1

A next-generation BigQuery analytics agent that understands your data context through deep metadata scanning and features an autonomous self-correction loop for SQL generation.

## 🚀 Key Features

- **Agentic SQL Workflow**: Features a 10-attempt self-correction loop. If BigQuery returns an error, the agent analyzes the stack trace and fixes the SQL automatically.
- **Deep Metadata Context**: Recursively scans Hive-style directory structures (`type=x/database=y/table=z`) to ingest `columns.md`, `ai_summary.md`, and `how_to_use.md`.
- **Pre-Execution Validation**: Static analysis layer to catch LLM-hallucinated placeholders (like `project_id`) before hitting the database.
- **Modern UI**: Built with SvelteKit 5, featuring real-time SSE streaming for chat and a comprehensive Table Explorer with Markdown rendering via `marked`.

---

## 🛠️ Prerequisites

- **Python 3.12+** (managed via `uv`)
- **Node.js 20+**
- **Anthropic API Key**: Supporting Next-Gen models (**Claude 4.7 Opus**, **Claude 4.6 Sonnet**, or **Claude 4.5 Haiku**).
- **Google Cloud Credentials**: Access to a BigQuery project.

---

## 📦 Installation & Setup

### 1. CLI Sidecar (Python)
The CLI handles database synchronization and SQL execution via Ibis.

```bash
cd cli
# Install core dependencies and the sidecar package
uv sync
uv pip install -e ".[bigquery]"
```

### 2. Backend (Fastify/Node.js)
The backend bridges the LLM, the Database, and the Frontend.

```bash
cd apps/backend
npm install
# Create .env in the backend directory
```

**Required `.env` Variables:**
```env
ANTHROPIC_API_KEY=sk-ant-xxx
PORT=5005
# Absolute path to your specific dataset folder containing 'databases'
CA3_DEFAULT_PROJECT_PATH=/Volumes/xxx/workspace/Context-Aware-Analytics-Agent/cli/redlake-ca3
```

### 3. Frontend (SvelteKit)
```bash
cd apps/frontend
npm install
```

---

## 🏁 How to Reproduce

### Step 1: Sync Your Database Metadata
Go to your project workspace inside the CLI and generate the context files:
```bash
cd cli/redlake-ca3
uv run ca3 sync
```
*Wait for the 'Sync Complete' message. This generates metadata across 11+ tables.*

### Step 2: Start the Stack
Open three separate terminal sessions:

1. **Python Sidecar** (Ibis Bridge):
   ```bash
   cd cli
   export PORT=8005
   .venv/bin/python ../apps/backend/fastapi/main.py
   ```
2. **Fastify API**:
   ```bash
   cd apps/backend
   npm run dev
   ```
3. **SvelteKit UI**:
   ```bash
   cd apps/frontend
   npm run dev
   ```

### Step 3: Run a Context-Aware Query
Navigate to `http://localhost:3000` and try:
> "Analyze the tech_keywords table and tell me the distribution of companies."

Watch the **Inspector** tab to see the Agent's thought process, the generated SQL, and any **Self-correction** steps it takes if the first attempt fails.

---

## 📜 License
MIT - v0.1-stable

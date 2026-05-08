# Context-Aware Analytics Agent (CA3) - v0.1

A next-generation BigQuery analytics agent that understands your data context through deep metadata scanning and features an autonomous self-correction loop for SQL generation.

## 🚀 Key Features

- **Agentic SQL Workflow**: Features a 10-attempt self-correction loop. If BigQuery returns an error, the agent analyzes the stack trace and fixes the SQL automatically.
- **Deep Metadata Context**: Recursively scans Hive-style directory structures (`type=x/database=y/table=z`) to ingest `columns.md`, `ai_summary.md`, and `how_to_use.md`.
- **Pre-Execution Validation**: Static analysis layer to catch LLM-hallucinated placeholders (like `project_id`) before hitting the database.
- **Modern UI**: Built with SvelteKit 5, featuring real-time SSE streaming for chat and a comprehensive Table Explorer with Markdown rendering.

---

## 🛠️ Prerequisites

- **Python 3.12+** (managed via `uv`)
- **Node.js 20+** (managed via `pnpm` or `npm`)
- **Anthropic API Key** (Claude 3.5 Sonnet / Claude 3 Haiku)
- **Google Cloud Credentials**: Access to a BigQuery project.

---

## 📦 Installation & Setup

### 1. CLI Sidecar (Python)
The CLI handles database synchronization and SQL execution via Ibis.

```bash
cd cli
# Install dependencies
uv sync
# Optional: Install bigquery extra if not present
uv pip install ".[bigquery]"
```

### 2. Backend (Fastify)
The backend bridges the LLM, the Database, and the Frontend.

```bash
cd apps/backend
npm install
# Create .env in root or backend
cp .env.example .env
```

**Required `.env` Variables:**
```env
ANTHROPIC_API_KEY=sk-ant-xxx
PORT=5005
CA3_DEFAULT_PROJECT_PATH=/path/to/your/redlake-ca3
```

### 3. Frontend (SvelteKit)
```bash
cd apps/frontend
npm install
```

---

## 🏁 How to Reproduce

### Step 1: Sync Your Database
Navigate to your project folder in `cli` and sync your BigQuery schema to local markdown files:
```bash
cd cli/redlake-ca3
uv run ca3 sync
```
*This generates the `databases/` folder used as context by the Agent.*

### Step 2: Start the Services
You need three terminals running:

1. **Python Sidecar** (Port 8005):
   ```bash
   cd cli
   export PORT=8005
   .venv/bin/python ../apps/backend/fastapi/main.py
   ```
2. **Backend API** (Port 5005):
   ```bash
   cd apps/backend
   npm run dev
   ```
3. **Frontend UI** (Port 3000):
   ```bash
   cd apps/frontend
   npm run dev
   ```

### Step 3: Start Chatting
Open `http://localhost:3000` and ask:
> "What are the top 5 records in the tech_keywords table?"

---

## 🛡️ Self-Correction in Action
If the Agent initially writes:
`SELECT * FROM project_id.dataset.table`
The **Pre-Execution Validator** will block it, feed the error back, and the Agent will see from the file paths that it should use `redlake-474918.redlake_dw.tech_keywords` instead. It will then auto-correct and re-run.

---

## 📜 License
MIT - v0.1-stable

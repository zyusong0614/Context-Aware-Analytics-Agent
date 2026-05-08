# CA3 Agent Architecture

This document describes the design and implementation of the **Context-Aware Analytics Agent (CA3)**, following the best practices from the `nao` repository.

## 🤖 Core Engine
CA3 uses the **Vercel AI SDK** with the `streamText` function and `maxSteps: 10`. This allows the agent to iteratively call tools and self-correct based on execution feedback.

### Key Logic
1. **System Prompt**: Provides high-level guidance on BigQuery best practices and security rules.
2. **Tool Loop**: The agent can perform up to 10 consecutive tool calls per user request.

---

## 🔍 Context Retrieval (RAG 2.0)
Unlike traditional RAG which might use vector search, CA3 leverages the **Filesystem as a Knowledge Graph**.

### Tools
- **`search_tables`**: Searches for tables based on the directory structure (`type=.../database=.../schema=.../table=...`).
- **`read_table_metadata`**: Reads specific `.md` files (schema, business summaries, and query examples) for a chosen table.

This "Discovery" pattern ensures the agent only reads relevant metadata, saving thousands of tokens and reducing hallucinations.

---

## ⚡ SQL Execution & Safety
The `execute_bigquery_sql` tool provides an industrial-grade interface to the database.

### Safety Layers
- **Pre-execution Check**: Blocks non-SELECT statements and placeholder usage.
- **Auto-Injection**: Automatically adds `LIMIT 100` if the agent forgets it.
- **Feedback Loop**: Returns raw BigQuery error messages to the agent, triggering the self-correction logic.

---

## 🛠️ Multi-Provider Support
The architecture is decoupled via `providers.ts`, supporting:
- **Anthropic**: Claude 4.7 Opus / 4.6 Sonnet / 4.5 Haiku.
- **Google Vertex AI**: (Ready for integration).
- **OpenAI**: (Ready for integration).

---

## 📈 Evaluation
The system is continuously verified against a suite of `.yml` tests located in `cli/*/tests/`. These tests measure:
- SQL correctness.
- Metadata retrieval accuracy.
- Answer quality.

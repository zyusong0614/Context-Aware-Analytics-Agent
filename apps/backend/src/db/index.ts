import Database from 'better-sqlite3';
import { drizzle } from 'drizzle-orm/better-sqlite3';
import * as schema from './schema';
import { join } from 'path';

// Use a local database file
const dbPath = join(process.cwd(), 'ca3_local.db');
const sqlite = new Database(dbPath);

// Create tables if they don't exist
sqlite.exec(`
	CREATE TABLE IF NOT EXISTS chats (
		id TEXT PRIMARY KEY,
		title TEXT NOT NULL,
		project_id TEXT NOT NULL,
		created_at INTEGER NOT NULL DEFAULT (unixepoch()),
		updated_at INTEGER NOT NULL DEFAULT (unixepoch())
	);
	
	CREATE TABLE IF NOT EXISTS messages (
		id TEXT PRIMARY KEY,
		chat_id TEXT NOT NULL,
		role TEXT NOT NULL,
		content TEXT NOT NULL,
		created_at INTEGER NOT NULL DEFAULT (unixepoch()),
		FOREIGN KEY (chat_id) REFERENCES chats(id) ON DELETE CASCADE
	);
`);

export const db = drizzle(sqlite, { schema });

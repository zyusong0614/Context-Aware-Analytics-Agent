import { sql } from 'drizzle-orm';
import { sqliteTable, text, integer } from 'drizzle-orm/sqlite-core';

export const chats = sqliteTable('chats', {
	id: text('id').primaryKey(),
	title: text('title').notNull(),
	projectId: text('project_id').notNull(),
	createdAt: integer('created_at').notNull().default(sql`(unixepoch())`),
	updatedAt: integer('updated_at').notNull().default(sql`(unixepoch())`),
});

export const messages = sqliteTable('messages', {
	id: text('id').primaryKey(),
	chatId: text('chat_id').notNull().references(() => chats.id, { onDelete: 'cascade' }),
	role: text('role').notNull(), // 'user' | 'assistant'
	content: text('content').notNull(),
	createdAt: integer('created_at').notNull().default(sql`(unixepoch())`),
});

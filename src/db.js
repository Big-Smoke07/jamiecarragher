import fs from 'node:fs';
import path from 'node:path';
import { DatabaseSync } from 'node:sqlite';

function keyForMember(guildId, userId) {
  return `${guildId}:${userId}`;
}

export class MessageCountStore {
  constructor(databasePath) {
    fs.mkdirSync(path.dirname(databasePath), { recursive: true });

    this.db = new DatabaseSync(databasePath);
    this.db.exec(`
      PRAGMA journal_mode = WAL;
      PRAGMA synchronous = NORMAL;
      PRAGMA foreign_keys = ON;

      CREATE TABLE IF NOT EXISTS guild_state (
        guild_id TEXT PRIMARY KEY,
        last_full_scan_at TEXT
      );

      CREATE TABLE IF NOT EXISTS members (
        guild_id TEXT NOT NULL,
        user_id TEXT NOT NULL,
        message_count INTEGER NOT NULL DEFAULT 0,
        PRIMARY KEY (guild_id, user_id)
      );

      CREATE TABLE IF NOT EXISTS messages (
        message_id TEXT PRIMARY KEY,
        guild_id TEXT NOT NULL,
        channel_id TEXT NOT NULL,
        author_id TEXT NOT NULL,
        created_at TEXT NOT NULL
      );

      CREATE INDEX IF NOT EXISTS idx_messages_guild_author
        ON messages (guild_id, author_id);
    `);

    this.insertMessageStatement = this.db.prepare(`
      INSERT OR IGNORE INTO messages (message_id, guild_id, channel_id, author_id, created_at)
      VALUES (?, ?, ?, ?, ?)
    `);

    this.incrementMemberCountStatement = this.db.prepare(`
      INSERT INTO members (guild_id, user_id, message_count)
      VALUES (?, ?, ?)
      ON CONFLICT(guild_id, user_id)
      DO UPDATE SET message_count = message_count + excluded.message_count
    `);

    this.decrementMemberCountStatement = this.db.prepare(`
      UPDATE members
      SET message_count = MAX(message_count - ?, 0)
      WHERE guild_id = ? AND user_id = ?
    `);

    this.getMessageCountStatement = this.db.prepare(`
      SELECT message_count
      FROM members
      WHERE guild_id = ? AND user_id = ?
    `);

    this.getGuildStateStatement = this.db.prepare(`
      SELECT guild_id, last_full_scan_at
      FROM guild_state
      WHERE guild_id = ?
    `);

    this.upsertGuildStateStatement = this.db.prepare(`
      INSERT INTO guild_state (guild_id, last_full_scan_at)
      VALUES (?, ?)
      ON CONFLICT(guild_id)
      DO UPDATE SET last_full_scan_at = excluded.last_full_scan_at
    `);

    this.clearGuildMessagesStatement = this.db.prepare(`
      DELETE FROM messages
      WHERE guild_id = ?
    `);

    this.clearGuildMembersStatement = this.db.prepare(`
      DELETE FROM members
      WHERE guild_id = ?
    `);

    this.clearGuildStateStatement = this.db.prepare(`
      INSERT INTO guild_state (guild_id, last_full_scan_at)
      VALUES (?, NULL)
      ON CONFLICT(guild_id)
      DO UPDATE SET last_full_scan_at = NULL
    `);

    this.selectMessageForDeleteStatement = this.db.prepare(`
      SELECT guild_id, author_id
      FROM messages
      WHERE message_id = ?
    `);

    this.deleteMessageStatement = this.db.prepare(`
      DELETE FROM messages
      WHERE message_id = ?
    `);
  }

  withTransaction(callback) {
    this.db.exec('BEGIN');

    try {
      const result = callback();
      this.db.exec('COMMIT');
      return result;
    } catch (error) {
      this.db.exec('ROLLBACK');
      throw error;
    }
  }

  recordMessage(record) {
    return this.recordMessages([record]);
  }

  recordMessages(records) {
    if (!records.length) {
      return 0;
    }

    return this.withTransaction(() => {
      const increments = new Map();
      let inserted = 0;

      for (const record of records) {
        const result = this.insertMessageStatement.run(
          record.messageId,
          record.guildId,
          record.channelId,
          record.authorId,
          record.createdAt
        );

        if (result.changes === 0) {
          continue;
        }

        inserted += 1;
        const memberKey = keyForMember(record.guildId, record.authorId);
        increments.set(memberKey, (increments.get(memberKey) || 0) + 1);
      }

      for (const [memberKey, count] of increments) {
        const [guildId, userId] = memberKey.split(':');
        this.incrementMemberCountStatement.run(guildId, userId, count);
      }

      return inserted;
    });
  }

  deleteMessage(messageId) {
    return this.withTransaction(() => {
      const row = this.selectMessageForDeleteStatement.get(messageId);

      if (!row) {
        return false;
      }

      this.deleteMessageStatement.run(messageId);
      this.decrementMemberCountStatement.run(1, row.guild_id, row.author_id);
      return true;
    });
  }

  deleteMessages(messageIds) {
    if (!messageIds.length) {
      return 0;
    }

    const placeholders = messageIds.map(() => '?').join(', ');
    const rows = this.db.prepare(`
      SELECT guild_id, author_id, COUNT(*) AS removed_count
      FROM messages
      WHERE message_id IN (${placeholders})
      GROUP BY guild_id, author_id
    `).all(...messageIds);

    if (!rows.length) {
      return 0;
    }

    return this.withTransaction(() => {
      const deleted = this.db.prepare(`
        DELETE FROM messages
        WHERE message_id IN (${placeholders})
      `).run(...messageIds);

      for (const row of rows) {
        this.decrementMemberCountStatement.run(row.removed_count, row.guild_id, row.author_id);
      }

      return deleted.changes;
    });
  }

  getMessageCount(guildId, userId) {
    const row = this.getMessageCountStatement.get(guildId, userId);
    return row?.message_count || 0;
  }

  getGuildState(guildId) {
    return this.getGuildStateStatement.get(guildId) || null;
  }

  resetGuild(guildId) {
    this.withTransaction(() => {
      this.clearGuildMessagesStatement.run(guildId);
      this.clearGuildMembersStatement.run(guildId);
      this.clearGuildStateStatement.run(guildId);
    });
  }

  markGuildBackfilled(guildId, timestamp = new Date().toISOString()) {
    this.upsertGuildStateStatement.run(guildId, timestamp);
  }
}

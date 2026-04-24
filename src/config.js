import path from 'node:path';
import process from 'node:process';
import dotenv from 'dotenv';

dotenv.config({ override: true });

const PLACEHOLDER_VALUES = new Set([
  'your-bot-token',
  'your-application-client-id',
  'your-test-server-id'
]);

function requireEnv(name) {
  const value = process.env[name]?.trim();

  if (!value) {
    throw new Error(`Missing required environment variable: ${name}`);
  }

  if (PLACEHOLDER_VALUES.has(value)) {
    throw new Error(`Environment variable ${name} is still using the example placeholder value.`);
  }

  return value;
}

function optionalEnv(name) {
  const value = process.env[name]?.trim();
  return value || null;
}

export const config = {
  token: requireEnv('DISCORD_TOKEN'),
  clientId: requireEnv('DISCORD_CLIENT_ID'),
  backfillGuildId: optionalEnv('BACKFILL_GUILD_ID'),
  databasePath: path.resolve(process.cwd(), process.env.DATABASE_PATH?.trim() || './data/messages.sqlite')
};

import {
  ChannelType,
  Client,
  Events,
  GatewayIntentBits,
  Partials
} from 'discord.js';
import { config } from './config.js';
import { MessageCountStore } from './db.js';

const store = new MessageCountStore(config.databasePath);

const client = new Client({
  intents: [
    GatewayIntentBits.Guilds,
    GatewayIntentBits.GuildMessages
  ],
  partials: [Partials.Channel, Partials.Message]
});

function shouldCountFetchedMessage(message) {
  return (
    message.author &&
    !message.author.bot &&
    !message.webhookId &&
    !message.system
  );
}

function isRootMessageChannel(channel) {
  return (
    channel &&
    typeof channel.isTextBased === 'function' &&
    channel.isTextBased() &&
    !channel.isThread() &&
    'messages' in channel &&
    channel.type !== ChannelType.GuildForum &&
    channel.type !== ChannelType.GuildMedia
  );
}

function isThreadParent(channel) {
  return (
    channel &&
    typeof channel.threads?.fetchArchived === 'function' &&
    (
      channel.type === ChannelType.GuildText ||
      channel.type === ChannelType.GuildAnnouncement ||
      channel.type === ChannelType.GuildForum ||
      channel.type === ChannelType.GuildMedia
    )
  );
}

async function collectGuildChannels(guild) {
  const fetchedChannels = await guild.channels.fetch();
  const channels = new Map();
  const threadParents = [];

  for (const channel of fetchedChannels.values()) {
    if (!channel) {
      continue;
    }

    if (isRootMessageChannel(channel)) {
      channels.set(channel.id, channel);
    }

    if (isThreadParent(channel)) {
      threadParents.push(channel);
    }
  }

  const activeThreads = await guild.channels.fetchActiveThreads();

  for (const thread of activeThreads.threads.values()) {
    channels.set(thread.id, thread);
  }

  for (const parent of threadParents) {
    const archivedPublic = await parent.threads.fetchArchived({ type: 'public' }).catch(() => null);

    if (archivedPublic) {
      for (const thread of archivedPublic.threads.values()) {
        channels.set(thread.id, thread);
      }
    }

    const archivedPrivate = await parent.threads.fetchArchived({ type: 'private' }).catch(() => null);

    if (archivedPrivate) {
      for (const thread of archivedPrivate.threads.values()) {
        channels.set(thread.id, thread);
      }
    }
  }

  return [...channels.values()];
}

async function indexChannel(channel, storeInstance) {
  let before;
  let indexed = 0;

  while (true) {
    const batch = await channel.messages.fetch({ limit: 100, before });

    if (!batch.size) {
      break;
    }

    const records = [...batch.values()]
      .filter(shouldCountFetchedMessage)
      .map((message) => ({
        messageId: message.id,
        guildId: message.guildId,
        channelId: message.channelId,
        authorId: message.author.id,
        createdAt: message.createdAt.toISOString()
      }));

    indexed += storeInstance.recordMessages(records);

    const oldestMessage = batch.last();
    before = oldestMessage?.id;

    if (batch.size < 100 || !before) {
      break;
    }
  }

  return indexed;
}

async function backfillGuild(guild) {
  console.log(`Starting backfill for ${guild.name} (${guild.id})`);
  store.resetGuild(guild.id);

  const channels = await collectGuildChannels(guild);
  let totalIndexed = 0;

  for (const channel of channels) {
    try {
      const indexed = await indexChannel(channel, store);
      totalIndexed += indexed;
      console.log(`Indexed ${indexed} messages from #${channel.name ?? channel.id}`);
    } catch (error) {
      console.warn(`Skipped channel ${channel.name ?? channel.id}: ${error.message}`);
    }
  }

  store.markGuildBackfilled(guild.id);
  console.log(`Finished ${guild.name}: ${totalIndexed} total messages indexed`);
}

client.once(Events.ClientReady, async (readyClient) => {
  console.log(`Backfill logged in as ${readyClient.user.tag}`);

  const guilds = config.backfillGuildId
    ? [await readyClient.guilds.fetch(config.backfillGuildId).then((guild) => guild.fetch())]
    : await Promise.all(
        readyClient.guilds.cache.map((guild) => guild.fetch())
      );

  for (const guild of guilds) {
    await backfillGuild(guild);
  }

  await client.destroy();
});

client.login(config.token).catch((error) => {
  console.error('Backfill failed to start');
  console.error(error);
  process.exit(1);
});

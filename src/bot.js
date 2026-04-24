import process from 'node:process';
import {
  Client,
  EmbedBuilder,
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

function shouldCountMessage(message) {
  return (
    message.inGuild() &&
    message.author &&
    !message.author.bot &&
    !message.webhookId &&
    !message.system
  );
}

function formatCount(value) {
  return new Intl.NumberFormat('en-US').format(value);
}

client.once(Events.ClientReady, (readyClient) => {
  console.log(`Logged in as ${readyClient.user.tag}`);
});

client.on(Events.MessageCreate, (message) => {
  if (!shouldCountMessage(message)) {
    return;
  }

  store.recordMessage({
    messageId: message.id,
    guildId: message.guildId,
    channelId: message.channelId,
    authorId: message.author.id,
    createdAt: message.createdAt.toISOString()
  });
});

client.on(Events.MessageDelete, (message) => {
  if (!message.inGuild()) {
    return;
  }

  store.deleteMessage(message.id);
});

client.on(Events.MessageBulkDelete, (messages) => {
  const messageIds = [...messages.keys()];
  store.deleteMessages(messageIds);
});

client.on(Events.InteractionCreate, async (interaction) => {
  if (!interaction.isChatInputCommand() || interaction.commandName !== 'messagecount') {
    return;
  }

  const targetUser = interaction.options.getUser('member') || interaction.user;
  const count = store.getMessageCount(interaction.guildId, targetUser.id);
  const guildState = store.getGuildState(interaction.guildId);
  const hasFullHistory = Boolean(guildState?.last_full_scan_at);

  const historyLine = hasFullHistory
    ? `Fully indexed through ${guildState.last_full_scan_at}.`
    : 'Live tracking is exact, but run the backfill once to include older messages.';

  const embed = new EmbedBuilder()
    .setColor(0x5865F2)
    .setAuthor({
      name: `Requested by ${interaction.user.tag}`,
      iconURL: interaction.user.displayAvatarURL()
    })
    .setTitle('Message Count')
    .setThumbnail(targetUser.displayAvatarURL())
    .setDescription(`${targetUser} has **${formatCount(count)}** tracked messages in this server.`)
    .addFields({
      name: 'Status',
      value: historyLine
    })
    .setTimestamp();

  await interaction.reply({
    embeds: [embed],
    ephemeral: false
  });
});

for (const signal of ['SIGINT', 'SIGTERM']) {
  process.on(signal, () => {
    client.destroy();
    process.exit(0);
  });
}

client.login(config.token);

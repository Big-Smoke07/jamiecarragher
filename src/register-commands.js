import { REST, Routes, SlashCommandBuilder } from 'discord.js';
import { config } from './config.js';

const commands = [
  new SlashCommandBuilder()
    .setName('messagecount')
    .setDescription('Show the exact tracked message count for yourself or another member.')
    .addUserOption((option) =>
      option
        .setName('member')
        .setDescription('The member to check.')
        .setRequired(false)
    )
    .toJSON()
];

const rest = new REST({ version: '10' }).setToken(config.token);

async function registerCommands() {
  await rest.put(
    Routes.applicationCommands(config.clientId),
    { body: commands }
  );

  console.log('Registered global slash commands for every server the bot joins');
}

registerCommands().catch((error) => {
  console.error('Failed to register slash commands');
  console.error(error);
  process.exit(1);
});

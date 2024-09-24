#!/bin/bash

# Session name
SESSION_NAME="discord_bot"

# Path to bot script
BOT_SCRIPT="bot.bot.py"

# Create a new tmux session and run the bot
tmux new-session -d -s $SESSION_NAME "python3 -m $BOT_SCRIPT"

# Print the tmux session name
echo "Bot is running in tmux session: $SESSION_NAME"
# NextRide Bot

NextRide Bot is a Discord bot designed to provide real-time information about train departures and alerts for the MTA (Metropolitan Transportation Authority) system. This bot enhances user experience by allowing easy access to transit information directly within Discord.

## Features

- **Get Upcoming Departures**: Fetch upcoming departures for a specific station or station + route.
- **Subscribe to Alerts**: Subscribe to specific stations or routes to receive alerts about delays and service changes.
- **View Subscriptions**: Check your current subscriptions to stay informed.
- **Unsubscribe**: Easily unsubscribe from alerts when they're no longer needed.
- **Help Command**: Access a list of available commands and how to use them.

## Prerequisites

Before running the bot, ensure you have the following:

- Python 3.8 or higher
- A Discord account and a server where you have permission to add bots
- A registered application on the [Discord Developer Portal](https://discord.com/developers/applications) to obtain your bot token.

## Installation

1. **Clone the repository**:
   ```bash
   git clone https://github.com/yourusername/NextRide-Bot.git
   cd NextRide-Bot
   
2. Install dependencies: Make sure to have pip installed and run:
    ```bash
    pip install -r requirements.txt

## Setup

1. Set Up Environment Variables: Create a .env file in the project root directory with the following content:
    ```bash
    DB_HOST=YOUR_HOST
    DB_NAME=YOUR_DATABASE_NAME
    DB_USER=YOUR_DATABASE_USER
    DB_PASSWORD=YOUR_DATABASE_PASSWORD
    DB_PORT=YOUR_DATABASE_PORT
    MTA_BOT_TOKEN=YOUR_DISCORD_BOT_TOKEN

## Running the bot

1. Run the static data pipeline: Execute the following command to download static MTA data, process it, and store it in the database:
    ```bash
    ./mta_data_pipeline/pipeline_execution.sh
2. Start updating real-time feeds: Run this command to open a tmux session and update the real-time and alerts feed:
    ```bash
    ./mta_data_pipeline/feed.sh
4. Start the bot: Execute the command to run the bot:
    ```bash
    ./bot/execute_bot.sh

## Commands

Here’s a list of commands you can use with the NextRide Bot:

/departures: Get the next departures for a specified station or station+route.

/subscribe_station_alerts: Subscribe to alerts for a specific station.

/subscribe_route_alerts: Subscribe to alerts for a specific route.

/view_subscriptions: Display your current subscriptions.

/unsubscribe: Select a subscription to unsubscribe from.

/view_alerts: View alerts for your current subscriptions.

/info: Get help on how to use this bot.

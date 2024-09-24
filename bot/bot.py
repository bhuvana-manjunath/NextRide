import nextcord
import os
from nextcord.ext import commands
from nextcord import Interaction, SlashOption
from dotenv import load_dotenv
from utils.bot_queries import get_trains
from utils.bot_helpers import *

# Load environment variables from .env file
load_dotenv()

# Retrieve the bot token from environment variables
TOKEN = os.getenv('MTA_BOT_TOKEN')

# Set up intents and create a bot instance
intents = nextcord.Intents.default()
intents.message_content = True  
client = commands.Bot(command_prefix='/', intents=intents)

# Initialize bot functionality
bf = BotFunctionality(client)

@client.event
async def on_ready():
    """Event triggered when the bot has successfully connected to Discord."""
    print(f'Logged in as {client.user}')

@client.event
async def on_member_join(member):
    """Event triggered when a new member joins the server.
    
    Sends a welcome message in a specified channel.
    """
    channel = discord.utils.get(member.guild.channels, name='welcome')  # Replace with your channel name
    if channel:
        await channel.send(f"Welcome {member.mention}! Use /info to get started.")

@client.event
async def on_member_join(member):
    """Event triggered when a new member joins the server.
    
    Sends a welcome message via DM.
    """
    await member.send(f"Welcome {member.mention}! Use /info to get started.")

@client.slash_command(name="info", description="See available Commands")
async def help(ctx):
    """Slash command to display available commands and how to use them."""
    commands = (
        "/departures - Get next departures for a station or station+route\n"
        "/subscribe_station_alerts - Subscribe to a station for alerts\n"
        "/subscribe_route_alerts - Subscribe to a route for alerts\n"
        "/view_subscriptions - Display your current subscriptions\n"
        "/unsubscribe - Select a subscription to unsubscribe from\n"
        "/view_alerts - View alerts for your subscriptions\n"
        "/help - Get help on how to use this bot\n"
    )
    info_message = (
        "Welcome to NextRide Bot! 🚆\n\n"
        "Here are the commands you can use:\n"
        f"{commands}"
        "Type '/' followed by the command you want to use to get started!"
    )
    await ctx.send(info_message)

@client.slash_command(name="subscribe_station_alerts", description="Subscribe to a station for alerts.")
async def subscribe_station(
    interaction: Interaction,
    station: str = SlashOption(name="station", description="Input station name", autocomplete=True, required=True)
):
    """Slash command to subscribe to alerts for a specific station.
    
    Can only be used in direct messages.
    """
    # Check if the command is used in a DM
    if interaction.guild is not None:  # interaction.guild is None in DMs
        await interaction.response.send_message("This command can only be used in a direct message.")
        return
    
    await bf.handle_station_subscription(interaction, station)

@subscribe_station.on_autocomplete("station")
async def station_autocomplete(interaction: Interaction, current: str):
    """Autocomplete for station names based on user input."""
    suggestions = [name for name in STATION_MAP_REVERSE.keys() if name.lower().startswith(current.lower())]
    suggestions = suggestions[:25]  # Limit the suggestions to 25
    await interaction.response.send_autocomplete(suggestions if suggestions else ["No matching stations found"])

@client.slash_command(name="subscribe_route_alerts", description="Subscribe to a route for alerts.")
async def subscribe_route(
    interaction: Interaction,
    route: str = SlashOption(name="route", description="Input route name", autocomplete=True, required=True)
):
    """Slash command to subscribe to alerts for a specific route.
    
    Can only be used in direct messages.
    """
    # Check if the command is used in a DM
    if interaction.guild is not None:  # interaction.guild is None in DMs
        await interaction.response.send_message("This command can only be used in a direct message.")
        return
    
    await bf.handle_route_subscription(interaction, route)

@subscribe_route.on_autocomplete('route')
async def route_autocomplete(interaction: Interaction, current: str):
    """Autocomplete for route names based on user input."""
    suggestions = [name for name in ROUTE_MAP_REVERSE.keys() if name.lower().startswith(current.lower())]
    suggestions = suggestions[:25]  # Limit suggestions to 25
    await interaction.response.send_autocomplete(suggestions)

@client.slash_command(name="view_subscriptions", description="Display your current subscriptions.")
async def view_alert_subscriptions(interaction: Interaction):
    """Slash command to display the user's current subscriptions."""
    await bf.show_user_subscriptions(interaction)

@client.slash_command(name="unsubscribe", description="Select a subscription to unsubscribe from.")
async def unsubscribe(interaction: Interaction):
    """Slash command to unsubscribe from a selected subscription."""
    await bf.handle_unsubscribe(interaction)

@client.slash_command(name="view_alerts", description="View alerts for your subscriptions.")
async def view_alerts(interaction: Interaction):
    """Slash command to view alerts for the user's subscriptions."""
    await bf.send_alerts(interaction)

@client.slash_command(name="departures", description="Get next departures for a station or station+route")
async def station(
    interaction: Interaction,
    station: str = SlashOption(name="station", description="Input station name", autocomplete=True),
    route: str = SlashOption(name="route", description="Input route (optional)", required=False)
):
    """Slash command to get the next departures for a specific station or route."""
    await bf.handle_departures(interaction, station, route)

@station.on_autocomplete("station")
async def station_autocomplete(interaction: Interaction, current: str):
    """Autocomplete for station names based on user input."""
    suggestions = [name for name in STATION_MAP_REVERSE.keys() if name.lower().startswith(current.lower())]
    suggestions = suggestions[:25]  # Limit suggestions to 25
    await interaction.response.send_autocomplete(suggestions)

@station.on_autocomplete('route')
async def route_autocomplete(interaction: Interaction, current: str, station: str):
    """Autocomplete for route names based on the selected station."""
    # Get the selected station ID
    selected_stop_id = STATION_MAP_REVERSE.get(station, None)
    north_trains = get_trains(selected_stop_id + "N")[0].split(", ")
    south_trains = get_trains(selected_stop_id + "S")[0].split(", ")
    
    if selected_stop_id:
        # Create suggestions for northbound trains
        north_suggestions = [
            f"{ROUTE_MAP[route_id]} - Northbound" 
            for route_id in north_trains
        ]
        # Create suggestions for southbound trains
        south_suggestions = [
            f"{ROUTE_MAP[route_id]} - Southbound"
            for route_id in south_trains
        ]
        # Combine both suggestions
        suggestions = north_suggestions + south_suggestions
    else:
        suggestions = ["Please select a station first."]
    
    await interaction.response.send_autocomplete(suggestions)

@client.event
async def on_message(message):
    """Event triggered on receiving a message.
    
    Processes commands unless the message is from the bot itself.
    """
    if message.author == client.user:
        return
    
    await client.process_commands(message)

# Run the bot with the provided token
client.run(TOKEN)

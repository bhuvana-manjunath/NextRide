import discord
from discord.ui import Select, View
from utils.db import Database
from utils.bot_queries import *

# Initialize the Database instance
db = Database()
db.initialize_pool()

# Fetch station and route options from the database
STATIONS = get_station_options()  
STATION_MAP = {stn[0]: f"{stn[1]}, {stn[2]} ({stn[3]})" for stn in STATIONS}  # Mapping of station IDs to formatted strings
STATION_MAP_REVERSE = {f"{stn[1]}, {stn[2]} ({stn[3]})": stn[0] for stn in STATIONS}  # Reverse mapping for lookup
ROUTES = get_route_options()  
ROUTE_MAP = {rte[0]: f"{rte[1]} ({rte[2]})" for rte in ROUTES}  # Mapping of route IDs to formatted strings
ROUTE_MAP_REVERSE = {f"{rte[1]} ({rte[2]})": rte[0] for rte in ROUTES}  # Reverse mapping for lookup

class BotFunctionality:
    def __init__(self, bot):
        """
        Initializes the BotFunctionality class.

        Args:
            bot (discord.Client): The Discord bot instance.
        """
        self.bot = bot

    def create_dropdown_menu(self, placeholder, options):
        """
        Create a dropdown menu for Discord with the given placeholder and options.

        Args:
            placeholder (str): The placeholder text for the dropdown menu.
            options (list of dict): A list of dictionaries containing 'label' and 'value' for each option.

        Returns:
            discord.ui.Select: A Select component for Discord.
        """
        select_options = [
            discord.SelectOption(
                label=option['label'],
                value=option['value']
            )
            for option in options
        ]

        return Select(placeholder=placeholder, options=select_options)
    
    async def get_or_create_user_id(self, user):
        """
        Retrieve the user_id from the database, or create a new entry if the user doesn't exist.

        Args:
            user (str): The username of the user.

        Returns:
            int: The user_id of the user.
        """
        user_record = db.execute_sql_query("SELECT user_id FROM users WHERE username = %s", (user,))
        if user_record:
            return user_record[0]['user_id']

        # Insert new user and retrieve the user_id
        db.insert_data("users", ("username",), [(user,)])

        user_record = db.execute_sql_query("SELECT user_id FROM users WHERE username = %s", (user,))
        return user_record[0]['user_id']

    async def handle_station_subscription(self, interaction, station):
        """
        Handle the process of subscribing the user to a station.

        Args:
            interaction (discord.Interaction): The interaction that triggered the subscription.
            station (str): The name of the station to subscribe to.
        """
        stop_id = STATION_MAP_REVERSE.get(station, None)
        user_id = await self.get_or_create_user_id(str(interaction.user.name))

        # Check if the user is already subscribed to the station
        exists = db.execute_sql_query(
            "SELECT 1 FROM subscriptions WHERE user_id = %s AND stop_id = %s", (user_id, stop_id)
        )
        if exists:
            await interaction.response.send_message(f'You are already subscribed to {station} station.')
        else:
            # Store the subscription
            try:
                db.insert_data("subscriptions", ("user_id", "stop_id", "route_id"), [(user_id, stop_id, None)])
                await interaction.response.send_message(f'Successfully subscribed to {station} station.')
            except Exception as e:
                await interaction.response.send_message(f'Failed to subscribe: {e}')

    async def handle_route_subscription(self, interaction, route_name):
        """
        Handle the process of subscribing the user to a route.

        Args:
            interaction (discord.Interaction): The interaction that triggered the subscription.
            route_name (str): The name of the route to subscribe to.
        """
        route_id = ROUTE_MAP_REVERSE.get(route_name, None)
        user_id = await self.get_or_create_user_id(str(interaction.user.name))

        # Check if the user is already subscribed
        exists = db.execute_sql_query(
            "SELECT 1 FROM subscriptions WHERE user_id = %s AND route_id = %s", (user_id, route_id)
        )
        if exists:
            await interaction.response.send_message(f'You are already subscribed to route {route_name}.')
        else:
            try:
                db.insert_data("subscriptions", ("user_id", "stop_id", "route_id"), [(user_id, None, route_id)])
                await interaction.response.send_message(f'Successfully subscribed to route {route_name}.')
            except Exception as e:
                await interaction.response.send_message(f'Failed to subscribe: {e}')
    
    async def show_user_subscriptions(self, interaction):
        """
        Displays the user's current subscriptions.

        Args:
            interaction (discord.Interaction): The interaction that triggered the display of subscriptions.
        """
        user_id = await self.get_or_create_user_id(str(interaction.user.name))

        subscriptions = get_user_subscriptions(user_id)
        if not subscriptions:
            await interaction.response.send_message("You don't have any subscriptions yet.")
            return

        response = "Your subscriptions:\n"
        for i, subs in enumerate(subscriptions):
            _, stop_id, route_id = subs
            if stop_id:
                response += f"{i}. {STATION_MAP.get(stop_id, None)}\n"
            elif route_id:
                response += f"{i}. {ROUTE_MAP.get(route_id, None)}\n"

        await interaction.response.send_message(response)

    async def handle_unsubscribe(self, interaction):
        """
        Handles the process of unsubscribing the user from one of their subscriptions.

        Args:
            interaction (discord.Interaction): The interaction that triggered the unsubscribe action.
        """
        user_id = await self.get_or_create_user_id(str(interaction.user.name))

        subscriptions = get_user_subscriptions(user_id)
        if not subscriptions:
            await interaction.response.send_message("You don't have any subscriptions to unsubscribe from.")
            return

        # Prepare the dropdown menu options
        options = [
            {
                'label': STATION_MAP.get(stop_id, ROUTE_MAP.get(route_id, None)),
                'value': str(subscription_id)
            }
            for subscription_id, stop_id, route_id in subscriptions
        ]

        select_menu = self.create_dropdown_menu("Choose a subscription to unsubscribe", options)

        async def unsubscribe_callback(interaction):
            """
            Callback for handling the unsubscription process after a selection is made.
            
            Args:
                interaction (discord.Interaction): The interaction for the unsubscription.
            """
            subscription_id = select_menu.values[0]
            db.delete_records("subscriptions", {"subscription_id": subscription_id, "user_id": user_id})
            await interaction.response.send_message("Successfully unsubscribed!")

        select_menu.callback = unsubscribe_callback
        view = View()
        view.add_item(select_menu)

        await interaction.response.send_message("Select the subscription you want to unsubscribe from:", view=view)

    async def send_alerts(self, interaction):
        """
        Fetch and send alerts to the user based on their subscriptions.

        Args:
            interaction (discord.Interaction): The interaction that triggered the alert sending.
        """
        user_id = await self.get_or_create_user_id(str(interaction.user.name))
        alerts = fetch_user_alerts(user_id)

        if not alerts:
            await interaction.response.send_message("No new alerts available.")
            return

        # Organize alerts by entity (route/stop)
        organized_alerts = {}
        for alert in alerts:
            entity_id = alert[6]
            alert_info = {
                'header_text': alert[1],
                'description_text': alert[2],
                'start_time': alert[3],
                'end_time': alert[4],
                'status': alert[5]
            }
            organized_alerts.setdefault(entity_id, []).append(alert_info)

        # Prepare and send alerts
        alert_messages = []
        for entity, entity_alerts in organized_alerts.items():
            entity_message = f"**Alerts for {entity}:**\n"
            for alert in entity_alerts:
                if alert['status'] == "active":
                    entity_message += (
                        f"**:warning:: {alert['header_text']}**\n"
                        f"{alert['description_text']}\n"
                        f"**Active From**: {alert['start_time']} to {alert['end_time']}\n\n"
                    )
            if entity_message:
                alert_messages.append(entity_message)

        # Send alerts, splitting messages if they exceed Discord's character limit
        for alert_message in alert_messages:
            if len(alert_message) > 4000:
                for i in range(0, len(alert_message), 2000):
                    chunk = alert_message[i:i + 2000]
                    await interaction.response.send_message(chunk)
            else:
                await interaction.response.send_message(alert_message)

    async def handle_departures(self, interaction, station, route):
        """
        Fetch and display upcoming departure times for a specified station and route.

        Args:
            interaction (discord.Interaction): The interaction that triggered the departure request.
            station (str): The name of the station to check departures for.
            route (str): The route to check departures for, if specified (format: "RouteName - Direction").
        """
        # Retrieve the stop ID for the selected station
        selected_stop_id = STATION_MAP_REVERSE.get(station, None)
        
        if route:
            # Split the route into its name and direction
            route_name, direction = route.split(" - ")
            selected_route_id = ROUTE_MAP_REVERSE.get(route_name, None)

            # Determine the direction and fetch departures accordingly
            if direction == "Northbound":
                departures = get_route_departures(selected_stop_id + "N", selected_route_id)
            elif direction == "Southbound":
                departures = get_route_departures(selected_stop_id + "S", selected_route_id)
            
            # Check if there are no departures found
            if not departures:
                await interaction.response.send_message("No upcoming departures")
                return

            # Prepare the message for upcoming departures for the specific route and direction
            message = f"Upcoming departures for {direction} {route_name} at {station}:\n"
            for _, _, eta in departures:
                message += f"  Departs in {eta} min\n"  # Include ETA for each departure
            
            await interaction.response.send_message(message)
        else:
            # Fetch northbound and southbound departures separately if no specific route is provided
            north_departures = get_station_departures(selected_stop_id + "N")
            south_departures = get_station_departures(selected_stop_id + "S")

            # Check if there are no departures in either direction
            if not north_departures and not south_departures:
                await interaction.response.send_message("No upcoming departures")
                return

            # Combine the departures into a list for both directions
            all_departures = [("Northbound", north_departures), ("Southbound", south_departures)]

            # Generate the message summarizing upcoming departures
            message = f"Upcoming departures for {station}:\n"
            message += "\n".join(
                f"{direction} - {route_id}: Departs in {eta} min"
                for direction, departures in all_departures
                for route_id, _, eta in departures  # Extract route ID and ETA from departures
            )
            
            await interaction.response.send_message(message)

#!/bin/bash

# Define the tmux session name
SESSION_NAME_1="real_time_feed"

# Create a new tmux session and run the bot
tmux new-session -d -s $SESSION_NAME_1 "while true; do python3 -m mta_data_pipeline.upload_real_time_feed_to_postgres; sleep 30; done"

# Print the tmux session name
echo "Real-time feed at: $SESSION_NAME_1"

# Define the tmux session name
SESSION_NAME_2="alerts_feed"

# Create a new tmux session and run the bot
tmux new-session -d -s $SESSION_NAME_2 "while true; do python3 -m mta_data_pipeline.upload_alerts_feed_to_postgres; sleep 30; done" 

# Print the tmux session name
echo "Alerts feed at: $SESSION_NAME_2"




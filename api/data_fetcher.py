import requests
import pandas as pd
from datetime import datetime
import os

# Create output directory
output_dir = "data/fpl_2024_25_data"
os.makedirs(output_dir, exist_ok=True)

# FPL API URL
url = "https://fantasy.premierleague.com/api/bootstrap-static/"

# Fetch JSON data from the FPL API
response = requests.get(url, verify=False)
if response.status_code != 200:
    raise Exception(f"Failed to fetch data: {response.status_code}")
data = response.json()

# Extract relevant data
players = data['elements']
teams = data['teams']
events = data['events']
positions = data['element_types']

# Convert to DataFrames
players_df = pd.DataFrame(players)
teams_df = pd.DataFrame(teams)
events_df = pd.DataFrame(events)
positions_df = pd.DataFrame(positions)

# Clean and convert dates
events_df['deadline_time'] = pd.to_datetime(events_df['deadline_time']).dt.tz_localize(None)

# Save to CSV files
players_df.to_csv(os.path.join(output_dir, "players.csv"), index=False)
teams_df.to_csv(os.path.join(output_dir, "teams.csv"), index=False)
events_df.to_csv(os.path.join(output_dir, "events.csv"), index=False)
positions_df.to_csv(os.path.join(output_dir, "positions.csv"), index=False)

print("FPL 2024-25 data fetched and saved successfully.")

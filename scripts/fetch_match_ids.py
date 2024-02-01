import requests
import json
import subprocess
import os
import sys
import datetime

# Function to read JSON from HDFS
def read_json_from_hdfs(hdfs_file_path):
    try:
        result = subprocess.run(["hdfs", "dfs", "-cat", hdfs_file_path], capture_output=True, text=True)
        return json.loads(result.stdout) if result.returncode == 0 else []
    except json.JSONDecodeError:
        return []

# Function to check if HDFS is running
def is_hdfs_running():
    result = subprocess.run(["hdfs", "dfsadmin", "-report"], capture_output=True)
    if result.returncode != 0:
        print("HDFS is not running. Exiting script.")
        sys.exit(1)
    return True

# Function to upload files to HDFS
def upload_to_hdfs(local_path, hdfs_path):
    try:
        subprocess.run(["hdfs", "dfs", "-mkdir", "-p", os.path.dirname(hdfs_path)], check=True)
        subprocess.run(["hdfs", "dfs", "-put", "-f", local_path, hdfs_path], check=True)
        print(f"Successfully uploaded {local_path} to {hdfs_path}")
    except subprocess.CalledProcessError as e:
        print(f"Failed to upload {local_path} to {hdfs_path}: {e}")
        sys.exit(1)

# Modified function to get the start of day epoch based on days before
def get_start_of_day_epoch(days_before=0):
    now = datetime.datetime.now() - datetime.timedelta(days=days_before)
    start_of_day = datetime.datetime(now.year, now.month, now.day)
    return int(start_of_day.timestamp())

# Function to get ranked match IDs
def get_ranked_match_ids(puuid, api_key, start_time, end_time=None, total_matches=100):
    base_url = f"https://europe.api.riotgames.com/lol/match/v5/matches/by-puuid/{puuid}/ids"
    params = {"type": "ranked", "count": total_matches, "startTime": start_time}
    if end_time:
        params["endTime"] = end_time
    headers = {"X-Riot-Token": api_key}
    response = requests.get(base_url, headers=headers, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"API call failed: Status Code {response.status_code}, Response {response.text}")
        return None

# Main Execution
if not is_hdfs_running():
    exit(1)

script_dir = os.path.dirname(os.path.abspath(__file__))
api_key = "RGAPI-7e521220-2146-413d-bf80-2e5655cd5927"
hdfs_summoner_details_path = "/user/hadoop/lol/raw/summoner_details.json"
hdfs_match_ids_directory = "/user/hadoop/lol/raw/match_ids"
local_directory = os.path.join(script_dir, "../output")

# Use the modified function for dynamic start time
days_before_start = 1  # How many days before today for startTime
days_before_end = None  # Optionally, set this for endTime
start_time = get_start_of_day_epoch(days_before_start)
end_time = get_start_of_day_epoch(days_before_end) if days_before_end else None

summoner_details = read_json_from_hdfs(hdfs_summoner_details_path)
if not summoner_details:
    print("No summoner details available. Exiting.")
    sys.exit(1)

for summoner_name, puuid in summoner_details.items():
    new_match_ids = get_ranked_match_ids(puuid, api_key, start_time, end_time)
    if new_match_ids is not None:
        local_path = f"{local_directory}/{summoner_name}_match_ids.json"
        hdfs_path = f"{hdfs_match_ids_directory}/{summoner_name}_match_ids.json"
        with open(local_path, 'w') as file:
            json.dump(new_match_ids, file, indent=4)
        upload_to_hdfs(local_path, hdfs_path)
        print(f"Updated Match IDs for {summoner_name} saved and uploaded to HDFS.")

print("Script execution completed.")

import requests
import json
import subprocess
import os
import sys

def is_hdfs_running():
    result = subprocess.run(["hdfs", "dfsadmin", "-report"], capture_output=True)
    if result.returncode != 0:
        print("HDFS is not running. Exiting script.")
        sys.exit(1)  # Exit with an error status
    return True

# Check if HDFS is running
if not is_hdfs_running():
    print("HDFS is not running. Exiting script.")
    exit(1)

def upload_to_hdfs(local_path, hdfs_path):
    try:
        subprocess.run(["hdfs", "dfs", "-put", "-f", local_path, hdfs_path], check=True)
        print(f"Successfully uploaded {local_path} to {hdfs_path}")
    except subprocess.CalledProcessError as e:
        print(f"Failed to upload {local_path} to {hdfs_path}: {e}")
        sys.exit(1)

def read_puuids_from_hdfs(hdfs_file_path):
    result = subprocess.run(["hdfs", "dfs", "-cat", hdfs_file_path], capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Error reading file from HDFS: {result.stderr}")
        sys.exit(1)
    return json.loads(result.stdout)

def get_ranked_match_ids(puuid, api_key, total_matches=100):
    base_url = f"https://europe.api.riotgames.com/lol/match/v5/matches/by-puuid/{puuid}/ids"
    params = {"type": "ranked", "count": total_matches}
    headers = {"X-Riot-Token": api_key}

    response = requests.get(base_url, headers=headers, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"API call failed: Status Code {response.status_code}, Response {response.text}")
        return None

def save_match_ids_to_hdfs_and_local(match_ids, summoner_name, hdfs_directory, local_directory):
    local_path = f"{local_directory}/{summoner_name}_match_ids.json"
    with open(local_path, 'w') as file:
        json.dump(match_ids, file, indent=4)
    
    upload_to_hdfs(local_path, hdfs_directory)

# Main Execution
script_dir = os.path.dirname(os.path.abspath(__file__))

api_key = "RGAPI-d3040259-9084-49bb-ad43-b01382eb358c"
hdfs_summoner_details_path = "/user/hadoop/lol/raw/summoner_details.json"
hdfs_match_ids_directory = "/user/hadoop/lol/raw/match_ids"
local_directory = os.path.join(script_dir, "../output")  # Adjust if needed

summoner_details = read_puuids_from_hdfs(hdfs_summoner_details_path)
if not summoner_details:
    print("No summoner details available. Exiting.")
    sys.exit(1)

successful_fetch = False
for summoner_name, puuid in summoner_details.items():
    match_ids = get_ranked_match_ids(puuid, api_key)
    if match_ids:
        save_match_ids_to_hdfs_and_local(match_ids, summoner_name, hdfs_match_ids_directory, local_directory)
        print(f"Match IDs for {summoner_name} saved locally and uploaded to HDFS.")
        successful_fetch = True
    else:
        print(f"Failed to fetch match IDs for {summoner_name}")

if not successful_fetch:
    print("Failed to fetch match IDs for all summoners. Exiting.")
    sys.exit(1)

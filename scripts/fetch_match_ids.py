import requests
import json
import subprocess
import os
import sys

def read_json_from_hdfs(hdfs_file_path):
    try:
        result = subprocess.run(["hdfs", "dfs", "-cat", hdfs_file_path], capture_output=True, text=True)
        return json.loads(result.stdout) if result.returncode == 0 else []
    except json.JSONDecodeError:
        return []

def is_hdfs_running():
    result = subprocess.run(["hdfs", "dfsadmin", "-report"], capture_output=True)
    if result.returncode != 0:
        print("HDFS is not running. Exiting script.")
        sys.exit(1)
    return True

def upload_to_hdfs(local_path, hdfs_path):
    try:
        subprocess.run(["hdfs", "dfs", "-mkdir", "-p", os.path.dirname(hdfs_path)], check=True)
        subprocess.run(["hdfs", "dfs", "-put", "-f", local_path, hdfs_path], check=True)
        print(f"Successfully uploaded {local_path} to {hdfs_path}")
    except subprocess.CalledProcessError as e:
        print(f"Failed to upload {local_path} to {hdfs_path}: {e}")
        sys.exit(1)

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

def update_match_ids(local_path, hdfs_path, new_match_ids, existing_ids):
    updated_ids = list(set(existing_ids).union(set(new_match_ids)))
    with open(local_path, 'w') as file:
        json.dump(updated_ids, file, indent=4)
    upload_to_hdfs(local_path, hdfs_path)
    return updated_ids

# Main Execution
if not is_hdfs_running():
    exit(1)

script_dir = os.path.dirname(os.path.abspath(__file__))
api_key = "RGAPI-847e8ec1-ff05-46fc-b51b-ce18b1e2a991"
hdfs_summoner_details_path = "/user/hadoop/lol/raw/summoner_details.json"
hdfs_match_ids_directory = "/user/hadoop/lol/raw/match_ids"
local_directory = os.path.join(script_dir, "../output")

summoner_details = read_json_from_hdfs(hdfs_summoner_details_path)
if not summoner_details:
    print("No summoner details available. Exiting.")
    sys.exit(1)

for summoner_name, puuid in summoner_details.items():
    new_match_ids = get_ranked_match_ids(puuid, api_key)
    if new_match_ids is not None:
        local_path = f"{local_directory}/{summoner_name}_match_ids.json"
        hdfs_path = f"{hdfs_match_ids_directory}/{summoner_name}_match_ids.json"
        existing_ids = set(read_json_from_hdfs(hdfs_path))
        updated_ids = update_match_ids(local_path, hdfs_path, new_match_ids, existing_ids)

        if updated_ids:
            print(f"Updated Match IDs for {summoner_name} saved locally and uploaded to HDFS.")
        else:
            print(f"No new match IDs to update for {summoner_name}.")
    else:
        print(f"Failed to fetch match IDs for {summoner_name}")

print("Script execution completed.")

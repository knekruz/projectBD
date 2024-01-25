import requests
import json
import subprocess
import os

# Change current working directory to the script's directory
os.chdir(os.path.dirname(os.path.abspath(__file__)))

def read_puuids_from_hdfs(hdfs_file_path):
    # Fetch the summoner details file from HDFS
    result = subprocess.run(["hdfs", "dfs", "-cat", hdfs_file_path], capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Error reading file from HDFS: {result.stderr}")
        return None
    return json.loads(result.stdout)

def get_ranked_match_ids(puuid, api_key, total_matches=100):
    base_url = f"https://europe.api.riotgames.com/lol/match/v5/matches/by-puuid/{puuid}/ids"
    params = {"type": "ranked", "count": total_matches}
    headers = {"X-Riot-Token": api_key}

    response = requests.get(base_url, headers=headers, params=params)

    if response.status_code == 200:
        return response.json()
    else:
        return f"Error: {response.status_code}"

def save_match_ids_to_hdfs_and_local(match_ids, summoner_name, hdfs_directory, local_directory):
    # Save to local file in the root folder
    local_path = f"{local_directory}/{summoner_name}_match_ids.json"
    with open(local_path, 'w') as file:
        json.dump(match_ids, file, indent=4)
    
    # Upload to HDFS
    hdfs_path = f"{hdfs_directory}/{summoner_name}_match_ids.json"
    subprocess.run(["hdfs", "dfs", "-put", local_path, hdfs_path])

# Main Execution
api_key = "RGAPI-09292aea-f9cb-4022-b22c-0b3c5271ba64"
hdfs_summoner_details_path = "/user/hadoop/lol/raw/summoner_details.json"
hdfs_match_ids_directory = "/user/hadoop/lol/raw/match_ids"
local_directory = "../"  # Replace with your project root directory

summoner_details = read_puuids_from_hdfs(hdfs_summoner_details_path)
if summoner_details:
    for summoner_name, puuid in summoner_details.items():
        match_ids = get_ranked_match_ids(puuid, api_key)
        if isinstance(match_ids, list):
            save_match_ids_to_hdfs_and_local(match_ids, summoner_name, hdfs_match_ids_directory, local_directory)
            print(f"Match IDs for {summoner_name} saved locally and uploaded to HDFS.")
        else:
            print(f"Failed to fetch match IDs for {summoner_name}: {match_ids}")

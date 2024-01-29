import requests
import json
import subprocess
import os
import sys
import time

# Change current working directory to the script's directory
script_dir = os.path.dirname(os.path.abspath(__file__))

def is_hdfs_running():
    result = subprocess.run(["hdfs", "dfsadmin", "-report"], capture_output=True)
    if result.returncode != 0:
        print("HDFS is not running. Exiting script.")
        sys.exit(1)
    return True

if not is_hdfs_running():
    sys.exit(1)

def fetch_match_details(match_id, api_key, region="europe"):
    base_url = f"https://{region}.api.riotgames.com/lol/match/v5/matches/{match_id}"
    headers = {"X-Riot-Token": api_key}
    response = requests.get(base_url, headers=headers)
    
    if response.status_code == 200 and response.json():
        return response.json()
    elif response.status_code == 429:  # Rate limit exceeded
        return "rate_limit_exceeded"
    else:
        print(f"API call failed for Match ID {match_id}: Status Code {response.status_code}, Response {response.text}")
        sys.exit(1)

def list_files_in_hdfs_directory(hdfs_directory):
    result = subprocess.run(["hdfs", "dfs", "-ls", hdfs_directory], capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Error listing files in HDFS directory: {result.stderr}")
        return []
    lines = result.stdout.strip().split('\n')[1:]  # Skip the first line which is a header
    files = [line.split()[-1] for line in lines if len(line.split()) > 0]
    return files

def read_match_ids_from_hdfs(hdfs_file_path):
    result = subprocess.run(["hdfs", "dfs", "-cat", hdfs_file_path], capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Error reading file from HDFS: {result.stderr}")
        return None
    return json.loads(result.stdout)

def save_match_history_to_hdfs_and_local(match_history, summoner_name, match_id, hdfs_directory, local_directory):
    local_path = f"{local_directory}/{summoner_name}_{match_id}.json"
    with open(local_path, 'w') as file:
        json.dump(match_history, file, indent=4)

    hdfs_path = f"{hdfs_directory}/{summoner_name}/{match_id}.json"
    subprocess.run(["hdfs", "dfs", "-mkdir", "-p", os.path.dirname(hdfs_path)])
    upload_result = subprocess.run(["hdfs", "dfs", "-put", local_path, hdfs_path], capture_output=True, text=True)
    if upload_result.returncode != 0:
        print(f"Error uploading file to HDFS: {upload_result.stderr}")
    else:
        print(f"Match history for {summoner_name}, Match ID: {match_id}, saved successfully.")

# Main Execution
api_key = "RGAPI-d3040259-9084-49bb-ad43-b01382eb358c"
hdfs_match_ids_directory = "/user/hadoop/lol/raw/match_ids"
hdfs_match_histories_directory = "/user/hadoop/lol/raw/match_histories"
local_directory = os.path.join(script_dir, "../output/history") 

match_id_files = list_files_in_hdfs_directory(hdfs_match_ids_directory)
matches_per_summoner = 100  # Number of matches to fetch per summoner

for file_path in match_id_files:
    summoner_name = os.path.basename(file_path).split('_')[0]
    match_ids = read_match_ids_from_hdfs(file_path)
    if match_ids:
        match_count = 0
        for match_id in match_ids:
            if match_count >= matches_per_summoner:
                break
            match_history = fetch_match_details(match_id, api_key)
            if match_history == "rate_limit_exceeded":
                print("Rate limit exceeded, waiting before next request...")
                time.sleep(30)  # Wait for a second before retrying
                continue
            elif match_history == "error":
                print(f"Error fetching match history for ID {match_id}. Exiting.")
                sys.exit(1)
            elif isinstance(match_history, dict):
                save_match_history_to_hdfs_and_local(match_history, summoner_name, match_id, hdfs_match_histories_directory, local_directory)
                match_count += 1

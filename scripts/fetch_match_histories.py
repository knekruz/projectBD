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

def fetch_match_details(match_id, api_key, region="europe"):
    base_url = f"https://{region}.api.riotgames.com/lol/match/v5/matches/{match_id}"
    headers = {"X-Riot-Token": api_key}
    response = requests.get(base_url, headers=headers)
    
    if response.status_code == 200 and response.json():
        return response.json()
    elif response.status_code == 429:
        print("Rate limit exceeded, waiting before next request...")
        time.sleep(30)
        return "rate_limit_exceeded"
    else:
        print(f"API call failed for Match ID {match_id}: Status Code {response.status_code}, Response {response.text}")
        return "error"

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

def match_history_exists(hdfs_path):
    check_result = subprocess.run(["hdfs", "dfs", "-test", "-e", hdfs_path], capture_output=True)
    return check_result.returncode == 0

def save_match_history_to_hdfs(match_history, hdfs_path):
    local_path = f"/tmp/{os.path.basename(hdfs_path)}"
    with open(local_path, 'w') as file:
        json.dump(match_history, file, indent=4)
    
    subprocess.run(["hdfs", "dfs", "-mkdir", "-p", os.path.dirname(hdfs_path)])
    upload_result = subprocess.run(["hdfs", "dfs", "-put", "-f", local_path, hdfs_path], capture_output=True, text=True)

    if upload_result.returncode != 0:
        print(f"Error uploading file to HDFS: {upload_result.stderr}")
    else:
        print(f"Match history saved to HDFS: {hdfs_path}")

if not is_hdfs_running():
    sys.exit(1)

api_key = "RGAPI-847e8ec1-ff05-46fc-b51b-ce18b1e2a991"
hdfs_match_ids_directory = "/user/hadoop/lol/raw/match_ids"
hdfs_match_histories_directory = "/user/hadoop/lol/raw/match_histories"

match_id_files = list_files_in_hdfs_directory(hdfs_match_ids_directory)
matches_per_summoner = 100

for file_path in match_id_files:
    summoner_name = os.path.basename(file_path).split('_')[0]
    match_ids = read_match_ids_from_hdfs(file_path)
    if not match_ids:
        print(f"No new matches to fetch for {summoner_name}")
        continue

    fetched_count, existing_count = 0, 0
    for match_id in match_ids:
        hdfs_match_path = f"{hdfs_match_histories_directory}/{summoner_name}/{match_id}.json"

        if match_history_exists(hdfs_match_path):
            print(f"Match ID {match_id} already exists for {summoner_name}")
            existing_count += 1
            continue

        if fetched_count >= matches_per_summoner:
            break

        match_history = fetch_match_details(match_id, api_key)
        if match_history == "rate_limit_exceeded":
            continue
        elif match_history == "error":
            continue
        elif isinstance(match_history, dict):
            save_match_history_to_hdfs(match_history, hdfs_match_path)
            fetched_count += 1

    print(f"For {summoner_name}, Total matches fetched: {fetched_count}, Total existing matches: {existing_count}")

print("Script execution completed.")

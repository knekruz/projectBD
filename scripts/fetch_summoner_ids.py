import requests
import json
import subprocess
import os
import sys

def upload_to_hdfs(local_path, hdfs_path):
    try:
        subprocess.run(["hdfs", "dfs", "-put", "-f", local_path, hdfs_path], check=True)
        print(f"Successfully uploaded {local_path} to {hdfs_path}")
    except subprocess.CalledProcessError as e:
        print(f"Failed to upload {local_path} to {hdfs_path}: {e}")
        sys.exit(1)

def is_hdfs_running():
    result = subprocess.run(["hdfs", "dfsadmin", "-report"], capture_output=True)
    if result.returncode != 0:
        print("HDFS is not running. Exiting script.")
        sys.exit(1)
    return True

def get_summoner_details(summoner_name, api_key):
    base_url = "https://euw1.api.riotgames.com/lol/summoner/v4/summoners/by-name/"
    url = f"{base_url}{summoner_name}"
    headers = {"X-Riot-Token": api_key}
    response = requests.get(url, headers=headers)
    return response.json().get("puuid", None) if response.status_code == 200 else None

def read_json_file(file_name):
    if os.path.exists(file_name):
        with open(file_name, "r") as file:
            return json.load(file)
    return {}

def write_results_to_file(file_name, data):
    with open(file_name, "w") as file:
        json.dump(data, file, indent=4)


api_key = "RGAPI-847e8ec1-ff05-46fc-b51b-ce18b1e2a991"
script_dir = os.path.dirname(os.path.abspath(__file__))
summoner_names_file = os.path.join(script_dir, "../data/summoner_names.json")
summoner_details_file = os.path.join(script_dir, "../output/summoner_details.json")

if not is_hdfs_running():
    sys.exit(1)

summoner_names = read_json_file(summoner_names_file)
summoner_details = read_json_file(summoner_details_file)

for summoner_name, custom_name in summoner_names.items():
    if custom_name not in summoner_details:
        puuid = get_summoner_details(summoner_name, api_key)
        if puuid:
            summoner_details[custom_name] = puuid
            print(f"Added {custom_name} with PUUID: {puuid}")
        else:
            print(f"Failed to fetch details for {summoner_name}")

write_results_to_file(summoner_details_file, summoner_details)
upload_to_hdfs(summoner_details_file, "/user/hadoop/lol/raw")

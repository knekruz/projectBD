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
        sys.exit(1)  # Exit with error status
    return True

def get_summoner_details(summoner_name, api_key):
    base_url = "https://euw1.api.riotgames.com/lol/summoner/v4/summoners/by-name/"
    url = f"{base_url}{summoner_name}"
    headers = {"X-Riot-Token": api_key}
    
    response = requests.get(url, headers=headers)
    if response.status_code == 200 and response.json():
        data = response.json()
        return {summoner_name: data.get("puuid", None)}
    else:
        print(f"API call failed for {summoner_name}: Status Code {response.status_code}, Response {response.text}")
        return None  # Return None for invalid responses

def read_json_file(file_name):
    if os.path.exists(file_name):
        with open(file_name, "r") as file:
            return json.load(file)
    return {}

def write_results_to_file(file_name, data):
    with open(file_name, "w") as file:
        json.dump(data, file)

# Replace with your actual API key
api_key = "RGAPI-d3040259-9084-49bb-ad43-b01382eb358c"

# Check if HDFS is running
if not is_hdfs_running():
    print("HDFS is not running. Exiting script.")
    exit(1)

# Paths for the summoner names file and the details file
script_dir = os.path.dirname(os.path.abspath(__file__))
summoner_names_file = os.path.join(script_dir, "../data/summoner_names.json")
summoner_details_file = os.path.join(script_dir, "../summoner_details.json")

summoner_names = read_json_file(summoner_names_file)

# Fetch summoner details
summoner_details = {}
successful_api_call = False
for name in summoner_names:
    detail = get_summoner_details(name, api_key)
    if detail is not None:
        summoner_details.update(detail)
        successful_api_call = True
    else:
        print(f"No data fetched for summoner: {name}")

if not successful_api_call:
    print("All API calls failed. Exiting script.")
    sys.exit(1)  # Exit with error status if all API calls fail

# Write updated details to local file and upload to HDFS
if summoner_details:
    write_results_to_file(summoner_details_file, summoner_details)
    hdfs_output_path = "/user/hadoop/lol/raw"
    upload_to_hdfs(summoner_details_file, hdfs_output_path)
else:
    print("No new data to update.")

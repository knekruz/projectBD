import requests
import json
import subprocess
import os

# Change current working directory to the script's directory
os.chdir(os.path.dirname(os.path.abspath(__file__)))

def get_summoner_details(summoner_name, api_key):
    base_url = "https://euw1.api.riotgames.com/lol/summoner/v4/summoners/by-name/"
    url = f"{base_url}{summoner_name}"
    headers = {"X-Riot-Token": api_key}
    
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        return f"Error: {response.status_code}"

def read_summoner_names(file_name):
    with open(file_name, "r") as file:
        return json.load(file)

def write_results_to_file(file_name, data):
    with open(file_name, "w") as file:
        json.dump(data, file)

def upload_to_hdfs(local_path, hdfs_path):
    subprocess.run(["hdfs", "dfs", "-put", local_path, hdfs_path])

# Replace with your actual API key
api_key = "RGAPI-09292aea-f9cb-4022-b22c-0b3c5271ba64"

# Path to the summoner names file in the 'data' directory
summoner_names_file = "../data/summoner_names.json"
summoner_names = read_summoner_names(summoner_names_file)

# Storing results
results = {}
for name in summoner_names:
    details = get_summoner_details(name, api_key)
    if isinstance(details, dict):  # Check if response is valid
        results[name] = details.get("puuid", "Not found")

# Path for the output file in the root folder
output_file = "../summoner_details.json"
write_results_to_file(output_file, results)

# Path for uploading to HDFS (modify as needed)
hdfs_output_path = "/user/hadoop/summoner_details"  # Update with your HDFS directory
upload_to_hdfs(output_file, hdfs_output_path)

# Optionally, delete the local file after uploading
# os.remove(output_file)

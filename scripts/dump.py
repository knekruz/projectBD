# Old move scprit to move files from hdfs to local use old structure formatted/Summoner/Match_id_folder

""" import subprocess
import os

# Define the paths
formatted_data_path = "/user/hadoop/lol/formatted"
local_directory = "/home/hadoop/Desktop/formatted"
files_to_copy_per_summoner = 2 # Number of files to copy per summoner

# Ensure local directory exists
os.makedirs(local_directory, exist_ok=True)

# Function to copy a limited number of files from HDFS to local directory
def copy_files_to_local(summoner_name):
    summoner_formatted_path = f"{formatted_data_path}/{summoner_name}"

    # List files in the formatted directory
    formatted_files_list_cmd = subprocess.Popen(["hdfs", "dfs", "-ls", summoner_formatted_path], stdout=subprocess.PIPE)
    formatted_files_output = formatted_files_list_cmd.communicate()[0].decode()
    formatted_files = [line.split()[-1] for line in formatted_files_output.strip().split('\n')[1:] if len(line.split()) > 0]

    # Copy a limited number of files to the local directory
    for file_path in formatted_files[:files_to_copy_per_summoner]:
        local_file_path = os.path.join(local_directory, os.path.basename(file_path))
        subprocess.run(["hdfs", "dfs", "-get", file_path, local_file_path])

# Process each summoner
summoners_list_cmd = subprocess.Popen(["hdfs", "dfs", "-ls", formatted_data_path], stdout=subprocess.PIPE)
summoners_output = summoners_list_cmd.communicate()[0].decode()
summoners = [line.split()[-1] for line in summoners_output.strip().split('\n')[1:] if len(line.split()) > 0]

for summoner_path in summoners:
    summoner_name = os.path.basename(summoner_path)
    copy_files_to_local(summoner_name) """

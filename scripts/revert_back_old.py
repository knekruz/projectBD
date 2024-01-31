import subprocess
import os

# Define the paths
raw_data_path = "/user/hadoop/lol/raw/match_histories"

# Function to move files from 'old' to summoner folder
def restore_files(summoner_name):
    print(f"Restoring files for summoner: {summoner_name}")
    summoner_raw_path = f"{raw_data_path}/{summoner_name}"
    summoner_old_path = f"{summoner_raw_path}/old"

    # Check if the 'old' directory exists
    check_old_dir_cmd = subprocess.Popen(["hdfs", "dfs", "-test", "-d", summoner_old_path], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    check_old_dir_cmd.communicate()

    if check_old_dir_cmd.returncode != 0:
        print(f"No 'old' directory found for summoner: {summoner_name}")
        return

    # List files in the old directory
    old_files_list_cmd = subprocess.Popen(["hdfs", "dfs", "-ls", summoner_old_path], stdout=subprocess.PIPE)
    old_files_output = old_files_list_cmd.communicate()[0].decode()
    old_files = [line.split()[-1] for line in old_files_output.strip().split('\n')[1:] if len(line.split()) > 0]

    for file_path in old_files:
        print(f"Moving file: {file_path}")
        subprocess.run(["hdfs", "dfs", "-mv", file_path, summoner_raw_path])

# Process each summoner
summoners_list_cmd = subprocess.Popen(["hdfs", "dfs", "-ls", raw_data_path], stdout=subprocess.PIPE)
summoners_output = summoners_list_cmd.communicate()[0].decode()
summoners = [line.split()[-1] for line in summoners_output.strip().split('\n')[1:] if len(line.split()) > 0]

for summoner_path in summoners:
    summoner_name = os.path.basename(summoner_path)
    restore_files(summoner_name)

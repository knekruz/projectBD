import json
import subprocess
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col
import os

# Define the paths with your Hadoop NameNode hostname and port
hdfs_base_path = "hdfs://localhost:9000"  # HDFS base path
raw_data_path = "/user/hadoop/lol/raw/match_histories"
formatted_data_path = "/user/hadoop/lol/formatted"
summoner_details_path = "/user/hadoop/lol/raw/summoner_details.json"  # Update with actual path
files_to_process_per_summoner = 2  # Limit the number of files processed per summoner

# Load summoner details
def load_summoner_details():
    summoner_details_path = "/user/hadoop/lol/raw/summoner_details.json"  # HDFS path
    result = subprocess.run(["hdfs", "dfs", "-cat", summoner_details_path], capture_output=True, text=True)
    if result.returncode != 0:
        raise Exception(f"Error reading summoner details: {result.stderr}")
    return json.loads(result.stdout)

# Create a Spark session
spark = SparkSession.builder.appName("LoL Match Formatting").getOrCreate()
summoner_details = load_summoner_details()

# Function to format and save data for a given summoner
def format_and_save_summoner_data(summoner_name, limit):
    print(f"Processing summoner: {summoner_name}")
    summoner_puuid = summoner_details.get(summoner_name)
    if not summoner_puuid:
        print(f"PUUID not found for summoner: {summoner_name}")
        return

    summoner_raw_path = f"{raw_data_path}/{summoner_name}"
    summoner_formatted_path = f"{formatted_data_path}/{summoner_name}"
    summoner_archive_path = f"{summoner_raw_path}/old"

    # Ensure archive directory exists
    subprocess.run(["hdfs", "dfs", "-mkdir", "-p", summoner_archive_path])

    # List files for the summoner
    raw_file_list_cmd = subprocess.Popen(["hdfs", "dfs", "-ls", f"{hdfs_base_path}{summoner_raw_path}"], stdout=subprocess.PIPE)
    raw_files_output = raw_file_list_cmd.communicate()[0].decode()
    match_files = [line.split()[-1] for line in raw_files_output.strip().split('\n')[1:] if len(line.split()) > 0 and "old" not in line]

    if not match_files:
        print(f"No match files found for summoner: {summoner_name}")
        return

    processed_count = 0
    for file_path in match_files:
        if processed_count >= limit:
            break

        print(f"Processing file: {file_path}")
        match_df = spark.read.option("multiLine", "true").json(file_path)

        # Check if game mode is CLASSIC
        game_mode = match_df.select("info.gameMode").first()["gameMode"]
        if game_mode != "CLASSIC":
            subprocess.run(["hdfs", "dfs", "-rm", file_path])  # Delete non-CLASSIC game history
            continue

        participants_df = match_df.select(explode("info.participants").alias("participant"), "metadata", "info")
        summoner_df = participants_df.filter(col("participant.puuid") == summoner_puuid)

        # Select and format data
        formatted_data = summoner_df.select(
            "metadata.matchId",
            "info.gameDuration",
            "info.gameMode",
            "participant.assists",
            "participant.championName",
            "participant.damageDealtToObjectives",
            "participant.summonerName",
            "participant.deaths",
            "participant.win"
        )

        # Save the formatted data to HDFS without overwriting
        formatted_data.write.mode("append").json(f"{hdfs_base_path}{summoner_formatted_path}")

        # Move the raw file to the archive
        subprocess.run(["hdfs", "dfs", "-mv", file_path, f"{hdfs_base_path}{summoner_archive_path}"])

        processed_count += 1

# Process each summoner
summoners_raw_list_cmd = subprocess.Popen(["hdfs", "dfs", "-ls", f"{hdfs_base_path}{raw_data_path}"], stdout=subprocess.PIPE)
summoners_raw_output = summoners_raw_list_cmd.communicate()[0].decode()
summoners = [line.split()[-1] for line in summoners_raw_output.strip().split('\n')[1:] if len(line.split()) > 0 and "old" not in line]

for summoner_path in summoners:
    summoner_name = os.path.basename(summoner_path)
    if summoner_name:
        format_and_save_summoner_data(summoner_name, files_to_process_per_summoner)

spark.stop()

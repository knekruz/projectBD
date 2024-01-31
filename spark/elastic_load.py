import json
import os
import subprocess
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, input_file_name

# Define the paths
hdfs_base_path = "hdfs://localhost:9000"
formatted_data_path = "/user/hadoop/lol/formatted"
es_hadoop_jar_path = "/usr/local/hadoop/lib/elasticsearch-spark-30_2.12-8.12.0.jar"

# Create a Spark session
spark = SparkSession.builder \
    .appName("Elasticsearch Load") \
    .config("spark.jars", es_hadoop_jar_path) \
    .getOrCreate()

# Function to load and write data for a given summoner to Elasticsearch
def load_and_write_summoner_data(summoner_name):
    print(f"Loading data for summoner: {summoner_name}")

    summoner_formatted_path = f"{formatted_data_path}/{summoner_name}"
    summoner_processed_path = f"{formatted_data_path}/{summoner_name}/processed"

    # Ensure processed directory exists
    subprocess.run(["hdfs", "dfs", "-mkdir", "-p", summoner_processed_path])

    # List files in the formatted directory
    formatted_files_list_cmd = subprocess.Popen(["hdfs", "dfs", "-ls", f"{hdfs_base_path}{summoner_formatted_path}"], stdout=subprocess.PIPE)
    formatted_files_output = formatted_files_list_cmd.communicate()[0].decode()
    formatted_files = [line.split()[-1] for line in formatted_files_output.strip().split('\n')[1:] if len(line.split()) > 0 and not line.endswith('_SUCCESS')]

    for file_path in formatted_files:
        print(f"Processing file: {file_path}")
        df = spark.read.option("multiLine", "true").json(file_path)

        # Add a column with the summoner's alias
        df_with_alias = df.withColumn("summonerAlias", lit(summoner_name))

        # Write data to Elasticsearch
        df_with_alias.write.format("org.elasticsearch.spark.sql") \
        .option("es.resource", "summoner_game_histories") \
        .option("es.mapping.id", "matchId") \
        .mode("append") \
        .save()


        print(f"Data from {file_path} loaded to Elasticsearch.")
        processed_file_path = f"{summoner_processed_path}/{os.path.basename(file_path)}"
        subprocess.run(["hdfs", "dfs", "-mv", file_path, f"{hdfs_base_path}{processed_file_path}"])

        print(f"Moved {file_path} to {processed_file_path}")

# Process each summoner
summoners_list_cmd = subprocess.Popen(["hdfs", "dfs", "-ls", f"{hdfs_base_path}{formatted_data_path}"], stdout=subprocess.PIPE)
summoners_output = summoners_list_cmd.communicate()[0].decode()
summoners = [line.split()[-1] for line in summoners_output.strip().split('\n')[1:] if len(line.split()) > 0 and not line.endswith('_SUCCESS')]

for summoner_path in summoners:
    summoner_name = os.path.basename(summoner_path)
    load_and_write_summoner_data(summoner_name)

# Close the Spark session
spark.stop()

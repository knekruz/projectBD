from pyspark.sql import SparkSession
import sys

# Initialize a Spark session
spark = SparkSession.builder.appName("CreateDataFrameAndWriteFile").getOrCreate()

# Create a variable to control the process
process_option = 1

if process_option == 1:
    # Create a PySpark DataFrame with sample data
    data = [(1, 'foo'), (2, 'bar')]
    df = spark.createDataFrame(data, ['id', 'value'])

    # Write the DataFrame to a JSON file on the desktop
    desktop_path = "file:///home/hadoop/Desktop"
    output_path = f"{desktop_path}/output.json"
    df.write.mode('overwrite').json(output_path)
elif process_option == 2:
    # Exit the script
    sys.exit(1)
else:
    print("Invalid option. Please enter 1 to perform the process or 2 to exit.")

# Stop the Spark session
spark.stop()

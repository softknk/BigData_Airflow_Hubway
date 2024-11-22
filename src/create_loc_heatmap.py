from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.functions import *
from pyspark.sql.functions import sum
import math
import pandas as pd
import argparse
import pyspark
from pyspark.sql.window import Window

import openpyxl

import folium
from folium.plugins import HeatMap



def get_args():
    parser = argparse.ArgumentParser(description='Basic Spark Job for Hubway data')
    parser.add_argument('--year', help='Partion Year To Process, e.g. 2024', required=True, type=str)
    parser.add_argument('--month', help='Partion Month To Process, e.g. 11', required=True, type=str)
    parser.add_argument('--day', help='Day of File To Process, e.g. 23', required=True, type=str)
    parser.add_argument('--hdfs_source_dir', help='HDFS source directory', required=True, type=str)
    return parser.parse_args()


def main():
    
    # Parse command line Args
    args = get_args()
    
    # Initialize Spark session
    sc = pyspark.SparkContext();
    spark = SparkSession(sc);

    # Load hubway data
    df = spark.read.format('csv').options(
            header='true',
            delimiter=',',
            inferschema='true',
            nullValue="\\N"
    ).load(args.hdfs_source_dir + '/hubway_trips/' + args.year + '/' + args.month + '/hubway_' + args.year + '-' + args.month + '-' + args.day + '.csv')

    
    # Make sure to have float values (for distance calculation)
    df = df.withColumn("tripduration", col("tripduration").cast("int"))
    df = df.withColumn("start station latitude", col("start station latitude").cast("float"))
    df = df.withColumn("start station longitude", col("start station longitude").cast("float"))
    df = df.withColumn("end station latitude", col("end station latitude").cast("float"))
    df = df.withColumn("end station longitude", col("end station longitude").cast("float"))

    df = df.sample(fraction=0.05)

    # Create Pandas Data Frame
    data = df.toPandas()

    print("Loaded data into Pandas df.")

    # Check if the latitude and longitude columns exist
    if 'start station latitude' in data.columns and 'start station longitude' in data.columns:
        # Extract the latitude and longitude into a list of tuples
        locations = data[['start station latitude', 'start station longitude']].dropna().values.tolist()
    else:
        print("CSV file must contain 'longitude' and 'latitude' columns.")
        exit()

    # Create a Folium map centered at the average location
    # Calculate the mean latitude and longitude to center the map
    mean_lat = data['start station latitude'].mean()
    mean_lon = data['start station longitude'].mean()

    # Create a Folium map centered on the generated points
    m = folium.Map(location=[mean_lat, mean_lon], zoom_start=12)

    print("Created folium HeatMap.")

    # Prepare heatmap data: a list of [lat, lon] pairs
    heat_data = [[row['start station latitude'], row['start station longitude']] for index, row in data.iterrows()]

    # Add the heatmap to the map with higher intensity for clustered points
    HeatMap(heat_data, radius=40, blur=15, min_opacity=0.3).add_to(m)

    print("Added data to heatmap")

    # Add "start station id" as markers (ensure each location is added only once)
    seen_locations = set()  # To track unique locations

    for index, row in data.iterrows():
        # Create a tuple for the latitude and longitude to identify unique locations
        location = (row['start station latitude'], row['start station longitude'])

        # Only add a marker if the location hasn't been added yet
        if location not in seen_locations:
            folium.Marker(
                location=[row['start station latitude'], row['start station longitude']],
                tooltip=f"Start Station ID: {row['start station id']}"  # Tooltip with the start station ID
            ).add_to(m)

            # Mark this location as seen
            seen_locations.add(location)

    # Save the map to an HTML file
    m.save('/home/airflow/airflow/python/heatmap.html')

    print("Heatmap with unique station IDs created successfully. Open 'heatmap.html' to view it.")
    



if __name__ == "__main__":
    main()


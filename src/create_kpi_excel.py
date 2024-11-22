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

    
    # Make sure to have float valus (for distance calculation)
    df = df.withColumn("tripduration", col("tripduration").cast("int"))
    df = df.withColumn("start station latitude", col("start station latitude").cast("float"))
    df = df.withColumn("start station longitude", col("start station longitude").cast("float"))
    df = df.withColumn("end station latitude", col("end station latitude").cast("float"))
    df = df.withColumn("end station longitude", col("end station longitude").cast("float"))


    
    # 1. Calculate Average Trip Duration
    df = df.withColumn("starttime", col("starttime").cast('timestamp'))

    df = df.withColumn('year', year(col('starttime')))
    df = df.withColumn('month', month(col('starttime')))

    avg_trip_duration_per_month = df.groupBy('year', 'month').agg(avg('tripduration').alias("avg_tripduration"))
    avg_trip_duration_sorted = avg_trip_duration_per_month.orderBy('year', 'month')
    avg_trip_duration_sorted = avg_trip_duration_sorted.withColumn("avg_tripduration", round(avg_trip_duration_sorted["avg_tripduration"], 2))
    avg_trip_duration_sorted.show()
    del avg_trip_duration_per_month
  

    # 2. Compute Average Trip Distance (using Euclidean distance approximation on the Earth's surface instead of Haversine formula)
    df_with_dist = df.withColumn("distance_km", sqrt((col("end station latitude") - col("start station latitude")) ** 2 + ((col("end station longitude") - col("start station longitude")) * cos(radians(col("start station latitude")))) ** 2) * 111) # Scale to kilometers
    df_avg_dist_per_month = df_with_dist.groupBy("month").agg(avg("distance_km").alias("avg_trip_distance_km"))
    df_avg_dist_per_month = df_avg_dist_per_month.orderBy("month")
    df_avg_dist_per_month = df_avg_dist_per_month.withColumn("avg_trip_distance_km", round(df_avg_dist_per_month["avg_trip_distance_km"], 2))
    df_avg_dist_per_month.show()
    del df_with_dist


    # 3. Usage share by gender (in percent)
    df = df.withColumn("gender", col("gender").cast("int"))
    gender_month_counts = df.filter(df["gender"] != 0) # 0 = undefined gender
    gender_month_counts = gender_month_counts.groupBy("month", "gender").agg(count("*").alias("gender_month_count"))
    monthly_totals = gender_month_counts.groupBy("month").agg(sum("gender_month_count").alias("total_month_count"))
    usage_share_gender = gender_month_counts.join(monthly_totals, on="month").withColumn("usage_share", round((col("gender_month_count") / col("total_month_count")) * 100, 2)).select("month", "gender", "usage_share")
    usage_share_gender = usage_share_gender.withColumn("gender", when(col("gender") == 1, "male").otherwise("female"))
    usage_share_gender = usage_share_gender.orderBy(col("month"), col("gender"))
    usage_share_gender.show()
    del gender_month_counts
    del monthly_totals


    # 4. Usage share by age (in percent)
    df_cleaned = df.filter(df["birth year"].isNotNull())
    start_year = year(col("starttime"))
    df_with_age = df_cleaned.withColumn("age", start_year - col("birth year"))
    age_month_counts = df_with_age.groupBy("month", "age").agg(count("*").alias("age_month_count"))
    monthly_totals_age = age_month_counts.groupBy("month").agg(sum("age_month_count").alias("total_month_count"))
    usage_share_age = age_month_counts.join(monthly_totals_age, on="month").withColumn("usage_share", round((col("age_month_count") / col("total_month_count")) * 100, 2)).select("month", "age", "usage_share")
    usage_share_age = usage_share_age.orderBy(col("month"), col("usage_share").desc())
    usage_share_age.show()
    del df_with_age
    del monthly_totals_age


    # 5. Top 10 most used bikes
    df_bikes_count = df.groupBy("month", "bikeid").count()
    window_spec = Window.partitionBy("month").orderBy(col("count").desc())
    top_bikes_per_month = df_bikes_count.withColumn("rank", row_number().over(window_spec)).filter(col("rank") <= 10).drop("rank")
    top_bikes_per_month = top_bikes_per_month.orderBy(col("month"))
    top_bikes_per_month.show()
    del df_bikes_count


    # 6. Top 10 most used start stations
    df_start_stations_count = df.groupBy("month", "start station id").count()
    window_spec2 = Window.partitionBy("month").orderBy(col("count").desc())
    top_start_stations_per_month = df_start_stations_count.withColumn("rank", row_number().over(window_spec2)).filter(col("rank") <= 10).drop("rank")
    station_name_df = df.select("start station id", "start station name")
    top_start_stations_per_month = top_start_stations_per_month.orderBy(col("month"), col("count").desc())
    #top_start_stations_per_month = top_start_stations_per_month.join(station_name_df, on="start station id", how="left")
    top_start_stations_per_month.show()
    del df_start_stations_count
    

    # 7. Top 10 most used end stations
    df_end_stations_count = df.groupBy("month", "end station id").count()
    window_spec3 = Window.partitionBy("month").orderBy(col("count").desc())
    top_end_stations_per_month = df_end_stations_count.withColumn("rank", row_number().over(window_spec3)).filter(col("rank") <= 10).drop("rank")
    end_station_name_df = df.select("end station id", "end station name")
    #top_end_stations_per_month = top_end_stations_per_month.join(end_station_name_df, on="end station id", how="left")
    top_end_stations_per_month = top_end_stations_per_month.orderBy(col("month"), col("count").desc())
    top_end_stations_per_month.show()
    del df_end_stations_count
    

    # 8. Usage share per timeslot (in percent)
    df = df.withColumn("time_slot", when(hour("starttime").between(0, 5), "00:00-06:00").when(hour("starttime").between(6, 11), "06:00-12:00").when(hour("starttime").between(12, 17), "12:00-18:00").otherwise("18:00-24:00"))
    
    time_slot_counts = df.groupBy("month", "time_slot").agg(count("*").alias("time_slot_count"))
    monthly_totals_slot = time_slot_counts.groupBy("month").agg(sum("time_slot_count").alias("total_month_count"))
    usage_share_per_slot = time_slot_counts.join(monthly_totals_slot, on="month").withColumn("usage_share", round((col("time_slot_count") / col("total_month_count")) * 100, 2)).select("month", "time_slot", "usage_share")

    usage_share_per_slot = usage_share_per_slot.orderBy("month", "time_slot")
    usage_share_per_slot.show()

    create_kpi_excel_file([avg_trip_duration_sorted, df_avg_dist_per_month, usage_share_gender, usage_share_age, top_bikes_per_month, top_start_stations_per_month, top_end_stations_per_month, usage_share_per_slot]) 



# CREATE KPI EXCEL OUT OF COMPUTED DATA FRAMES

def create_kpi_excel_file(pyspark_df_list):
    print("Creating Excel file ...")

    pandas_df_list = []
    for pyspark_df in pyspark_df_list:
        pandas_df_list.append(pyspark_df.toPandas())

    with pd.ExcelWriter("/home/airflow/airflow/python/hubway_kpi_result.xlsx", engine="openpyxl") as writer:
        pandas_df_list[0].to_excel(writer, sheet_name="Avg Trip Duration", index=False)
        pandas_df_list[1].to_excel(writer, sheet_name="Avg Trip Distance", index=False) 
        pandas_df_list[2].to_excel(writer, sheet_name="Usage Share Gender", index=False)
        pandas_df_list[3].to_excel(writer, sheet_name="Usage Share Ag", index=False)
        pandas_df_list[4].to_excel(writer, sheet_name="Top Bikes", index=False)
        pandas_df_list[5].to_excel(writer, sheet_name="Top Start Stations", index=False)
        pandas_df_list[6].to_excel(writer, sheet_name="Top End Stations", index=False)
        pandas_df_list[7].to_excel(writer, sheet_name="Usage Share Per Slot", index=False)


    print("Excel file 'hubway_kpi_result.xlsx' has been created successfully.")


if __name__ == "__main__":
    main()


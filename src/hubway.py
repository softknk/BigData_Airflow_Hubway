# -*- coding: utf-8 -*-

"""
Title: Hubway DAG 
Author: Daniel Kuenkel
Description: 
Just for educational purposes, not to be used in any productive mannor.
Downloads IMDb data, puts them into HDFS and creates HiveTable.
See Lecture Material: https://github.com/marcelmittelstaedt/BigData
"""

from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.http_download_operations import HttpDownloadOperator
from airflow.operators.zip_file_operations import UnzipFileOperator
from airflow.operators.hdfs_operations import HdfsPutFileOperator, HdfsGetFileOperator, HdfsMkdirFileOperator
from airflow.operators.filesystem_operations import CreateDirectoryOperator
from airflow.operators.filesystem_operations import ClearDirectoryOperator
from airflow.operators.hive_operator import HiveOperator

args = {
    'owner': 'airflow'
}


#hiveSQL_create_hubway_trips_table = '''
#CREATE EXTERNAL TABLE IF NOT EXISTS hubway_trips (
#    tripduration INT,
#    starttime TIMESTAMP,
#    stoptime TIMESTAMP,
#    `start station id` INT,
#    `start station name` STRING,
#    `start station latitude` DOUBLE,
#    `start station longitude` DOUBLE,
#    `end station id` INT,
#    `end station name` STRING,
#    `end station latitude` DOUBLE,
#    `end station longitude` DOUBLE,
#    bikeid INT,
#    usertype STRING,
#    `birth year` INT,
#    gender INT
#)
#COMMENT 'Hubway Bike Trip Data' 
#PARTITIONED BY (partition_year INT, partition_month INT) 
#ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' -- Assuming CSV file
#STORED AS TEXTFILE LOCATION '/user/hadoop/hubway/hubway_trips'
#TBLPROPERTIES ('skip.header.line.count'='1');
#'''
#
#hiveSQL_add_partition_hubway_trips = '''
#ALTER TABLE hubway_trips ADD IF NOT EXISTS partition(partition_year={{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}}, partition_month={{ macros.ds_format(ds, "%Y-%m-%d", "%m")}})
#LOCATION '/user/hadoop/hubway/hubway_trips/{{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%m")}}/'; 
#
#LOAD DATA INPATH '/user/hadoop/hubway/hubway_trips/2024/11/hubway_2024-11-13.csv' INTO TABLE hubway_trips;
#'''


dag = DAG('Hubway', default_args=args, description='Hubway KPI',
          schedule_interval='56 18 * * *',
          start_date=datetime(2019, 10, 16), catchup=False, max_active_runs=1)

create_local_import_dir = CreateDirectoryOperator(
    task_id='create_import_dir',
    path='/home/airflow',
    directory='hubway',
    dag=dag,
)

clear_old_csv_folder = BashOperator(
    task_id='clear_old_csv',
    bash_command='rm -r -f /home/airflow/hubway/csv',
    dag=dag,
)

clear_local_import_dir = ClearDirectoryOperator(
    task_id='clear_import_dir',
    directory='/home/airflow/hubway',
    pattern='*',
    dag=dag,
)

download_hubway_kaggle_data = HttpDownloadOperator(
    task_id='download_hubway_data',
    download_uri='https://www.kaggle.com/api/v1/datasets/download/acmeyer/hubway-data',
    save_to='/home/airflow/hubway/hubway_{{ ds }}.zip',
    dag=dag,
)

unzip_hubway_data = BashOperator(
    task_id='unzip_hubway_data',
    bash_command='unzip /home/airflow/hubway/hubway_{{ ds }}.zip -d /home/airflow/hubway/',
    dag=dag,
)

# There are files in the downloaded directory that are not needed, so we move our needed files to csv dir
create_hubway_csv_dir = CreateDirectoryOperator(
    task_id='create_hubway_csv_dir',
    path='/home/airflow/hubway',
    directory='csv',
    dag=dag,
)

filter_and_copy_csv = BashOperator(
    task_id='filter_and_copy_csv',
    bash_command='find /home/airflow/hubway/ -type f -name \"*hubway-tripdata.csv\" -exec cp {} /home/airflow/hubway/csv/ \;',
    dag=dag,
)

merge_csv_files = BashOperator(
    task_id='merge_hubway_csv_files',
    bash_command='cd /home/airflow/hubway/csv && csvstack *.csv > hubway_{{ ds }}.csv',
    dag=dag,
)

# HDFS
create_hdfs_hubway_partition_dir = HdfsMkdirFileOperator(
    task_id='mkdir_hdfs_hubway_dir',
    directory='/user/hadoop/hubway/hubway_trips/{{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%m")}}/',
    hdfs_conn_id='hdfs',
    dag=dag,
)


hdfs_put_hubway_data = HdfsPutFileOperator(
    task_id='upload_hubway_data_to_hdfs',
    local_file='/home/airflow/hubway/csv/hubway_{{ ds }}.csv',
    remote_file='/user/hadoop/hubway/hubway_trips/{{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%m")}}/hubway_{{ ds }}.csv',
    hdfs_conn_id='hdfs',
    dag=dag,
)

#create_HiveTable_hubway_trips = HiveOperator(
#    task_id='create_hubway_trips_table',
#    hql=hiveSQL_create_hubway_trips_table,
#    hive_cli_conn_id='beeline',
#    dag=dag
#)

#addPartition_HiveTable_hubway_trips = HiveOperator(
#    task_id='add_partition_hubway_trips_table',
#    hql=hiveSQL_add_partition_hubway_trips,
#    hive_cli_conn_id='beeline',
#    dag=dag
#)

dummy_op = DummyOperator(
    task_id='dummy',
    dag=dag
 )


pyspark_create_kpi = SparkSubmitOperator(
    task_id='pyspark_create_kpi',
    conn_id='spark',
    application='/home/airflow/airflow/python/create_kpi_excel.py',
    total_executor_cores='2',
    executor_cores='2',
    executor_memory='3g',
    num_executors='2',
    name='spark_create_kpi',
    verbose=True,   
    application_args=['--year', '{{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}}', '--month', '{{ macros.ds_format(ds, "%Y-%m-%d", "%m")}}', '--day',  '{{ macros.ds_format(ds, "%Y-%m-%d", "%d")}}', '--hdfs_source_dir', '/user/hadoop/hubway'],
    dag=dag
)


pyspark_create_loc_heatmap = SparkSubmitOperator(
    task_id='pyspark_create_loc_heatmap',
    conn_id='spark',
    application='/home/airflow/airflow/python/create_loc_heatmap.py',
    total_executor_cores='2',
    executor_cores='2',
    executor_memory='3g',
    num_executors='2',
    name='spark_create_loc_heatmap',
    verbose=True,
    application_args=['--year', '{{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}}', '--month', '{{ macros.ds_format(ds, "%Y-%m-%d", "%m")}}', '--day',  '{{ macros.ds_format(ds, "%Y-%m-%d", "%d")}}', '--hdfs_source_dir', '/user/hadoop/hubway'],
    dag=dag        
)


create_local_import_dir >> clear_old_csv_folder >> clear_local_import_dir >> download_hubway_kaggle_data >> unzip_hubway_data >> create_hubway_csv_dir >> filter_and_copy_csv >> merge_csv_files >> create_hdfs_hubway_partition_dir >> hdfs_put_hubway_data >> dummy_op
dummy_op >> pyspark_create_kpi
dummy_op >> pyspark_create_loc_heatmap



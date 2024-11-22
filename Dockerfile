# Use the base image
FROM marcelmittelstaedt/airflow:latest

# Set the working directory (optional but recommended for clarity)
WORKDIR /home/airflow

# Change permissions of the /home/airflow/airflow/python directory to 666 (read-write-only for everyone)
# This is the directory where the produced heatmap and kpi files will be saved
RUN chmod 666 /home/airflow/airflow/python

# Copy the required files into the specified directories
COPY src/hubway.py /home/airflow/airflow/dags/hubway.py
COPY src/create_kpi_excel.py /home/airflow/airflow/python/create_kpi_excel.py
COPY src/create_loc_heatmap.py /home/airflow/airflow/python/create_loc_heatmap.py

# Install unzip and csvkit as root user
USER root
RUN apt-get update && \
    apt-get install -y unzip csvkit

# Switch back to the airflow user
USER airflow

# Install Python dependencies as the airflow user
RUN pip3 install pandas openpyxl folium==0.12.0
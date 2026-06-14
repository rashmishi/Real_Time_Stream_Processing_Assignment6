# IoMT Health Monitoring using Spark Structured Streaming
## Project Overview

This project implements a Spark Structured Streaming pipeline using the IoMT Health Monitoring dataset.

The pipeline:

- Simulates streaming from a CSV dataset
- Uses a 2-minute tumbling window
- Computes average heart rate per patient
- Detects elevated heart rate events
- Identifies patients with sustained elevated heart rate (>100 bpm) across consecutive windows
- Generates clinical alerts

---

## Dataset
Dataset used:
patients_data_with_alerts.csv


Important columns:

- timestamp
- Patient Number
- Heart Rate (bpm)

---

## Environment

Project developed and executed using:

- Google Colab
- Python 3
- Apache Spark (PySpark)
- Spark Structured Streaming

---

## Steps to Run the Project

### Step 1: Upload Dataset

Upload the dataset:
patients_data_with_alerts.csv


to Google Colab.

---

### Step 2: Install and Configure Spark

Run the Spark setup cells in the notebook.


```python
!pip install pyspark

Create Spark session:
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("IoMT Streaming") \
    .getOrCreate()

---

### Step 3: Create Streaming Directory

Create a folder used for streaming simulation:
stream_dir = "/content/stream_data"
Copy the dataset into the streaming folder.

---

### Step 4: Define Schema

Define the schema for:
timestamp
Patient Number
Heart Rate (bpm)
and other required attributes.

---

### Step 5: Read Streaming Data

streaming_df = spark.readStream \
    .format("csv") \
    .option("header", "true") \
    .schema(schema) \
    .load(stream_dir)

---

### Step 6: Create 2-Minute Tumbling Window

windowed_df = streaming_df \
    .withWatermark("timestamp", "10 minutes") \
    .groupBy(
        window(col("timestamp"), "2 minutes"),
        col("Patient Number")
    ) \
    .agg(
        avg("Heart Rate (bpm)").alias("avg_heart_rate")
    )

---

### Step 7: Generate Alerts

Patients are flagged when:
Average Heart Rate > 100 bpm

Clinical alerts are generated when:
Average Heart Rate > 100 bpm
in two consecutive windows

---

### Step 8: Display Results

Display aggregated windows:
windowed_batch.orderBy(
    desc("avg_heart_rate")
).show(20, truncate=False)

---

====================================
CLINICAL ALERTS
====================================

Total Clinical Alerts Generated: 1

+--------------+-------------------+-------------------+--------------+
|Patient Number|window_start       |window_end         |avg_heart_rate|
+--------------+-------------------+-------------------+--------------+
|48599         |2026-05-20 22:24:00|2026-05-20 22:26:00|149.0         |
+--------------+-------------------+-------------------+--------------+

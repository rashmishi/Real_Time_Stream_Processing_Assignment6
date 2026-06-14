# Real_Time_Stream_Processing_Assignment6
# IoMT Health Monitoring – Spark Structured Streaming Pipeline

## Overview

This project implements a Spark Structured Streaming pipeline using the IoMT Health Monitoring dataset.

The pipeline performs the following tasks:

1. Reads patient health data from a CSV file.
2. Simulates streaming ingestion.
3. Creates 2-minute tumbling windows.
4. Computes average heart rate per patient for each window.
5. Detects sustained elevated heart rate (>100 bpm) across two consecutive windows.
6. Generates clinical alerts with patient numbers.
7. Saves alert results for further analysis.

---

## Project Structure
project/
│
├── patients_data_with_alerts.csv
├── iomt_alert_pipeline.py
├── README.md
└── clinical_alerts_output/


---

## Requirements

### Python
Python 3.9+

### Apache Spark
Spark 3.x

### PySpark
Install PySpark:
```bash
pip install pyspark


****Dataset****
Place the dataset file in the project directory:
patients_data_with_alerts.csv

**Required columns:**
timestamp
Patient Number
Heart Rate (bpm)

##############################
Step 1: Install Spark
!apt-get install openjdk-11-jdk-headless -qq > /dev/null
!wget -q https://archive.apache.org/dist/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz
!tar xf spark-3.5.0-bin-hadoop3.tgz

!pip install pyspark
Step 2: Upload Dataset

Upload:

patients_data_with_alerts.csv

to Colab.

Step 3: Run Notebook Cells

Execute all notebook cells sequentially.

Pipeline Workflow
Step 1: Load Dataset
df = spark.read.csv(
    "patients_data_with_alerts.csv",
    header=True,
    inferSchema=True
)
Step 2: Convert Timestamp
df = df.withColumn(
    "timestamp",
    to_timestamp(col("timestamp"))
)
Step 3: Create 2-Minute Tumbling Windows
windowed_batch = (
    df.groupBy(
        window("timestamp", "2 minutes"),
        col("Patient Number")
    )
    .agg(avg("Heart Rate (bpm)").alias("avg_heart_rate"))
)
Step 4: Detect Sustained Elevated Heart Rate

A clinical alert is generated when:

Average Heart Rate > 100 bpm
for two consecutive windows

Logic:

lag(avg_heart_rate)

is used to compare the current window with the previous window for the same patient.
#################################
====================================
CLINICAL ALERTS
====================================

Total Clinical Alerts Generated: 1

+--------------+-------------------+-------------------+--------------+
|Patient Number|window_start       |window_end         |avg_heart_rate|
+--------------+-------------------+-------------------+--------------+
|48599         |2026-05-20 22:24:00|2026-05-20 22:26:00|149.0         |
+--------------+-------------------+-------------------+--------------+

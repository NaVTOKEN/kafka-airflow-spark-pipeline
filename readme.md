🚀 Uber Real-Time Data Pipeline (Kafka + PySpark + Airflow)

📌 Project Overview

This project simulates a real-time ride-hailing data pipeline similar to Uber. It ingests streaming trip data using Apache Kafka, processes it using PySpark Structured Streaming, and orchestrates workflows using Apache Airflow. The pipeline generates business-critical insights such as cancellation rates, demand patterns, user activity, and anomaly detection.

🎯 Business Problem

A ride-hailing platform needs to:
Track daily cancellation rate
Monitor trip demand in real-time
Identify active vs inactive users
Detect fraud or abnormal trip behavior

🏗 Architecture
Kafka Producer → Kafka Topic → PySpark Streaming → Parquet Storage → Airflow Scheduling

⚙️ Tech Stack
Python
Apache Kafka
PySpark Structured Streaming
Apache Airflow
Parquet (Data Lake)
SQL concepts (Window functions, Aggregations)

🔥 Business Solutions (CORE PART)

1️⃣ Track Daily Cancellation Rate

Cancellation Rate = Cancelled Trips / Total Trips
✅ PySpark Code
Python
from pyspark.sql.functions import col, count, when

cancel_rate = json_df.groupBy("city") \
    .agg(
        (count(when(col("status") == "cancelled", True)) / count("*")).alias("cancel_rate")
    )

👉 Output

City|Cancel Rate
Delhi|0.25
Mumbai|0.18

2️⃣ Monitor Trip Demand in Real-Time

Trips per minute / city
✅ PySpark Code
Python
from pyspark.sql.functions import window

demand = json_df.groupBy(
    window(col("timestamp"), "1 minute"),
    col("city")
).count()
👉 Shows:
Peak hours
High-demand cities

3️⃣ Identify Active vs Inactive Users
💡 Logic
Active = Users with trips
Inactive = No trips
✅ Example
Python
active_users = json_df.select("user_id").distinct()


inactive_users = users_df.join(active_users, "user_id", "left_anti")
👉 Used for:
Marketing campaigns
User retention

4️⃣ Detect Fraud / Abnormal Behavior

🚨 High Fare Detection
Python
fraud = json_df.filter(col("fare") > 2000)
🚨 Too Many Trips (bot behavior)
Python
from pyspark.sql.functions import count

suspicious_users = json_df.groupBy("user_id") \
    .agg(count("*").alias("trip_count")) \
    .filter(col("trip_count") > 50)
👉 Used in:
Fraud detection systems
Security monitoring

▶️ How to Run

1. Start Kafka

zookeeper-server-start.sh config/zookeeper.properties  
kafka-server-start.sh config/server.properties
2. Run Producer

python kafka/producer.py
3. Run Spark Streaming

python spark/streaming_job.py
4. Start Airflow

airflow standalone

📊 Output
Stored in:

output/uber_stream/

🧠 Key Learnings
Real-time data processing with Kafka
Streaming analytics using PySpark
ETL orchestration using Airflow
Business-driven data engineering

As I have made it flexible to be used with real time data sets and sample trips data both ; as the sample dataset has:
JSON
"user_id"
"timestamp"


You might have to update Spark code to use timestamp accordingly which I have left it to the user,with mandatory changes as :
 1.Schema Update (ADD new fields)
 2. Add Timestamp Conversion             (important) 
 3. Add Real-Time Demand Logic
 4. Add Cancellation Rate Loging\
 5. Add Fraud Detection
 6. Replace Write Section (IMPORTANT)
 7. Replace Termination

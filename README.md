
**The Smart Watch Data Pipeline project is designed to collect, process, and analyze data from smart watches in a scalable and modular architecture. The system leverages containerized services, event-driven processing, and an orchestration framework to manage tasks. Below is an overview of the system components and how they interact with each other.**

# System Architecture
![streaming_pipeline](https://github.com/user-attachments/assets/c638b5b7-39c9-4226-92ff-c24ade842956)

## Key Components

### Client (client/)
The client module is responsible for streaming events to a server in real time.

### Server (server/)
The server-side module handles requests from clients, process incoming events in real time, interacts with the kafka messasging system and stores these events in a kafka topic.

### Processing (code/)
- landing_spark_job.py: Reads data from kafka topic, Denormalize the data to handle inconsistencies, and stores the processed raw data in parquet format for reliable storage.
- daily_agg_spark_job.py: Performs transformations to caculate daily aggregates (total_steps per day, avg_heart_rate during sleep) and writes the final data to postgres table.


### Data Storage (data/)
The data/ folder is used to store raw, intermediate, and processed data. Depending on the scale of the project, this could include integration with cloud-based storage services like AWS S3 or HDFS file system.

### Configuration (config/, .env)
Configuration files, environment variables, and credentials are stored in the config/ directory and .env file. These settings control the behavior of different services, making the system highly configurable for different deployment environments.

## System Flow
### Data Collection
- Smart watch data is collected by the client module and sent to the server.

### Data Ingestion and Processing
- The server forwards the data to the kafka topic where the data is staged for further processing by spark engine.
- Spark engine reads the events stored in the kafka topic, denormalize it and stores the output as parquet files for reliable storage. (S3/HDFS)

### Data Storage and Analysis
- Processed data is stored in parquet format for storage solution.
- The raw data is enriched with derived metrics and stored in postgres table for querying

***Technologies:***

Python, Apache Kafka, Apache Zookeeper, Apache Spark, PostgreSQL, Docker

## Getting Started

### WebUI links

`Kafka UI` : <http://localhost:8085/>

### Clone the repository:

```
git clone https://github.com/entangledbit/smart_watch_data_pipeline.git
```

### Navigate to the project directory:

```
cd smart_watch_data_pipeline
```

### Set execute permissions on the entrypoint script
```
chmod +x server/entrypoint.sh
```

### Build the server image
```
docker build -t streaming-server -f server/Dockerfile.server server/
```

### Build the client image
```
docker build -t streaming-client -f client/Dockerfile.client client/
```

### Start Kafka, Spark, Zookeeper and other necessary services
```
docker compose up -d
```

### Start the streaming server
```
docker run -it --rm --network="host" streaming-server --host localhost --port 62333
```

### Start the streaming client
```
docker run -it --rm --network="host" streaming-client --server_url http://localhost:62333/stream --fast_mode
```

### Submit spark streaming job
```
app_path='/opt/spark-apps/smart_watch_data_pipeline'
lib_path=$app_path/lib/dependencies.zip
landing_job=$app_path/code/landing_spark_job.py

docker exec -it spark-master spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 --py-files $lib_path $landing_job
```    

### Submit spark job to perform daily, weekly & monthly aggregates

```
daily_agg_spark_job=$app_path/code/daily_agg_spark_job.py
weekly_agg_spark_job=$app_path/code/weekly_agg_spark_job.py
monthly_agg_spark_job=$app_path/code/monthly_agg_spark_job.py

docker exec -it spark-master spark-submit --packages org.postgresql:postgresql:42.2.5,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 --py-files $lib_path $daily_agg_spark_job

docker exec -it spark-master spark-submit --packages org.postgresql:postgresql:42.2.5,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 --py-files $lib_path $weekly_agg_spark_job

docker exec -it spark-master spark-submit --packages org.postgresql:postgresql:42.2.5,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 --py-files $lib_path $monthly_agg_spark_job
```

### Querying processed data

Login to postgres docker container using the below commands and use the psql shell for querying tables.

```
docker exec -it postgres bash

psql -h postgres -U airflow -d airflow
```

Provide password as airflow to login to the psql shell.


### [OPTIONAL] Create postgres table to store aggregated data

```
CREATE TABLE daily_aggregates (
    user_id VARCHAR,
    event_date DATE,
    total_steps BIGINT,
    avg_heart_rate DOUBLE PRECISION
) PARTITION BY RANGE (event_date);

-- Create partitions
CREATE TABLE daily_aggregates_2024_08 PARTITION OF daily_aggregates
    FOR VALUES FROM ('2024-08-01') TO ('2024-09-01');
```

# Real-time Data Ingestion and Analytics Pipeline

## Overview
This project implements a complete streaming and batch data pipeline using Apache Airflow, Kafka, and SQLite. The pipeline is designed to handle real-time data ingestion from an API, perform data cleaning, and produce daily analytics summaries. It uses three main Apache Airflow jobs (DAGs) to achieve this:

- **Job 1:** Continuous data ingestion from an API into Kafka.
- **Job 2:** Hourly cleaning of data from Kafka and storage into SQLite.
- **Job 3:** Daily analytics job that reads data from SQLite, computes summaries, and stores them back in the database.

## Project Structure

```
project/
│ README.md
│ requirements.txt
├─ src/
│ ├─ job1_producer.py
│ ├─ job2_cleaner.py
│ ├─ job3_analytics.py
│ └─ db_utils.py
├─ airflow/
│ └─ dags/
│ ├─ job1_ingestion_dag.py
│ ├─ job2_clean_store_dag.py
│ └─ job3_daily_summary_dag.py
├─ data/
│ └─ app.db
└─ report/
└─ report.pdf
```


## Requirements
1. **Python 3.7+**
2. **Apache Airflow 2.x**
3. **Pandas** for data cleaning and analytics
4. **SQLite** for storing cleaned data and analytics
5. **Kafka** for handling real-time data streams
6. **Requests** for API integration
7. **Kafka-Python** for Kafka interaction

To install all dependencies, run the following:

```bash
pip install -r requirements.txt
```

## Setup

### 1. Kafka Setup
Ensure Kafka is running locally or configure it to connect to a remote Kafka broker. The project assumes Kafka is running at `localhost:29092`, but this can be adjusted in the environment variables.

### 2. SQLite Setup
SQLite is used as the storage layer. The database file (`app.db`) will be created in the `data/` directory automatically when the system runs.

### 3. API Configuration
The project fetches real-time data from an API. You can configure the API URL in the environment variables. The default API is Binance for cryptocurrency prices.

```bash
export API_URL="https://api.binance.com/api/v3/ticker/24hr?symbol=BTCUSDT"
```


## DAGs (Airflow Jobs)

### DAG 1 – Continuous Data Ingestion

This DAG continuously fetches data from the API every 30 seconds to a few minutes and sends the data to a Kafka topic.

* **Flow:** API → Job 1 → Kafka (raw topic)

The producer (`job1_producer.py`) runs in a loop, polling the API and sending messages to Kafka.

### DAG 2 – Hourly Data Cleaning and Storage

This DAG runs hourly and reads the raw data from the Kafka topic. It cleans the data and stores it into the SQLite `events` table.

* **Flow:** Kafka → Job 2 → Cleaning → SQLite (events)

The cleaning task (`job2_cleaner.py`) handles data transformations like type conversion and filtering.

### DAG 3 – Daily Analytics

This DAG runs daily and computes analytical summaries from the cleaned data stored in SQLite. The results are written to the `daily_summary` table.

* **Flow:** SQLite (events) → Job 3 → Analytics → SQLite (daily_summary)

The analytics task (`job3_analytics.py`) aggregates the data and stores daily summaries.

## How to Run

### 1. Start Apache Airflow:

Ensure Apache Airflow is running. You can follow the official Airflow installation guide: [https://airflow.apache.org/docs/apache-airflow/stable/start/index.html](https://airflow.apache.org/docs/apache-airflow/stable/start/index.html)

Trigger the DAGs from the Airflow UI or via the Airflow API.

### 2. Run the Pipeline:

Trigger the `job1_ingestion_dag.py` manually or let it run continuously.

The other jobs (`job2_clean_store_dag.py` and `job3_daily_summary_dag.py`) will run according to their schedules.

### 3. Monitor Logs:

You can monitor the progress of each DAG in the Airflow UI. Logs for each task will show detailed information about data processing and errors (if any).

## SQLite Schema

### `events` table:

Stores the cleaned data.

| Column Name | Type |
| ----------- | ---- |
| event_id    | TEXT |
| symbol      | TEXT |
| price       | REAL |
| event_time  | TEXT |
| source      | TEXT |
| ingested_at | TEXT |

### `daily_summary` table:

Stores the aggregated analytics data.

| Column Name | Type    |
| ----------- | ------- |
| day         | TEXT    |
| symbol      | TEXT    |
| avg_price   | REAL    |
| min_price   | REAL    |
| max_price   | REAL    |
| n_records   | INTEGER |
| computed_at | TEXT    |

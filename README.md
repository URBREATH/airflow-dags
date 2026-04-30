# airflow-dags

**Provided by:** Engineering Ingegneria Informatica S.p.A. (ENG)

## Description

**airflow-dags** is a collection of [Apache Airflow](https://airflow.apache.org/) DAGs developed for Smart City data pipelines. Each DAG handles a specific data source or integration scenario, covering data ingestion from event streaming platforms and external traffic APIs, with processed output stored on S3-compatible object storage (MinIO).

Key features:

* **Kafka Consumer DAG** — Reads a single message from a Kafka topic (`analytcs_result`), decodes the JSON payload, and logs the result. Designed for one-shot event inspection within an existing Kafka-based pipeline.
* **Telraam Monthly Pipeline DAG** — Full ETL pipeline that queries the [Telraam](https://telraam.net/) Traffic API for multiple road segments, stores raw per-segment CSV files on MinIO, then processes them to produce aggregated statistics and modal-share breakdowns (pedestrian, car, bike, heavy vehicles).
* **Airflow Variables-driven configuration** — Credentials and endpoints are stored as Airflow Variables, keeping secrets out of the DAG code.
* **S3/MinIO integration** — Uses `s3fs` to read and write CSV files directly on MinIO with a structured path convention: `{City}/{Theme}/KPIs/{timestamp}/{raw_data|processed_data}/`.

---

## Architecture

```
[Airflow Scheduler]
       │
       ├─── dag_kafka_reader (schedule: @once)
       │         │
       │         └── consume_kafka ──► KafkaConsumer (topic: analytcs_result)
       │                                    └── parse JSON → log output
       │
       └─── telraam_minio_monthly_pipeline_vars (schedule: 0 5 1 * *)
                 │
                 ├── fetch_data_to_minio
                 │       │
                 │       ├── Telraam API (POST /v1/reports/traffic, per segment ID)
                 │       └── MinIO  →  Leuven/Mobility/KPIs/{ts}/raw_data/{id}.csv
                 │
                 └── process_data_from_minio
                         │
                         ├── MinIO (read raw CSVs via s3fs)
                         └── MinIO (write processed CSVs)
                                 ├── all_data.csv
                                 ├── summed_data.csv
                                 ├── {segment_id}_share.csv  (per segment)
                                 └── total_shares.csv
```

---

## Tech Stack

| Component | Technology |
|---|---|
| Orchestrator | Apache Airflow |
| Language | Python 3 |
| Event Streaming | Apache Kafka |
| Traffic API | Telraam API v1 |
| Object Storage | MinIO (S3-compatible) |
| S3 Client | s3fs |
| Data Processing | pandas |

---

## DAGs

### `dag_kafka_reader`

| Property | Value |
|---|---|
| DAG ID | `dag_kafka_reader` |
| Location | `airflow-kafka-nbs/dag_kafka_reader.py` |
| Schedule | `@once` |
| Kafka Topic | `analytcs_result` |
| Bootstrap Server | `kafka-broker-1:9092` |
| Consumer Group | `airflow-group` |

**What it does:** Connects to the Kafka broker, reads the first available message from the `analytcs_result` topic, decodes it as JSON, and logs the fields `status`, `bucket`, `object_name`, and `original_message`.

---

### `telraam_minio_monthly_pipeline_vars`

| Property | Value |
|---|---|
| DAG ID | `telraam_minio_monthly_pipeline_vars` |
| Location | `airflow-telraam-data/telraam_dag.py` |
| Schedule | `0 5 1 * *` (1st of each month, 05:00) |
| Start Date | 2025-06-01 |
| MinIO Bucket | `urbreath-public-repo` |
| Output Path | `Leuven/Mobility/KPIs/{timestamp}/` |

**Tasks:**

1. **`fetch_data_to_minio`** — Iterates over all configured segment IDs, calls the Telraam API for the DAG's `data_interval_start`→`data_interval_end` window (hourly granularity, Europe/Brussels timezone), and uploads each segment's report as `raw_data/{segment_id}.csv` on MinIO. Pushes the raw data path via XCom.

2. **`process_data_from_minio`** — Reads all raw CSVs from the path received via XCom, concatenates them, and writes four output files to `processed_data/`:
   - `all_data.csv` — all records sorted by date, with a `source_id` column identifying the segment.
   - `summed_data.csv` — numeric columns summed per date across all segments.
   - `{segment_id}_share.csv` — modal share percentage (pedestrian / car / bike / heavy) per segment.
   - `total_shares.csv` — overall modal share across all segments.

**Required Airflow Variables:**

| Variable | Description |
|---|---|
| `minio_endpoint_url` | MinIO endpoint URL (e.g. `http://minio:9000`) |
| `minio_access_key` | MinIO access key |
| `minio_secret_key` | MinIO secret key |
| `telraam_api_url` | Telraam API base URL for traffic reports |
| `telraam_segment_ids` | JSON array of road segment IDs to query (e.g. `[9000001234, 9000005678]`) |

---

## Installation Prerequisites

* [Apache Airflow](https://airflow.apache.org/docs/apache-airflow/stable/installation/index.html) (2.x or later) deployed and running
* Python packages available in the Airflow environment:

```bash
pip install kafka-python pandas requests s3fs
```

* A running **Kafka** broker reachable at `kafka-broker-1:9092` (for the Kafka DAG)
* A running **MinIO** instance (for the Telraam DAG)
* A valid **Telraam API token** (see [Telraam API docs](https://app.swaggerhub.com/apis-docs/telraam/Telraam-API/1.2.0))

---

## Installation Instructions

### 1. Clone the repository

```bash
git clone https://github.com/Gerbinix/airflow-dags.git
cd airflow-dags
```

### 2. Copy DAG files to the Airflow DAGs folder

```bash
cp airflow-kafka-nbs/dag_kafka_reader.py  $AIRFLOW_HOME/dags/
cp airflow-telraam-data/telraam_dag.py    $AIRFLOW_HOME/dags/
```

### 3. Set Airflow Variables (Telraam DAG)

Via the Airflow UI (**Admin → Variables**) or the CLI:

```bash
airflow variables set minio_endpoint_url   "http://minio:9000"
airflow variables set minio_access_key     "<your-access-key>"
airflow variables set minio_secret_key     "<your-secret-key>"
airflow variables set telraam_api_url      "https://telraam-api.net/v1/reports/traffic"
airflow variables set telraam_segment_ids  '[9000001234, 9000005678]'
```

### 4. Trigger the DAGs

Kafka DAG (one-shot):

```bash
airflow dags trigger dag_kafka_reader
```

Telraam DAG (wait for the monthly schedule, or trigger manually):

```bash
airflow dags trigger telraam_minio_monthly_pipeline_vars
```

---

## Project Structure

```
airflow-dags/
├── airflow-kafka-nbs/
│   └── dag_kafka_reader.py          # Kafka consumer DAG (one-shot)
└── airflow-telraam-data/
    └── telraam_dag.py               # Telraam monthly ETL pipeline DAG
```

---

## External Resources

* [Apache Airflow](https://airflow.apache.org/) — Workflow orchestration platform
* [Telraam](https://telraam.net/) — Citizen traffic monitoring sensors
* [Telraam API v1 docs](https://app.swaggerhub.com/apis-docs/telraam/Telraam-API/1.2.0) — REST API reference
* [Apache Kafka](https://kafka.apache.org/) — Distributed event streaming platform
* [MinIO](https://min.io/) — S3-compatible object storage
* [s3fs](https://s3fs.readthedocs.io/) — Python S3 filesystem interface

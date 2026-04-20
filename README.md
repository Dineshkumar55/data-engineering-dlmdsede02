# NYC Taxi Batch Processing Pipeline
**Data Engineering (DLMDSEDE02) — IU International University of Applied Sciences**

A fully containerised batch data processing pipeline for the NYC Yellow Taxi Trip dataset, built with five microservices and deployed locally using Docker Compose as Infrastructure as Code (IaC).

---

## Architecture

```
CSV Dataset → [MS1 Ingestion] → [MS2 Kafka] → [MS3 Spark/PySpark] → [MS4 PostgreSQL] → [MS5 FastAPI]
```

| Microservice | Technology | Role |
|---|---|---|
| MS1 Ingestion | Python / Pandas | Reads CSV dataset, publishes JSON records to Kafka |
| MS2 Message Broker | Apache Kafka + ZooKeeper | Decouples producer from consumer, buffers records |
| MS3 Batch Processor | Apache Spark / PySpark | Cleans data, computes hourly aggregations, writes to DB |
| MS4 Storage | PostgreSQL 15 | Stores clean trips and hourly aggregate tables |
| MS5 API | FastAPI / Uvicorn | REST API exposing processed data and health endpoint |

---

## Dataset

- **Source:** NYC Yellow Taxi Trip Data (March 2016)
- **Volume:** ~130,000 records
- **Format:** CSV (2016 schema: `pickup_longitude/latitude`)
- **Download:** [Kaggle NYC Taxi Dataset](https://www.kaggle.com/datasets/elemento/nyc-yellow-taxi-trip-data)

Place the downloaded file at:
```
data/yellow_tripdata.csv
```

---

## Data Processing

### Quality Filters Applied (PySpark)
- Removes records with null `pickup_datetime` or `fare_amount`
- Removes trips with `fare_amount <= 0`
- Removes trips with `trip_distance <= 0` or `trip_distance >= 200`
- Filters `passenger_count` to valid range (1–6)

### Aggregations Computed
- Trip count per hour
- Average fare, tip, and distance
- Average trip duration (minutes)
- Total revenue
- Average passenger count

### Output Tables (PostgreSQL)
- `clean_trips` — individual cleaned trip records
- `hourly_aggregates` — per-hour statistics grouped by `pickup_date` and `pickup_hour`

---

## API Endpoints

| Method | Endpoint | Description |
|---|---|---|
| GET | `/health` | Database connectivity check |
| GET | `/aggregates/hourly` | Hourly trip statistics |
| GET | `/aggregates/summary` | Daily KPI summary |
| GET | `/trips` | Individual cleaned trip records |

**Example responses:**
```json
GET /health
{"status": "ok", "database": "connected"}

GET /aggregates/hourly
{"count": 2, "data": [{"pickup_date": "2016-03-10", "pickup_hour": 7,
  "trip_count": 3811, "avg_fare": 12.15, "avg_tip": 1.87,
  "avg_distance": 2.98, "total_revenue": 57813.65}]}
```

---

## Project Structure

```
data-engineering-dlmdsede02/
├── docker-compose.yml              # Full IaC definition — all 6 services
├── ingestion/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── producer.py                 # MS1: Kafka producer with auto schema detection
├── processing/
│   ├── Dockerfile                  # Python 3.11 + Java + PySpark + JARs
│   └── batch_processor.py          # MS3: PySpark cleaning + aggregation + JDBC write
├── api/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── main.py                     # MS5: FastAPI with 4 endpoints
├── postgres/
│   └── init/
│       └── 01_schema.sql           # MS4: Auto-initialised schema on container start
├── data/
│   └── yellow_tripdata.csv         # Dataset (not committed — download separately)
└── README.md
```

---

## Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (v4.0+)
- [Git](https://git-scm.com/)
- NYC Taxi CSV dataset placed in `data/`

---

## How to Run

### 1. Clone the repository
```bash
git clone https://github.com/Dineshkumar55/data-engineering-dlmdsede02.git
cd data-engineering-dlmdsede02
```

### 2. Add the dataset
```bash
mkdir -p data
# Place yellow_tripdata.csv in the data/ folder
```

### 3. Start all services
```bash
docker compose up --build
```

### 4. Verify the pipeline
```bash
# Health check
curl http://localhost:8000/health

# View hourly aggregates
curl http://localhost:8000/aggregates/hourly
```

### 5. Stop all services
```bash
docker compose down
```

### Troubleshooting (Kafka volume conflict)
If Kafka fails with `InconsistentClusterIdException`, run:
```bash
docker compose down
docker volume prune -f
docker compose up --build
```

---

## Technology Stack

| Component | Version |
|---|---|
| Python | 3.11 |
| Apache Kafka | 7.5.0 (Confluent) |
| Apache Spark / PySpark | 3.5.0 |
| PostgreSQL | 15 (Alpine) |
| FastAPI | Latest |
| Docker Compose | v2 |
| Java | OpenJDK 21 |

---

## Pipeline Execution Results

Successfully processed the full March 2016 NYC Taxi dataset:

- **Records ingested:** ~130,000
- **Records after cleaning:** ~2,986 (first Spark batch on 1,996 Kafka messages)
- **Peak hour:** 07:00 — 3,811 trips, avg fare $12.15, total revenue $57,813
- **API confirmed working:** `{"status":"ok","database":"connected"}`

---

## Course Information

- **Course:** Data Engineering (DLMDSEDE02)
- **University:** IU International University of Applied Sciences
- **Task:** Task 1 — Batch Processing Data Architecture
- **GitHub:** https://github.com/Dineshkumar55/data-engineering-dlmdsede02

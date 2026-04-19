# NYC Taxi Batch Processing Pipeline
### DLMDSEDE02 — Data Engineering | IU International University
**Author:** Dineshkumar55

## Architecture
Kaggle CSV → Python Ingestion → Apache Kafka → Apache Spark (PySpark) → PostgreSQL → FastAPI

## Run
```bash
docker compose up --build
```

## API Endpoints
- http://localhost:8000/health
- http://localhost:8000/docs
- http://localhost:8000/aggregates/hourly
- http://localhost:8000/aggregates/summary
- http://localhost:8000/trips

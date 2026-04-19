import os, json, time, logging
import pandas as pd
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

logging.basicConfig(level=logging.INFO, format='%(asctime)s [INGESTION] %(message)s')
log = logging.getLogger(__name__)

KAFKA_SERVERS  = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
TOPIC          = os.getenv('KAFKA_TOPIC', 'nyc-taxi-raw')
DATA_FILE      = os.getenv('DATA_FILE', '/data/yellow_tripdata.csv')
BATCH_SIZE     = int(os.getenv('BATCH_SIZE', 1000))
BATCH_INTERVAL = int(os.getenv('BATCH_INTERVAL_SECONDS', 5))

def build_producer(retries=10, delay=5):
    for attempt in range(1, retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks='all', retries=5, max_block_ms=60000,
            )
            log.info('Kafka producer connected on attempt %d', attempt)
            return producer
        except NoBrokersAvailable:
            log.warning('Kafka not ready (%d/%d). Retrying in %ds...', attempt, retries, delay)
            time.sleep(delay)
    raise RuntimeError('Could not connect to Kafka')

def get_actual_columns(filepath):
    """Read first row to detect actual column names in this CSV."""
    sample = pd.read_csv(filepath, nrows=1)
    return list(sample.columns)

def ingest(producer):
    log.info('Reading dataset: %s', DATA_FILE)
    actual_cols = get_actual_columns(DATA_FILE)
    log.info('Detected columns: %s', actual_cols)

    # Map to standard names — handle both old (2016) and new (2017+) schemas
    col_map = {}
    for c in actual_cols:
        cl = c.lower()
        if 'pickup_datetime' in cl or 'tpep_pickup' in cl:
            col_map[c] = 'pickup_datetime'
        elif 'dropoff_datetime' in cl or 'tpep_dropoff' in cl:
            col_map[c] = 'dropoff_datetime'
        elif cl == 'passenger_count':
            col_map[c] = 'passenger_count'
        elif cl == 'trip_distance':
            col_map[c] = 'trip_distance'
        elif cl == 'fare_amount':
            col_map[c] = 'fare_amount'
        elif cl == 'tip_amount':
            col_map[c] = 'tip_amount'
        elif cl == 'total_amount':
            col_map[c] = 'total_amount'
        elif cl in ('pulocationid', 'pickup_longitude'):
            col_map[c] = 'pickup_location'
        elif cl in ('dolocationid', 'dropoff_longitude'):
            col_map[c] = 'dropoff_location'

    use_cols = list(col_map.keys())
    log.info('Using columns: %s', use_cols)

    chunks = pd.read_csv(DATA_FILE, usecols=use_cols, chunksize=BATCH_SIZE)
    total_sent = 0

    for chunk_idx, chunk in enumerate(chunks):
        chunk = chunk.rename(columns=col_map)
        chunk = chunk.dropna(subset=['pickup_datetime', 'fare_amount'])
        chunk = chunk[chunk['fare_amount'] > 0]

        for _, row in chunk.iterrows():
            record = {
                'pickup_datetime':  str(row.get('pickup_datetime', '')),
                'dropoff_datetime': str(row.get('dropoff_datetime', '')),
                'passenger_count':  int(row.get('passenger_count') or 0),
                'trip_distance':    float(row.get('trip_distance') or 0),
                'pickup_location':  float(row.get('pickup_location') or 0),
                'dropoff_location': float(row.get('dropoff_location') or 0),
                'fare_amount':      float(row.get('fare_amount') or 0),
                'tip_amount':       float(row.get('tip_amount') or 0),
                'total_amount':     float(row.get('total_amount') or 0),
            }
            producer.send(TOPIC, value=record)
            total_sent += 1

        producer.flush()
        log.info('Batch %d sent — %d records total', chunk_idx + 1, total_sent)
        time.sleep(BATCH_INTERVAL)

    log.info('Ingestion complete. Total: %d', total_sent)

if __name__ == '__main__':
    log.info('Starting ingestion service')
    producer = build_producer()
    ingest(producer)
    producer.close()

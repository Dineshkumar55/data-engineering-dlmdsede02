import os, logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *

logging.basicConfig(level=logging.INFO, format='%(asctime)s [SPARK] %(message)s')
log = logging.getLogger(__name__)

KAFKA_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
KAFKA_TOPIC   = os.getenv('KAFKA_TOPIC', 'nyc-taxi-raw')
POSTGRES_URL  = os.getenv('POSTGRES_URL', 'jdbc:postgresql://postgres:5432/taxidb')
POSTGRES_USER = os.getenv('POSTGRES_USER', 'taxiuser')
POSTGRES_PASS = os.getenv('POSTGRES_PASSWORD', 'taxipass')

RAW_SCHEMA = StructType([
    StructField('pickup_datetime',  StringType()),
    StructField('dropoff_datetime', StringType()),
    StructField('passenger_count',  IntegerType()),
    StructField('trip_distance',    DoubleType()),
    StructField('pickup_location',  IntegerType()),
    StructField('dropoff_location', IntegerType()),
    StructField('fare_amount',      DoubleType()),
    StructField('tip_amount',       DoubleType()),
    StructField('total_amount',     DoubleType()),
])

def build_spark():
    return (SparkSession.builder.appName('NYCTaxiBatchProcessor')
            .config('spark.sql.shuffle.partitions', '8')
            .config('spark.executor.memory', '1g')
            .getOrCreate())

def read_from_kafka(spark):
    log.info('Reading from Kafka topic: %s', KAFKA_TOPIC)
    raw = (spark.read.format('kafka')
           .option('kafka.bootstrap.servers', KAFKA_SERVERS)
           .option('subscribe', KAFKA_TOPIC)
           .option('startingOffsets', 'earliest')
           .option('endingOffsets', 'latest')
           .load())
    parsed = raw.select(F.from_json(F.col('value').cast('string'), RAW_SCHEMA).alias('d')).select('d.*')
    log.info('Raw records: %d', parsed.count())
    return parsed

def preprocess(df):
    log.info('Pre-processing...')
    clean = (df
        .withColumn('pickup_dt',  F.to_timestamp('pickup_datetime'))
        .withColumn('dropoff_dt', F.to_timestamp('dropoff_datetime'))
        .dropna(subset=['pickup_dt','dropoff_dt','fare_amount'])
        .filter(F.col('fare_amount') > 0)
        .filter(F.col('trip_distance') > 0)
        .filter(F.col('trip_distance') < 200)
        .filter(F.col('passenger_count').between(1, 6))
        .withColumn('trip_duration_mins',
            (F.unix_timestamp('dropoff_dt') - F.unix_timestamp('pickup_dt')) / 60)
        .withColumn('pickup_date', F.to_date('pickup_dt'))
        .withColumn('pickup_hour', F.hour('pickup_dt'))
        .drop('pickup_datetime','dropoff_datetime'))
    log.info('Clean records: %d', clean.count())
    return clean

def aggregate(df):
    log.info('Aggregating...')
    return (df.groupBy('pickup_date','pickup_hour','pickup_location')
            .agg(
                F.count('*').alias('trip_count'),
                F.avg('fare_amount').alias('avg_fare'),
                F.avg('tip_amount').alias('avg_tip'),
                F.avg('trip_distance').alias('avg_distance'),
                F.avg('trip_duration_mins').alias('avg_duration_mins'),
                F.sum('total_amount').alias('total_revenue'),
                F.avg('passenger_count').alias('avg_passengers'),
            ).orderBy('pickup_date','pickup_hour'))

def write_pg(df, table):
    log.info('Writing to PostgreSQL: %s', table)
    (df.write.format('jdbc')
       .option('url', POSTGRES_URL)
       .option('dbtable', table)
       .option('user', POSTGRES_USER)
       .option('password', POSTGRES_PASS)
       .option('driver', 'org.postgresql.Driver')
       .mode('append').save())
    log.info('Write complete.')

if __name__ == '__main__':
    spark = build_spark()
    spark.sparkContext.setLogLevel('WARN')
    log.info('=== Batch Job START ===')
    raw_df   = read_from_kafka(spark)
    clean_df = preprocess(raw_df)
    agg_df   = aggregate(clean_df)
    write_pg(clean_df.select(
        'pickup_dt','dropoff_dt','pickup_location','dropoff_location',
        'trip_distance','trip_duration_mins','fare_amount',
        'tip_amount','total_amount','passenger_count','pickup_date','pickup_hour'
    ), 'clean_trips')
    write_pg(agg_df, 'hourly_aggregates')
    log.info('=== Batch Job COMPLETE ===')
    spark.stop()

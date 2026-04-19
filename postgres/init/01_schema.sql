CREATE TABLE IF NOT EXISTS clean_trips (
    id SERIAL PRIMARY KEY, pickup_dt TIMESTAMP NOT NULL, dropoff_dt TIMESTAMP NOT NULL,
    pickup_location INTEGER, dropoff_location INTEGER, trip_distance NUMERIC(8,2),
    trip_duration_mins NUMERIC(8,2), fare_amount NUMERIC(8,2), tip_amount NUMERIC(8,2),
    total_amount NUMERIC(8,2), passenger_count INTEGER, pickup_date DATE,
    pickup_hour INTEGER, inserted_at TIMESTAMP DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS hourly_aggregates (
    id SERIAL PRIMARY KEY, pickup_date DATE NOT NULL, pickup_hour INTEGER NOT NULL,
    pickup_location INTEGER, trip_count BIGINT, avg_fare NUMERIC(8,2), avg_tip NUMERIC(8,2),
    avg_distance NUMERIC(8,2), avg_duration_mins NUMERIC(8,2), total_revenue NUMERIC(12,2),
    avg_passengers NUMERIC(4,2), inserted_at TIMESTAMP DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_trips_pickup_date ON clean_trips(pickup_date);
CREATE INDEX IF NOT EXISTS idx_agg_date_hour ON hourly_aggregates(pickup_date, pickup_hour);
DO $$ BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'apiuser') THEN
        CREATE ROLE apiuser LOGIN PASSWORD 'apipass';
    END IF;
END $$;
GRANT SELECT ON clean_trips, hourly_aggregates TO apiuser;

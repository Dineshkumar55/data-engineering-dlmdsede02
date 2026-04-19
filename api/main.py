import os
import psycopg2, psycopg2.extras
from fastapi import FastAPI, HTTPException, Query
from typing import Optional

app = FastAPI(title='NYC Taxi Data API', version='1.0.0')

def get_conn():
    return psycopg2.connect(
        host=os.getenv('POSTGRES_HOST','postgres'),
        port=int(os.getenv('POSTGRES_PORT', 5432)),
        dbname=os.getenv('POSTGRES_DB','taxidb'),
        user=os.getenv('POSTGRES_USER','taxiuser'),
        password=os.getenv('POSTGRES_PASSWORD','taxipass'),
    )

@app.get('/health')
def health():
    try:
        get_conn().close()
        return {'status': 'ok', 'database': 'connected'}
    except Exception as e:
        raise HTTPException(status_code=503, detail=str(e))

@app.get('/aggregates/hourly')
def hourly(date: Optional[str]=None, hour: Optional[int]=Query(None,ge=0,le=23), limit: int=Query(100,le=1000)):
    q = 'SELECT * FROM hourly_aggregates WHERE 1=1'
    p = []
    if date:  q += ' AND pickup_date = %s';  p.append(date)
    if hour is not None: q += ' AND pickup_hour = %s'; p.append(hour)
    q += ' ORDER BY pickup_date DESC, pickup_hour ASC LIMIT %s'; p.append(limit)
    try:
        conn = get_conn(); cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(q, p); rows = cur.fetchall(); conn.close()
        return {'count': len(rows), 'data': rows}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get('/aggregates/summary')
def summary(date: Optional[str]=None):
    q = '''SELECT pickup_date, SUM(trip_count) AS total_trips,
           ROUND(AVG(avg_fare)::numeric,2) AS avg_fare,
           ROUND(SUM(total_revenue)::numeric,2) AS daily_revenue
           FROM hourly_aggregates'''
    p = []
    if date: q += ' WHERE pickup_date = %s'; p.append(date)
    q += ' GROUP BY pickup_date ORDER BY pickup_date DESC LIMIT 30'
    try:
        conn = get_conn(); cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(q, p); rows = cur.fetchall(); conn.close()
        return {'count': len(rows), 'data': rows}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get('/trips')
def trips(date: Optional[str]=None, limit: int=Query(50,le=500)):
    q = 'SELECT * FROM clean_trips WHERE 1=1'
    p = []
    if date: q += ' AND pickup_date = %s'; p.append(date)
    q += ' ORDER BY pickup_dt DESC LIMIT %s'; p.append(limit)
    try:
        conn = get_conn(); cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(q, p); rows = cur.fetchall(); conn.close()
        return {'count': len(rows), 'data': rows}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

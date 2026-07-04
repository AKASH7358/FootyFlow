import os
import json
import psycopg2
from kafka import KafkaConsumer

KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')
PG_HOST = os.getenv('PG_HOST', 'localhost')
PG_PORT = int(os.getenv('PG_PORT', '5433'))

conn = psycopg2.connect(
    host = PG_HOST,
    port = PG_PORT,
    database = "footyflow_db",
    user = "footyflow",
    password = "footyflow123"
)
cursor = conn.cursor()

cursor.execute("""
    CREATE TABLE IF NOT EXISTS raw_matches (
               offset_id INTEGER,
               match_id INTEGER,
               home_team TEXT,
               away_team TEXT,
               score TEXT,
               minute INTEGER,
               status TEXT,
               matchday INTEGER,
               utc_date TEXT,
               consumed_at TIMESTAMP DEFAULT NOW())
               """)
conn.commit()

print("Table ready.")

consumer = KafkaConsumer(
    'matches',
    bootstrap_servers = KAFKA_BROKER,
    group_id = 'footyflow-consumer-group',
    auto_offset_reset = 'earliest',
    value_deserializer = lambda v: json.loads(v.decode('utf-8')) 
)

print("Consumer listening.... (Ctrl+C to stop)")

try:
    for message in consumer:
        data = message.value
        offset = message.offset

        print(f"Received offset {offset}: {data['home']} vs {data['away']}")

        cursor.execute("""
        INSERT INTO raw_matches (offset_id, match_id, home_team, away_team, score, minute, status, matchday, utc_date)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (match_id) DO UPDATE SET
            score = EXCLUDED.score,
            minute = EXCLUDED.minute,
            status = EXCLUDED.status,
            consumed_at = NOW()
    """, (offset, data['match_id'], data['home'], data['away'], data['score'], data['minute'], data.get('status', 'UNKNOWN'), data.get('matchday', 0), data.get('utc_date', '')))

        conn.commit()
        print(f"  → Saved to PostgreSQL ✓")

except KeyboardInterrupt:
    print("\nStopping consumer cleanly...")

finally:
    # This always runs whether you Ctrl+C or it crashes
    # Always close your connections — in production, unclosed connections
    # pile up and eventually exhaust your database's connection limit
    cursor.close()
    conn.close()
    consumer.close()
    print("Connections closed. Goodbye.")
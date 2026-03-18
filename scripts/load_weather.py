import psycopg2
import os

def get_connection():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST"),
        database=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD")
    )

def load_raw(**context):
    ti = context["ti"]
    data = ti.xcom_pull(task_ids="extract")

    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS weather_raw (
            id SERIAL PRIMARY KEY,
            city TEXT,
            temperature FLOAT,
            collected_at TIMESTAMP
        )
    """)

    cur.execute("""
        INSERT INTO weather_raw (city, temperature, collected_at)
        VALUES (%s, %s, %s)
    """, (data["city"], data["temperature"], data["collected_at"]))

    conn.commit()
    cur.close()
    conn.close()


def load_trusted(**context):
    ti = context["ti"]
    data = ti.xcom_pull(task_ids="transform")

    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS weather_trusted (
            city TEXT PRIMARY KEY,
            temperature FLOAT,
            collected_at TIMESTAMP
        )
    """)

    cur.execute("""
        INSERT INTO weather_trusted (city, temperature, collected_at)
        VALUES (%s, %s, %s)
        ON CONFLICT (city)
        DO UPDATE SET
            temperature = EXCLUDED.temperature,
            collected_at = EXCLUDED.collected_at
    """, (data["city"], data["temperature"], data["collected_at"]))

    conn.commit()
    cur.close()
    conn.close()

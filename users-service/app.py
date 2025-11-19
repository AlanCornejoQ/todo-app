import os
import threading
import json
import time
from flask import Flask, request, jsonify
import psycopg2
from kafka import KafkaConsumer

app = Flask(__name__)

DB_NAME = os.getenv("POSTGRES_DB", "usersdb")
DB_USER = os.getenv("POSTGRES_USER", "users_user")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "users_secret")
DB_HOST = os.getenv("POSTGRES_HOST", "users-db")
DB_PORT = int(os.getenv("POSTGRES_PORT", "5432"))

# Kafka config
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka-broker:29092")
TASKS_TOPIC = os.getenv("TASKS_TOPIC", "tasks-events")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "users-service-group")

def get_conn():
    return psycopg2.connect(
        dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST, port=DB_PORT
    )

def ensure_users_table():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    id SERIAL PRIMARY KEY,
                    name TEXT NOT NULL,
                    email TEXT NOT NULL
                )
            """)
            conn.commit()

def ensure_stats_table():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS tasks_per_user (
                    user_id INTEGER PRIMARY KEY,
                    total_tasks INTEGER NOT NULL DEFAULT 0
                )
            """)
            conn.commit()

@app.get("/")
def home():
    return {"version": "v1.1.0", "message": "users-service con Kafka"}

@app.get("/health")
def health():
    try:
        ensure_users_table()
        ensure_stats_table()
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
        return {"status": "ok"}, 200
    except Exception as e:
        return {"status": "error", "details": str(e)}, 500

@app.get("/api/users")
def list_users():
    ensure_users_table()
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id, name, email FROM users ORDER BY id")
            rows = cur.fetchall()
    return jsonify([{"id": i, "name": n, "email": e} for (i, n, e) in rows])

@app.post("/api/users")
def create_user():
    ensure_users_table()
    data = request.get_json(force=True)
    name = data.get("name", "").strip()
    email = data.get("email", "").strip()
    if not name or not email:
        return {"error": "name and email required"}, 400

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO users(name, email) VALUES(%s, %s) RETURNING id",
                (name, email),
            )
            new_id = cur.fetchone()[0]
            conn.commit()
    return {"id": new_id, "name": name, "email": email}, 201

@app.get("/api/users/<int:uid>")
def get_user(uid):
    ensure_users_table()
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id, name, email FROM users WHERE id=%s", (uid,))
            row = cur.fetchone()
    if not row:
        return {"error": "user not found"}, 404
    i, n, e = row
    return {"id": i, "name": n, "email": e}

# ---------- Stats basadas en eventos ----------

@app.get("/api/stats/by-user")
def stats_by_user():
    ensure_stats_table()
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT u.id, u.name, COALESCE(s.total_tasks, 0)
                FROM users u
                LEFT JOIN tasks_per_user s ON u.id = s.user_id
                ORDER BY u.id
            """)
            rows = cur.fetchall()
    result = [
        {"user_id": i, "name": n, "total_tasks": t}
        for (i, n, t) in rows
    ]
    return jsonify(result)

# ---------- Procesamiento de eventos ----------

def process_event(event: dict):
    ensure_stats_table()
    event_type = event.get("type")
    user_id = event.get("user_id")
    if not user_id:
        return

    with get_conn() as conn:
        with conn.cursor() as cur:
            if event_type == "task_created":
                cur.execute("""
                    INSERT INTO tasks_per_user(user_id, total_tasks)
                    VALUES(%s, 1)
                    ON CONFLICT (user_id) DO UPDATE
                    SET total_tasks = tasks_per_user.total_tasks + 1
                """, (user_id,))
            elif event_type == "task_deleted":
                cur.execute("""
                    INSERT INTO tasks_per_user(user_id, total_tasks)
                    VALUES(%s, 0)
                    ON CONFLICT (user_id) DO UPDATE
                    SET total_tasks = GREATEST(tasks_per_user.total_tasks - 1, 0)
                """, (user_id,))
            conn.commit()

def kafka_consumer_loop():
    """
    Consumer resiliente: reintenta conexión a Kafka en loop infinito.
    Si Kafka no está listo al inicio, espera 5s y vuelve a intentar.
    """
    while True:
        try:
            print(f"[users-service] intentando conectar a Kafka en {KAFKA_BOOTSTRAP}...", flush=True)
            consumer = KafkaConsumer(
                TASKS_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP,
                group_id=KAFKA_GROUP_ID,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                max_poll_records=50,
            )
            print(f"[users-service] KafkaConsumer escuchando {TASKS_TOPIC} en {KAFKA_BOOTSTRAP}", flush=True)
            for msg in consumer:
                event = msg.value
                print(f"[users-service] evento recibido: {event}", flush=True)
                try:
                    process_event(event)
                except Exception as e:
                    print(f"[users-service] error procesando evento {event}: {e}", flush=True)
        except Exception as e:
            print(f"[users-service] error conectando/leyendo de Kafka: {e}. Reintentando en 5s...", flush=True)
            time.sleep(5)

def start_kafka_consumer_thread():
    t = threading.Thread(target=kafka_consumer_loop, daemon=True)
    t.start()
    print("[users-service] hilo KafkaConsumer iniciado", flush=True)

# Iniciar el consumer cuando se carga la app (por proceso de gunicorn)
start_kafka_consumer_thread()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)

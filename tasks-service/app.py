import os, json, time
from flask import Flask, request, jsonify
import psycopg2
import redis
import requests
import tenacity
import pybreaker
from kafka import KafkaProducer

app = Flask(__name__)

# ==== Config DB (para tasks-service) ====
DB_NAME = os.getenv("POSTGRES_DB", "tasksdb")
DB_USER = os.getenv("POSTGRES_USER", "tasks_user")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "tasks_secret")
DB_HOST = os.getenv("POSTGRES_HOST", "tasks-db")
DB_PORT = int(os.getenv("POSTGRES_PORT", "5432"))

# ==== Config Redis ====
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

# ==== Config servicio de usuarios (B) ====
USERS_SERVICE_URL = os.getenv("USERS_SERVICE_URL", "http://users-service:8000")

# ==== Config Kafka (publicar eventos) ====
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka-broker:29092")
TASKS_TOPIC = os.getenv("TASKS_TOPIC", "tasks-events")

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

def get_conn():
    return psycopg2.connect(
        dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST, port=DB_PORT
    )

# ---------- Kafka producer resiliente ----------
def create_producer():
    """
    Intenta conectar a Kafka en loop hasta lograrlo.
    Así evitamos que producer quede en None si Kafka tarda en levantar.
    """
    while True:
        try:
            print(f"[tasks-service] intentando conectar KafkaProducer a {KAFKA_BOOTSTRAP}...", flush=True)
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                retries=3,
            )
            print(f"[tasks-service] KafkaProducer conectado a {KAFKA_BOOTSTRAP}", flush=True)
            return producer
        except Exception as e:
            print(f"[tasks-service] error creando KafkaProducer: {e}. Reintentando en 5s...", flush=True)
            time.sleep(5)

producer = create_producer()

def send_task_event(event_type: str, payload: dict):
    event = {"type": event_type, **payload}
    try:
        producer.send(TASKS_TOPIC, event)
        producer.flush()
        print(f"[tasks-service] evento enviado a {TASKS_TOPIC}: {event}", flush=True)
    except Exception as e:
        print(f"[tasks-service] error enviando evento a Kafka: {e} (evento={event})", flush=True)

# ================== Health / Home ===================

@app.get("/")
def home():
    return {"version": "v2.1.0", "message": "tasks-service con Kafka"}

@app.get("/health")
def health():
    try:
        r.ping()
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
        return {"status": "ok"}, 200
    except Exception as e:
        return {"status": "error", "details": str(e)}, 500

# ============ Retry + Circuit Breaker para llamar a users-service ============

circuit_breaker = pybreaker.CircuitBreaker(
    fail_max=3,
    reset_timeout=30
)

@circuit_breaker
@tenacity.retry(
    stop=tenacity.stop_after_attempt(3),
    wait=tenacity.wait_fixed(1)
)
def fetch_user(user_id: int):
    resp = requests.get(f"{USERS_SERVICE_URL}/api/users/{user_id}", timeout=2)
    resp.raise_for_status()
    return resp.json()

# ================== Endpoints de tareas (microservicio A) ===================

def ensure_table():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS tasks (
                    id SERIAL PRIMARY KEY,
                    title TEXT NOT NULL,
                    done BOOLEAN DEFAULT FALSE,
                    user_id INTEGER NOT NULL
                )
            """)
            conn.commit()

@app.get("/api/tasks")
def list_tasks():
    ensure_table()
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id, title, done, user_id FROM tasks ORDER BY id")
            rows = cur.fetchall()
    return jsonify([
        {"id": i, "title": t, "done": d, "user_id": u}
        for (i, t, d, u) in rows
    ])

@app.post("/api/tasks")
def create_task():
    """
    Crea una tarea para un usuario.
    - Si no viene user_id, se usa 1 por defecto.
    - Antes de insertar, se llama a users-service con Retry + Circuit Breaker.
    - Luego se publica un evento 'task_created' en Kafka.
    """
    ensure_table()
    data = request.get_json(force=True)
    title = data.get("title", "").strip()
    if not title:
        return {"error": "title required"}, 400

    user_id = int(data.get("user_id", 1))

    # Llamar a users-service con Retry + Circuit Breaker
    try:
        user = fetch_user(user_id)
    except pybreaker.CircuitBreakerError:
        return {
            "error": "user-service unavailable (circuit open)"
        }, 503
    except Exception as e:
        return {
            "error": "error calling user-service",
            "details": str(e)
        }, 502

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO tasks(title, done, user_id) VALUES(%s, false, %s) RETURNING id",
                (title, user_id),
            )
            new_id = cur.fetchone()[0]
            conn.commit()

    # Publicar evento en Kafka
    send_task_event("task_created", {
        "task_id": new_id,
        "user_id": user_id,
        "title": title,
        "done": False,
    })

    return {
        "id": new_id,
        "title": title,
        "done": False,
        "user_id": user_id,
        "user": user,
    }, 201

@app.put("/api/tasks/<int:tid>")
def update_task(tid):
    ensure_table()
    data = request.get_json(force=True)
    title = data.get("title")
    done  = data.get("done")

    with get_conn() as conn:
        with conn.cursor() as cur:
            if title is not None:
                cur.execute("UPDATE tasks SET title=%s WHERE id=%s", (title, tid))
            if done is not None:
                cur.execute("UPDATE tasks SET done=%s WHERE id=%s", (bool(done), tid))
            conn.commit()

    send_task_event("task_updated", {
        "task_id": tid,
        "title": title,
        "done": done,
    })

    return {"ok": True}

@app.delete("/api/tasks/<int:tid>")
def delete_task(tid):
    ensure_table()
    user_id = None
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT user_id FROM tasks WHERE id=%s", (tid,))
            row = cur.fetchone()
            if row:
                user_id = row[0]
            cur.execute("DELETE FROM tasks WHERE id=%s", (tid,))
            conn.commit()

    send_task_event("task_deleted", {
        "task_id": tid,
        "user_id": user_id,
    })

    return {"ok": True}

import os, json
from flask import Flask, request, jsonify
import psycopg2
import redis
import requests
import tenacity
import pybreaker

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

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

def get_conn():
    return psycopg2.connect(
        dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST, port=DB_PORT
    )

@app.get("/")
def home():
    return {"version": "v2.0.0", "message": "tasks-service"}

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
    fail_max=3,           # después de 3 fallos, abre el circuito
    reset_timeout=30      # en 30s intenta de nuevo
)

@circuit_breaker
@tenacity.retry(
    stop=tenacity.stop_after_attempt(3),    # 3 reintentos
    wait=tenacity.wait_fixed(1)            # 1s entre reintentos
)
def fetch_user(user_id: int):
    """
    Llama al users-service para verificar que el usuario existe.
    Implementa Retry + Circuit Breaker.
    """
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

    # Si llegamos aquí, el usuario existe
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO tasks(title, done, user_id) VALUES(%s, false, %s) RETURNING id",
                (title, user_id),
            )
            new_id = cur.fetchone()[0]
            conn.commit()
    return {
        "id": new_id,
        "title": title,
        "done": False,
        "user_id": user_id,
        "user": user,   # opcional: devolver info del usuario
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
    return {"ok": True}

@app.delete("/api/tasks/<int:tid>")
def delete_task(tid):
    ensure_table()
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM tasks WHERE id=%s", (tid,))
            conn.commit()
    return {"ok": True}

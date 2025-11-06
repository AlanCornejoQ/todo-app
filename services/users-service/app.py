import os
from flask import Flask, jsonify
import requests
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import pybreaker
import psycopg

EQUIPOS_BASE_URL = os.getenv("TASKS_BASE_URL", "http://lb-tasks")
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://users:users@db_users:5432/users_db")

def init_db():
    with psycopg.connect(DATABASE_URL, autocommit=True) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS users(
                id SERIAL PRIMARY KEY,
                email TEXT UNIQUE NOT NULL
            );
        """)

app = Flask(__name__)
init_db()

breaker = pybreaker.CircuitBreaker(
    fail_max=5,               # volumen de llamadas antes de evaluar
    reset_timeout=5,          # 5s medio-abierto
    exclude=[requests.exceptions.HTTPError]  # 4xx no abren el circuito
)

@retry(
    reraise=True,
    stop=stop_after_attempt(3),                  # Retry 3 intentos
    wait=wait_exponential(multiplier=0.3, min=0.5, max=2),
    retry=retry_if_exception_type((requests.exceptions.RequestException,))
)
@breaker
def get_tasks_via_lb():
    r = requests.get(f"{EQUIPOS_BASE_URL}/tasks", timeout=2.0)
    r.raise_for_status()
    return r.json()

@app.get("/health")
def health():
    return jsonify(ok=True, service="users")

@app.get("/me/tasks")
def me_tasks():
    try:
        data = get_tasks_via_lb()
        return jsonify({"source":"users","tasks":data})
    except pybreaker.CircuitBreakerError:
        # Fallback si el circuito está abierto
        return jsonify({"fallback":True,"tasks":[],"message":"tasks no disponible"}), 503
    except requests.exceptions.RequestException:
        # Error de red luego de reintentos
        return jsonify({"error":"upstream error"}), 502

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT","6000")))

import json
import os
import threading
from flask import Flask, jsonify, request
from kafka import KafkaProducer

app = Flask(__name__)

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC_TASKS = os.getenv("TOPIC_TASKS", "tasks")

# ---------- Kafka Producer (lazy) ----------
_producer = None
_lock = threading.Lock()

def get_kafka_producer():
    global _producer
    if _producer is None:
        with _lock:
            if _producer is None:
                _producer = KafkaProducer(
                    bootstrap_servers=KAFKA_BROKER,
                    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                    retries=3,
                    linger_ms=10,
                )
    return _producer

# ---------- In-memory tasks (demo) ----------
TASKS = []
TASK_ID = 1

@app.get("/health")
def health():
    return jsonify({"status": "ok", "service": "tasks-service"})

@app.get("/tasks")
def list_tasks():
    return jsonify({"tasks": TASKS})

@app.post("/tasks")
def create_task():
    global TASK_ID
    data = request.get_json(force=True, silent=True) or {}
    title = data.get("title", f"Tarea {TASK_ID}")
    user_id = data.get("user_id", 1)

    task = {"id": TASK_ID, "title": title, "done": False, "user_id": user_id}
    TASKS.append(task)
    TASK_ID += 1

    # ---- PUBLICAR EVENTO EN KAFKA ----
    evt = {
        "type": "task.created",
        "payload": task,
        "source": "tasks-service",
    }
    try:
        prod = get_kafka_producer()
        prod.send(TOPIC_TASKS, evt)
        prod.flush(2.0)
    except Exception as e:
        # No romper el flujo si Kafka no está disponible
        app.logger.error(f"[KAFKA] Error publicando evento: {e}")

    return jsonify(task), 201

@app.post("/tasks/<int:task_id>/done")
def complete_task(task_id: int):
    for t in TASKS:
        if t["id"] == task_id:
            t["done"] = True
            evt = {
                "type": "task.completed",
                "payload": t,
                "source": "tasks-service",
            }
            try:
                prod = get_kafka_producer()
                prod.send(TOPIC_TASKS, evt)
                prod.flush(2.0)
            except Exception as e:
                app.logger.error(f"[KAFKA] Error publicando evento: {e}")
            return jsonify(t)
    return jsonify({"error": "task not found"}), 404

if __name__ == "__main__":
    port = int(os.getenv("FLASK_RUN_PORT", "5000"))
    app.run(host="0.0.0.0", port=port)

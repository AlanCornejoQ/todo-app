import json
import os
import threading
import time
from flask import Flask, jsonify
from kafka import KafkaConsumer

app = Flask(__name__)

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC_TASKS = os.getenv("TOPIC_TASKS", "tasks")
GROUP_ID = os.getenv("GROUP_ID", "users-consumer-group")

# Estado "procesado" (demo): conteo de tareas por usuario
USER_TASK_COUNTS = {}
consumer_thread_started = False
thread_lock = threading.Lock()

def consumer_loop():
    """Hilo consumidor de Kafka."""
    app.logger.info("[KAFKA] Iniciando consumer...")
    # autocommit simple para demo
    consumer = KafkaConsumer(
        TOPIC_TASKS,
        bootstrap_servers=KAFKA_BROKER,
        group_id=GROUP_ID,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        enable_auto_commit=True,
        auto_offset_reset="earliest",
        consumer_timeout_ms=1000,
    )
    while True:
        try:
            for msg in consumer:
                evt = msg.value
                evt_type = evt.get("type")
                payload = evt.get("payload", {})
                user_id = payload.get("user_id", None)

                if evt_type == "task.created" and user_id is not None:
                    USER_TASK_COUNTS[user_id] = USER_TASK_COUNTS.get(user_id, 0) + 1
                    app.logger.info(f"[KAFKA] task.created user={user_id} total={USER_TASK_COUNTS[user_id]}")
                elif evt_type == "task.completed":
                    # aquí podrías actualizar otro estado/auditoría
                    app.logger.info(f"[KAFKA] task.completed id={payload.get('id')}")
        except Exception as e:
            app.logger.error(f"[KAFKA] Error en consumer: {e}")
            time.sleep(1.0)

def ensure_consumer_started():
    global consumer_thread_started
    if not consumer_thread_started:
        with thread_lock:
            if not consumer_thread_started:
                t = threading.Thread(target=consumer_loop, daemon=True)
                t.start()
                consumer_thread_started = True

@app.get("/health")
def health():
    ensure_consumer_started()
    return jsonify({"status": "ok", "service": "users-service"})

@app.get("/stats/users")
def stats_users():
    ensure_consumer_started()
    return jsonify({"users_task_counts": USER_TASK_COUNTS})

if __name__ == "__main__":
    port = int(os.getenv("FLASK_RUN_PORT", "6000"))
    ensure_consumer_started()
    app.run(host="0.0.0.0", port=port)

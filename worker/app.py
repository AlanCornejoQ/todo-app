import os, json, time, sys
from kafka import KafkaConsumer

BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.getenv("TOPIC", "todo-events")
GROUP_ID = os.getenv("GROUP_ID", "todo-worker")

def run():
    while True:
        try:
            consumer = KafkaConsumer(
                TOPIC,
                bootstrap_servers=BOOTSTRAP_SERVERS,
                group_id=GROUP_ID,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                enable_auto_commit=True,
                auto_offset_reset="earliest",
                max_poll_records=50,
            )
            print(f"[worker] escuchando topic '{TOPIC}' en {BOOTSTRAP_SERVERS} con group_id='{GROUP_ID}'", flush=True)
            for msg in consumer:
                event = msg.value
                # Acá haces el “procesamiento” del evento (logs, métricas, side-effects, etc.)
                print(f"[worker] evento @partition {msg.partition} offset {msg.offset}: {event}", flush=True)
        except Exception as e:
            print(f"[worker] error: {e}. Reintentando en 3s...", file=sys.stderr, flush=True)
            time.sleep(3)

if __name__ == "__main__":
    run()

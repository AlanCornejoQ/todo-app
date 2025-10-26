import os, json
from flask import Flask, request, jsonify
import psycopg2
import redis

app = Flask(__name__)

DB_NAME = os.getenv("POSTGRES_DB", "tododb")
DB_USER = os.getenv("POSTGRES_USER", "todo")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "todo_secret")
DB_HOST = os.getenv("POSTGRES_HOST", "db")
DB_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

@app.get("/")
def home():
    return {"version": "v1.1.0", "message": "API actualizada"}

def get_conn():
    return psycopg2.connect(
        dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST, port=DB_PORT
    )

@app.get("/health")
def health():
    try:
        r.ping()
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
        return {"status":"ok"}, 200
    except Exception as e:
        return {"status":"error", "details":str(e)}, 500

@app.get("/api/tasks")
def list_tasks():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("CREATE TABLE IF NOT EXISTS tasks (id SERIAL PRIMARY KEY, title TEXT NOT NULL, done BOOLEAN DEFAULT FALSE)")
            cur.execute("SELECT id,title,done FROM tasks ORDER BY id")
            rows = cur.fetchall()
    return jsonify([{"id":i,"title":t,"done":d} for (i,t,d) in rows])

@app.post("/api/tasks")
def create_task():
    data = request.get_json(force=True)
    title = data.get("title","").strip()
    if not title: return {"error":"title required"}, 400
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO tasks(title,done) VALUES(%s,false) RETURNING id", (title,))
            new_id = cur.fetchone()[0]
            conn.commit()
    return {"id":new_id,"title":title,"done":False}, 201

@app.put("/api/tasks/<int:tid>")
def update_task(tid):
    data = request.get_json(force=True)
    title = data.get("title")
    done  = data.get("done")
    with get_conn() as conn:
        with conn.cursor() as cur:
            if title is not None:
                cur.execute("UPDATE tasks SET title=%s WHERE id=%s", (title,tid))
            if done is not None:
                cur.execute("UPDATE tasks SET done=%s WHERE id=%s", (bool(done),tid))
            conn.commit()
    return {"ok":True}

@app.delete("/api/tasks/<int:tid>")
def delete_task(tid):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM tasks WHERE id=%s", (tid,))
            conn.commit()
    return {"ok":True}

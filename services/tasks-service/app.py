import os
from flask import Flask, jsonify, request
import psycopg

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://tasks:tasks@db_tasks:5432/tasks_db")

def init_db():
    with psycopg.connect(DATABASE_URL, autocommit=True) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS tasks(
                id SERIAL PRIMARY KEY,
                title TEXT NOT NULL,
                done BOOLEAN DEFAULT FALSE
            );
        """)

app = Flask(__name__)
init_db()

@app.get("/health")
def health():
    return jsonify(ok=True, service="tasks")

@app.get("/tasks")
def list_tasks():
    with psycopg.connect(DATABASE_URL) as conn:
        rows = conn.execute("SELECT id,title,done FROM tasks ORDER BY id").fetchall()
        return jsonify([{"id":r[0], "title":r[1], "done":r[2]} for r in rows])

@app.post("/tasks")
def create_task():
    data = request.get_json(silent=True) or {}
    title = data.get("title", "Untitled")
    with psycopg.connect(DATABASE_URL, autocommit=True) as conn:
        row = conn.execute("INSERT INTO tasks(title) VALUES(%s) RETURNING id,title,done", (title,)).fetchone()
        return jsonify({"id":row[0], "title":row[1], "done":row[2]}), 201

@app.post("/tasks/<int:tid>/done")
def mark_done(tid:int):
    data = request.get_json(silent=True) or {}
    done = bool(data.get("done", True))
    with psycopg.connect(DATABASE_URL, autocommit=True) as conn:
        row = conn.execute("UPDATE tasks SET done=%s WHERE id=%s RETURNING id,title,done", (done, tid)).fetchone()
        if not row:
            return jsonify({"error":"not found"}), 404
        return jsonify({"id":row[0], "title":row[1], "done":row[2]})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")))

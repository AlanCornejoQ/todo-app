import os
from flask import Flask, request, jsonify
import psycopg2

app = Flask(__name__)

DB_NAME = os.getenv("POSTGRES_DB", "usersdb")
DB_USER = os.getenv("POSTGRES_USER", "users_user")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "users_secret")
DB_HOST = os.getenv("POSTGRES_HOST", "users-db")
DB_PORT = int(os.getenv("POSTGRES_PORT", "5432"))

def get_conn():
    return psycopg2.connect(
        dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST, port=DB_PORT
    )

def ensure_table():
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

@app.get("/")
def home():
    return {"version": "v1.0.0", "message": "users-service"}

@app.get("/health")
def health():
    try:
        ensure_table()
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
        return {"status": "ok"}, 200
    except Exception as e:
        return {"status": "error", "details": str(e)}, 500

@app.get("/api/users")
def list_users():
    ensure_table()
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id, name, email FROM users ORDER BY id")
            rows = cur.fetchall()
    return jsonify([{"id": i, "name": n, "email": e} for (i, n, e) in rows])

@app.post("/api/users")
def create_user():
    ensure_table()
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
    ensure_table()
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id, name, email FROM users WHERE id=%s", (uid,))
            row = cur.fetchone()
    if not row:
        return {"error": "user not found"}, 404
    i, n, e = row
    return {"id": i, "name": n, "email": e}

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)

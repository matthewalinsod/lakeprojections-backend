from flask import Flask, jsonify
import sqlite3
import os

app = Flask(__name__)

# ==============================
# DATABASE CONFIG
# ==============================

DB_PATH = "/data/lakeprojections.db"

def get_db_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

# ==============================
# HEALTH CHECK
# ==============================

@app.route("/")
def home():
    return jsonify({"status": "LakeProjections API is running"})

# ==============================
# SIMPLE TEST QUERY
# ==============================

@app.route("/test-count")
def test_count():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) as count FROM historic_daily_data")
        result = cursor.fetchone()
        conn.close()

        return jsonify({
            "historic_daily_data_rows": result["count"]
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ==============================
# START SERVER
# ==============================

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)

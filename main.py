from flask import Flask, jsonify, request
import sqlite3
import os
import requests
from datetime import datetime, timedelta

app = Flask(__name__)

DB_PATH = "/data/lakeprojections.db"
UPDATE_TOKEN = os.environ.get("UPDATE_TOKEN")


# ==============================
# DATABASE
# ==============================

def get_db_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


# ==============================
# SECURITY CHECK
# ==============================

def authorize(req):
    token = req.headers.get("X-Update-Token")
    return token == UPDATE_TOKEN


# ==============================
# HEALTH
# ==============================

@app.route("/")
def home():
    return jsonify({"status": "LakeProjections API is running"})


# ==============================
# HISTORIC UPDATE
# ==============================

@app.route("/internal/update/historic", methods=["POST"])
def update_historic():

    if not authorize(request):
        return jsonify({"error": "Unauthorized"}), 403

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT MAX(historic_datetime) FROM historic_daily_data")
    max_dt = cursor.fetchone()[0]

    if not max_dt:
        return jsonify({"error": "No existing historic data found"}), 400

    last_date = datetime.strptime(max_dt[:10], "%Y-%m-%d")
    start_date = last_date + timedelta(days=1)
    yesterday = datetime.utcnow().date() - timedelta(days=1)

    if start_date.date() > yesterday:
        return jsonify({"status": "No historic update needed"})

    t1 = start_date.strftime("%Y-%m-%dT00:00")
    t2 = (yesterday + timedelta(days=1)).strftime("%Y-%m-%dT00:00")

    api_url = f"https://www.usbr.gov/pn-bin/hdb/hdb.pl?svr=lchdb&sdi=1930%2C1863%2C2070%2C2100%2C2166%2C2071%2C2101%2C2146%2C2072&tstp=DY&t1={t1}&t2={t2}&table=R&format=json"

    response = requests.get(api_url)
    data = response.json()

    inserted = 0

    for series in data.get("Series", []):
        sd_id = int(series["SDI"])
        for point in series.get("Data", []):
            dt = datetime.strptime(point["t"], "%m/%d/%Y %I:%M:%S %p")
            iso_dt = dt.strftime("%Y-%m-%dT%H:%M:%S")
            value = float(point["v"])

            cursor.execute("""
                INSERT OR IGNORE INTO historic_daily_data
                (historic_datetime, sd_id, value)
                VALUES (?, ?, ?)
            """, (iso_dt, sd_id, value))

            inserted += cursor.rowcount

    conn.commit()
    conn.close()

    return jsonify({
        "historic_inserted": inserted,
        "start": t1,
        "end": t2
    })


# ==============================
# FORECAST UPDATE
# ==============================

@app.route("/internal/update/forecast", methods=["POST"])
def update_forecast():

    if not authorize(request):
        return jsonify({"error": "Unauthorized"}), 403

    conn = get_db_connection()
    cursor = conn.cursor()

    today = datetime.utcnow().date()
    end_date = today + timedelta(days=8)

    t1 = today.strftime("%Y-%m-%dT00:00")
    t2 = end_date.strftime("%Y-%m-%dT00:00")

    now_accessed = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")

    api_url = f"https://www.usbr.gov/pn-bin/hdb/hdb.pl?svr=lchdb&sdi=2166%2C2146%2C14163%2C14164%2C14165%2C14166%2C14167%2C14168%2C14169%2C14170%2C14171&tstp=HR&t1={t1}&t2={t2}&table=M&mrid=2&format=json"

    response = requests.get(api_url)
    data = response.json()

    inserted = 0

    for series in data.get("Series", []):
        sd_id = int(series["SDI"])
        for point in series.get("Data", []):
            dt = datetime.strptime(point["t"], "%m/%d/%Y %I:%M:%S %p")
            iso_dt = dt.strftime("%Y-%m-%dT%H:%M:%S")
            value = float(point["v"])

            cursor.execute("""
                INSERT OR IGNORE INTO forecasted_hourly_data
                (forecasted_datetime, sd_id, datetime_accessed, value)
                VALUES (?, ?, ?, ?)
            """, (iso_dt, sd_id, now_accessed, value))

            inserted += cursor.rowcount

    conn.commit()
    conn.close()

    return jsonify({
        "forecast_inserted": inserted,
        "range_start": t1,
        "range_end": t2
    })


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)

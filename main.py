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


@app.route("/health")
def health():
    return "OK", 200


# ==============================
# HISTORIC DAILY UPDATE
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
        conn.close()
        return jsonify({"error": "No existing historic data found"}), 400

    last_date = datetime.strptime(max_dt[:10], "%Y-%m-%d")
    start_date = last_date + timedelta(days=1)
    yesterday = datetime.utcnow().date() - timedelta(days=1)

    if start_date.date() > yesterday:
        conn.close()
        return jsonify({"status": "No historic update needed"})

    t1 = start_date.strftime("%Y-%m-%dT00:00")
    t2 = (yesterday + timedelta(days=1)).strftime("%Y-%m-%dT00:00")

    api_url = (
        "https://www.usbr.gov/pn-bin/hdb/hdb.pl"
        f"?svr=lchdb"
        f"&sdi=1930%2C1863%2C2070%2C2100%2C2166%2C2071%2C2101%2C2146%2C2072"
        f"&tstp=DY"
        f"&t1={t1}"
        f"&t2={t2}"
        f"&table=R"
        f"&format=json"
    )

    try:
        response = requests.get(api_url, timeout=60)
        response.raise_for_status()
        data = response.json()
    except Exception as e:
        conn.close()
        return jsonify({"error": "API failure", "details": str(e)}), 500

    inserted = 0
    skipped = 0

    for series in data.get("Series", []):
        sd_id = int(series["SDI"])

        for point in series.get("Data", []):
            try:
                dt = datetime.strptime(point["t"], "%m/%d/%Y %I:%M:%S %p")
                iso_dt = dt.strftime("%Y-%m-%dT%H:%M:%S")
                value = float(point["v"])

                cursor.execute("""
                    INSERT OR IGNORE INTO historic_daily_data
                    (historic_datetime, sd_id, value)
                    VALUES (?, ?, ?)
                """, (iso_dt, sd_id, value))

                inserted += cursor.rowcount

            except:
                skipped += 1

    conn.commit()
    conn.close()

    return jsonify({
        "historic_inserted": inserted,
        "historic_skipped": skipped,
        "range_start": t1,
        "range_end": t2
    })

# ==============================
# HISTORIC HOURLY UPDATE
# ==============================

@app.route("/internal/update/historic/hourly", methods=["POST"])
def update_historic_hourly():

    if not authorize(request):
        return jsonify({"error": "Unauthorized"}), 403

    conn = get_db_connection()
    cursor = conn.cursor()

    # Find latest datetime in table
    cursor.execute("SELECT MAX(historic_datetime) FROM historic_hourly_data")
    max_dt = cursor.fetchone()[0]

    if not max_dt:
        conn.close()
        return jsonify({"error": "No existing historic hourly data found"}), 400

    last_dt = datetime.strptime(max_dt, "%Y-%m-%dT%H:%M:%S")
    start_dt = last_dt + timedelta(hours=1)

    today_utc = datetime.utcnow().date()
    end_dt = datetime.combine(today_utc, datetime.min.time())  # Midnight today UTC

    if start_dt >= end_dt:
        conn.close()
        return jsonify({"status": "No historic hourly update needed"})

    t1 = start_dt.strftime("%Y-%m-%dT%H:%M")
    t2 = end_dt.strftime("%Y-%m-%dT%H:%M")

    api_url = (
        "https://www.usbr.gov/pn-bin/hdb/hdb.pl"
        f"?svr=lchdb"
        f"&sdi=2166%2C2146%2C14163%2C14164%2C14165%2C14166%2C14167%2C14168%2C14169%2C14170%2C14171"
        f"&tstp=HR"
        f"&t1={t1}"
        f"&t2={t2}"
        f"&table=R"
        f"&mrid=2"
        f"&format=json"
    )

    try:
        response = requests.get(api_url, timeout=60)
        response.raise_for_status()
        data = response.json()
    except Exception as e:
        conn.close()
        return jsonify({"error": "API failure", "details": str(e)}), 500

    inserted = 0
    skipped = 0

    for series in data.get("Series", []):
        sd_id = int(series["SDI"])

        for point in series.get("Data", []):
            try:
                dt = datetime.strptime(point["t"], "%m/%d/%Y %I:%M:%S %p")
                iso_dt = dt.strftime("%Y-%m-%dT%H:%M:%S")
                value = float(point["v"])

                cursor.execute("""
                    INSERT OR IGNORE INTO historic_hourly_data
                    (historic_datetime, sd_id, value)
                    VALUES (?, ?, ?)
                """, (iso_dt, sd_id, value))

                inserted += cursor.rowcount

            except:
                skipped += 1

    conn.commit()
    conn.close()

    return jsonify({
        "historic_hourly_inserted": inserted,
        "historic_hourly_skipped": skipped,
        "range_start": t1,
        "range_end": t2
    })

# ==============================
# FORECAST DAILY UPDATE
# ==============================

@app.route("/internal/update/forecast/daily", methods=["POST"])
def update_forecast_daily():

    if not authorize(request):
        return jsonify({"error": "Unauthorized"}), 403

    conn = get_db_connection()
    cursor = conn.cursor()

    today = datetime.utcnow()
    start_date = today.strftime("%Y-%m-%dT00:00")
    end_date = (today + timedelta(days=90)).strftime("%Y-%m-%dT00:00")

    datetime_accessed = today.strftime("%Y-%m-%dT%H:%M:%S")

    SDI_LIST = "1930,1863,2070,2100,2166,2071,2101,2146,2072"

    api_url = (
        "https://www.usbr.gov/pn-bin/hdb/hdb.pl"
        f"?svr=lchdb"
        f"&sdi={SDI_LIST}"
        f"&tstp=DY"
        f"&t1={start_date}"
        f"&t2={end_date}"
        f"&table=M"
        f"&mrid=4"
        f"&format=json"
    )

    try:
        response = requests.get(api_url, timeout=60)
        response.raise_for_status()
        data = response.json()
    except Exception as e:
        conn.close()
        return jsonify({"error": "API failure", "details": str(e)}), 500

    # Load valid SDIDs
    cursor.execute("SELECT sd_id FROM sdid_mapping")
    valid_sdids = {row[0] for row in cursor.fetchall()}

    inserted = 0
    skipped = 0

    for series in data.get("Series", []):
        sd_id = int(series["SDI"])

        if sd_id not in valid_sdids:
            continue

        for point in series.get("Data", []):
            try:
                dt = datetime.strptime(point["t"], "%m/%d/%Y %I:%M:%S %p")
                iso_dt = dt.strftime("%Y-%m-%dT%H:%M:%S")
                value = float(point["v"])

                cursor.execute("""
                    INSERT OR IGNORE INTO forecasted_daily_data
                    (forecasted_datetime, sd_id, datetime_accessed, value)
                    VALUES (?, ?, ?, ?)
                """, (iso_dt, sd_id, datetime_accessed, value))

                inserted += cursor.rowcount

            except:
                skipped += 1

    conn.commit()
    conn.close()

    return jsonify({
        "forecast_daily_inserted": inserted,
        "forecast_daily_skipped": skipped,
        "range_start": start_date,
        "range_end": end_date
    })

# ==============================
# FORECAST HOURLY UPDATE
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

    api_url = (
        "https://www.usbr.gov/pn-bin/hdb/hdb.pl"
        f"?svr=lchdb"
        f"&sdi=2166%2C2146%2C14163%2C14164%2C14165%2C14166%2C14167%2C14168%2C14169%2C14170%2C14171"
        f"&tstp=HR"
        f"&t1={t1}"
        f"&t2={t2}"
        f"&table=M"
        f"&mrid=2"
        f"&format=json"
    )

    try:
        response = requests.get(api_url, timeout=60)
        response.raise_for_status()
        data = response.json()
    except Exception as e:
        conn.close()
        return jsonify({"error": "API failure", "details": str(e)}), 500

    inserted = 0
    skipped = 0

    for series in data.get("Series", []):
        sd_id = int(series["SDI"])

        for point in series.get("Data", []):
            try:
                dt = datetime.strptime(point["t"], "%m/%d/%Y %I:%M:%S %p")
                iso_dt = dt.strftime("%Y-%m-%dT%H:%M:%S")
                value = float(point["v"])

                cursor.execute("""
                    INSERT OR IGNORE INTO forecasted_hourly_data
                    (forecasted_datetime, sd_id, datetime_accessed, value)
                    VALUES (?, ?, ?, ?)
                """, (iso_dt, sd_id, now_accessed, value))

                inserted += cursor.rowcount

            except:
                skipped += 1

    conn.commit()
    conn.close()

    return jsonify({
        "forecast_inserted": inserted,
        "forecast_skipped": skipped,
        "range_start": t1,
        "range_end": t2
    })


# ==============================
# DEBUG ENDPOINTS (PROTECTED)
# ==============================

@app.route("/debug/historic/latest", methods=["GET"])
def debug_latest_historic():

    if not authorize(request):
        return jsonify({"error": "Unauthorized"}), 403

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT *
        FROM historic_daily_data
        ORDER BY historic_datetime DESC
        LIMIT 10
    """)

    rows = cursor.fetchall()
    conn.close()

    return jsonify([dict(row) for row in rows])


@app.route("/debug/forecast/latest", methods=["GET"])
def debug_latest_forecast():

    if not authorize(request):
        return jsonify({"error": "Unauthorized"}), 403

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT *
        FROM forecasted_hourly_data
        ORDER BY forecasted_datetime DESC
        LIMIT 10
    """)

    rows = cursor.fetchall()
    conn.close()

    return jsonify([dict(row) for row in rows])


@app.route("/debug/counts", methods=["GET"])
def debug_counts():

    if not authorize(request):
        return jsonify({"error": "Unauthorized"}), 403

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT COUNT(*) FROM historic_daily_data")
    historic_count = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM forecasted_hourly_data")
    forecast_count = cursor.fetchone()[0]

    conn.close()

    return jsonify({
        "historic_rows": historic_count,
        "forecast_rows": forecast_count
    })


# ==============================
# RENDER PORT BIND
# ==============================

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)

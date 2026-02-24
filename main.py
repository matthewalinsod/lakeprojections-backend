from flask import Flask, jsonify, request
import sqlite3
import os
import requests
import time
from datetime import datetime, timedelta
from flask import render_template

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
# DATABASE ADD INDEX
# ==============================

def ensure_indexes(conn):
    """
    Create indexes for faster time-series reads.
    Safe to run repeatedly because we use IF NOT EXISTS.
    """
    conn.executescript("""
    -- =========================
    -- HISTORIC DAILY
    -- =========================
    CREATE INDEX IF NOT EXISTS idx_historic_daily_dt
        ON historic_daily_data (historic_datetime);

    CREATE INDEX IF NOT EXISTS idx_historic_daily_sdid_dt
        ON historic_daily_data (sd_id, historic_datetime);

    -- =========================
    -- HISTORIC HOURLY
    -- =========================
    CREATE INDEX IF NOT EXISTS idx_historic_hourly_dt
        ON historic_hourly_data (historic_datetime);

    CREATE INDEX IF NOT EXISTS idx_historic_hourly_sdid_dt
        ON historic_hourly_data (sd_id, historic_datetime);

    -- =========================
    -- FORECASTED DAILY
    -- =========================
    CREATE INDEX IF NOT EXISTS idx_forecasted_daily_dt
        ON forecasted_daily_data (forecasted_datetime);

    CREATE INDEX IF NOT EXISTS idx_forecasted_daily_sdid_dt
        ON forecasted_daily_data (sd_id, forecasted_datetime);

    CREATE INDEX IF NOT EXISTS idx_forecasted_daily_accessed
        ON forecasted_daily_data (datetime_accessed);

    -- =========================
    -- FORECASTED HOURLY
    -- =========================
    CREATE INDEX IF NOT EXISTS idx_forecasted_hourly_dt
        ON forecasted_hourly_data (forecasted_datetime);

    CREATE INDEX IF NOT EXISTS idx_forecasted_hourly_sdid_dt
        ON forecasted_hourly_data (sd_id, forecasted_datetime);

    CREATE INDEX IF NOT EXISTS idx_forecasted_hourly_accessed
        ON forecasted_hourly_data (datetime_accessed);

    -- =========================
    -- FORECASTED 24-MONTH STUDY
    -- =========================
    CREATE INDEX IF NOT EXISTS idx_forecasted_24ms_mrid_sdid_dt
        ON forecasted_24ms_data (mr_id, sd_id, forecasted_datetime);

    CREATE INDEX IF NOT EXISTS idx_forecasted_24ms_dt
        ON forecasted_24ms_data (forecasted_datetime);
    """)

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
def dashboard():
    return render_template("dashboard.html")


@app.route("/health")
def health():
    return "OK", 200

@app.route("/internal/db/indexes", methods=["POST"])
def create_db_indexes():
    if not authorize(request):
        return jsonify({"error": "Unauthorized"}), 403

    t0 = time.time()

    conn = get_db_connection()
    try:
        ensure_indexes(conn)
        conn.commit()
    finally:
        conn.close()

    return jsonify({
        "status": "indexes ensured",
        "elapsed_seconds": round(time.time() - t0, 3)
    })

#API elevation

@app.route("/api/elevation", methods=["GET"])
def api_elevation():
    """
    Returns merged historic + forecast daily elevation for a dam.
    Range options:
      30d (MTD), 90d (3M), 365d (YTD), 5y
    """

    dam = (request.args.get("dam") or "hoover").lower().strip()
    range_key = (request.args.get("range") or "30d").lower().strip()

    dam_to_sdid = {
        "hoover": 1930,
        "davis": 2100,
        "parker": 2101
    }

    range_days = {
        "30d": 30,
        "90d": 90,
        "365d": 365,
        "5y": 1825
    }

    if dam not in dam_to_sdid:
        return jsonify({"error": "Invalid dam"}), 400
    if range_key not in range_days:
        return jsonify({"error": "Invalid range"}), 400

    sd_id = dam_to_sdid[dam]
    days_back = range_days[range_key]

    conn = get_db_connection()
    cursor = conn.cursor()

    # Determine true cutover = latest historic timestamp
    cursor.execute("""
        SELECT MAX(historic_datetime)
        FROM historic_daily_data
        WHERE sd_id = ?
    """, (sd_id,))
    cutover = cursor.fetchone()[0]

    if not cutover:
        conn.close()
        return jsonify({"error": "No historic data found"}), 400

    cutover_dt = datetime.strptime(cutover, "%Y-%m-%dT%H:%M:%S")
    start_dt = cutover_dt - timedelta(days=days_back)

    start_iso = start_dt.strftime("%Y-%m-%dT%H:%M:%S")

    # Historic window
    cursor.execute("""
        SELECT historic_datetime, value
        FROM historic_daily_data
        WHERE sd_id = ?
          AND historic_datetime >= ?
          AND historic_datetime <= ?
        ORDER BY historic_datetime ASC
    """, (sd_id, start_iso, cutover))
    historic_rows = cursor.fetchall()

    last_hist_value = historic_rows[-1]["value"] if historic_rows else None

    # Latest forecast run
    cursor.execute("""
        SELECT MAX(datetime_accessed)
        FROM forecasted_daily_data
        WHERE sd_id = ?
    """, (sd_id,))
    latest_accessed = cursor.fetchone()[0]

    forecast_rows = []
    if latest_accessed:
        cursor.execute("""
            SELECT forecasted_datetime, value
            FROM forecasted_daily_data
            WHERE sd_id = ?
              AND datetime_accessed = ?
              AND forecasted_datetime >= ?
            ORDER BY forecasted_datetime ASC
        """, (sd_id, latest_accessed, cutover))
        forecast_rows = cursor.fetchall()

    conn.close()

    historic = [{"t": r["historic_datetime"], "v": r["value"]} for r in historic_rows]
    forecast = [{"t": r["forecasted_datetime"], "v": r["value"]} for r in forecast_rows]

    return jsonify({
        "dam": dam,
        "range": range_key,
        "cutover": cutover,
        "historic": historic,
        "forecast": forecast,
        "last_historic": {
            "t": cutover,
            "v": last_hist_value
        }
    })

# ==============================
# 24 MONTH STUDY (24MS) API
# ==============================

# Hardcoded SDID map
SDID_MAP = {
    "hoover": {
        "elevation": 1930,
        "release": 1863,
        "energy": 2070
    },
    "davis": {
        "elevation": 2100,
        "release": 2166,
        "energy": 2071
    },
    "parker": {
        "elevation": 2101,
        "release": 2146,
        "energy": 2072
    }
}


# --------------------------------
# Get Available 24MS Months
# --------------------------------
@app.route("/api/24ms/months", methods=["GET"])
def get_24ms_months():

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT DISTINCT
            substr(run_name, 1, instr(run_name, ' 24MS') - 1) AS month_label
        FROM mrid_mapping
        WHERE run_name LIKE '%24MS%'
        ORDER BY month_label
    """)

    rows = cursor.fetchall()
    conn.close()

    months = [row["month_label"] for row in rows if row["month_label"]]

    return jsonify(months)

# --------------------------------
# Get 24MS Data
# --------------------------------
@app.route("/api/24ms", methods=["GET"])
def get_24ms_data():

    dam = request.args.get("dam", "").lower()
    variable = request.args.get("variable", "").lower()
    month = request.args.get("month", "")

    if dam not in SDID_MAP:
        return jsonify({"error": "Invalid dam"}), 400

    if variable not in SDID_MAP[dam]:
        return jsonify({"error": "Invalid variable"}), 400

    if not month:
        return jsonify({"error": "Month required"}), 400

    sd_id = SDID_MAP[dam][variable]

    conn = get_db_connection()
    cursor = conn.cursor()

    # Get MRIDs for selected month
    cursor.execute("""
        SELECT mr_id, run_name
        FROM mrid_mapping
        WHERE run_name LIKE ?
    """, (f"{month} 24MS%",))

    mr_rows = cursor.fetchall()

    if not mr_rows:
        conn.close()
        return jsonify({"error": "No runs found for month"}), 404

    mrid_to_label = {}

    for row in mr_rows:
        run_name = row["run_name"]

        if "Min" in run_name:
            label = "Min"
        elif "Most" in run_name:
            label = "Most"
        elif "Max" in run_name:
            label = "Max"
        else:
            continue

        mrid_to_label[row["mr_id"]] = label

    mr_ids = list(mrid_to_label.keys())

    if not mr_ids:
        conn.close()
        return jsonify({"error": "No valid scenarios found"}), 404

    placeholders = ",".join("?" for _ in mr_ids)

    query = f"""
        SELECT forecasted_datetime, value, mr_id
        FROM forecasted_24ms_data
        WHERE sd_id = ?
        AND mr_id IN ({placeholders})
        ORDER BY forecasted_datetime
    """

    cursor.execute(query, [sd_id] + mr_ids)

    data_rows = cursor.fetchall()
    conn.close()

    # Group data by mr_id
    traces = {}

    for row in data_rows:
        mr_id = row["mr_id"]
        label = mrid_to_label.get(mr_id)

        if not label:
            continue

        if label not in traces:
            traces[label] = []

        traces[label].append([
            row["forecasted_datetime"],
            row["value"]
        ])

    formatted_traces = []

    for label, data in traces.items():
        formatted_traces.append({
            "name": label,
            "data": data
        })

    return jsonify({
        "dam": dam,
        "variable": variable,
        "month": month,
        "traces": formatted_traces
    })

# ==============================
# HISTORIC DAILY UPDATE
# ==============================

@app.route("/internal/update/historic", methods=["POST"])
def update_historic():

    print("=== HISTORIC DAILY UPDATE STARTED ===")

    if not authorize(request):
        return jsonify({"error": "Unauthorized"}), 403

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT MAX(historic_datetime) FROM historic_daily_data")
    max_dt = cursor.fetchone()[0]
    print("Max datetime before update:", max_dt)

    if not max_dt:
        conn.close()
        return jsonify({"error": "No existing historic data found"}), 400

    last_date = datetime.strptime(max_dt[:10], "%Y-%m-%d")
    start_date = last_date + timedelta(days=1)
    yesterday = datetime.utcnow().date() - timedelta(days=1)

    if start_date.date() > yesterday:
        print("Historic daily already up to date.")
        conn.close()
        return jsonify({"status": "No historic update needed"})

    t1 = start_date.strftime("%Y-%m-%dT00:00")
    t2 = (yesterday + timedelta(days=1)).strftime("%Y-%m-%dT00:00")

    print("Requesting range:", t1, "to", t2)

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

    cursor.execute("SELECT MAX(historic_datetime) FROM historic_daily_data")
    new_max = cursor.fetchone()[0]

    print("Inserted:", inserted)
    print("Skipped:", skipped)
    print("Max datetime after update:", new_max)
    print("=== HISTORIC DAILY UPDATE COMPLETE ===")

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

    print("=== HISTORIC HOURLY UPDATE STARTED ===")

    if not authorize(request):
        return jsonify({"error": "Unauthorized"}), 403

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT MAX(historic_datetime) FROM historic_hourly_data")
    max_dt = cursor.fetchone()[0]
    print("Max datetime before update:", max_dt)

    if not max_dt:
        conn.close()
        return jsonify({"error": "No existing historic hourly data found"}), 400

    last_dt = datetime.strptime(max_dt, "%Y-%m-%dT%H:%M:%S")
    start_dt = last_dt + timedelta(hours=1)

    today_utc = datetime.utcnow().date()
    end_dt = datetime.combine(today_utc, datetime.min.time())

    if start_dt >= end_dt:
        print("Historic hourly already up to date.")
        conn.close()
        return jsonify({"status": "No historic hourly update needed"})

    t1 = start_dt.strftime("%Y-%m-%dT%H:%M")
    t2 = end_dt.strftime("%Y-%m-%dT%H:%M")

    print("Requesting range:", t1, "to", t2)

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

    cursor.execute("SELECT MAX(historic_datetime) FROM historic_hourly_data")
    new_max = cursor.fetchone()[0]

    print("Inserted:", inserted)
    print("Skipped:", skipped)
    print("Max datetime after update:", new_max)
    print("=== HISTORIC HOURLY UPDATE COMPLETE ===")

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

    print("=== FORECAST DAILY UPDATE STARTED ===")

    if not authorize(request):
        return jsonify({"error": "Unauthorized"}), 403

    conn = get_db_connection()
    cursor = conn.cursor()

    today = datetime.utcnow()
    start_date = today.strftime("%Y-%m-%dT00:00")
    end_date = (today + timedelta(days=90)).strftime("%Y-%m-%dT00:00")
    datetime_accessed = today.strftime("%Y-%m-%dT%H:%M:%S")

    print("Requesting range:", start_date, "to", end_date)

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

    cursor.execute("SELECT MAX(forecasted_datetime) FROM forecasted_daily_data")
    max_forecast = cursor.fetchone()[0]

    cursor.execute("SELECT MAX(datetime_accessed) FROM forecasted_daily_data")
    max_accessed = cursor.fetchone()[0]

    print("Inserted:", inserted)
    print("Skipped:", skipped)
    print("Max forecasted_datetime:", max_forecast)
    print("Max datetime_accessed:", max_accessed)
    print("=== FORECAST DAILY UPDATE COMPLETE ===")

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

    print("=== FORECAST HOURLY UPDATE STARTED ===")

    if not authorize(request):
        return jsonify({"error": "Unauthorized"}), 403

    conn = get_db_connection()
    cursor = conn.cursor()

    today = datetime.utcnow().date()
    end_date = today + timedelta(days=8)

    t1 = today.strftime("%Y-%m-%dT00:00")
    t2 = end_date.strftime("%Y-%m-%dT00:00")
    now_accessed = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")

    print("Requesting range:", t1, "to", t2)

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

    cursor.execute("SELECT MAX(forecasted_datetime) FROM forecasted_hourly_data")
    max_forecast = cursor.fetchone()[0]

    cursor.execute("SELECT MAX(datetime_accessed) FROM forecasted_hourly_data")
    max_accessed = cursor.fetchone()[0]

    print("Inserted:", inserted)
    print("Skipped:", skipped)
    print("Max forecasted_datetime:", max_forecast)
    print("Max datetime_accessed:", max_accessed)
    print("=== FORECAST HOURLY UPDATE COMPLETE ===")

    conn.close()

    return jsonify({
        "forecast_inserted": inserted,
        "forecast_skipped": skipped,
        "range_start": t1,
        "range_end": t2
    })


# ==============================
# RENDER PORT BIND
# ==============================

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)

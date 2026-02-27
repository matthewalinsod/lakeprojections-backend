from flask import Flask, jsonify, request
import sqlite3
import os
import requests
import time
import re
from datetime import datetime, timedelta
from flask import render_template, abort, redirect, url_for
from zoneinfo import ZoneInfo

app = Flask(__name__)
print("MAIN.PY LOADED")
PRIMARY_DB_PATH = "/data/lakeprojections.db"
TEST_DB_PATH = "/data/old_lakeprojections.db"
LOCAL_PRIMARY_DB_PATH = os.path.join(os.path.dirname(__file__), "data", "lakeprojections.db")
LOCAL_TEST_DB_PATH = os.path.join(os.path.dirname(__file__), "data", "old_lakeprojections.db")


def _resolve_db_path():
    explicit_db_path = os.environ.get("LAKEPROJECTIONS_DB_PATH")
    if explicit_db_path:
        return explicit_db_path

    use_test_db = os.environ.get("LAKEPROJECTIONS_USE_TEST_DB", "").strip().lower()
    if use_test_db in {"1", "true", "yes", "on"}:
        if os.path.exists(TEST_DB_PATH):
            return TEST_DB_PATH
        if os.path.exists(LOCAL_TEST_DB_PATH):
            return LOCAL_TEST_DB_PATH

    if os.path.exists(PRIMARY_DB_PATH):
        return PRIMARY_DB_PATH
    if os.path.exists(LOCAL_PRIMARY_DB_PATH):
        return LOCAL_PRIMARY_DB_PATH

    return PRIMARY_DB_PATH


DB_PATH = _resolve_db_path()
print("DB PATH EXISTS:", os.path.exists(DB_PATH))
UPDATE_TOKEN = os.environ.get("UPDATE_TOKEN")

LAKES = {
    "lake-mead": {"dam": "hoover", "lake_name": "Lake Mead", "dam_name": "Hoover Dam"},
    "lake-mohave": {"dam": "davis", "lake_name": "Lake Mohave", "dam_name": "Davis Dam"},
    "lake-havasu": {"dam": "parker", "lake_name": "Lake Havasu", "dam_name": "Parker Dam"},
}

DAMS = {
    lake_data["dam"]: f"{lake_data['lake_name']} ({lake_data['dam_name']})"
    for lake_data in LAKES.values()
}

DAM_TO_LAKE_SLUG = {lake_data["dam"]: lake_slug for lake_slug, lake_data in LAKES.items()}

SUBPAGES = {
    "overview": "Overview",
    "elevation": "Elevation",
    "releases": "Releases",
    "energy": "Energy",
    "24-month-study": "24-Month Study",
}

LEGACY_SUBPAGES = {
    "study": "24-month-study",
}


def _page_context(page_kind="home", dam=None, subpage=None):
    active_lake = DAM_TO_LAKE_SLUG.get(dam) if dam else None
    weather_city = "Las Vegas"
    if active_lake == "lake-mohave":
        weather_city = "Bullhead City"
    elif active_lake == "lake-havasu":
        weather_city = "Lake Havasu City"

    return {
        "lakes": LAKES,
        "dams": DAMS,
        "subpages": SUBPAGES,
        "active_page": page_kind,
        "active_dam": dam,
        "active_lake": active_lake,
        "active_subpage": subpage,
        "weather_city": weather_city,
    }




def _parse_db_datetime(value):
    if not value:
        return None

    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue

    return datetime.fromisoformat(value)


def _az_today_start_naive():
    """
    Return Arizona "today at 00:00:00" as a naive datetime so it aligns
    with naive DB timestamps stored in local AZ wall time.
    """
    az_now = datetime.now(ZoneInfo("America/Phoenix"))
    return datetime.combine(az_now.date(), datetime.min.time())


def _upsert_historic_value(cursor, table_name, datetime_col, iso_dt, sd_id, value):
    """
    Keep historic rows in sync with BOR source values.
    We update existing points first, then insert when missing.
    """
    cursor.execute(
        f"""
        UPDATE {table_name}
        SET value = ?
        WHERE {datetime_col} = ?
          AND sd_id = ?
        """,
        (value, iso_dt, sd_id),
    )

    if cursor.rowcount:
        return "updated"

    cursor.execute(
        f"""
        INSERT INTO {table_name}
        ({datetime_col}, sd_id, value)
        VALUES (?, ?, ?)
        """,
        (iso_dt, sd_id, value),
    )

    return "inserted"

def _parse_24ms_month_label(month_label):
    """
    Parse a 24MS month label into a datetime for stable chronological sorting.
    Returns None when parsing fails.
    """
    if not month_label:
        return None

    clean_label = month_label.strip()

    known_formats = [
        "%b %Y",   # Jan 2025
        "%B %Y",   # January 2025
        "%Y-%m",   # 2025-01
        "%Y/%m",   # 2025/01
        "%m/%Y",   # 01/2025
    ]

    for fmt in known_formats:
        try:
            return datetime.strptime(clean_label, fmt)
        except ValueError:
            continue

    # Fallback: extract month name + year anywhere in the label.
    match = re.search(r"(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)[a-z]*\s+([12]\d{3})", clean_label, re.IGNORECASE)
    if match:
        month_text = match.group(1).lower()
        month_lookup = {
            "jan": 1, "feb": 2, "mar": 3, "apr": 4,
            "may": 5, "jun": 6, "jul": 7, "aug": 8,
            "sep": 9, "sept": 9, "oct": 10, "nov": 11, "dec": 12
        }
        month_num = month_lookup.get(month_text)
        if month_num:
            return datetime(int(match.group(2)), month_num, 1)

    return None


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
def home():
    return render_template("home.html", **_page_context("home"))


@app.route("/<lake_slug>")
def lake_overview(lake_slug):
    return lake_subpage(lake_slug, "overview")


@app.route("/<lake_slug>/<subpage>")
def lake_subpage(lake_slug, subpage):
    lake_slug = (lake_slug or "").lower().strip()
    subpage = (subpage or "").lower().strip()

    if subpage in LEGACY_SUBPAGES:
        return redirect(
            url_for("lake_subpage", lake_slug=lake_slug, subpage=LEGACY_SUBPAGES[subpage]),
            code=301,
        )

    lake_data = LAKES.get(lake_slug)

    if not lake_data and lake_slug in DAM_TO_LAKE_SLUG and subpage in SUBPAGES:
        return redirect(
            url_for("lake_subpage", lake_slug=DAM_TO_LAKE_SLUG[lake_slug], subpage=subpage),
            code=301,
        )

    if not lake_data or subpage not in SUBPAGES:
        abort(404)

    dam = lake_data["dam"]

    return render_template(
        "dashboard.html",
        dam=dam,
        lake_slug=lake_slug,
        lake_name=lake_data["lake_name"],
        dam_name=lake_data["dam_name"],
        subpage=subpage,
        dam_label=DAMS[dam],
        subpage_label=SUBPAGES[subpage],
        **_page_context("dam", dam=dam, subpage=subpage),
    )
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

    payload = _build_daily_stitched_payload(sd_id, days_back)
    if "error" in payload:
        return jsonify(payload), 400

    return jsonify({
        "dam": dam,
        "range": range_key,
        **payload
    })


def _build_daily_stitched_payload(sd_id, days_back):
    conn = get_db_connection()
    cursor = conn.cursor()

    az_today_start = _az_today_start_naive()
    az_today_start_iso = az_today_start.strftime("%Y-%m-%dT%H:%M:%S")

    cursor.execute("""
        SELECT MAX(historic_datetime)
        FROM historic_daily_data
        WHERE sd_id = ?
          AND historic_datetime < ?
    """, (sd_id, az_today_start_iso))
    cutover = cursor.fetchone()[0]

    if not cutover:
        conn.close()
        return {"error": "No historic data found"}

    cutover_dt = _parse_db_datetime(cutover)
    start_dt = cutover_dt - timedelta(days=days_back)
    start_iso = start_dt.strftime("%Y-%m-%dT%H:%M:%S")

    cursor.execute("""
        SELECT historic_datetime, value
        FROM historic_daily_data
        WHERE sd_id = ?
          AND historic_datetime >= ?
          AND historic_datetime <= ?
          AND historic_datetime < ?
        ORDER BY historic_datetime ASC
    """, (sd_id, start_iso, cutover, az_today_start_iso))
    historic_rows = cursor.fetchall()

    last_year_target_dt = cutover_dt - timedelta(days=365)
    last_year_target_iso = last_year_target_dt.strftime("%Y-%m-%dT%H:%M:%S")
    cursor.execute("""
        SELECT historic_datetime, value
        FROM historic_daily_data
        WHERE sd_id = ?
          AND historic_datetime <= ?
        ORDER BY historic_datetime DESC
        LIMIT 1
    """, (sd_id, last_year_target_iso))
    last_year_row = cursor.fetchone()

    last_hist_value = historic_rows[-1]["value"] if historic_rows else None

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
              AND forecasted_datetime > ?
            ORDER BY forecasted_datetime ASC
        """, (sd_id, latest_accessed, cutover))
        forecast_rows = cursor.fetchall()

    conn.close()

    historic = [{"t": r["historic_datetime"], "v": r["value"]} for r in historic_rows]
    forecast = [{"t": r["forecasted_datetime"], "v": r["value"]} for r in forecast_rows]

    return {
        "cutover": cutover,
        "as_of": latest_accessed,
        "historic": historic,
        "forecast": forecast,
        "last_year_historic": {
            "t": last_year_row["historic_datetime"],
            "v": last_year_row["value"]
        } if last_year_row else None,
        "last_historic": {
            "t": cutover,
            "v": last_hist_value
        }
    }


def _api_daily_metric(metric_name, sd_id):
    range_key = (request.args.get("range") or "30d").lower().strip()
    range_days = {
        "30d": 30,
        "90d": 90,
        "365d": 365,
        "5y": 1825
    }

    if range_key not in range_days:
        return jsonify({"error": "Invalid range"}), 400

    payload = _build_daily_stitched_payload(sd_id, range_days[range_key])
    if "error" in payload:
        return jsonify(payload), 400

    return jsonify({
        "metric": metric_name,
        "data_granularity": "daily",
        "historic_source_table": "historic_daily_data",
        "forecast_source_table": "forecasted_daily_data",
        "range": range_key,
        **payload
    })


@app.route("/api/release/daily", methods=["GET"])
def api_release_daily_by_dam():
    """
    Explicit daily release endpoint by dam.
    This route always reads from historic_daily_data + forecasted_daily_data.
    """

    dam = (request.args.get("dam") or "").lower().strip()
    dam_to_sdid = {
        "hoover": 1863,
        "davis": 2166,
        "parker": 2146
    }

    if dam not in dam_to_sdid:
        return jsonify({"error": "Invalid dam"}), 400

    range_key = (request.args.get("range") or "30d").lower().strip()
    range_days = {
        "30d": 30,
        "90d": 90,
        "365d": 365,
        "5y": 1825
    }

    if range_key not in range_days:
        return jsonify({"error": "Invalid range"}), 400

    payload = _build_daily_stitched_payload(dam_to_sdid[dam], range_days[range_key])
    if "error" in payload:
        return jsonify(payload), 400

    return jsonify({
        "dam": dam,
        "metric": "release",
        "data_granularity": "daily",
        "historic_source_table": "historic_daily_data",
        "forecast_source_table": "forecasted_daily_data",
        "range": range_key,
        **payload
    })


@app.route("/api/lake-mead/releases", methods=["GET"])
def api_lake_mead_releases():
    return _api_daily_metric("release", 1863)


@app.route("/api/lake-mohave/releases", methods=["GET"])
def api_lake_mohave_releases():
    return _api_daily_metric("release", 2166)


@app.route("/api/lake-havasu/releases", methods=["GET"])
def api_lake_havasu_releases():
    return _api_daily_metric("release", 2146)


@app.route("/api/lake-mead/energy", methods=["GET"])
def api_lake_mead_energy():
    return _api_daily_metric("energy", 2070)


# API release hourly for Chart 3 (Davis/Parker)
@app.route("/api/release/hourly/dates", methods=["GET"])
def api_release_hourly_dates():
    dam = (request.args.get("dam") or "").lower().strip()

    dam_to_sdid = {
        "davis": 2166,
        "parker": 2146
    }

    if dam not in dam_to_sdid:
        return jsonify({"error": "Chart 3 is only available for Davis and Parker"}), 400

    sd_id = dam_to_sdid[dam]

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT DISTINCT substr(historic_datetime, 1, 10) AS dt
        FROM historic_hourly_data
        WHERE sd_id = ?
        UNION
        SELECT DISTINCT substr(forecasted_datetime, 1, 10) AS dt
        FROM forecasted_hourly_data
        WHERE sd_id = ?
        ORDER BY dt ASC
    """, (sd_id, sd_id))

    rows = cursor.fetchall()
    conn.close()

    dates = [row["dt"] for row in rows if row["dt"]]

    return jsonify({
        "dam": dam,
        "dates": dates
    })


@app.route("/api/release/hourly", methods=["GET"])
def api_release_hourly():
    dam = (request.args.get("dam") or "").lower().strip()
    selected_date = (request.args.get("date") or "").strip()

    dam_to_sdid = {
        "davis": 2166,
        "parker": 2146
    }

    if dam not in dam_to_sdid:
        return jsonify({"error": "Chart 3 is only available for Davis and Parker"}), 400

    if not re.match(r"^\d{4}-\d{2}-\d{2}$", selected_date):
        return jsonify({"error": "date must be YYYY-MM-DD"}), 400

    sd_id = dam_to_sdid[dam]
    day_start = f"{selected_date}T00:00:00"
    day_end = f"{selected_date}T23:59:59"

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT MAX(datetime_accessed)
        FROM forecasted_hourly_data
        WHERE sd_id = ?
    """, (sd_id,))
    latest_accessed = cursor.fetchone()[0]

    cursor.execute("""
        SELECT historic_datetime, value
        FROM historic_hourly_data
        WHERE sd_id = ?
          AND historic_datetime >= ?
          AND historic_datetime <= ?
        ORDER BY historic_datetime ASC
    """, (sd_id, day_start, day_end))
    historic_rows = cursor.fetchall()

    forecast_rows = []
    if latest_accessed:
        cursor.execute("""
            SELECT forecasted_datetime, value
            FROM forecasted_hourly_data
            WHERE sd_id = ?
              AND datetime_accessed = ?
              AND forecasted_datetime >= ?
              AND forecasted_datetime <= ?
            ORDER BY forecasted_datetime ASC
        """, (sd_id, latest_accessed, day_start, day_end))
        forecast_rows = cursor.fetchall()

    conn.close()

    historic = [
        {
            "t": row["historic_datetime"],
            "hour": int(row["historic_datetime"][11:13]),
            "v": row["value"]
        }
        for row in historic_rows
    ]

    forecast = [
        {
            "t": row["forecasted_datetime"],
            "hour": int(row["forecasted_datetime"][11:13]),
            "v": row["value"]
        }
        for row in forecast_rows
    ]

    return jsonify({
        "dam": dam,
        "date": selected_date,
        "as_of": latest_accessed,
        "historic": historic,
        "forecast": forecast
    })


def _find_existing_column(columns, candidates):
    for candidate in candidates:
        if candidate in columns:
            return candidate
    return None


CHART4_UNIT_CONFIG = {
    "davis": [
        {"sd_id": 14163, "unit": "D1", "unit_number": 1},
        {"sd_id": 14164, "unit": "D2", "unit_number": 2},
        {"sd_id": 14165, "unit": "D3", "unit_number": 3},
        {"sd_id": 14166, "unit": "D4", "unit_number": 4},
        {"sd_id": 14167, "unit": "D5", "unit_number": 5},
    ],
    "parker": [
        {"sd_id": 14168, "unit": "P1", "unit_number": 1},
        {"sd_id": 14169, "unit": "P2", "unit_number": 2},
        {"sd_id": 14170, "unit": "P3", "unit_number": 3},
        {"sd_id": 14171, "unit": "P4", "unit_number": 4},
    ],
}


def _get_energy_unit_rows(cursor, dam):
    fallback_units = [dict(unit) for unit in CHART4_UNIT_CONFIG.get(dam, [])]

    cursor.execute("PRAGMA table_info(sdid_mapping)")
    mapping_columns = [row[1] for row in cursor.fetchall()]

    if not mapping_columns:
        return fallback_units

    id_col = _find_existing_column(mapping_columns, ["sd_id", "sdi", "sdid", "id"])
    label_col = _find_existing_column(mapping_columns, [
        "unit", "unit_name", "label", "name", "series_name", "sdi_name", "description"
    ])
    dam_col = _find_existing_column(mapping_columns, ["dam", "dam_name", "reservoir", "location", "site"])

    if not id_col or not label_col:
        return fallback_units

    dam_prefix = "P" if dam == "parker" else "D"
    dam_name_like = "Parker" if dam == "parker" else "Davis"

    where_clauses = [
        "(" + " OR ".join([
            f"UPPER({label_col}) GLOB ?",
            f"UPPER({label_col}) LIKE ?",
            f"UPPER({label_col}) LIKE ?"
        ]) + ")"
    ]
    params = [
        f"{dam_prefix}[0-9]*",
        f"%{dam_name_like.upper()}%UNIT%",
        f"%{dam_prefix}%UNIT%"
    ]

    if dam_col:
        where_clauses.append(f"UPPER({dam_col}) LIKE ?")
        params.append(f"%{dam_name_like.upper()}%")

    query = f"""
        SELECT {id_col} AS sd_id, {label_col} AS unit_label
        FROM sdid_mapping
        WHERE {' AND '.join(where_clauses)}
    """

    cursor.execute(query, params)
    rows = cursor.fetchall()

    units = []
    for row in rows:
        label = str(row["unit_label"] or "").strip().upper()
        match = re.search(rf"\b{dam_prefix}\s*-?\s*(\d+)\b", label)
        if not match:
            match = re.search(rf"\b{dam_name_like.upper()}\s+UNIT\s*-?\s*(\d+)\b", label)
        if not match:
            continue
        units.append({
            "sd_id": int(row["sd_id"]),
            "unit": f"{dam_prefix}{int(match.group(1))}",
            "unit_number": int(match.group(1))
        })

    if not units:
        return fallback_units

    units.sort(key=lambda u: u["unit_number"])
    return units


@app.route("/api/energy/hourly/units/dates", methods=["GET"])
def api_energy_hourly_unit_dates():
    dam = (request.args.get("dam") or "").lower().strip()

    if dam not in ["davis", "parker"]:
        return jsonify({"error": "Chart 4 is only available for Davis and Parker"}), 400

    conn = get_db_connection()
    cursor = conn.cursor()

    unit_rows = _get_energy_unit_rows(cursor, dam)
    if not unit_rows:
        conn.close()
        return jsonify({"dam": dam, "dates": []})

    sd_ids = [row["sd_id"] for row in unit_rows]
    placeholders = ",".join(["?"] * len(sd_ids))

    cursor.execute(f"""
        SELECT DISTINCT substr(historic_datetime, 1, 10) AS dt
        FROM historic_hourly_data
        WHERE sd_id IN ({placeholders})
        UNION
        SELECT DISTINCT substr(forecasted_datetime, 1, 10) AS dt
        FROM forecasted_hourly_data
        WHERE sd_id IN ({placeholders})
        ORDER BY dt ASC
    """, sd_ids + sd_ids)

    rows = cursor.fetchall()
    conn.close()

    dates = [row["dt"] for row in rows if row["dt"]]

    return jsonify({
        "dam": dam,
        "dates": dates
    })


@app.route("/api/energy/hourly/units", methods=["GET"])
def api_energy_hourly_units():
    dam = (request.args.get("dam") or "").lower().strip()
    selected_date = (request.args.get("date") or "").strip()

    if dam not in ["davis", "parker"]:
        return jsonify({"error": "Chart 4 is only available for Davis and Parker"}), 400

    if not re.match(r"^\d{4}-\d{2}-\d{2}$", selected_date):
        return jsonify({"error": "date must be YYYY-MM-DD"}), 400

    conn = get_db_connection()
    cursor = conn.cursor()

    unit_rows = _get_energy_unit_rows(cursor, dam)
    if not unit_rows:
        conn.close()
        return jsonify({
            "dam": dam,
            "date": selected_date,
            "as_of": None,
            "units": [],
            "historic": [],
            "forecast": []
        })

    sd_ids = [row["sd_id"] for row in unit_rows]
    placeholders = ",".join(["?"] * len(sd_ids))
    day_start = f"{selected_date}T00:00:00"
    day_end = f"{selected_date}T23:59:59"

    cursor.execute(f"""
        SELECT MAX(datetime_accessed)
        FROM forecasted_hourly_data
        WHERE sd_id IN ({placeholders})
    """, sd_ids)
    latest_accessed = cursor.fetchone()[0]

    cursor.execute(f"""
        SELECT sd_id, historic_datetime, value
        FROM historic_hourly_data
        WHERE sd_id IN ({placeholders})
          AND historic_datetime >= ?
          AND historic_datetime <= ?
        ORDER BY sd_id ASC, historic_datetime ASC
    """, sd_ids + [day_start, day_end])
    historic_rows = cursor.fetchall()

    forecast_rows = []
    if latest_accessed:
        cursor.execute(f"""
            SELECT sd_id, forecasted_datetime, value
            FROM forecasted_hourly_data
            WHERE sd_id IN ({placeholders})
              AND datetime_accessed = ?
              AND forecasted_datetime >= ?
              AND forecasted_datetime <= ?
            ORDER BY sd_id ASC, forecasted_datetime ASC
        """, sd_ids + [latest_accessed, day_start, day_end])
        forecast_rows = cursor.fetchall()

    conn.close()

    historic = [
        {
            "sd_id": int(row["sd_id"]),
            "t": row["historic_datetime"],
            "hour": int(row["historic_datetime"][11:13]),
            "v": row["value"]
        }
        for row in historic_rows
    ]

    forecast = [
        {
            "sd_id": int(row["sd_id"]),
            "t": row["forecasted_datetime"],
            "hour": int(row["forecasted_datetime"][11:13]),
            "v": row["value"]
        }
        for row in forecast_rows
    ]

    return jsonify({
        "dam": dam,
        "date": selected_date,
        "as_of": latest_accessed,
        "units": unit_rows,
        "historic": historic,
        "forecast": forecast
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
    """)

    rows = cursor.fetchall()
    conn.close()

    months = [row["month_label"] for row in rows if row["month_label"]]

    # Sort newest -> oldest so Chart #2 defaults to the most recent study.
    months.sort(
        key=lambda label: _parse_24ms_month_label(label) or datetime.min,
        reverse=True
    )

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

    az = ZoneInfo("America/Phoenix")

    conn = get_db_connection()
    cursor = conn.cursor()

    # Arizona time (no DST issues)
    now_az = datetime.now(az)
    today_az = now_az.date()

    end_date_param = (request.args.get("end_date") or "").strip()
    if end_date_param:
        try:
            end_date = datetime.strptime(end_date_param, "%Y-%m-%d").date()
        except ValueError:
            conn.close()
            return jsonify({"error": "Invalid end_date format. Use YYYY-MM-DD."}), 400
    else:
        end_date = today_az - timedelta(days=1)

    # Daily historic must end at yesterday in AZ, never today.
    if end_date >= today_az:
        end_date = today_az - timedelta(days=1)

    # Always fetch last 7 full days
    start_date = end_date - timedelta(days=6)

    t1 = start_date.strftime("%Y-%m-%dT00:00")
    t2 = end_date.strftime("%Y-%m-%dT23:59")

    print("AZ Today:", today_az)
    print("Historic end date:", end_date)
    print("Requesting range:", t1, "to", t2)

    api_url = (
        "https://www.usbr.gov/pn-bin/hdb/hdb.pl"
        f"?svr=lchdb"
        f"&sdi=1930%2C1863%2C2070%2C2100%2C2166%2C2071%2C2101%2C2146%2C2072"
        f"&tstp=DY"
        f"&t1={t1}"
        f"&t2={t2}"
        f"&table=R"
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

    inserted = 0
    updated = 0
    skipped = 0

    for series in data.get("Series", []):
        sd_id = int(series["SDI"])
        for point in series.get("Data", []):
            try:
                dt = datetime.strptime(point["t"], "%m/%d/%Y %I:%M:%S %p")
                iso_dt = dt.strftime("%Y-%m-%dT%H:%M:%S")
                raw_value = point.get("v")
                if raw_value in (None, ""):
                    raise ValueError("blank value")
                value = float(raw_value)

                action = _upsert_historic_value(
                    cursor,
                    "historic_daily_data",
                    "historic_datetime",
                    iso_dt,
                    sd_id,
                    value,
                )

                if action == "inserted":
                    inserted += 1
                else:
                    updated += 1

            except Exception as e:
                print("Row skipped due to error:", e)
                skipped += 1

    conn.commit()

    cursor.execute("SELECT MAX(historic_datetime) FROM historic_daily_data")
    new_max = cursor.fetchone()[0]

    print("Inserted:", inserted)
    print("Updated:", updated)
    print("Skipped:", skipped)
    print("Max datetime after update:", new_max)
    print("=== HISTORIC DAILY UPDATE COMPLETE ===")

    conn.close()

    return jsonify({
        "historic_inserted": inserted,
        "historic_updated": updated,
        "historic_skipped": skipped,
        "range_start": t1,
        "range_end": t2
    })

# ==============================
# HISTORIC HOURLY UPDATE
# ==============================

@app.route("/internal/update/historic/daily/requery-7d", methods=["POST"])
def requery_historic_daily_7d():

    print("=== HISTORIC DAILY 7D REQUERY STARTED ===")

    if not authorize(request):
        return jsonify({"error": "Unauthorized"}), 403

    az = ZoneInfo("America/Phoenix")

    conn = get_db_connection()
    cursor = conn.cursor()

    now_az = datetime.now(az)
    today_az = now_az.date()
    yesterday = today_az - timedelta(days=1)
    start_date = yesterday - timedelta(days=6)

    delete_start_iso = start_date.strftime("%Y-%m-%dT00:00:00")
    # Also clear any stray "today" rows so graph stitching cannot pick them up.
    delete_end_iso = today_az.strftime("%Y-%m-%dT23:59:59")

    t1 = start_date.strftime("%Y-%m-%dT00:00")
    # Use an explicit end-of-day upper bound to avoid requesting today's daily row.
    t2 = yesterday.strftime("%Y-%m-%dT23:59")

    print("AZ Today:", today_az)
    print("Deleting range:", delete_start_iso, "to", delete_end_iso)
    print("Requesting range:", t1, "to", t2)

    cursor.execute(
        """
        DELETE FROM historic_daily_data
        WHERE historic_datetime >= ?
          AND historic_datetime <= ?
        """,
        (delete_start_iso, delete_end_iso),
    )
    deleted = cursor.rowcount

    api_url = (
        "https://www.usbr.gov/pn-bin/hdb/hdb.pl"
        f"?svr=lchdb"
        f"&sdi=1930%2C1863%2C2070%2C2100%2C2166%2C2071%2C2101%2C2146%2C2072"
        f"&tstp=DY"
        f"&t1={t1}"
        f"&t2={t2}"
        f"&table=R"
        f"&mrid=4"
        f"&format=json"
    )

    try:
        response = requests.get(api_url, timeout=60)
        response.raise_for_status()
        data = response.json()
    except Exception as e:
        conn.rollback()
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
                raw_value = point.get("v")
                if raw_value in (None, ""):
                    raise ValueError("blank value")
                value = float(raw_value)

                cursor.execute(
                    """
                    INSERT INTO historic_daily_data
                    (historic_datetime, sd_id, value)
                    VALUES (?, ?, ?)
                    """,
                    (iso_dt, sd_id, value),
                )
                inserted += 1
            except Exception as e:
                print("Row skipped due to error:", e)
                skipped += 1

    conn.commit()

    print("Deleted:", deleted)
    print("Inserted:", inserted)
    print("Skipped:", skipped)
    print("=== HISTORIC DAILY 7D REQUERY COMPLETE ===")

    conn.close()

    return jsonify({
        "historic_daily_deleted": deleted,
        "historic_daily_inserted": inserted,
        "historic_daily_skipped": skipped,
        "range_start": t1,
        "range_end": t2,
    })


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

    last_dt = _parse_db_datetime(max_dt)
    start_dt = last_dt + timedelta(hours=1)

    end_date_param = (request.args.get("end_date") or "").strip()
    if end_date_param:
        try:
            end_date = datetime.strptime(end_date_param, "%Y-%m-%d").date()
        except ValueError:
            conn.close()
            return jsonify({"error": "Invalid end_date format. Use YYYY-MM-DD."}), 400
    else:
        end_date = datetime.utcnow().date() - timedelta(days=1)

    end_dt = datetime.combine(end_date + timedelta(days=1), datetime.min.time())

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
    updated = 0
    skipped = 0

    for series in data.get("Series", []):
        sd_id = int(series["SDI"])
        for point in series.get("Data", []):
            try:
                dt = datetime.strptime(point["t"], "%m/%d/%Y %I:%M:%S %p")
                iso_dt = dt.strftime("%Y-%m-%dT%H:%M:%S")
                value = float(point["v"])

                action = _upsert_historic_value(
                    cursor,
                    "historic_hourly_data",
                    "historic_datetime",
                    iso_dt,
                    sd_id,
                    value,
                )

                if action == "inserted":
                    inserted += 1
                else:
                    updated += 1
            except Exception:
                skipped += 1

    conn.commit()

    cursor.execute("SELECT MAX(historic_datetime) FROM historic_hourly_data")
    new_max = cursor.fetchone()[0]

    print("Inserted:", inserted)
    print("Updated:", updated)
    print("Skipped:", skipped)
    print("Max datetime after update:", new_max)
    print("=== HISTORIC HOURLY UPDATE COMPLETE ===")

    conn.close()

    return jsonify({
        "historic_hourly_inserted": inserted,
        "historic_hourly_updated": updated,
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

#debug section
@app.route("/debug/sql", methods=["POST"])
def debug_sql():

    if not authorize(request):
        return jsonify({"error": "Unauthorized"}), 403

    data = request.get_json()
    if not data or "query" not in data:
        return jsonify({"error": "Provide SQL query"}), 400

    query = data["query"].strip().lower()

    # Only allow SELECT statements
    if not query.startswith("select"):
        return jsonify({"error": "Only SELECT queries allowed"}), 400

    # Block dangerous keywords
    forbidden = ["drop", "delete", "update", "insert", "alter", "pragma", "attach"]
    if any(word in query for word in forbidden):
        return jsonify({"error": "Forbidden SQL keyword detected"}), 400

    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(data["query"])
        rows = cursor.fetchall()
        conn.close()

        return jsonify([dict(row) for row in rows])

    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ==============================
# RENDER PORT BIND
# ==============================

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)

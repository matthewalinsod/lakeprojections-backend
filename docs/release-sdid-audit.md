# Release SDID Audit (Daily + Hourly Release Graphs)

## Confirmed release SDIDs used by the app

### Daily release graph (`{{ lake_name }} Releases (Historic + Forecast)`)
- **Mead Release** uses **SDID 1863** via `GET /api/lake-mead/releases`.
- **Mohave Release** uses **SDID 2166** via `GET /api/lake-mohave/releases`.
- **Havasu Release** uses **SDID 2146** via `GET /api/lake-havasu/releases`.

This is wired in Flask routes:
- `api_lake_mead_releases() -> _api_daily_metric("release", 1863)`
- `api_lake_mohave_releases() -> _api_daily_metric("release", 2166)`
- `api_lake_havasu_releases() -> _api_daily_metric("release", 2146)`

And the front end selects those endpoints by dam:
- `hoover -> lake-mead`
- `davis -> lake-mohave`
- `parker -> lake-havasu`


### Explicit daily release endpoint
- `GET /api/release/daily?dam=hoover|davis|parker&range=30d|90d|365d|5y`
- Response includes metadata proving source tables:
  - `"data_granularity": "daily"`
  - `"historic_source_table": "historic_daily_data"`
  - `"forecast_source_table": "forecasted_daily_data"`

### Hourly release graph (`Release by Hour (CFS)` for Davis/Parker)
- **Davis (Lake Mohave)** uses **SDID 2166**.
- **Parker (Lake Havasu)** uses **SDID 2146**.

The hourly endpoints use the same two SDIDs in `dam_to_sdid`:
- `GET /api/release/hourly/dates?dam=davis|parker`
- `GET /api/release/hourly?dam=davis|parker&date=YYYY-MM-DD`

## Database verification result (local test DB)

In `data/old_lakeprojections.db`, `sdid_mapping` contains:
- `(1863, ..., 'Mead Release')`
- `(2166, ..., 'Mohave Release')`
- `(2146, ..., 'Havasu Release')`

So the app’s hard-coded SDIDs align with the mapping table labels.

## SQL checks you can run against your production DB

```sql
-- 1) Verify SDID labels
SELECT sd_id, measure_id, reservoir_id, sdid_name
FROM sdid_mapping
WHERE sd_id IN (1863, 2166, 2146)
ORDER BY sd_id;

-- 2) Check recent historic daily datapoints per SDID
SELECT sd_id,
       COUNT(*) AS row_count,
       MIN(historic_datetime) AS min_dt,
       MAX(historic_datetime) AS max_dt
FROM historic_daily_data
WHERE sd_id IN (1863, 2166, 2146)
GROUP BY sd_id
ORDER BY sd_id;

-- 3) Check latest forecast snapshot coverage per SDID
WITH latest AS (
  SELECT sd_id, MAX(datetime_accessed) AS latest_accessed
  FROM forecasted_daily_data
  WHERE sd_id IN (1863, 2166, 2146)
  GROUP BY sd_id
)
SELECT f.sd_id,
       l.latest_accessed,
       COUNT(*) AS forecast_rows,
       MIN(f.forecasted_datetime) AS min_forecast_dt,
       MAX(f.forecasted_datetime) AS max_forecast_dt
FROM forecasted_daily_data f
JOIN latest l
  ON l.sd_id = f.sd_id
 AND l.latest_accessed = f.datetime_accessed
GROUP BY f.sd_id, l.latest_accessed
ORDER BY f.sd_id;
```

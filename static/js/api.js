async function fetchElevationSeries(dam, range) {
  const url = `/api/elevation?dam=${encodeURIComponent(dam)}&range=${encodeURIComponent(range)}`;
  const res = await fetch(url);
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`API error ${res.status}: ${text}`);
  }
  return await res.json();
}

async function fetchLakeMeadReleaseSeries(range) {
  const url = `/api/lake-mead/releases?range=${encodeURIComponent(range)}`;
  const res = await fetch(url);
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`API error ${res.status}: ${text}`);
  }
  return await res.json();
}

async function fetchLakeMeadEnergySeries(range) {
  const url = `/api/lake-mead/energy?range=${encodeURIComponent(range)}`;
  const res = await fetch(url);
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`API error ${res.status}: ${text}`);
  }
  return await res.json();
}


async function fetchReleaseHourlyDates(dam) {
  const url = `/api/release/hourly/dates?dam=${encodeURIComponent(dam)}`;
  const res = await fetch(url);
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`API error ${res.status}: ${text}`);
  }
  return await res.json();
}

async function fetchReleaseHourlySeries(dam, date) {
  const url = `/api/release/hourly?dam=${encodeURIComponent(dam)}&date=${encodeURIComponent(date)}`;
  const res = await fetch(url);
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`API error ${res.status}: ${text}`);
  }
  return await res.json();
}

async function fetchEnergyUnitHourlyDates(dam) {
  const url = `/api/energy/hourly/units/dates?dam=${encodeURIComponent(dam)}`;
  const res = await fetch(url);
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`API error ${res.status}: ${text}`);
  }
  return await res.json();
}

async function fetchEnergyUnitHourlySeries(dam, date) {
  const url = `/api/energy/hourly/units?dam=${encodeURIComponent(dam)}&date=${encodeURIComponent(date)}`;
  const res = await fetch(url);
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`API error ${res.status}: ${text}`);
  }
  return await res.json();
}

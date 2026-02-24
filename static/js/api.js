async function fetchElevationSeries(dam, range) {
  const url = `/api/elevation?dam=${encodeURIComponent(dam)}&range=${encodeURIComponent(range)}`;
  const res = await fetch(url);
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`API error ${res.status}: ${text}`);
  }
  return await res.json();
}

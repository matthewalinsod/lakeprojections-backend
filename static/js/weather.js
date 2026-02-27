document.addEventListener("DOMContentLoaded", async () => {
  const widget = document.getElementById("weather-widget");
  if (!widget) return;

  const city = widget.dataset.city;
  const condition = widget.querySelector(".weather-condition");
  const icon = widget.querySelector(".weather-icon");
  const metrics = widget.querySelector(".weather-metrics");
  if (!city || !condition || !icon || !metrics) return;

  const knownCoordinates = {
    "Las Vegas": { latitude: 36.1716, longitude: -115.1391 },
    "Bullhead City": { latitude: 35.1478, longitude: -114.5683 },
    "Lake Havasu City": { latitude: 34.4839, longitude: -114.3225 },
  };

  const weatherCodeMap = {
    0: { label: "Clear", icon: "☀️" },
    1: { label: "Mostly clear", icon: "🌤️" },
    2: { label: "Partly cloudy", icon: "⛅" },
    3: { label: "Overcast", icon: "☁️" },
    45: { label: "Fog", icon: "🌫️" },
    48: { label: "Rime fog", icon: "🌫️" },
    51: { label: "Light drizzle", icon: "🌦️" },
    53: { label: "Drizzle", icon: "🌦️" },
    55: { label: "Heavy drizzle", icon: "🌧️" },
    56: { label: "Freezing drizzle", icon: "🌧️" },
    57: { label: "Heavy freezing drizzle", icon: "🌧️" },
    61: { label: "Light rain", icon: "🌦️" },
    63: { label: "Rain", icon: "🌧️" },
    65: { label: "Heavy rain", icon: "🌧️" },
    66: { label: "Freezing rain", icon: "🌧️" },
    67: { label: "Heavy freezing rain", icon: "🌧️" },
    71: { label: "Light snow", icon: "🌨️" },
    73: { label: "Snow", icon: "🌨️" },
    75: { label: "Heavy snow", icon: "❄️" },
    77: { label: "Snow grains", icon: "❄️" },
    80: { label: "Light rain showers", icon: "🌦️" },
    81: { label: "Rain showers", icon: "🌧️" },
    82: { label: "Heavy rain showers", icon: "⛈️" },
    85: { label: "Light snow showers", icon: "🌨️" },
    86: { label: "Snow showers", icon: "🌨️" },
    95: { label: "Thunderstorm", icon: "⛈️" },
    96: { label: "Thunderstorm + hail", icon: "⛈️" },
    99: { label: "Strong thunderstorm", icon: "⛈️" },
  };

  const formatNumber = (value, suffix = "") => {
    if (value === null || value === undefined || Number.isNaN(Number(value))) return "--";
    return `${Math.round(Number(value))}${suffix}`;
  };

  try {
    let place = knownCoordinates[city];

    if (!place) {
      const geoResp = await fetch(
        `https://geocoding-api.open-meteo.com/v1/search?name=${encodeURIComponent(city)}&count=1&language=en&format=json`
      );
      if (!geoResp.ok) throw new Error("Geocoding request failed");

      const geoData = await geoResp.json();
      place = geoData?.results?.[0];
      if (!place) throw new Error("No geocoding result");
    }

    const weatherResp = await fetch(
      `https://api.open-meteo.com/v1/forecast?latitude=${place.latitude}&longitude=${place.longitude}&current=temperature_2m,apparent_temperature,relative_humidity_2m,precipitation,weather_code,wind_speed_10m&temperature_unit=fahrenheit&wind_speed_unit=mph&precipitation_unit=inch`
    );
    if (!weatherResp.ok) throw new Error("Weather request failed");

    const weatherData = await weatherResp.json();
    const current = weatherData?.current;
    if (!current) throw new Error("No weather data");

    const weatherCode = Number(current.weather_code);
    const descriptor = weatherCodeMap[weatherCode] || { label: "Current conditions", icon: "🌡️" };

    icon.textContent = descriptor.icon;
    condition.textContent = `${descriptor.label} ${formatNumber(current.temperature_2m, "°F")}`;
    metrics.textContent = [
      `Feels ${formatNumber(current.apparent_temperature, "°F")}`,
      `Humidity ${formatNumber(current.relative_humidity_2m, "%")}`,
      `Wind ${formatNumber(current.wind_speed_10m, " mph")}`,
      `Rain ${Number(current.precipitation || 0).toFixed(2)} in`,
    ].join(" • ");
  } catch (err) {
    icon.textContent = "⚠️";
    condition.textContent = "Weather unavailable";
    metrics.textContent = "Unable to load current conditions.";
  }
});

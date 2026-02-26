document.addEventListener("DOMContentLoaded", async () => {
  const widget = document.getElementById("weather-widget");
  if (!widget) return;

  const city = widget.dataset.city;
  const status = widget.querySelector(".weather-status");
  if (!city || !status) return;

  try {
    const geoResp = await fetch(
      `https://geocoding-api.open-meteo.com/v1/search?name=${encodeURIComponent(city)}&count=1&language=en&format=json`
    );
    if (!geoResp.ok) throw new Error("Geocoding request failed");

    const geoData = await geoResp.json();
    const place = geoData?.results?.[0];
    if (!place) throw new Error("No geocoding result");

    const weatherResp = await fetch(
      `https://api.open-meteo.com/v1/forecast?latitude=${place.latitude}&longitude=${place.longitude}&current=temperature_2m,weather_code&temperature_unit=fahrenheit`
    );
    if (!weatherResp.ok) throw new Error("Weather request failed");

    const weatherData = await weatherResp.json();
    const current = weatherData?.current;
    if (!current) throw new Error("No weather data");

    const temp = Math.round(current.temperature_2m);
    status.textContent = `${temp}°F`;
  } catch (err) {
    status.textContent = "Weather unavailable";
  }
});

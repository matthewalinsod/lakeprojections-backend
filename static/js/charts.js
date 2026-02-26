let chart24msInstance = null;
let chartReleaseHourlyInstance = null;
let releaseAvailableDates = [];
let releaseDateSet = new Set();
let energyUnitAvailableDates = [];
let energyUnitDateSet = new Set();
let chartEnergyUnitHourlyInstance = null;
let stitchedChartInstances = {};

const HISTORIC_SERIES_COLOR = "#1f78ff";
const FORECAST_SERIES_COLOR = "#2e8b57";
const HISTORIC_AREA_COLOR = "rgba(31, 120, 255, 0.35)";
const FORECAST_AREA_COLOR = "rgba(46, 139, 87, 0.30)";

// ==============================
// WAIT UNTIL DOM IS READY
// ==============================

document.addEventListener("DOMContentLoaded", function () {
  initialize24MS();
});

// ==============================
// UTILITIES
// ==============================

function isoToMs(iso) {
  return new Date(iso).getTime();
}

function buildSeriesPoints(rows) {
  return rows
    .filter(r => r.t && r.v !== null && r.v !== undefined)
    .map(r => [isoToMs(r.t), Number(r.v)]);
}

function roundToDecimals(value, decimals) {
  const factor = 10 ** decimals;
  return Math.round(Number(value) * factor) / factor;
}

function roundByVariable(value, variable) {
  if (variable === "elevation") {
    return roundToDecimals(value, 2);
  }

  // Release and energy should render as whole numbers.
  return Math.round(Number(value));
}

function get24MSTraceStyle(traceName) {
  const colorMap = {
    Min: "#ff0000",
    Most: "#008000",
    Max: "#0000ff"
  };

  const color = colorMap[traceName] || "#5470c6";
  return {
    lineStyle: { width: 2, color },
    itemStyle: { color }
  };
}

function getActiveDam() {
  return document.body?.dataset?.dam || "hoover";
}

// ==============================
// GRAPH 1 — Elevation
// ==============================

function renderStitchedDailyChart({
  containerId,
  payload,
  yAxisName = "",
  messageId,
  asOfLabelPrefix = "As of"
}) {
  const el = document.getElementById(containerId);
  if (!el || !payload) return;

  if (!stitchedChartInstances[containerId]) {
    stitchedChartInstances[containerId] = echarts.init(el);
    window.addEventListener("resize", () => stitchedChartInstances[containerId].resize());
  }

  const chart = stitchedChartInstances[containerId];
  const historic = buildSeriesPoints(payload.historic || []);
  const cutoverMs = isoToMs(payload.cutover);

  let forecast = buildSeriesPoints(payload.forecast || []);
  forecast = forecast.filter(point => point[0] > cutoverMs);

  if (
    payload.last_historic &&
    payload.last_historic.t &&
    payload.last_historic.v !== null
  ) {
    const stitchPoint = [
      isoToMs(payload.last_historic.t),
      Number(payload.last_historic.v)
    ];

    if (!forecast.length || forecast[0][0] !== stitchPoint[0]) {
      forecast = [stitchPoint, ...forecast];
    }
  }

  chart.setOption({
    animation: false,
    grid: { left: 60, right: 20, top: 50, bottom: 40 },
    tooltip: { trigger: "axis", axisPointer: { type: "cross" } },
    legend: { top: 10 },
    xAxis: {
      type: "time",
      axisLabel: {
        hideOverlap: true,
        formatter: function (value) {
          const d = new Date(value);
          return `${d.getMonth() + 1}/${d.getDate()}/${d.getFullYear()}`;
        }
      },
      splitNumber: 6
    },
    yAxis: { type: "value", scale: true, name: yAxisName },
    series: [
      {
        name: "Historic",
        type: "line",
        showSymbol: false,
        data: historic,
        lineStyle: { width: 2, color: HISTORIC_SERIES_COLOR },
        itemStyle: { color: HISTORIC_SERIES_COLOR },
        areaStyle: {
          color: HISTORIC_AREA_COLOR
        }
      },
      {
        name: "Forecast",
        type: "line",
        showSymbol: false,
        data: forecast,
        lineStyle: { width: 2, type: "dashed", color: FORECAST_SERIES_COLOR },
        itemStyle: { color: FORECAST_SERIES_COLOR },
        areaStyle: {
          color: FORECAST_AREA_COLOR
        }
      }
    ]
  }, true);

  drawTodayLine(chart, cutoverMs);

  const note = document.getElementById(messageId);
  if (note) {
    note.textContent = `${asOfLabelPrefix} ${formatAsOfDateTime(payload.as_of)}.`;
  }
}

function renderElevationChart(containerId, payload) {
  renderStitchedDailyChart({
    containerId,
    payload,
    messageId: "g1-message",
    asOfLabelPrefix: "As of"
  });
}

function drawTodayLine(chartInstance, cutoverMs) {

  if (!chartInstance) return;

  const xPixel = chartInstance.convertToPixel(
    { xAxisIndex: 0 },
    cutoverMs
  );

  const grid = chartInstance
    .getModel()
    .getComponent("grid")
    .coordinateSystem.getRect();

  chartInstance.setOption({
    graphic: [
      {
        type: "line",
        shape: {
          x1: xPixel,
          y1: grid.y,
          x2: xPixel,
          y2: grid.y + grid.height
        },
        style: { stroke: "#000", lineWidth: 2 },
        silent: true
      },
      {
        type: "text",
        left: xPixel - 70,
        top: grid.y - 10,
        style: {
          text: "HISTORIC",
          fill: HISTORIC_SERIES_COLOR,
          font: "bold 12px Arial",
          textAlign: "right",
          textVerticalAlign: "bottom"
        },
        silent: true
      },
      {
        type: "text",
        left: xPixel + 8,
        top: grid.y - 10,
        style: {
          text: "FORECASTED",
          fill: FORECAST_SERIES_COLOR,
          font: "bold 12px Arial",
          textAlign: "left",
          textVerticalAlign: "bottom"
        },
        silent: true
      }
    ]
  });
}


function renderLakeMeadReleaseChart(payload) {
  renderStitchedDailyChart({
    containerId: "chartLakeMeadRelease",
    payload,
    yAxisName: "cfs",
    messageId: "g3-mead-message",
    asOfLabelPrefix: "As of"
  });
}

function renderLakeMeadEnergyChart(payload) {
  renderStitchedDailyChart({
    containerId: "chartLakeMeadEnergy",
    payload,
    yAxisName: "MWh",
    messageId: "g4-mead-message",
    asOfLabelPrefix: "As of"
  });
}

// ==============================
// GRAPH 2 — 24MS
// ==============================

function getSdIdForDam(dam, variable) {

  const map = {
    hoover: { elevation: 1930, release: 1863, energy: 2070 },
    davis: { elevation: 2100, release: 2166, energy: 2071 },
    parker: { elevation: 2101, release: 2146, energy: 2072 }
  };

  return map[dam][variable];
}

async function initialize24MS() {

  const monthSelect = document.getElementById("g2-month");
  const variableSelect = document.getElementById("g2-variable");

  if (!monthSelect || !variableSelect) return;

  try {
    const response = await fetch("/api/24ms/months");
    const months = await response.json();

    if (!months || months.length === 0) return;

    // Defensive client-side sort in case API returns unexpected ordering.
    months.sort((a, b) => parse24MSMonthLabel(b) - parse24MSMonthLabel(a));

    monthSelect.innerHTML = "";

    months.forEach(month => {
      const option = document.createElement("option");
      option.value = month;
      option.textContent = month;
      monthSelect.appendChild(option);
    });

    monthSelect.value = months[0];

    await load24MSData(months[0]);

    variableSelect.addEventListener("change", () => {
      load24MSData(monthSelect.value);
    });

    monthSelect.addEventListener("change", () => {
      load24MSData(monthSelect.value);
    });

  } catch (err) {
    console.error("24MS initialization failed:", err);
  }
}

function parse24MSMonthLabel(label) {

  if (!label) return Number.NEGATIVE_INFINITY;

  const normalizedLabel = String(label).trim();
  const parsed = Date.parse(`${normalizedLabel} 1`);

  if (!Number.isNaN(parsed)) {
    return parsed;
  }

  const fallback = Date.parse(normalizedLabel);
  return Number.isNaN(fallback) ? Number.NEGATIVE_INFINITY : fallback;
}

async function load24MSData(month) {

  const dam = getActiveDam();
  const variable = document.getElementById("g2-variable").value;

  const url = `/api/24ms?dam=${dam}&variable=${variable}&month=${encodeURIComponent(month)}`;

  const response = await fetch(url);
  const payload = await response.json();

  if (!payload.traces) return;

  if (!chart24msInstance) {
    chart24msInstance = echarts.init(
      document.getElementById("chart24ms")
    );
    window.addEventListener("resize", () => chart24msInstance.resize());
  }

  const series = payload.traces.map(trace => {
    const styledTrace = get24MSTraceStyle(trace.name);
    const roundedData = (trace.data || []).map(point => [
      point[0],
      roundByVariable(point[1], variable)
    ]);

    return {
      name: trace.name,
      type: "line",
      smooth: true,
      showSymbol: false,
      data: roundedData,
      lineStyle: styledTrace.lineStyle,
      itemStyle: styledTrace.itemStyle
    };
  });

  chart24msInstance.setOption({
    animation: false,
    tooltip: {
      trigger: "axis",
      valueFormatter: (value) => {
        if (variable === "elevation") {
          return Number(value).toFixed(2);
        }
        return `${Math.round(Number(value))}`;
      }
    },
    legend: { top: 10 },
    xAxis: { type: "time" },
    yAxis: { type: "value", scale: true },
    series: series
  }, true);
}

// ==============================
// GRAPH 3 — Hourly Release (Davis/Parker)
// ==============================

function formatHourLabel(hour) {
  return String(hour);
}

function normalizeDateInput(value) {
  if (!value) return "";
  return String(value).slice(0, 10);
}

function setReleaseMessage(message) {
  const note = document.getElementById("g3-message");
  if (note) note.textContent = message || "";
}

function formatAsOfDateTime(value) {
  if (!value) return "Unknown";

  const normalized = String(value).includes("T") ? String(value) : String(value).replace(" ", "T");
  const parsed = new Date(normalized);

  if (Number.isNaN(parsed.getTime())) {
    return value;
  }

  return parsed.toLocaleString(undefined, {
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false
  });
}

function formatDateWithDay(value) {
  if (!value) return "Unknown date";

  const parsed = new Date(String(value));
  if (Number.isNaN(parsed.getTime())) {
    return value;
  }

  return parsed.toLocaleDateString(undefined, {
    weekday: "long",
    year: "numeric",
    month: "2-digit",
    day: "2-digit"
  });
}

function setElevationMessage(message) {
  const note = document.getElementById("g1-message");
  if (note) note.textContent = message || "";
}

function setReleaseNavButtonState(date) {
  const prevButton = document.getElementById("g3-prev-day");
  const nextButton = document.getElementById("g3-next-day");

  if (!prevButton || !nextButton) return;

  if (!releaseAvailableDates.length) {
    prevButton.disabled = true;
    nextButton.disabled = true;
    return;
  }

  const currentIndex = releaseAvailableDates.indexOf(date);

  if (currentIndex === -1) {
    prevButton.disabled = true;
    nextButton.disabled = true;
    return;
  }

  prevButton.disabled = currentIndex <= 0;
  nextButton.disabled = currentIndex >= releaseAvailableDates.length - 1;
}

function updateReleaseDateInputState(dateInput, enabled) {
  if (!dateInput) return;
  dateInput.disabled = !enabled;

  if (!enabled) {
    dateInput.removeAttribute("min");
    dateInput.removeAttribute("max");
    setReleaseNavButtonState("");
    return;
  }

  if (releaseAvailableDates.length) {
    dateInput.min = releaseAvailableDates[0];
    dateInput.max = releaseAvailableDates[releaseAvailableDates.length - 1];
  }

  setReleaseNavButtonState(normalizeDateInput(dateInput.value));
}

function renderReleaseHourlyChart(payload) {
  const container = document.getElementById("chartReleaseHourly");
  if (!container || !payload) return;

  if (!chartReleaseHourlyInstance) {
    chartReleaseHourlyInstance = echarts.init(container);
    window.addEventListener("resize", () => chartReleaseHourlyInstance.resize());
  }

  const hours = Array.from({ length: 24 }, (_, idx) => idx);
  const hourLabels = hours.map(formatHourLabel);

  const historicMap = new Map((payload.historic || []).map(row => [Number(row.hour), Math.round(Number(row.v))]));
  const forecastMap = new Map((payload.forecast || []).map(row => [Number(row.hour), Math.round(Number(row.v))]));

  const historicData = hours.map(hour => historicMap.has(hour) ? historicMap.get(hour) : null);
  const forecastData = hours.map(hour => forecastMap.has(hour) ? forecastMap.get(hour) : null);

  chartReleaseHourlyInstance.setOption({
    animation: false,
    tooltip: {
      trigger: "axis",
      axisPointer: { type: "shadow" },
      valueFormatter: (value) => value === null || value === undefined ? "No data" : `${Math.round(Number(value))} cfs`
    },
    legend: { top: 10 },
    grid: { left: 60, right: 20, top: 50, bottom: 45 },
    xAxis: {
      type: "category",
      name: "Hour Start",
      nameLocation: "middle",
      nameGap: 30,
      data: hourLabels
    },
    yAxis: {
      type: "value",
      name: "cfs"
    },
    series: [
      {
        name: "Historic",
        type: "bar",
        data: historicData,
        barCategoryGap: "0%",
        barGap: "-100%",
        itemStyle: { color: HISTORIC_AREA_COLOR },
        emphasis: { focus: "series" }
      },
      {
        name: "Forecast",
        type: "bar",
        data: forecastData,
        barCategoryGap: "0%",
        barGap: "-100%",
        itemStyle: { color: FORECAST_AREA_COLOR },
        emphasis: { focus: "series" }
      }
    ]
  }, true);
}

async function loadReleaseHourlyDataForDate(dam, date) {
  const payload = await fetchReleaseHourlySeries(dam, date);
  renderReleaseHourlyChart(payload);

  const dateInput = document.getElementById("g3-date");
  if (dateInput) {
    dateInput.value = payload.date;
  }

  setReleaseNavButtonState(payload.date);

  const formattedDam = dam.charAt(0).toUpperCase() + dam.slice(1);
  setReleaseMessage(`Showing ${formattedDam} release for ${formatDateWithDay(payload.date)}. As of ${formatAsOfDateTime(payload.as_of)}.`);
}

async function initializeReleaseHourlyChart(dam) {
  const dateInput = document.getElementById("g3-date");
  const prevButton = document.getElementById("g3-prev-day");
  const nextButton = document.getElementById("g3-next-day");
  const container = document.getElementById("chartReleaseHourly");
  if (!dateInput || !container || !prevButton || !nextButton) return;

  if (!["davis", "parker"].includes(dam)) {
    updateReleaseDateInputState(dateInput, false);
    dateInput.value = "";
    releaseAvailableDates = [];
    releaseDateSet = new Set();
    setReleaseNavButtonState("");

    if (chartReleaseHourlyInstance) {
      chartReleaseHourlyInstance.clear();
      chartReleaseHourlyInstance.setOption({
        xAxis: { show: false },
        yAxis: { show: false },
        series: []
      });
    }

    setReleaseMessage("Chart 3 is available only for Davis and Parker.");
    return;
  }

  const datesPayload = await fetchReleaseHourlyDates(dam);
  releaseAvailableDates = (datesPayload.dates || []).slice().sort();
  releaseDateSet = new Set(releaseAvailableDates);

  if (!releaseAvailableDates.length) {
    updateReleaseDateInputState(dateInput, false);
    dateInput.value = "";

    if (chartReleaseHourlyInstance) {
      chartReleaseHourlyInstance.clear();
    }

    setReleaseNavButtonState("");
    setReleaseMessage("No hourly release data is available for this dam yet.");
    return;
  }

  updateReleaseDateInputState(dateInput, true);

  const currentInput = normalizeDateInput(dateInput.value);
  const selectedDate = releaseDateSet.has(currentInput)
    ? currentInput
    : releaseAvailableDates[releaseAvailableDates.length - 1];

  dateInput.value = selectedDate;

  if (!dateInput.dataset.boundReleaseListener) {
    dateInput.addEventListener("change", async (event) => {
      const picked = normalizeDateInput(event.target.value);

      if (!releaseDateSet.has(picked)) {
        event.target.value = releaseAvailableDates[releaseAvailableDates.length - 1] || "";
        setReleaseMessage("Selected date has no data and cannot be used.");
        setReleaseNavButtonState(normalizeDateInput(event.target.value));
        return;
      }

      const activeDam = getActiveDam();
      if (!["davis", "parker"].includes(activeDam)) return;

      try {
        await loadReleaseHourlyDataForDate(activeDam, picked);
      } catch (error) {
        console.error("Failed to load hourly release:", error);
        setReleaseMessage("Unable to load hourly release data.");
      }
    });
    dateInput.dataset.boundReleaseListener = "true";
  }

  if (!prevButton.dataset.boundReleaseListener) {
    prevButton.addEventListener("click", async () => {
      const activeDam = getActiveDam();
      if (!["davis", "parker"].includes(activeDam)) return;

      const current = normalizeDateInput(dateInput.value);
      const currentIndex = releaseAvailableDates.indexOf(current);
      if (currentIndex <= 0) return;

      const previousDate = releaseAvailableDates[currentIndex - 1];
      try {
        await loadReleaseHourlyDataForDate(activeDam, previousDate);
      } catch (error) {
        console.error("Failed to load previous hourly release:", error);
        setReleaseMessage("Unable to load hourly release data.");
      }
    });
    prevButton.dataset.boundReleaseListener = "true";
  }

  if (!nextButton.dataset.boundReleaseListener) {
    nextButton.addEventListener("click", async () => {
      const activeDam = getActiveDam();
      if (!["davis", "parker"].includes(activeDam)) return;

      const current = normalizeDateInput(dateInput.value);
      const currentIndex = releaseAvailableDates.indexOf(current);
      if (currentIndex === -1 || currentIndex >= releaseAvailableDates.length - 1) return;

      const nextDate = releaseAvailableDates[currentIndex + 1];
      try {
        await loadReleaseHourlyDataForDate(activeDam, nextDate);
      } catch (error) {
        console.error("Failed to load next hourly release:", error);
        setReleaseMessage("Unable to load hourly release data.");
      }
    });
    nextButton.dataset.boundReleaseListener = "true";
  }

  await loadReleaseHourlyDataForDate(dam, selectedDate);
}

// ==============================
// GRAPH 4 — Hourly Energy by Unit (Davis/Parker)
// ==============================

function setEnergyUnitMessage(message) {
  const note = document.getElementById("g4-message");
  if (note) note.textContent = message || "";
}

function setEnergyUnitNavButtonState(date) {
  const prevButton = document.getElementById("g4-prev-day");
  const nextButton = document.getElementById("g4-next-day");

  if (!prevButton || !nextButton) return;

  if (!energyUnitAvailableDates.length) {
    prevButton.disabled = true;
    nextButton.disabled = true;
    return;
  }

  const currentIndex = energyUnitAvailableDates.indexOf(date);

  if (currentIndex === -1) {
    prevButton.disabled = true;
    nextButton.disabled = true;
    return;
  }

  prevButton.disabled = currentIndex <= 0;
  nextButton.disabled = currentIndex >= energyUnitAvailableDates.length - 1;
}

function updateEnergyUnitDateInputState(dateInput, enabled) {
  if (!dateInput) return;
  dateInput.disabled = !enabled;

  if (!enabled) {
    dateInput.removeAttribute("min");
    dateInput.removeAttribute("max");
    setEnergyUnitNavButtonState("");
    return;
  }

  if (energyUnitAvailableDates.length) {
    dateInput.min = energyUnitAvailableDates[0];
    dateInput.max = energyUnitAvailableDates[energyUnitAvailableDates.length - 1];
  }

  setEnergyUnitNavButtonState(normalizeDateInput(dateInput.value));
}

function renderEnergyUnitHourlyChart(payload) {
  const container = document.getElementById("chartEnergyUnitHourly");
  if (!container || !payload) return;

  if (!chartEnergyUnitHourlyInstance) {
    chartEnergyUnitHourlyInstance = echarts.init(container);
    window.addEventListener("resize", () => chartEnergyUnitHourlyInstance.resize());
  }

  const hours = Array.from({ length: 24 }, (_, idx) => idx);
  const hourLabels = hours.map(formatHourLabel);
  const units = (payload.units || []).map(row => row.unit);

  const unitIndexBySdId = new Map((payload.units || []).map((row, idx) => [Number(row.sd_id), idx]));
  const cellByUnitAndHour = new Map();

  (payload.historic || []).forEach(row => {
    const unitIndex = unitIndexBySdId.get(Number(row.sd_id));
    if (unitIndex === undefined) return;

    const hour = Number(row.hour);
    const key = `${unitIndex}-${hour}`;
    cellByUnitAndHour.set(key, {
      x: hour,
      y: unitIndex,
      value: Math.round(Number(row.v)),
      source: "Historic"
    });
  });

  (payload.forecast || []).forEach(row => {
    const unitIndex = unitIndexBySdId.get(Number(row.sd_id));
    if (unitIndex === undefined) return;

    const hour = Number(row.hour);
    const key = `${unitIndex}-${hour}`;
    cellByUnitAndHour.set(key, {
      x: hour,
      y: unitIndex,
      value: Math.round(Number(row.v)),
      source: "Forecast"
    });
  });

  const values = Array.from(cellByUnitAndHour.values()).map(cell => cell.value);
  const hasValues = values.length > 0;
  const minValue = hasValues ? Math.min(...values) : 0;
  const maxValue = hasValues ? Math.max(...values) : 1;

  const heatmapData = [];
  cellByUnitAndHour.forEach(cell => {
    heatmapData.push([cell.x, cell.y, cell.value, cell.source]);
  });

  chartEnergyUnitHourlyInstance.setOption({
    animation: false,
    tooltip: {
      position: "top",
      formatter: (params) => {
        if (!params || !params.data) return "No data";
        const hour = hourLabels[params.data[0]];
        const unit = units[params.data[1]];
        const value = params.data[2];
        const source = params.data[3];
        return `${unit}, hour ${hour}: ${value} MWh (${source})`;
      }
    },
    grid: { left: 70, right: 20, top: 40, bottom: 55 },
    xAxis: {
      type: "category",
      data: hourLabels,
      name: "Hour Start",
      nameLocation: "middle",
      nameGap: 30,
      splitArea: { show: true }
    },
    yAxis: {
      type: "category",
      data: units,
      inverse: false,
      splitArea: { show: true }
    },
    visualMap: {
      min: minValue,
      max: maxValue,
      calculable: false,
      orient: "horizontal",
      left: "center",
      bottom: 5,
      inRange: {
        color: ["#edf4ff", "#9ec5ff", "#2f6fcc"]
      }
    },
    series: [
      {
        name: "Energy",
        type: "heatmap",
        data: heatmapData,
        label: {
          show: true,
          formatter: (params) => {
            const value = Number(params.data[2]);
            return value > 0 ? `{positive|${value}}` : `${value}`;
          },
          rich: {
            positive: {
              fontWeight: "bold"
            }
          },
          color: "#0e2239",
          fontSize: 11
        },
        emphasis: {
          itemStyle: {
            shadowBlur: 10,
            shadowColor: "rgba(0, 0, 0, 0.5)"
          }
        }
      }
    ]
  }, true);
}

async function loadEnergyUnitHourlyDataForDate(dam, date) {
  const payload = await fetchEnergyUnitHourlySeries(dam, date);
  renderEnergyUnitHourlyChart(payload);

  const dateInput = document.getElementById("g4-date");
  if (dateInput) {
    dateInput.value = payload.date;
  }

  setEnergyUnitNavButtonState(payload.date);

  const formattedDam = dam.charAt(0).toUpperCase() + dam.slice(1);
  setEnergyUnitMessage(`Showing ${formattedDam} unit energy for ${formatDateWithDay(payload.date)}. As of ${formatAsOfDateTime(payload.as_of)}.`);
}

async function initializeEnergyUnitHourlyChart(dam) {
  const dateInput = document.getElementById("g4-date");
  const prevButton = document.getElementById("g4-prev-day");
  const nextButton = document.getElementById("g4-next-day");
  const container = document.getElementById("chartEnergyUnitHourly");
  if (!dateInput || !container || !prevButton || !nextButton) return;

  if (!["davis", "parker"].includes(dam)) {
    updateEnergyUnitDateInputState(dateInput, false);
    dateInput.value = "";
    energyUnitAvailableDates = [];
    energyUnitDateSet = new Set();
    setEnergyUnitNavButtonState("");

    if (chartEnergyUnitHourlyInstance) {
      chartEnergyUnitHourlyInstance.clear();
      chartEnergyUnitHourlyInstance.setOption({
        xAxis: { show: false },
        yAxis: { show: false },
        series: []
      });
    }

    setEnergyUnitMessage("Chart 4 is available only for Davis and Parker.");
    return;
  }

  const datesPayload = await fetchEnergyUnitHourlyDates(dam);
  energyUnitAvailableDates = (datesPayload.dates || []).slice().sort();
  energyUnitDateSet = new Set(energyUnitAvailableDates);

  if (!energyUnitAvailableDates.length) {
    updateEnergyUnitDateInputState(dateInput, false);
    dateInput.value = "";

    if (chartEnergyUnitHourlyInstance) {
      chartEnergyUnitHourlyInstance.clear();
    }

    setEnergyUnitNavButtonState("");
    setEnergyUnitMessage("No hourly unit energy data is available for this dam yet.");
    return;
  }

  updateEnergyUnitDateInputState(dateInput, true);

  const currentInput = normalizeDateInput(dateInput.value);
  const selectedDate = energyUnitDateSet.has(currentInput)
    ? currentInput
    : energyUnitAvailableDates[energyUnitAvailableDates.length - 1];

  dateInput.value = selectedDate;

  if (!dateInput.dataset.boundEnergyUnitListener) {
    dateInput.addEventListener("change", async (event) => {
      const picked = normalizeDateInput(event.target.value);

      if (!energyUnitDateSet.has(picked)) {
        event.target.value = energyUnitAvailableDates[energyUnitAvailableDates.length - 1] || "";
        setEnergyUnitMessage("Selected date has no data and cannot be used.");
        setEnergyUnitNavButtonState(normalizeDateInput(event.target.value));
        return;
      }

      const activeDam = getActiveDam();
      if (!["davis", "parker"].includes(activeDam)) return;

      try {
        await loadEnergyUnitHourlyDataForDate(activeDam, picked);
      } catch (error) {
        console.error("Failed to load hourly unit energy:", error);
        setEnergyUnitMessage("Unable to load hourly unit energy data.");
      }
    });
    dateInput.dataset.boundEnergyUnitListener = "true";
  }

  if (!prevButton.dataset.boundEnergyUnitListener) {
    prevButton.addEventListener("click", async () => {
      const activeDam = getActiveDam();
      if (!["davis", "parker"].includes(activeDam)) return;

      const current = normalizeDateInput(dateInput.value);
      const currentIndex = energyUnitAvailableDates.indexOf(current);
      if (currentIndex <= 0) return;

      const previousDate = energyUnitAvailableDates[currentIndex - 1];
      try {
        await loadEnergyUnitHourlyDataForDate(activeDam, previousDate);
      } catch (error) {
        console.error("Failed to load previous hourly unit energy:", error);
        setEnergyUnitMessage("Unable to load hourly unit energy data.");
      }
    });
    prevButton.dataset.boundEnergyUnitListener = "true";
  }

  if (!nextButton.dataset.boundEnergyUnitListener) {
    nextButton.addEventListener("click", async () => {
      const activeDam = getActiveDam();
      if (!["davis", "parker"].includes(activeDam)) return;

      const current = normalizeDateInput(dateInput.value);
      const currentIndex = energyUnitAvailableDates.indexOf(current);
      if (currentIndex === -1 || currentIndex >= energyUnitAvailableDates.length - 1) return;

      const nextDate = energyUnitAvailableDates[currentIndex + 1];
      try {
        await loadEnergyUnitHourlyDataForDate(activeDam, nextDate);
      } catch (error) {
        console.error("Failed to load next hourly unit energy:", error);
        setEnergyUnitMessage("Unable to load hourly unit energy data.");
      }
    });
    nextButton.dataset.boundEnergyUnitListener = "true";
  }

  await loadEnergyUnitHourlyDataForDate(dam, selectedDate);
}

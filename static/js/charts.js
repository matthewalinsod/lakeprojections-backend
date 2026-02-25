let elevationChartInstance = null;
let chart24msInstance = null;

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
  const activeTab = document.querySelector(".tab-button.active");
  return activeTab ? activeTab.dataset.dam : "hoover";
}

// ==============================
// GRAPH 1 — Elevation
// ==============================

function renderElevationChart(containerId, payload) {

  const el = document.getElementById(containerId);
  if (!el || !payload) return;

  if (!elevationChartInstance) {
    elevationChartInstance = echarts.init(el);
    window.addEventListener("resize", () => elevationChartInstance.resize());
  }

  const historic = buildSeriesPoints(payload.historic || []);

  const cutoverMs = isoToMs(payload.cutover);

  let forecast = buildSeriesPoints(payload.forecast || []);

  // Forecasted elevation values should display at 2-decimal precision.
  forecast = forecast.map(point => [point[0], roundToDecimals(point[1], 2)]);

  // 🔥 Remove forecast values BEFORE today
  forecast = forecast.filter(point => point[0] > cutoverMs);

  // Stitch forecast to historic
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

  elevationChartInstance.setOption({
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
    yAxis: { type: "value", scale: true },
    series: [
      {
        name: "Historic",
        type: "line",
        showSymbol: false,
        data: historic,
        lineStyle: { width: 2, color: "#1f78ff" },
        itemStyle: { color: "#1f78ff" },
        areaStyle: {
          color: "rgba(31, 120, 255, 0.35)"
        }
      },
      {
        name: "Forecast",
        type: "line",
        showSymbol: false,
        data: forecast,
        lineStyle: { width: 2, type: "dashed", color: "#2e8b57" },
        itemStyle: { color: "#2e8b57" },
        areaStyle: {
          color: "rgba(46, 139, 87, 0.30)"
        }
      }
    ]
  }, true);

  drawTodayLine(cutoverMs);
}

function drawTodayLine(cutoverMs) {

  if (!elevationChartInstance) return;

  const xPixel = elevationChartInstance.convertToPixel(
    { xAxisIndex: 0 },
    cutoverMs
  );

  const grid = elevationChartInstance
    .getModel()
    .getComponent("grid")
    .coordinateSystem.getRect();

  elevationChartInstance.setOption({
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
          fill: "#1f78ff",
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
          fill: "#2e8b57",
          font: "bold 12px Arial",
          textAlign: "left",
          textVerticalAlign: "bottom"
        },
        silent: true
      }
    ]
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

// Reload when dam tab changes
document.querySelectorAll(".tab-button").forEach(btn => {
  btn.addEventListener("click", function () {
    document.querySelectorAll(".tab-button")
      .forEach(b => b.classList.remove("active"));

    this.classList.add("active");
    document.getElementById("activeDam").textContent =
      this.textContent;

    const selectedMonth =
      document.getElementById("g2-month")?.value;

    if (selectedMonth) {
      load24MSData(selectedMonth);
    }
  });
});

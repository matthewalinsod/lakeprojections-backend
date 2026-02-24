let elevationChartInstance = null;

function isoToMs(iso) {
  // iso like "2026-02-24T00:00:00"
  return new Date(iso + "Z").getTime(); // treat as UTC
}

function buildSeriesPoints(rows) {
  // rows: [{t, v}]
  return rows
    .filter(r => r.t && r.v !== null && r.v !== undefined)
    .map(r => [isoToMs(r.t), Number(r.v)]);
}

function renderElevationChart(containerId, payload) {
  const el = document.getElementById(containerId);
  if (!el) return;

  if (!elevationChartInstance) {
    elevationChartInstance = echarts.init(el);
    window.addEventListener("resize", () => elevationChartInstance.resize());
  }

  const historic = buildSeriesPoints(payload.historic || []);
  const forecast = buildSeriesPoints(payload.forecast || []);
  const todayMs = isoToMs(payload.today_line);

  const option = {
    animation: false,
    grid: { left: 50, right: 20, top: 30, bottom: 35 },
    tooltip: {
      trigger: "axis",
      axisPointer: { type: "cross" }
    },
    xAxis: {
      type: "time",
      axisLabel: { hideOverlap: true }
    },
    yAxis: {
      type: "value",
      scale: true
    },
    series: [
      {
        name: "Historic",
        type: "line",
        showSymbol: false,
        data: historic,
        lineStyle: { width: 2 }
      },
      {
        name: "Forecast",
        type: "line",
        showSymbol: false,
        data: forecast,
        lineStyle: { width: 2, type: "dashed" }
      }
    ],
    // Vertical black line at today
    graphic: [
      {
        type: "line",
        // Convert today x value into pixel coordinate via convertToPixel
        // We set placeholder; we'll update after setOption using resize handler below.
        shape: { x1: 0, y1: 0, x2: 0, y2: 0 },
        style: { stroke: "#000", lineWidth: 2 },
        silent: true
      }
    ]
  };

  elevationChartInstance.setOption(option, true);

  // Draw "today" line precisely (after chart lays out)
  const updateTodayLine = () => {
    if (!elevationChartInstance) return;
    const x = elevationChartInstance.convertToPixel({ xAxisIndex: 0 }, todayMs);
    const top = elevationChartInstance.convertToPixel({ yAxisIndex: 0 }, elevationChartInstance.getOption().yAxis[0].max ?? 0);
    const bottom = elevationChartInstance.convertToPixel({ yAxisIndex: 0 }, elevationChartInstance.getOption().yAxis[0].min ?? 0);

    // If convertToPixel returns NaN (rare), fall back to grid bounds
    const grid = elevationChartInstance.getModel().getComponent("grid").coordinateSystem.getRect();
    const y1 = isFinite(top) ? top : grid.y;
    const y2 = isFinite(bottom) ? bottom : (grid.y + grid.height);

    elevationChartInstance.setOption({
      graphic: [{
        type: "line",
        shape: { x1: x, y1: grid.y, x2: x, y2: grid.y + grid.height },
        style: { stroke: "#000", lineWidth: 2 },
        silent: true
      }]
    });
  };

  updateTodayLine();
  elevationChartInstance.off("finished");
  elevationChartInstance.on("finished", updateTodayLine);
}

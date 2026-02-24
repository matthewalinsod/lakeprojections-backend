let elevationChartInstance = null;

function isoToMs(iso) {
  // Interpret timestamps exactly as stored (no forced UTC)
  return new Date(iso).getTime();
}

function buildSeriesPoints(rows) {
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
  let forecast = buildSeriesPoints(payload.forecast || []);
  const cutoverMs = isoToMs(payload.cutover);

  // --- Stitch forecast to historic ---
  if (payload.last_historic && payload.last_historic.t && payload.last_historic.v !== null) {
    const stitchPoint = [
      isoToMs(payload.last_historic.t),
      Number(payload.last_historic.v)
    ];

    if (forecast.length === 0 || forecast[0][0] !== stitchPoint[0]) {
      forecast = [stitchPoint, ...forecast];
    }
  }

  const option = {
    animation: false,
    grid: { left: 60, right: 20, top: 50, bottom: 40 },
    tooltip: {
      trigger: "axis",
      axisPointer: { type: "cross" }
    },
    legend: {
      top: 10
    },
    xAxis: {
  type: "time",
  axisLabel: {
    hideOverlap: true,
    formatter: function (value) {
      const d = new Date(value);
      const month = d.getMonth() + 1;
      const day = d.getDate();
      const year = d.getFullYear();

      return `${month}/${day}/${year}`;
    }
  },
  axisTick: {
    alignWithLabel: true
  },
  splitNumber: 6   // controls density
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
    ]
  };

  elevationChartInstance.setOption(option, true);

  // --- Draw Today vertical line + label ---
  function drawTodayLine() {
    const xPixel = elevationChartInstance.convertToPixel({ xAxisIndex: 0 }, cutoverMs);
    const grid = elevationChartInstance.getModel().getComponent("grid").coordinateSystem.getRect();

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
          style: {
            stroke: "#000",
            lineWidth: 2
          },
          silent: true
        },
        {
          type: "text",
          left: xPixel + 4,
          top: grid.y - 20,
          style: {
            text: "Today",
            fill: "#000",
            font: "bold 12px Arial"
          },
          silent: true
        }
      ]
    });
  }

  elevationChartInstance.off("finished");
  elevationChartInstance.on("finished", drawTodayLine);
  drawTodayLine();
}

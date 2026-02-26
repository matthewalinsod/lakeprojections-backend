document.addEventListener("DOMContentLoaded", () => {
  const tabButtons = document.querySelectorAll(".tab-button");
  const activeDamLabel = document.getElementById("activeDam");

  const rangeButtons = document.querySelectorAll(".range-btn");
  let activeDam = "hoover";
  let activeRange = "30d"; // MTD default

  function titleCase(s) {
    return s.charAt(0).toUpperCase() + s.slice(1);
  }

  async function loadElevation() {
    const payload = await fetchElevationSeries(activeDam, activeRange);
    renderElevationChart("chartElevation", payload);
  }

  async function loadReleaseHourly() {
    if (typeof initializeReleaseHourlyChart !== "function") return;
    await initializeReleaseHourlyChart(activeDam);
  }

  async function loadEnergyByUnitHourly() {
    if (typeof initializeEnergyUnitHourlyChart !== "function") return;
    await initializeEnergyUnitHourlyChart(activeDam);
  }

  // Tabs
  tabButtons.forEach(btn => {
    btn.addEventListener("click", async () => {
      tabButtons.forEach(b => b.classList.remove("active"));
      btn.classList.add("active");

      activeDam = btn.dataset.dam;
      activeDamLabel.textContent = titleCase(activeDam);

      await loadElevation();
      await loadReleaseHourly();
      await loadEnergyByUnitHourly();
    });
  });

  // Range filters
  rangeButtons.forEach(btn => {
    btn.addEventListener("click", async () => {
      rangeButtons.forEach(b => b.classList.remove("active"));
      btn.classList.add("active");

      activeRange = btn.dataset.range;
      await loadElevation();
    });
  });

  // Initial load
  Promise.all([
    loadElevation(),
    loadReleaseHourly(),
    loadEnergyByUnitHourly()
  ]).catch(err => console.error(err));
});

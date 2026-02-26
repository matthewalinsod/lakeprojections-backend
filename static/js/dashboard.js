document.addEventListener("DOMContentLoaded", () => {
  const page = document.body.dataset.page;
  const dam = document.body.dataset.dam;
  const subpage = document.body.dataset.subpage;

  if (page !== "dam" || !dam || !subpage) return;

  async function loadElevation() {
    const activeButton = document.querySelector(".range-btn.active");
    const activeRange = activeButton ? activeButton.dataset.range : "30d";
    const payload = await fetchElevationSeries(dam, activeRange);
    renderElevationChart("chartElevation", payload);
  }

  async function loadReleases() {
    if (typeof initializeReleaseHourlyChart !== "function") return;
    await initializeReleaseHourlyChart(dam);
  }

  async function loadEnergy() {
    if (typeof initializeEnergyUnitHourlyChart !== "function") return;
    await initializeEnergyUnitHourlyChart(dam);
  }

  if (subpage === "elevation") {
    const rangeButtons = document.querySelectorAll(".range-btn");
    rangeButtons.forEach((btn) => {
      btn.addEventListener("click", async () => {
        rangeButtons.forEach((b) => b.classList.remove("active"));
        btn.classList.add("active");
        await loadElevation();
      });
    });

    loadElevation().catch((err) => console.error(err));
  }

  if (subpage === "releases") {
    loadReleases().catch((err) => console.error(err));
  }

  if (subpage === "energy") {
    loadEnergy().catch((err) => console.error(err));
  }
});

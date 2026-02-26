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

  async function loadLakeMeadRelease() {
    const activeButton = document.querySelector(".range-btn-release.active");
    const activeRange = activeButton ? activeButton.dataset.range : "30d";
    const payload = await fetchLakeMeadReleaseSeries(activeRange);
    renderLakeMeadReleaseChart(payload);
  }

  async function loadLakeMeadEnergy() {
    const activeButton = document.querySelector(".range-btn-energy.active");
    const activeRange = activeButton ? activeButton.dataset.range : "30d";
    const payload = await fetchLakeMeadEnergySeries(activeRange);
    renderLakeMeadEnergyChart(payload);
  }

  async function loadReleases() {
    if (dam === "hoover") {
      await loadLakeMeadRelease();
      return;
    }

    if (typeof initializeReleaseHourlyChart !== "function") return;
    await initializeReleaseHourlyChart(dam);
  }

  async function loadEnergy() {
    if (dam === "hoover") {
      await loadLakeMeadEnergy();
      return;
    }

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
    if (dam === "hoover") {
      const rangeButtons = document.querySelectorAll(".range-btn-release");
      rangeButtons.forEach((btn) => {
        btn.addEventListener("click", async () => {
          rangeButtons.forEach((b) => b.classList.remove("active"));
          btn.classList.add("active");
          await loadReleases();
        });
      });
    }

    loadReleases().catch((err) => console.error(err));
  }

  if (subpage === "energy") {
    if (dam === "hoover") {
      const rangeButtons = document.querySelectorAll(".range-btn-energy");
      rangeButtons.forEach((btn) => {
        btn.addEventListener("click", async () => {
          rangeButtons.forEach((b) => b.classList.remove("active"));
          btn.classList.add("active");
          await loadEnergy();
        });
      });
    }

    loadEnergy().catch((err) => console.error(err));
  }
});

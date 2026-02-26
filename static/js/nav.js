document.addEventListener("DOMContentLoaded", () => {
  const navToggle = document.querySelector(".nav-toggle");
  const navList = document.querySelector(".site-nav-list");
  const dropdownToggles = document.querySelectorAll(".nav-dropdown-toggle");

  if (navToggle && navList) {
    navToggle.addEventListener("click", () => {
      const isOpen = navList.classList.toggle("open");
      navToggle.setAttribute("aria-expanded", String(isOpen));
    });
  }

  dropdownToggles.forEach((toggleButton) => {
    toggleButton.addEventListener("click", (event) => {
      event.preventDefault();
      event.stopPropagation();

      const item = toggleButton.closest(".has-dropdown");
      if (!item) return;

      const isOpen = item.classList.toggle("open");
      toggleButton.setAttribute("aria-expanded", String(isOpen));
    });
  });

  document.addEventListener("click", (event) => {
    document.querySelectorAll(".has-dropdown.open").forEach((openItem) => {
      if (!openItem.contains(event.target)) {
        openItem.classList.remove("open");
        const button = openItem.querySelector(".nav-dropdown-toggle");
        if (button) {
          button.setAttribute("aria-expanded", "false");
        }
      }
    });
  });
});

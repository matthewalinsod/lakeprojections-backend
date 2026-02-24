document.addEventListener("DOMContentLoaded", function() {

    const buttons = document.querySelectorAll(".tab-button");
    const activeDamLabel = document.getElementById("activeDam");

    buttons.forEach(button => {
        button.addEventListener("click", function() {

            buttons.forEach(btn => btn.classList.remove("active"));
            this.classList.add("active");

            const selectedDam = this.dataset.dam;
            activeDamLabel.textContent = selectedDam.charAt(0).toUpperCase() + selectedDam.slice(1);

            console.log("Active dam changed to:", selectedDam);

            // Later: call API reload here
        });
    });

});

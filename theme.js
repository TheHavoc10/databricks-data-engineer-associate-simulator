/* Preferencia visual independiente del progreso del simulador. */
(function () {
  "use strict";
  var KEY = "dbprep_theme_mode";
  var selected = "auto";
  try { selected = localStorage.getItem(KEY) || "auto"; } catch (e) {}
  if (["auto", "light", "dark"].indexOf(selected) < 0) selected = "auto";

  var media = window.matchMedia ? window.matchMedia("(prefers-color-scheme: dark)") : null;
  function update() {
    var resolved = selected === "auto" ? (media && media.matches ? "dark" : "light") : selected;
    document.documentElement.setAttribute("data-effective-theme", resolved);
    document.documentElement.style.colorScheme = resolved;

    var meta = document.querySelector('meta[name="theme-color"]');
    if (meta) meta.setAttribute("content", resolved === "dark" ? "#0c151b" : "#f6f7f9");

    var button = document.getElementById("themeToggle");
    if (button) {
      var label = selected === "auto" ? "Automático" : selected === "light" ? "Claro" : "Oscuro";
      var icon = selected === "auto" ? "◐" : selected === "light" ? "☀" : "☾";
      var iconPart = button.querySelector(".theme-symbol");
      var labelPart = button.querySelector(".theme-label");
      if (iconPart) iconPart.textContent = icon;
      if (labelPart) labelPart.textContent = label;
      button.setAttribute("aria-label", "Tema " + label + ". Cambiar apariencia");
      button.title = "Tema: " + label + ". Pulsa para cambiar";
    }
  }
  function toggle() {
    selected = selected === "auto" ? "light" : selected === "light" ? "dark" : "auto";
    try { localStorage.setItem(KEY, selected); } catch (e) {}
    update();
  }
  if (media) {
    if (media.addEventListener) media.addEventListener("change", function () { if (selected === "auto") update(); });
    else if (media.addListener) media.addListener(function () { if (selected === "auto") update(); });
  }
  document.addEventListener("DOMContentLoaded", function () {
    var button = document.getElementById("themeToggle");
    if (button) button.addEventListener("click", toggle);
    update();
  });
  update();
})();
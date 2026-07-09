/* Drop Bot dashboard — progressive enhancement.
   Everything here is optional polish: theme toggle, toast feedback, and
   AJAX money actions. If JS is off (or anything throws), the forms submit
   normally and the pages still work. */
(function () {
  "use strict";
  var root = document.documentElement;
  var THEME_COLORS = { dark: "#0b0b10", light: "#f6f7f9" };

  /* ── Theme toggle ─────────────────────────────────────────────────── */
  var toggle = document.getElementById("themeToggle");
  var metaTheme = document.querySelector('meta[name="theme-color"]');

  function paintTheme() {
    var mode = root.getAttribute("data-theme") === "light" ? "light" : "dark";
    if (toggle) toggle.textContent = mode === "light" ? "☀️" : "🌙";
    if (metaTheme) metaTheme.setAttribute("content", THEME_COLORS[mode]);
  }
  paintTheme();

  if (toggle) {
    toggle.addEventListener("click", function () {
      var next = root.getAttribute("data-theme") === "light" ? "dark" : "light";
      root.setAttribute("data-theme", next);
      try { localStorage.setItem("theme", next); } catch (e) {}
      paintTheme();
    });
  }

  /* ── Toasts ───────────────────────────────────────────────────────── */
  var host = document.getElementById("toasts");
  function toast(msg, kind) {
    if (!host) return;
    var el = document.createElement("div");
    el.className = "toast " + (kind || "ok");
    el.textContent = msg;
    host.appendChild(el);
    requestAnimationFrame(function () { el.classList.add("show"); });
    setTimeout(function () {
      el.classList.remove("show");
      setTimeout(function () { el.remove(); }, 300);
    }, 3200);
  }

  /* ── Region swap (server stays the source of truth) ───────────────── */
  function swapRegion(html) {
    var current = document.getElementById("paidRegion");
    if (!current) return false;
    try {
      var doc = new DOMParser().parseFromString(html, "text/html");
      var fresh = doc.getElementById("paidRegion");
      if (!fresh) return false;
      current.innerHTML = fresh.innerHTML;
      return true;
    } catch (e) {
      return false;
    }
  }

  /* ── AJAX submit for row-level money actions ──────────────────────── */
  document.addEventListener("submit", function (ev) {
    var form = ev.target;
    if (!form || !form.classList || !form.classList.contains("js-ajax")) return;

    ev.preventDefault();
    var submitter = ev.submitter;
    var msg =
      (submitter && submitter.getAttribute("data-toast")) ||
      form.getAttribute("data-toast") ||
      "Saved";

    fetch(form.action, {
      method: (form.getAttribute("method") || "POST").toUpperCase(),
      body: new FormData(form),
      credentials: "same-origin",
      headers: { "X-Requested-With": "fetch" }
    })
      .then(function (r) {
        if (!r.ok) throw new Error("bad status");
        return r.text();
      })
      .then(function (html) {
        if (swapRegion(html)) {
          toast(msg, "ok");
        } else {
          form.submit(); // fallback: full navigation
        }
      })
      .catch(function () {
        form.submit(); // fallback: full navigation
      });
  });

  /* ── Background auto-refresh for a live drop ──────────────────────── */
  var region = document.getElementById("paidRegion");
  if (region && region.hasAttribute("data-live-poll")) {
    var url = region.getAttribute("data-poll-url") || location.pathname;
    setInterval(function () {
      if (document.hidden) return;
      fetch(url, { credentials: "same-origin", headers: { "X-Requested-With": "fetch" } })
        .then(function (r) { return r.ok ? r.text() : null; })
        .then(function (html) { if (html) swapRegion(html); })
        .catch(function () {});
    }, 25000);
  }
})();

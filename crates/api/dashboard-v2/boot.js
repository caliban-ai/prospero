// Bootstrap the wasm module.
//
// This lives in its own file rather than an inline <script> because the page is
// served under `script-src 'self'` with no 'unsafe-inline' and no nonce — an
// inline bootstrap is blocked outright. Keeping it external means the CSP stays
// tight instead of being widened to accommodate a few lines of glue.

// Apply the stored theme BEFORE the module loads.
//
// The app is WASM, so first paint happens well before Rust runs. Stamping the
// root element only once the module has booted would show the system theme and
// then snap to the chosen one — a visible flash on every load. Reading storage
// here costs nothing and removes it.
//
// Anything unrecognised is ignored, leaving the attribute unset so the
// stylesheet's prefers-color-scheme query applies. A corrupt preference must
// never be able to break the page, so this is wrapped: localStorage throws
// outright in some privacy modes.
try {
  const stored = localStorage.getItem("prospero.theme");
  if (stored === "light" || stored === "dark") {
    document.documentElement.setAttribute("data-theme", stored);
  }
} catch (_) {
  // No storage available — fall back to following the system.
}

import init from "/v2/prospero-dashboard.js";

init();

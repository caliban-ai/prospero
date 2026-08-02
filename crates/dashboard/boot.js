// Bootstrap the wasm module.
//
// This lives in its own file rather than an inline <script> because the page is
// served under `script-src 'self'` with no 'unsafe-inline' and no nonce — an
// inline bootstrap is blocked outright. Keeping it external means the CSP stays
// tight instead of being widened to accommodate three lines of glue.
import init from "/v2/prospero-dashboard.js";

init();

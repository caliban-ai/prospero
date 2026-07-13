// Prospero dashboard — vanilla JS, no build step.
// Lists repos with their agents grouped underneath, polls the fleet, and
// live-streams a selected agent's events over SSE.

const fleetEl = document.getElementById("fleet");
const fleetListEl = document.getElementById("fleet-list");
const streamEl = document.getElementById("stream");
const streamHeadEl = document.getElementById("stream-head");
const streamLogEl = document.getElementById("stream-log");
const hostEl = document.getElementById("host");
const modalRoot = document.getElementById("modal-root");

let selectedAgent = null;
let evtSource = null;
let healthyWorkspaces = []; // names of reachable repos, for the launch picker
let lastFleet = null; // most recent fleet snapshot, for stream-head lookups
let streamCtx = null; // { id, name, status, model, cost, turns, outcome, lastSeq, terminal } for the streamed agent
let streamEvents = []; // accumulated events for the selected agent (folded into the timeline)
let reconnectTimer = null; // pending manual SSE reconnect (see openAgentStream)
const expandedTools = new Set(); // tool span keys (event seq) the user expanded

// Backend capabilities (GET /api/capabilities), fetched once before the first
// render. `admin` gates registry controls; `async_workspace_ops` selects the
// k8s config UI (named-provider list + Secret refs) over the local single-
// provider env-var form, and the "reconciling" save semantics. Defaults are the
// local shape, so a failed fetch degrades to today's behavior. (#143)
let caps = { admin: true, async_workspace_ops: false };
// True when the active backend is the k8s config plane.
function isK8s() { return !!caps.async_workspace_ops; }

async function loadCapabilities() {
  try {
    const c = await api("GET", "/api/capabilities");
    if (c) caps = c;
  } catch (_) {
    // Keep the local-shaped defaults; the dashboard still functions.
  }
}

// Apply capability gating to the static chrome (the per-workspace gear/remove
// are gated inside renderFleet). Hide the add-workspace button when no admin
// plane is wired.
function applyCapabilities() {
  const addBtn = document.getElementById("add-repo-btn");
  if (addBtn) addBtn.classList.toggle("hidden", !caps.admin);
}

function findAgent(id) {
  if (!lastFleet) return null;
  for (const r of lastFleet.workspaces) {
    for (const a of r.agents) if (a.id === id) return a;
  }
  return null;
}

function streamIsLive() {
  return !!evtSource && evtSource.readyState === 1; // EventSource.OPEN
}

// --- API + UX helpers -------------------------------------------------------

// JSON API call. Returns parsed JSON (or null for empty bodies).
// Throws Error(message) carrying the server's `error` string on non-2xx.
async function api(method, path, body) {
  const opts = { method, headers: {} };
  if (body !== undefined) {
    opts.headers["Content-Type"] = "application/json";
    opts.body = JSON.stringify(body);
  }
  const res = await fetch(path, opts);
  const text = await res.text();
  let data = null;
  if (text) {
    try { data = JSON.parse(text); } catch (_) {}
  }
  if (!res.ok) {
    throw new Error((data && data.error) || `${res.status} ${res.statusText}`);
  }
  return data;
}

// Transient, click-to-dismiss error banner above the fleet list.
function showBanner(msg) {
  const bar = document.getElementById("banner");
  bar.textContent = msg;
  bar.classList.remove("hidden");
  bar.onclick = () => bar.classList.add("hidden");
}

// Modal infrastructure — one node at a time inside #modal-root.
function openModal(node) {
  modalRoot.innerHTML = "";
  const scrim = document.createElement("div");
  scrim.className = "scrim";
  scrim.onclick = (e) => { if (e.target === scrim) closeModal(); };
  const box = document.createElement("div");
  box.className = "modal";
  box.appendChild(node);
  scrim.appendChild(box);
  modalRoot.appendChild(scrim);
  modalRoot.classList.remove("hidden");
}

function closeModal() {
  modalRoot.classList.add("hidden");
  modalRoot.innerHTML = "";
}

// --- Add-repo modal ---------------------------------------------------------

function openAddRepoModal() {
  const form = document.createElement("div");
  // The local backend needs a checkout path; the k8s config plane derives
  // sources from the config editor instead, so it omits the path field.
  const pathField = isK8s()
    ? ""
    : `<label class="fl">path<input class="in" id="ar-root" placeholder="/path/to/workspace"></label>`;
  form.innerHTML =
    `<div class="form-title">add workspace</div>` +
    `<label class="fl">name<input class="in" id="ar-name" placeholder="my-workspace"></label>` +
    pathField;
  openModal(form);
  appendProviderFields(form, {});
  const err = document.createElement("div");
  err.className = "form-err";
  err.id = "ar-err";
  form.appendChild(err);
  const actions = document.createElement("div");
  actions.className = "form-actions";
  actions.innerHTML =
    `<button class="ctl-btn" id="ar-cancel">cancel</button>` +
    `<button class="ctl-btn primary" id="ar-submit">add</button>`;
  form.appendChild(actions);
  form.querySelector("#ar-cancel").onclick = closeModal;
  const submit = form.querySelector("#ar-submit");
  submit.onclick = async () => {
    const name = form.querySelector("#ar-name").value.trim();
    const rootEl = form.querySelector("#ar-root");
    const root = rootEl ? rootEl.value.trim() : "";
    err.textContent = "";
    if (!name) { err.textContent = "name is required"; return; }
    if (!isK8s() && !root) { err.textContent = "name and path are required"; return; }
    const config = readProviderConfig(form);
    // k8s: the Workspace CRD requires >=1 source and >=1 provider (minItems:1),
    // each fully specified — an empty config would be rejected as a 422 (#150).
    // Validate up front so the user gets an inline error instead of a failed
    // apply.
    if (isK8s()) {
      const sources = config.sources || [];
      const providers = config.providers || [];
      if (!sources.length || sources.some((s) => !s.name || !s.repo || !s.path)) {
        err.textContent = "add at least one source with a name, repo, and path";
        return;
      }
      if (!providers.length) {
        err.textContent = "add at least one provider (name + kind)";
        return;
      }
    }
    submit.disabled = true;
    try {
      const body = { name, config };
      if (root) body.root = root;
      await api("POST", "/api/workspaces", body);
      closeModal();
      refreshFleet();
    } catch (e) {
      err.textContent = String(e.message || e);
      submit.disabled = false;
    }
  };
}

// --- Provider/workspace config form (backend-aware) -------------------------
//
// Local backend: a single provider (provider/base_url/api_key_from_env) + env.
// k8s config plane: a named-provider *list* + git *sources* + Secret-reference
// credentials + env — mapped to the WorkspaceConfig the config plane accepts.
// `appendProviderFields`/`readProviderConfig` dispatch on the active backend.

const PROVIDERS = ["", "ollama", "anthropic", "openai", "google", "bedrock", "vertex"];
const PROVIDER_KINDS = ["ollama", "anthropic", "openai", "google", "bedrock", "vertex"];

function appendProviderFields(form, cfg) {
  if (isK8s()) appendK8sWorkspaceFields(form, cfg);
  else appendLocalProviderFields(form, cfg);
}

function readProviderConfig(form) {
  return isK8s() ? readK8sWorkspaceConfig(form) : readLocalProviderConfig(form);
}

// Append provider/base_url/api-key/raw-env fields to `form`, prefilled from `cfg`.
function appendLocalProviderFields(form, cfg) {
  cfg = cfg || {};
  const opts = PROVIDERS.map((p) =>
    `<option value="${p}"${p === (cfg.provider || "") ? " selected" : ""}>${p || "(default)"}</option>`
  ).join("");
  const wrap = document.createElement("div");
  wrap.innerHTML =
    `<label class="fl">provider<select class="in" id="pc-provider">${opts}</select></label>` +
    `<label class="fl">base URL<input class="in" id="pc-baseurl" placeholder="http://host:11434"></label>` +
    `<label class="fl">API key from env var<input class="in" id="pc-keyenv" placeholder="e.g. ANTHROPIC_API_KEY"></label>` +
    `<div class="adv-toggle" id="pc-adv-toggle">▸ advanced env</div>` +
    `<div class="hidden" id="pc-adv"><div id="pc-env-rows"></div>` +
      `<span class="env-add" id="pc-env-add">+ add env var</span></div>`;
  form.appendChild(wrap);
  form.querySelector("#pc-baseurl").value = cfg.base_url || "";
  form.querySelector("#pc-keyenv").value = cfg.api_key_from_env || "";

  const rows = form.querySelector("#pc-env-rows");
  const addRow = (k, v) => {
    const row = document.createElement("div");
    row.className = "env-row";
    row.innerHTML = `<input class="in pc-k" placeholder="KEY"><input class="in pc-v" placeholder="VALUE"><button type="button">×</button>`;
    row.querySelector(".pc-k").value = k || "";
    row.querySelector(".pc-v").value = v || "";
    row.querySelector("button").onclick = () => row.remove();
    rows.appendChild(row);
  };
  for (const [k, v] of Object.entries(cfg.env || {})) addRow(k, v);
  const adv = form.querySelector("#pc-adv");
  const advToggle = form.querySelector("#pc-adv-toggle");
  advToggle.onclick = () => {
    adv.classList.toggle("hidden");
    advToggle.textContent = adv.classList.contains("hidden") ? "▸ advanced env" : "▾ advanced env";
  };
  form.querySelector("#pc-env-add").onclick = () => addRow("", "");
  if (Object.keys(cfg.env || {}).length) adv.classList.remove("hidden");
}

function readLocalProviderConfig(form) {
  const cfg = {};
  const provider = form.querySelector("#pc-provider").value.trim();
  const baseUrl = form.querySelector("#pc-baseurl").value.trim();
  const keyEnv = form.querySelector("#pc-keyenv").value.trim();
  if (provider) cfg.provider = provider;
  if (baseUrl) cfg.base_url = baseUrl;
  if (keyEnv) cfg.api_key_from_env = keyEnv;
  const env = {};
  for (const row of form.querySelectorAll(".env-row")) {
    const k = row.querySelector(".pc-k").value.trim();
    const v = row.querySelector(".pc-v").value.trim();
    if (k) env[k] = v;
  }
  if (Object.keys(env).length) cfg.env = env;
  return cfg;
}

// k8s config-plane editor: display name + sources[] + named providers[] (with
// Secret-reference credentials) + env. `cfg` prefills from a workspace summary
// (display_name/sources/providers/default_provider). Note: Secret references
// are never returned by the API, so on *edit* the secretName/key fields start
// blank — re-enter them to keep a provider's credentials (leaving them blank
// makes the provider keyless).
function appendK8sWorkspaceFields(form, cfg) {
  cfg = cfg || {};
  // The sources/provider row editors need more width than the default modal.
  const modalBox = form.closest(".modal");
  if (modalBox) modalBox.classList.add("wide");
  const wrap = document.createElement("div");
  wrap.innerHTML =
    `<label class="fl">display name<input class="in" id="wc-display" placeholder="Team A"></label>` +
    `<div class="cfg-section-label">sources</div><div id="wc-sources" class="cfg-rows"></div>` +
    `<span class="env-add" id="wc-src-add">+ add source</span>` +
    `<div class="cfg-section-label">providers</div><div id="wc-providers" class="cfg-rows"></div>` +
    `<span class="env-add" id="wc-prov-add">+ add provider</span>` +
    `<div class="adv-toggle" id="wc-adv-toggle">▸ advanced env</div>` +
    `<div class="hidden" id="wc-adv"><div id="wc-env-rows"></div>` +
      `<span class="env-add" id="wc-env-add">+ add env var</span></div>`;
  form.appendChild(wrap);
  form.querySelector("#wc-display").value = cfg.display_name || "";

  const srcRows = form.querySelector("#wc-sources");
  const addSrc = (s) => {
    s = s || {};
    const row = document.createElement("div");
    row.className = "cfg-row src-row";
    row.innerHTML =
      `<input class="in src-name" placeholder="name">` +
      `<input class="in src-repo" placeholder="git remote">` +
      `<input class="in src-ref" placeholder="ref (main)">` +
      `<input class="in src-path" placeholder="/work/name">` +
      `<button type="button" class="rm">×</button>`;
    row.querySelector(".src-name").value = s.name || "";
    row.querySelector(".src-repo").value = s.repo || "";
    row.querySelector(".src-ref").value = s.ref || "";
    row.querySelector(".src-path").value = s.path || "";
    row.querySelector(".rm").onclick = () => row.remove();
    srcRows.appendChild(row);
  };
  ((cfg.sources && cfg.sources.length) ? cfg.sources : [{}]).forEach(addSrc);
  form.querySelector("#wc-src-add").onclick = () => addSrc({});

  const provRows = form.querySelector("#wc-providers");
  const addProv = (p) => {
    p = p || {};
    const row = document.createElement("div");
    row.className = "cfg-row prov-row";
    const kindOpts = PROVIDER_KINDS.map((k) =>
      `<option value="${k}"${k === (p.kind || "") ? " selected" : ""}>${k}</option>`).join("");
    // On edit, a provider may have credentials that the API never returns; hint
    // that leaving the Secret fields blank clears them.
    const hasCreds = !!p.has_credentials;
    const secretPh = hasCreds ? "secretName (set — re-enter to keep)" : "secretName (optional)";
    row.innerHTML =
      `<input class="in prov-name" placeholder="name (e.g. planner)">` +
      `<select class="in prov-kind">${kindOpts}</select>` +
      `<input class="in prov-url" placeholder="base URL (optional)">` +
      `<input class="in prov-model" placeholder="model (optional)">` +
      `<input class="in prov-secret" placeholder="${secretPh}">` +
      `<input class="in prov-key" placeholder="key (optional)">` +
      `<label class="chk prov-def"><input type="radio" name="wc-default"> default</label>` +
      `<button type="button" class="rm">×</button>`;
    row.querySelector(".prov-name").value = p.name || "";
    row.querySelector(".prov-url").value = p.base_url || "";
    row.querySelector(".prov-model").value = p.model || "";
    if (p.credentials_ref) {
      row.querySelector(".prov-secret").value = p.credentials_ref.secret_name || "";
      row.querySelector(".prov-key").value = p.credentials_ref.key || "";
    }
    if (p.name && p.name === cfg.default_provider) {
      row.querySelector(".prov-def input").checked = true;
    }
    row.querySelector(".rm").onclick = () => row.remove();
    provRows.appendChild(row);
  };
  ((cfg.providers && cfg.providers.length) ? cfg.providers : [{}]).forEach(addProv);
  form.querySelector("#wc-prov-add").onclick = () => addProv({});

  const envRows = form.querySelector("#wc-env-rows");
  const addEnv = (k, v) => {
    const row = document.createElement("div");
    row.className = "env-row";
    row.innerHTML = `<input class="in pc-k" placeholder="KEY"><input class="in pc-v" placeholder="VALUE"><button type="button">×</button>`;
    row.querySelector(".pc-k").value = k || "";
    row.querySelector(".pc-v").value = v || "";
    row.querySelector("button").onclick = () => row.remove();
    envRows.appendChild(row);
  };
  for (const [k, v] of Object.entries(cfg.env || {})) addEnv(k, v);
  const adv = form.querySelector("#wc-adv");
  const advToggle = form.querySelector("#wc-adv-toggle");
  advToggle.onclick = () => {
    adv.classList.toggle("hidden");
    advToggle.textContent = adv.classList.contains("hidden") ? "▸ advanced env" : "▾ advanced env";
  };
  form.querySelector("#wc-env-add").onclick = () => addEnv("", "");
  if (Object.keys(cfg.env || {}).length) adv.classList.remove("hidden");
}

function readK8sWorkspaceConfig(form) {
  const cfg = {};
  const dn = form.querySelector("#wc-display").value.trim();
  if (dn) cfg.display_name = dn;

  const sources = [];
  for (const row of form.querySelectorAll(".src-row")) {
    const name = row.querySelector(".src-name").value.trim();
    const repo = row.querySelector(".src-repo").value.trim();
    const path = row.querySelector(".src-path").value.trim();
    const ref = row.querySelector(".src-ref").value.trim();
    if (!name && !repo && !path) continue;
    const s = { name, repo, path };
    if (ref) s.ref = ref;
    sources.push(s);
  }
  if (sources.length) cfg.sources = sources;

  const providers = [];
  let defaultProvider = null;
  for (const row of form.querySelectorAll(".prov-row")) {
    const name = row.querySelector(".prov-name").value.trim();
    if (!name) continue;
    const p = { name, kind: row.querySelector(".prov-kind").value };
    const url = row.querySelector(".prov-url").value.trim();
    const model = row.querySelector(".prov-model").value.trim();
    const secret = row.querySelector(".prov-secret").value.trim();
    const key = row.querySelector(".prov-key").value.trim();
    if (url) p.base_url = url;
    if (model) p.model = model;
    if (secret) p.credentials_ref = { secret_name: secret, key };
    if (row.querySelector(".prov-def input").checked) defaultProvider = name;
    providers.push(p);
  }
  if (providers.length) cfg.providers = providers;
  if (defaultProvider) cfg.default_provider = defaultProvider;

  const env = {};
  for (const row of form.querySelectorAll(".env-row")) {
    const k = row.querySelector(".pc-k").value.trim();
    const v = row.querySelector(".pc-v").value.trim();
    if (k) env[k] = v;
  }
  if (Object.keys(env).length) cfg.env = env;
  return cfg;
}

// --- Repo-settings modal ----------------------------------------------------

async function openRepoSettings(repo) {
  // Prefill: local reads the flat provider `config`; k8s reads the workspace
  // summary's top-level config (display_name/sources/providers/default_provider).
  // On k8s, Secret references and env are not returned by the API — re-enter
  // them to keep them (see the editor's hint).
  let cfg = {};
  try {
    const repos = await api("GET", "/api/workspaces");
    const found = (repos || []).find((r) => r.name === repo.name);
    cfg = isK8s() ? (found || {}) : ((found && found.config) || {});
  } catch (e) { showBanner(String(e.message || e)); return; }

  const runningCount = (repo.agents || []).filter((a) => isActive(a.status)).length;
  const form = document.createElement("div");
  form.innerHTML = `<div class="form-title">settings — ${escapeHtml(repo.name)}</div>`;
  openModal(form);
  appendProviderFields(form, cfg);
  const err = document.createElement("div");
  err.className = "form-err";
  form.appendChild(err);
  const actions = document.createElement("div");
  actions.className = "form-actions";
  actions.innerHTML = `<button class="ctl-btn" id="rs-cancel">cancel</button><button class="ctl-btn primary" id="rs-save">save</button>`;
  form.appendChild(actions);
  form.querySelector("#rs-cancel").onclick = closeModal;
  const save = form.querySelector("#rs-save");
  save.onclick = async () => {
    // Local applies synchronously and restarts the workspace's caliband; k8s
    // patches the Workspace CR and the operator re-reconciles (running agents
    // keep their pinned config), so no restart warning there.
    if (!isK8s() && runningCount > 0 &&
        !window.confirm(`Restart caliban for ${repo.name}? This stops ${runningCount} running agent(s).`)) {
      return;
    }
    save.disabled = true;
    try {
      await api("PUT", `/api/workspaces/${encodeURIComponent(repo.name)}/config`, readProviderConfig(form));
      closeModal();
      refreshFleet();
    } catch (e) {
      err.textContent = String(e.message || e);
      save.disabled = false;
    }
  };
}

// --- Launch-agent modal -----------------------------------------------------

function openLaunchModal(repoName) {
  const form = document.createElement("div");
  // k8s: an agent binds one of the workspace's named providers.
  const providerField = isK8s()
    ? `<label class="fl" id="la-provider-wrap">provider<select class="in" id="la-provider"></select></label>`
    : "";
  form.innerHTML =
    `<div class="form-title">launch agent</div>` +
    `<label class="fl">workspace<select class="in" id="la-repo"></select></label>` +
    providerField +
    `<label class="fl">task<textarea class="in" id="la-task" rows="3" placeholder="describe the task"></textarea></label>` +
    `<label class="chk"><input type="checkbox" id="la-wt" checked> worktree isolation</label>` +
    `<label class="chk"><input type="checkbox" id="la-interactive"> interactive (awaits your input)</label>` +
    `<div class="adv-toggle" id="la-adv-toggle">▸ advanced</div>` +
    `<div class="hidden" id="la-adv">` +
      `<label class="fl">label<input class="in" id="la-label"></label>` +
      `<label class="fl">model<input class="in" id="la-model"></label>` +
      `<label class="fl">tools<input class="in" id="la-tools" placeholder="comma,separated"></label>` +
    `</div>` +
    `<div class="form-err" id="la-err"></div>` +
    `<div class="form-actions">` +
      `<button class="ctl-btn" id="la-cancel">cancel</button>` +
      `<button class="ctl-btn primary" id="la-submit">spawn</button>` +
    `</div>`;
  openModal(form);
  // Populate the repo picker from the reachable repos, defaulting to the one
  // whose launch button was clicked. Options are built via the DOM so repo
  // names are never interpolated into markup.
  const repoSel = form.querySelector("#la-repo");
  for (const r of healthyWorkspaces) {
    const opt = document.createElement("option");
    opt.value = r;
    opt.textContent = r;
    if (r === repoName) opt.selected = true;
    repoSel.appendChild(opt);
  }

  // k8s: populate the provider picker from the selected workspace's providers,
  // and keep it in sync when the workspace changes. Built via the DOM so
  // provider names are never interpolated into markup.
  const provSel = form.querySelector("#la-provider");
  const fillProviders = (wsName) => {
    if (!provSel) return;
    provSel.innerHTML = "";
    const ws = (lastFleet ? lastFleet.workspaces : []).find((w) => w.name === wsName);
    const providers = (ws && ws.providers) || [];
    const def = document.createElement("option");
    def.value = "";
    def.textContent = providers.length ? "(workspace default)" : "(default)";
    provSel.appendChild(def);
    for (const p of providers) {
      const opt = document.createElement("option");
      opt.value = p.name;
      opt.textContent = p.model ? `${p.name} · ${p.kind} · ${p.model}` : `${p.name} · ${p.kind}`;
      if (p.name === (ws && ws.default_provider)) opt.selected = true;
      provSel.appendChild(opt);
    }
  };
  if (provSel) {
    fillProviders(repoSel.value);
    repoSel.addEventListener("change", () => fillProviders(repoSel.value));
  }

  const adv = form.querySelector("#la-adv");
  const advToggle = form.querySelector("#la-adv-toggle");
  advToggle.onclick = () => {
    adv.classList.toggle("hidden");
    advToggle.textContent = adv.classList.contains("hidden") ? "▸ advanced" : "▾ advanced";
  };

  form.querySelector("#la-cancel").onclick = closeModal;
  const submit = form.querySelector("#la-submit");
  submit.onclick = async () => {
    const repo = form.querySelector("#la-repo").value.trim();
    const prompt = form.querySelector("#la-task").value.trim();
    const err = form.querySelector("#la-err");
    err.textContent = "";
    if (!repo || !prompt) { err.textContent = "workspace and task are required"; return; }

    const body = { prompt };
    if (!form.querySelector("#la-wt").checked) body.isolation = "shared";
    if (form.querySelector("#la-interactive").checked) body.interactive = true;
    if (provSel && provSel.value) body.provider_ref = provSel.value;
    const label = form.querySelector("#la-label").value.trim();
    const model = form.querySelector("#la-model").value.trim();
    const tools = form.querySelector("#la-tools").value.trim();
    if (label) body.label = label;
    if (model) body.model = model;
    if (tools) body.tool_allowlist = tools.split(",").map((s) => s.trim()).filter(Boolean);

    submit.disabled = true;
    try {
      const res = await api("POST", `/api/workspaces/${encodeURIComponent(repo)}/agents`, body);
      closeModal();
      if (res && res.agent_id) selectAgent(res.agent_id);
      else refreshFleet();
    } catch (e) {
      err.textContent = String(e.message || e);
      submit.disabled = false;
    }
  };
}

// --- Row actions ------------------------------------------------------------

// UI partition: which agents are "alive" (offer kill) vs finished (offer
// respawn/remove). Intentionally broader than the backend's stream-oriented
// AgentStatus::is_active (Spawning|Running) — `idle` is awaiting input but
// still killable; the remove path is kill → terminal → remove.
const ACTIVE_STATUSES = new Set(["spawning", "running", "idle"]);
function isActive(status) { return ACTIVE_STATUSES.has(status); }

// Build a small inline action button. `onClick` receives the button element so
// the handler can disable it while the request is in flight.
function actionBtn(text, cls, onClick) {
  const b = document.createElement("button");
  b.className = "act-btn" + (cls ? " " + cls : "");
  b.textContent = text;
  b.onclick = (e) => { e.stopPropagation(); onClick(b); };
  return b;
}

// Fire a mutating request. `confirmMsg` (when set) gates via window.confirm.
// Disables `btn` while in flight; surfaces failures in the banner; refreshes on success.
async function rowAction(method, path, confirmMsg, btn) {
  if (confirmMsg && !window.confirm(confirmMsg)) return;
  if (btn) btn.disabled = true;
  try {
    await api(method, path);
    refreshFleet();
  } catch (e) {
    showBanner(String(e.message || e));
    if (btn) btn.disabled = false;
  }
}

async function refreshFleet() {
  try {
    const res = await fetch("/api/fleet");
    const fleet = await res.json();
    hostEl.textContent = fleet.host;
    renderFleet(fleet);
  } catch (e) {
    fleetListEl.innerHTML = `<div class="health unreachable">fleet unreachable: ${e}</div>`;
  }
}

// Human-ish elapsed string from an RFC-3339 timestamp (client-side, no new data).
function elapsed(startedAt) {
  const ms = Date.now() - new Date(startedAt).getTime();
  if (!isFinite(ms) || ms < 0) return "";
  const s = Math.floor(ms / 1000);
  if (s < 60) return `${s}s`;
  const m = Math.floor(s / 60);
  if (m < 60) return `${m}m`;
  return `${Math.floor(m / 60)}h`;
}

// Derivable per-agent meta line (no cost/turns — not in the snapshot).
function agentMeta(agent) {
  const e = elapsed(agent.started_at);
  if (!e) return "";
  if (agent.status === "idle") return ` · idle ${e}`;
  if (isActive(agent.status)) return ` · ${e}`;
  return ` · ${agent.status} ${e} ago`;
}

// A workspace can accept new agents when it's reconciled Ready (k8s) or its
// caliband is reachable (local).
function workspaceLaunchable(r) {
  return r.status ? r.status.phase === "Ready" : r.health.state === "healthy";
}

function renderFleet(fleet) {
  lastFleet = fleet;
  healthyWorkspaces = fleet.workspaces
    .filter(workspaceLaunchable)
    .map((r) => r.name);
  const agentTotal = fleet.workspaces.reduce((n, r) => n + r.agents.length, 0);
  const rc = fleet.workspaces.length;
  document.getElementById("fleet-count").textContent =
    `fleet · ${rc} workspace${rc === 1 ? "" : "s"} · ${agentTotal} agent${agentTotal === 1 ? "" : "s"}`;
  if (!fleet.workspaces.length) {
    fleetListEl.innerHTML = `<div class="muted">no workspaces registered</div>`;
    return;
  }
  // Preserve in-progress agent-input typing across this poll rebuild.
  const savedInputs = {};
  let focusedAgentId = null;
  let focusedCaret = null;
  for (const el of fleetListEl.querySelectorAll(".agent-input .in")) {
    if (el.value) savedInputs[el.dataset.agentId] = el.value;
    if (el === document.activeElement) {
      focusedAgentId = el.dataset.agentId;
      focusedCaret = el.selectionStart;
    }
  }
  fleetListEl.innerHTML = "";
  for (const repo of fleet.workspaces) {
    const box = document.createElement("div");
    box.className = "repo";
    const healthy = repo.health.state === "healthy";
    const healthTxt = healthy ? "healthy" : `unreachable: ${repo.health.reason || ""}`;

    const head = document.createElement("div");
    head.className = "repo-head";
    const name = document.createElement("span");
    name.className = "name";
    name.textContent = repo.name;
    head.appendChild(name);
    const acts = document.createElement("div");
    acts.className = "repo-head-actions";
    // Registry controls (configure, remove) only when the backend has an admin
    // plane wired.
    if (caps.admin) {
      const gear = document.createElement("button");
      gear.className = "gear";
      gear.textContent = "⚙";
      gear.onclick = () => openRepoSettings(repo);
      acts.appendChild(gear);
      acts.appendChild(actionBtn("remove", "danger", (b) =>
        rowAction("DELETE", `/api/workspaces/${encodeURIComponent(repo.name)}`,
                  `Remove workspace ${repo.name}?`, b)));
    }
    head.appendChild(acts);
    box.appendChild(head);

    // k8s workspaces carry a reconciliation status (pending/reconciling/ready/
    // failed) — shown as a pill with the failure message on hover. Local
    // workspaces show caliband reachability instead.
    if (repo.status) {
      const phase = (repo.status.phase || "").toLowerCase();
      const pill = document.createElement("div");
      pill.className = `ws-status ${phase}`;
      pill.textContent = phase;
      if (repo.status.message) pill.title = repo.status.message;
      box.appendChild(pill);
    } else {
      const health = document.createElement("div");
      health.className = `health ${healthy ? "healthy" : "unreachable"}`;
      health.textContent = healthTxt;
      box.appendChild(health);
    }

    const sourceNames = (repo.sources || []).map((s) => s.name);
    if (sourceNames.length) {
      const src = document.createElement("div");
      src.className = "sources";
      src.textContent = `sources: ${sourceNames.join(", ")}`;
      box.appendChild(src);
    }

    // k8s: the workspace's named providers, marking the default with *.
    const providerNames = (repo.providers || [])
      .map((p) => p.name + (p.name === repo.default_provider ? "*" : ""));
    if (providerNames.length) {
      const prov = document.createElement("div");
      prov.className = "sources";
      prov.textContent = `providers: ${providerNames.join(", ")}`;
      box.appendChild(prov);
    }

    if (workspaceLaunchable(repo)) {
      const launch = document.createElement("button");
      launch.className = "ctl-btn launch";
      launch.textContent = "＋ launch agent";
      launch.onclick = () => openLaunchModal(repo.name);
      box.appendChild(launch);
    }

    if (!repo.agents.length) {
      const none = document.createElement("div");
      none.className = "muted";
      none.textContent = "  (no agents)";
      box.appendChild(none);
    }
    for (const agent of repo.agents) {
      box.appendChild(renderAgent(agent));
    }
    fleetListEl.appendChild(box);
  }
  // Restore any preserved input value + focus/caret onto the rebuilt rows.
  for (const el of fleetListEl.querySelectorAll(".agent-input .in")) {
    const id = el.dataset.agentId;
    if (savedInputs[id] !== undefined) el.value = savedInputs[id];
    if (id === focusedAgentId) {
      el.focus();
      if (focusedCaret != null) el.setSelectionRange(focusedCaret, focusedCaret);
    }
  }
  if (streamCtx) {
    const a = findAgent(streamCtx.id);
    if (a) {
      streamCtx.name = a.name;
      streamCtx.status = a.status;
      paintStreamHead();
    }
  }
}

function renderAgent(agent) {
  const row = document.createElement("div");
  row.className = "agent" + (agent.id === selectedAgent ? " selected" : "");

  const wt = agent.isolated ? `<span class="wt">⌥ worktree</span>` : `<span class="wt">shared</span>`;
  const info = document.createElement("span");
  info.innerHTML =
    `<span class="name">${escapeHtml(agent.name)}</span> ${wt}<br>` +
    `<span class="id">${escapeHtml(agent.id)}</span><span class="meta">${escapeHtml(agentMeta(agent))}</span>`;

  const right = document.createElement("span");
  right.className = "agent-right";
  const badge = document.createElement("span");
  badge.className = `badge ${agent.status}`;
  badge.textContent = agent.status;
  right.appendChild(badge);

  const acts = document.createElement("div");
  acts.className = "acts";
  const aid = encodeURIComponent(agent.id);
  if (isActive(agent.status)) {
    acts.appendChild(actionBtn("kill", "danger", (b) =>
      rowAction("POST", `/api/agents/${aid}/kill`, `Kill agent ${agent.name}?`, b)));
  } else {
    acts.appendChild(actionBtn("respawn", "", (b) =>
      rowAction("POST", `/api/agents/${aid}/respawn`, null, b)));
    acts.appendChild(actionBtn("remove", "danger", (b) =>
      rowAction("DELETE", `/api/agents/${aid}`, `Remove agent ${agent.name}?`, b)));
  }
  right.appendChild(acts);

  row.appendChild(info);
  row.appendChild(right);
  row.onclick = () => selectAgent(agent.id);
  if (agent.interactive && agent.status === "idle") {
    const box = document.createElement("div");
    box.className = "agent-input";
    const input = document.createElement("input");
    input.className = "in";
    input.placeholder = "send a message…";
    input.dataset.agentId = agent.id; // so the poll rebuild can restore typing
    const send = document.createElement("button");
    send.textContent = "send";
    const end = document.createElement("button");
    end.className = "end";
    end.textContent = "end input";
    const doSend = async () => {
      const text = input.value.trim();
      if (!text) return;
      send.disabled = true;
      try {
        await api("POST", `/api/agents/${encodeURIComponent(agent.id)}/input`, { text });
        input.value = "";
        selectAgent(agent.id); // (re)open the stream to watch the resumed turn
      } catch (e) {
        showBanner(String(e.message || e));
        send.disabled = false;
      }
    };
    send.onclick = (e) => { e.stopPropagation(); doSend(); };
    input.onclick = (e) => e.stopPropagation();
    input.onkeydown = (e) => { if (e.key === "Enter") { e.stopPropagation(); doSend(); } };
    end.onclick = async (e) => {
      e.stopPropagation();
      end.disabled = true;
      try {
        await api("POST", `/api/agents/${encodeURIComponent(agent.id)}/end-input`);
        refreshFleet();
      } catch (err) {
        showBanner(String(err.message || err));
        end.disabled = false;
      }
    };
    box.appendChild(input);
    box.appendChild(send);
    box.appendChild(end);
    row.appendChild(box);
  }
  return row;
}

function openEmptyStream() {
  selectedAgent = null;
  streamCtx = null;
  if (evtSource) { evtSource.close(); evtSource = null; }
  streamHeadEl.classList.add("hidden");
  streamLogEl.innerHTML =
    `<div class="empty"><div class="empty-title">No agent selected</div>` +
    `<div class="empty-hint">Pick an agent from the fleet to stream its live output.</div></div>`;
}

function paintStreamHead() {
  if (!streamCtx) return;
  const c = streamCtx;
  const badge = `<span class="badge ${c.status}">${escapeHtml(c.status)}</span>`;
  const model = c.model ? `<span class="sh-meta">${escapeHtml(c.model)}</span>` : "";
  // No cost: caliban reports token usage, not USD, so the normalizer leaves
  // cost_usd at 0.0 — showing "$0.0000" was misleading. Surface outcome + turns.
  const meta = c.turns != null
    ? `<span class="sh-meta">· ${escapeHtml(c.outcome || "done")} · ${c.turns} turns</span>`
    : `<span class="sh-meta">· running</span>`;
  const dot = streamIsLive()
    ? `<span class="sh-conn live">● live</span>` : `<span class="sh-conn closed">⚠ closed</span>`;
  streamHeadEl.innerHTML =
    `<button class="sh-back" title="back to fleet">← fleet</button>` +
    `<span class="sh-name">${escapeHtml(c.name)}</span> ${badge} ${model} ${meta} ${dot}`;
  streamHeadEl.querySelector(".sh-back").onclick = () => document.body.classList.remove("show-stream");
  streamHeadEl.classList.remove("hidden");
}

function selectAgent(id) {
  selectedAgent = id;
  document.body.classList.add("show-stream"); // responsive: reveal the stream pane
  const a = findAgent(id);
  streamCtx = { id, name: (a && a.name) || id, status: (a && a.status) || "", model: null, cost: null, turns: null, outcome: null, lastSeq: 0, terminal: false };
  streamEvents = [];
  expandedTools.clear();
  refreshFleet();
  streamLogEl.innerHTML = "";
  if (reconnectTimer) { clearTimeout(reconnectTimer); reconnectTimer = null; }
  if (evtSource) { evtSource.close(); evtSource = null; }
  openAgentStream(id, 0);
  paintStreamHead();
}

// Open (or reopen) the SSE stream from `fromSeq`. We manage reconnection
// ourselves rather than leaning on EventSource's built-in retry: the built-in
// path always reopens the *original* URL (`from=0`), so on a terminal agent —
// whose stream the server closes for good — it would loop forever, re-replaying
// the whole history and growing the timeline without bound. Instead we resume
// from the last seq we saw, and stop entirely once the agent is terminal.
function openAgentStream(id, fromSeq) {
  if (evtSource) { evtSource.close(); evtSource = null; }
  evtSource = new EventSource(`/api/agents/${encodeURIComponent(id)}/stream?from=${fromSeq}`);
  evtSource.onopen = () => paintStreamHead();
  evtSource.onmessage = (e) => appendEvent(JSON.parse(e.data));
  // The backend self-heals a slow-consumer gap (replays the missed events from
  // the durable store) and sends this `gap` signal so we can show it happened.
  evtSource.addEventListener("gap", (e) => {
    let info = {};
    try { info = JSON.parse(e.data); } catch { /* ignore malformed */ }
    // Fold the recovery notice into the timeline like any other marker.
    streamEvents.push({ seq: -1, kind: {
      kind: "store_persist_failed",
      lost_seq: info.skipped ?? 0,
      detail: "recovered dropped events from history",
    } });
    renderTimeline();
  });
  evtSource.onerror = () => {
    paintStreamHead();
    // Take control of the closed connection (default retry would replay from 0).
    if (evtSource) { evtSource.close(); evtSource = null; }
    // A terminal agent's stream is closed permanently — do NOT reconnect. Also
    // bail if the user has since selected a different agent.
    if (!streamCtx || streamCtx.id !== id || streamCtx.terminal) return;
    // Live agent, transient drop: reconnect once from the next unseen seq (so we
    // don't replay already-seen events), after a small backoff.
    if (reconnectTimer) return;
    reconnectTimer = setTimeout(() => {
      reconnectTimer = null;
      if (streamCtx && streamCtx.id === id && !streamCtx.terminal) openAgentStream(id, streamCtx.lastSeq + 1);
    }, 1000);
  };
}

function appendEvent(ev) {
  // Dedup by seq: a reconnect (or the server's own replay) may re-deliver events
  // we've already folded in. Real events carry seq >= 1; synthetic UI markers
  // (the `gap` notice) use seq -1 and are exempt.
  if (streamCtx && ev.seq > 0) {
    if (ev.seq <= streamCtx.lastSeq) return;
    streamCtx.lastSeq = ev.seq;
  }
  streamEvents.push(ev);
  const k = ev.kind;
  if (streamCtx) {
    if (k.kind === "agent_init") streamCtx.model = k.model;
    else if (k.kind === "agent_finished") {
      streamCtx.cost = k.cost_usd; streamCtx.turns = k.turns; streamCtx.outcome = k.outcome;
      streamCtx.terminal = true;
    } else if (k.kind === "agent_gone") {
      streamCtx.terminal = true;
    } else if (k.kind === "status_changed") streamCtx.status = k.to;
    paintStreamHead();
  }
  renderTimeline();
}

// Fold the ordered event list into a flat, chronological segment list. Total by
// construction: every event becomes a segment or a raw fallback. Consecutive
// assistant `output` chunks coalesce into one block; tool spans pair
// ToolStarted→ToolFinished by name (FIFO for repeats). We render a flat timeline
// rather than per-turn groups because `EventKind` carries no turn-boundary event
// (caliban's TurnStart isn't normalized to a variant) — the true turn *count*
// is shown in the header from `AgentFinished.turns`.
function groupEvents(events) {
  const segs = [];
  let out = null; // current coalescing output block { kind:"out", text:"" }
  const openById = {}; // tool_use_id -> tool row awaiting its finish
  const openFifo = []; // open rows in start order (fallback when id is absent)
  const flushOut = () => { out = null; };
  for (const ev of events) {
    const k = ev.kind, seq = ev.seq;
    switch (k.kind) {
      case "agent_init":
        flushOut();
        segs.push({ kind: "init", model: k.model, tools: k.tools || [] });
        break;
      case "output":
        if (!out) { out = { kind: "out", text: "" }; segs.push(out); }
        out.text += k.chunk;
        break;
      case "tool_started": {
        flushOut();
        const row = { kind: "tool", seq, id: k.id, name: k.name, input: k.input, ok: null };
        segs.push(row);
        if (k.id) openById[k.id] = row;
        openFifo.push(row);
        break;
      }
      case "tool_finished": {
        // caliban's ToolCallEnd carries the tool_use_id but no name, so pair the
        // finish to its start on `id`. Fall back to the oldest still-open tool
        // (FIFO) for pre-#106 events that carry no id.
        let row = k.id ? openById[k.id] : null;
        if (!row) row = openFifo.find((r) => r.ok === null);
        if (row) {
          row.ok = k.ok;
          if (row.id) delete openById[row.id];
          const i = openFifo.indexOf(row);
          if (i >= 0) openFifo.splice(i, 1);
        }
        break;
      }
      case "status_changed":
        flushOut();
        segs.push({ kind: "status", from: k.from, to: k.to });
        break;
      case "agent_finished":
        flushOut();
        segs.push({ kind: "finished", outcome: k.outcome, cost: k.cost_usd, turns: k.turns });
        break;
      case "store_persist_failed":
        flushOut();
        segs.push({ kind: "sys", text: `recovered dropped events (seq ${k.lost_seq})` });
        break;
      default:
        flushOut();
        segs.push({ kind: "raw", text: k.kind });
    }
  }
  return segs;
}

function renderTimeline() {
  const nearBottom = streamEl.scrollHeight - streamEl.scrollTop - streamEl.clientHeight < 40;
  const segs = groupEvents(streamEvents);
  let html = "";
  for (const s of segs) {
    if (s.kind === "init") {
      html += `<div class="tl-init"><span class="k">init</span> `
        + `model=${escapeHtml(s.model || "?")} · ${s.tools.length} tools</div>`;
    } else if (s.kind === "tool") {
      const pill = s.ok === null ? `<span class="tl-pill run">running</span>`
        : s.ok ? `<span class="tl-pill ok">ok</span>` : `<span class="tl-pill fail">fail</span>`;
      const open = expandedTools.has(s.seq);
      html += `<div class="tl-tool" data-seq="${s.seq}">`
        + `<div class="tl-tool-h"><span class="tool">▸ ${escapeHtml(s.name)}</span>${pill}</div>`
        + (open ? `<pre class="tl-input">${escapeHtml(JSON.stringify(s.input, null, 2))}</pre>` : "")
        + `</div>`;
    } else if (s.kind === "out") {
      if (s.text.trim()) html += `<div class="tl-out">${escapeHtml(s.text)}</div>`;
    } else if (s.kind === "status") {
      html += `<div class="tl-status muted">${escapeHtml(s.from)} → ${escapeHtml(s.to)}</div>`;
    } else if (s.kind === "finished") {
      html += `<div class="tl-fin fin">● ${escapeHtml(s.outcome)} · ${s.turns} turns</div>`;
    } else if (s.kind === "sys") {
      html += `<div class="tl-status muted">⚠ ${escapeHtml(s.text)}</div>`;
    } else {
      html += `<div class="ev muted">${escapeHtml(s.text)}</div>`;
    }
  }
  streamLogEl.innerHTML = html || `<div class="muted">waiting for events…</div>`;
  streamLogEl.querySelectorAll(".tl-tool").forEach((el) => {
    el.querySelector(".tl-tool-h").onclick = () => {
      const seq = Number(el.dataset.seq);
      if (expandedTools.has(seq)) expandedTools.delete(seq); else expandedTools.add(seq);
      renderTimeline();
    };
  });
  if (nearBottom) streamEl.scrollTop = streamEl.scrollHeight;
}

function escapeHtml(s) {
  return String(s).replace(/[&<>]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;" })[c]);
}

document.getElementById("add-repo-btn").onclick = openAddRepoModal;
openEmptyStream();
// Fetch backend capabilities once, gate the chrome, then render. On k8s this
// switches the config UI to the named-provider/Secret-reference form and the
// reconciliation-status pills. (#143)
loadCapabilities().then(() => {
  applyCapabilities();
  refreshFleet();
});
setInterval(refreshFleet, 3000);

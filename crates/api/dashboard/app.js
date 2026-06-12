// Prospero dashboard — vanilla JS, no build step.
// Lists repos with their agents grouped underneath, polls the fleet, and
// live-streams a selected agent's events over SSE.

const fleetEl = document.getElementById("fleet");
const fleetListEl = document.getElementById("fleet-list");
const streamEl = document.getElementById("stream");
const hostEl = document.getElementById("host");
const connEl = document.getElementById("conn");
const modalRoot = document.getElementById("modal-root");

let selectedAgent = null;
let evtSource = null;
let healthyRepos = []; // names of reachable repos, for the launch picker

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
  form.innerHTML =
    `<div class="form-title">add repo</div>` +
    `<label class="fl">name<input class="in" id="ar-name" placeholder="my-repo"></label>` +
    `<label class="fl">path<input class="in" id="ar-root" placeholder="/path/to/repo"></label>`;
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
    const root = form.querySelector("#ar-root").value.trim();
    err.textContent = "";
    if (!name || !root) { err.textContent = "name and path are required"; return; }
    submit.disabled = true;
    try {
      await api("POST", "/api/repos", { name, root, config: readProviderConfig(form) });
      closeModal();
      refreshFleet();
    } catch (e) {
      err.textContent = String(e.message || e);
      submit.disabled = false;
    }
  };
}

// --- Provider-config form fields (shared by add-repo + repo-settings) --------

const PROVIDERS = ["", "ollama", "anthropic", "openai", "google", "bedrock", "vertex"];

// Append provider/base_url/api-key/raw-env fields to `form`, prefilled from `cfg`.
function appendProviderFields(form, cfg) {
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

// Read the provider config object back out of the fields appendProviderFields added.
function readProviderConfig(form) {
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

// --- Repo-settings modal ----------------------------------------------------

async function openRepoSettings(repo) {
  let cfg = {};
  try {
    const repos = await api("GET", "/api/repos");
    const found = (repos || []).find((r) => r.name === repo.name);
    cfg = (found && found.config) || {};
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
    if (runningCount > 0 &&
        !window.confirm(`Restart caliban for ${repo.name}? This stops ${runningCount} running agent(s).`)) {
      return;
    }
    save.disabled = true;
    try {
      await api("PUT", `/api/repos/${encodeURIComponent(repo.name)}/config`, readProviderConfig(form));
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
  form.innerHTML =
    `<div class="form-title">launch agent</div>` +
    `<label class="fl">repo<select class="in" id="la-repo"></select></label>` +
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
  for (const r of healthyRepos) {
    const opt = document.createElement("option");
    opt.value = r;
    opt.textContent = r;
    if (r === repoName) opt.selected = true;
    repoSel.appendChild(opt);
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
    if (!repo || !prompt) { err.textContent = "repo and task are required"; return; }

    const body = { prompt };
    if (!form.querySelector("#la-wt").checked) body.isolation = "shared";
    if (form.querySelector("#la-interactive").checked) body.interactive = true;
    const label = form.querySelector("#la-label").value.trim();
    const model = form.querySelector("#la-model").value.trim();
    const tools = form.querySelector("#la-tools").value.trim();
    if (label) body.label = label;
    if (model) body.model = model;
    if (tools) body.tool_allowlist = tools.split(",").map((s) => s.trim()).filter(Boolean);

    submit.disabled = true;
    try {
      const res = await api("POST", `/api/repos/${encodeURIComponent(repo)}/agents`, body);
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

function renderFleet(fleet) {
  healthyRepos = fleet.repos
    .filter((r) => r.health.state === "healthy")
    .map((r) => r.name);
  if (!fleet.repos.length) {
    fleetListEl.innerHTML = `<div class="muted">no repos registered</div>`;
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
  for (const repo of fleet.repos) {
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
    const gear = document.createElement("button");
    gear.className = "gear";
    gear.textContent = "⚙";
    gear.onclick = () => openRepoSettings(repo);
    acts.appendChild(gear);
    acts.appendChild(actionBtn("remove", "danger", (b) =>
      rowAction("DELETE", `/api/repos/${encodeURIComponent(repo.name)}`,
                `Remove repo ${repo.name}?`, b)));
    head.appendChild(acts);
    box.appendChild(head);

    const health = document.createElement("div");
    health.className = `health ${healthy ? "healthy" : "unreachable"}`;
    health.textContent = healthTxt;
    box.appendChild(health);

    if (healthy) {
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
}

function renderAgent(agent) {
  const row = document.createElement("div");
  row.className = "agent" + (agent.id === selectedAgent ? " selected" : "");

  const wt = agent.isolated ? `<span class="wt">⌥ worktree</span>` : `<span class="wt">shared</span>`;
  const info = document.createElement("span");
  info.innerHTML =
    `<span class="name">${escapeHtml(agent.name)}</span> ${wt}<br><span class="id">${escapeHtml(agent.id)}</span>`;

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

function selectAgent(id) {
  selectedAgent = id;
  refreshFleet();
  streamEl.innerHTML = "";
  if (evtSource) evtSource.close();
  connEl.textContent = `▶ streaming ${id}`;
  evtSource = new EventSource(`/api/agents/${id}/stream`);
  evtSource.onmessage = (e) => appendEvent(JSON.parse(e.data));
  evtSource.onerror = () => {
    connEl.textContent = `⚠ stream closed for ${id}`;
  };
}

function appendEvent(ev) {
  const line = document.createElement("div");
  line.className = "ev";
  const k = ev.kind;
  let body;
  switch (k.kind) {
    case "output":
      body = `<span class="out">${escapeHtml(k.chunk)}</span>`;
      break;
    case "tool_started":
      body = `<span class="tool">⚙ ${k.name}(${escapeHtml(JSON.stringify(k.input))})</span>`;
      break;
    case "tool_finished":
      body = `<span class="tool">${k.ok ? "✓" : "✗"} ${k.name}</span>`;
      break;
    case "agent_init":
      body = `<span class="k">init</span> model=${k.model} tools=[${k.tools.join(", ")}]`;
      break;
    case "agent_finished":
      body = `<span class="fin">● finished (${k.outcome}) — $${k.cost_usd.toFixed(4)}, ${k.turns} turns</span>`;
      break;
    case "status_changed":
      body = `<span class="k">status</span> ${k.from} → ${k.to}`;
      break;
    default:
      body = `<span class="k">${k.kind}</span>`;
  }
  line.innerHTML = body;
  streamEl.appendChild(line);
  streamEl.scrollTop = streamEl.scrollHeight;
}

function escapeHtml(s) {
  return String(s).replace(/[&<>]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;" })[c]);
}

document.getElementById("add-repo-btn").onclick = openAddRepoModal;
refreshFleet();
setInterval(refreshFleet, 3000);

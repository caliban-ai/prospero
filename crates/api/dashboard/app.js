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
let streamCtx = null; // { id, name, status, model, cost, turns, outcome } for the streamed agent
let streamEvents = []; // accumulated events for the selected agent (folded into the timeline)
const expandedTools = new Set(); // tool span keys (event seq) the user expanded

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
  form.innerHTML =
    `<div class="form-title">add workspace</div>` +
    `<label class="fl">name<input class="in" id="ar-name" placeholder="my-workspace"></label>` +
    `<label class="fl">path<input class="in" id="ar-root" placeholder="/path/to/workspace"></label>`;
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
      await api("POST", "/api/workspaces", { name, root, config: readProviderConfig(form) });
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
    const repos = await api("GET", "/api/workspaces");
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
  form.innerHTML =
    `<div class="form-title">launch agent</div>` +
    `<label class="fl">workspace<select class="in" id="la-repo"></select></label>` +
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

function renderFleet(fleet) {
  lastFleet = fleet;
  healthyWorkspaces = fleet.workspaces
    .filter((r) => r.health.state === "healthy")
    .map((r) => r.name);
  const agentTotal = fleet.workspaces.reduce((n, r) => n + r.agents.length, 0);
  const rc = fleet.workspaces.length;
  document.getElementById("fleet-count").textContent =
    `fleet ·  workspace · ${agentTotal} agent${agentTotal === 1 ? "" : "s"}`;
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
    const gear = document.createElement("button");
    gear.className = "gear";
    gear.textContent = "⚙";
    gear.onclick = () => openRepoSettings(repo);
    acts.appendChild(gear);
    acts.appendChild(actionBtn("remove", "danger", (b) =>
      rowAction("DELETE", `/api/workspaces/${encodeURIComponent(repo.name)}`,
                `Remove workspace ${repo.name}?`, b)));
    head.appendChild(acts);
    box.appendChild(head);

    const health = document.createElement("div");
    health.className = `health ${healthy ? "healthy" : "unreachable"}`;
    health.textContent = healthTxt;
    box.appendChild(health);

    const sourceNames = (repo.sources || []).map((s) => s.name);
    if (sourceNames.length) {
      const src = document.createElement("div");
      src.className = "sources";
      src.textContent = `sources: ${sourceNames.join(", ")}`;
      box.appendChild(src);
    }

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
  const cost = c.cost != null && c.turns != null
    ? `<span class="sh-meta">· ${escapeHtml(c.outcome || "done")} · ${c.turns} turns · $${c.cost.toFixed(4)}</span>`
    : `<span class="sh-meta">· running</span>`;
  const dot = streamIsLive()
    ? `<span class="sh-conn live">● live</span>` : `<span class="sh-conn closed">⚠ closed</span>`;
  streamHeadEl.innerHTML =
    `<button class="sh-back" title="back to fleet">← fleet</button>` +
    `<span class="sh-name">${escapeHtml(c.name)}</span> ${badge} ${model} ${cost} ${dot}`;
  streamHeadEl.querySelector(".sh-back").onclick = () => document.body.classList.remove("show-stream");
  streamHeadEl.classList.remove("hidden");
}

function selectAgent(id) {
  selectedAgent = id;
  document.body.classList.add("show-stream"); // responsive: reveal the stream pane
  const a = findAgent(id);
  streamCtx = { id, name: (a && a.name) || id, status: (a && a.status) || "", model: null, cost: null, turns: null, outcome: null };
  streamEvents = [];
  expandedTools.clear();
  refreshFleet();
  streamLogEl.innerHTML = "";
  if (evtSource) evtSource.close();
  evtSource = new EventSource(`/api/agents/${encodeURIComponent(id)}/stream`);
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
  evtSource.onerror = () => paintStreamHead();
  paintStreamHead();
}

function appendEvent(ev) {
  streamEvents.push(ev);
  const k = ev.kind;
  if (streamCtx) {
    if (k.kind === "agent_init") streamCtx.model = k.model;
    else if (k.kind === "agent_finished") {
      streamCtx.cost = k.cost_usd; streamCtx.turns = k.turns; streamCtx.outcome = k.outcome;
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
  const openTools = {}; // name -> [tool rows awaiting their finish]
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
        const row = { kind: "tool", seq, name: k.name, input: k.input, ok: null };
        segs.push(row);
        (openTools[k.name] = openTools[k.name] || []).push(row);
        break;
      }
      case "tool_finished": {
        const q = openTools[k.name];
        if (q && q.length) q.shift().ok = k.ok;
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
      html += `<div class="tl-fin fin">● ${escapeHtml(s.outcome)} · ${s.turns} turns · $${s.cost.toFixed(4)}</div>`;
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
refreshFleet();
setInterval(refreshFleet, 3000);

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
    `<label class="fl">path<input class="in" id="ar-root" placeholder="/path/to/repo"></label>` +
    `<div class="form-err" id="ar-err"></div>` +
    `<div class="form-actions">` +
      `<button class="ctl-btn" id="ar-cancel">cancel</button>` +
      `<button class="ctl-btn primary" id="ar-submit">add</button>` +
    `</div>`;
  openModal(form);
  form.querySelector("#ar-cancel").onclick = closeModal;
  const submit = form.querySelector("#ar-submit");
  submit.onclick = async () => {
    const name = form.querySelector("#ar-name").value.trim();
    const root = form.querySelector("#ar-root").value.trim();
    const err = form.querySelector("#ar-err");
    err.textContent = "";
    if (!name || !root) { err.textContent = "name and path are required"; return; }
    submit.disabled = true;
    try {
      await api("POST", "/api/repos", { name, root });
      closeModal();
      refreshFleet();
    } catch (e) {
      err.textContent = String(e.message || e);
      submit.disabled = false;
    }
  };
}

// --- Launch-agent modal -----------------------------------------------------

function openLaunchModal(repoName) {
  const form = document.createElement("div");
  form.innerHTML =
    `<div class="form-title">launch agent</div>` +
    `<label class="fl">repo<input class="in" id="la-repo" value="${escapeHtml(repoName)}"></label>` +
    `<label class="fl">task<textarea class="in" id="la-task" rows="3" placeholder="describe the task"></textarea></label>` +
    `<label class="chk"><input type="checkbox" id="la-wt" checked> worktree isolation</label>` +
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
  if (!fleet.repos.length) {
    fleetListEl.innerHTML = `<div class="muted">no repos registered</div>`;
    return;
  }
  fleetListEl.innerHTML = "";
  for (const repo of fleet.repos) {
    const box = document.createElement("div");
    box.className = "repo";
    const healthy = repo.health.state === "healthy";
    const healthTxt = healthy ? "healthy" : `unreachable: ${repo.health.reason || ""}`;

    const name = document.createElement("div");
    name.className = "name";
    name.textContent = repo.name;
    box.appendChild(name);

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
  if (isActive(agent.status)) {
    acts.appendChild(actionBtn("kill", "danger", (b) =>
      rowAction("POST", `/api/agents/${agent.id}/kill`, `Kill agent ${agent.name}?`, b)));
  } else {
    acts.appendChild(actionBtn("respawn", "", (b) =>
      rowAction("POST", `/api/agents/${agent.id}/respawn`, null, b)));
    acts.appendChild(actionBtn("remove", "danger", (b) =>
      rowAction("DELETE", `/api/agents/${agent.id}`, `Remove agent ${agent.name}?`, b)));
  }
  right.appendChild(acts);

  row.appendChild(info);
  row.appendChild(right);
  row.onclick = () => selectAgent(agent.id);
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

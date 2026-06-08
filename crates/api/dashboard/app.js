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
  const data = text ? JSON.parse(text) : null;
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
    box.innerHTML =
      `<div class="name">${repo.name}</div>` +
      `<div class="health ${healthy ? "healthy" : "unreachable"}">${healthTxt}</div>`;
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
  row.innerHTML =
    `<span><span class="name">${agent.name}</span> ${wt}<br><span class="id">${agent.id}</span></span>` +
    `<span class="badge ${agent.status}">${agent.status}</span>`;
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

refreshFleet();
setInterval(refreshFleet, 3000);

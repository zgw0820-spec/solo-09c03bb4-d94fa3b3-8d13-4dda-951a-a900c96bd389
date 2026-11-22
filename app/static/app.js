"use strict";

const $ = (id) => document.getElementById(id);

const stateBadge = $("stateBadge");
const stateText = $("stateText");
const searchForm = $("searchForm");
const searchBtn = $("searchBtn");
const errorBox = $("errorBox");
const resultMeta = $("resultMeta");
const resultsEl = $("results");
const emptyHint = $("emptyHint");
const eventList = $("eventList");

let searching = false;

const STATE_LABELS = {
  stopped: "已停止",
  starting: "启动中",
  ready: "就绪",
  stopping: "回收中",
};

function badgeClass(state) {
  return ["ready", "starting", "stopping"].includes(state) ? state : "failed";
}

function fmtMs(v) {
  return v === null || v === undefined ? "—" : `${v} ms`;
}

function esc(text) {
  return String(text).replace(/[&<>"']/g, (ch) => ({
    "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;",
  }[ch]));
}

function highlight(raw, term) {
  const safe = esc(raw);
  if (!term) return safe;
  const escapedTerm = term.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  try {
    return safe.replace(new RegExp(escapedTerm, "gi"), (m) => `<mark>${m}</mark>`);
  } catch (_) {
    return safe;
  }
}

async function refreshStatus() {
  if (!$("autoRefresh").checked) return;
  try {
    const resp = await fetch("/api/status", { cache: "no-store" });
    if (!resp.ok) return;
    const st = await resp.json();
    renderStatus(st);
  } catch (_) {
    /* transient; next tick retries */
  }
}

function renderStatus(st) {
  const state = st.state || "stopped";
  const failedRecently = state === "stopped" && st.last_error &&
    (st.last_error.kind === "startup_timeout" || st.last_error.kind === "startup_failed" ||
     st.last_error.kind === "backend_exited");
  const shownState = failedRecently ? "failed" : state;
  stateBadge.className = `badge ${badgeClass(shownState)}`;
  stateText.textContent = failedRecently ? "已停止（上次启动失败）" : (STATE_LABELS[state] || state);

  $("stState").textContent = state + (st.ready ? " · 可检索" : "");
  $("stPid").textContent = st.backend_pid ?? "—";
  $("stPort").textContent = st.port ?? "—";
  $("stRound").textContent = st.round_id ?? "—";
  $("stUptime").textContent = st.uptime_s === null || st.uptime_s === undefined ? "—" : `${st.uptime_s} s`;
  $("stWaiters").textContent = `${st.waiters ?? 0} / ${st.in_flight ?? 0}`;
  $("stIdle").textContent = st.idle_reap_in_s === null || st.idle_reap_in_s === undefined
    ? (state === "stopped" ? "—" : "计时中")
    : `${st.idle_reap_in_s} s`;
  const c = st.counters || {};
  $("stCounters").textContent = `${c.starts ?? 0} / ${c.failures ?? 0} / ${c.reaps ?? 0}`;

  if (st.settings) {
    if (document.activeElement !== $("startupDelay")) {
      $("startupDelay").value = st.settings.startup_delay_ms;
      $("startupDelayLabel").textContent = st.settings.startup_delay_ms;
    }
    if (document.activeElement !== $("responseDelay")) {
      $("responseDelay").value = st.settings.response_delay_ms;
      $("responseDelayLabel").textContent = st.settings.response_delay_ms;
    }
  }

  const events = (st.events || []).slice(-8).reverse();
  if (!events.length) {
    eventList.innerHTML = "<li>暂无</li>";
  } else {
    eventList.innerHTML = events.map((ev) => {
      const t = new Date(ev.at * 1000).toLocaleTimeString("zh-CN", { hour12: false });
      const extra = ev.kind ? ` ${esc(ev.kind)}` : "";
      const pid = ev.pid ? ` pid=${ev.pid}` : "";
      const round = ev.round ? ` #${ev.round}` : "";
      return `<li>${t} <span class="name">${esc(ev.event)}${round}${pid}${extra}</span></li>`;
    }).join("");
  }
}

function showError(err) {
  const kind = err.error || "error";
  const detail = err.detail || "";
  errorBox.innerHTML = `<div class="err-box"><b>${esc(kind)}</b> — ${esc(detail || "查询失败")}</div>`;
}

function clearError() {
  errorBox.innerHTML = "";
}

function renderResults(payload, term) {
  const rows = payload.results || [];
  resultMeta.innerHTML = [
    `命中 <b>${payload.count ?? 0}</b> 条`,
    `扫描 <b>${payload.total_scanned ?? 0}</b> 行`,
    `后端耗时 <b>${fmtMs(payload.backend_elapsed_ms)}</b>`,
    `后端 PID <b>${payload.backend_pid ?? "—"}</b>`,
  ].join(" ");
  resultsEl.innerHTML = rows.map((row) => {
    const log = row.log || {};
    const level = log.level || "";
    const head = [log.ts, level, log.service, log.host].filter(Boolean).map(esc).join(" · ");
    return `<li class="${esc(level)}">
      <div class="head"><span>#${row.line}</span><span>${head}</span></div>
      <div>${highlight(row.raw ?? "", term)}</div>
    </li>`;
  }).join("");
  emptyHint.style.display = rows.length ? "none" : "block";
  if (!rows.length) emptyHint.textContent = "没有匹配的日志行。";
}

searchForm.addEventListener("submit", async (event) => {
  event.preventDefault();
  if (searching) return;
  const term = $("query").value.trim();
  const limit = Math.max(1, Math.min(200, parseInt($("limit").value || "50", 10)));
  const timeout = Math.max(100, Math.min(60000, parseInt($("readyTimeout").value || "3000", 10)));
  searching = true;
  searchBtn.disabled = true;
  clearError();
  const started = performance.now();
  try {
    const resp = await fetch("/api/search", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ query: term, limit, ready_timeout_ms: timeout }),
    });
    const payload = await resp.json();
    if (!resp.ok) {
      showError(payload);
      return;
    }
    renderResults(payload, term);
  } catch (err) {
    showError({ error: "network_error", detail: String(err) });
  } finally {
    const elapsed = Math.round(performance.now() - started);
    resultMeta.insertAdjacentHTML("beforeend", ` 往返 <b>${elapsed} ms</b>`);
    searching = false;
    searchBtn.disabled = false;
    refreshStatus();
  }
});

$("startupDelay").addEventListener("input", (e) => { $("startupDelayLabel").textContent = e.target.value; });
$("responseDelay").addEventListener("input", (e) => { $("responseDelayLabel").textContent = e.target.value; });

$("saveDelays").addEventListener("click", async () => {
  const btn = $("saveDelays");
  btn.disabled = true;
  const timeoutInput = $("readyTimeout");
  const payload = {
    startup_delay_ms: parseInt($("startupDelay").value, 10),
    response_delay_ms: parseInt($("responseDelay").value, 10),
  };
  if (timeoutInput.dataset.dirty === "1") {
    payload.default_ready_timeout_ms = Math.max(
      100, Math.min(60000, parseInt(timeoutInput.value || "3000", 10))
    );
  }
  try {
    const resp = await fetch("/api/settings", {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    if (!resp.ok) {
      showError(await resp.json().catch(() => ({ detail: "保存失败" })));
    } else {
      timeoutInput.dataset.dirty = "0";
      clearError();
    }
  } catch (err) {
    showError({ error: "network_error", detail: String(err) });
  } finally {
    btn.disabled = false;
    refreshStatus();
  }
});

$("readyTimeout").addEventListener("input", (e) => { e.target.dataset.dirty = "1"; });

async function loadSettings() {
  try {
    const resp = await fetch("/api/settings", { cache: "no-store" });
    if (!resp.ok) return;
    const s = await resp.json();
    $("startupDelay").value = s.startup_delay_ms;
    $("startupDelayLabel").textContent = s.startup_delay_ms;
    $("responseDelay").value = s.response_delay_ms;
    $("responseDelayLabel").textContent = s.response_delay_ms;
    $("readyTimeout").value = s.default_ready_timeout_ms;
  } catch (_) { /* ignore */ }
}

loadSettings();
refreshStatus();
setInterval(refreshStatus, 800);

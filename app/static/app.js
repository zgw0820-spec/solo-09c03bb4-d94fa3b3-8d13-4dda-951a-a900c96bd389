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
const drillVoidHint = $("drillVoidHint");
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

function renderResults(payload, term, acceptedAt) {
  const rows = payload.results || [];
  drillVoidHint.style.display = "none";
  drillVoidHint.textContent = "";
  const stamp = acceptedAt
    ? `<span class="stamp">时点 ${esc(acceptedAt)}</span>`
    : "";
  resultMeta.innerHTML = [
    stamp,
    `命中 <b>${payload.count ?? 0}</b> 条`,
    `扫描 <b>${payload.total_scanned ?? 0}</b> 行`,
    `后端耗时 <b>${fmtMs(payload.backend_elapsed_ms)}</b>`,
    `后端 PID <b>${payload.backend_pid ?? "—"}</b>`,
  ].filter(Boolean).join(" ");
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

/* ------------------------------------------------------------------ drill */
/*
 * The clock is authoritative on the server.  The page only extrapolates
 * locally between polls so the display moves smoothly; every render is
 * re-anchored to the last snapshot.  Each drill search result is labelled
 * with the instant the gateway fixed when it accepted the search, which is
 * what makes pause/play observable without ever rewriting earlier answers.
 */
const drill = {
  snap: null,          // last /api/drill snapshot
  anchorLocal: 0,      // performance.now() when snap was received
  offsetMs: 0,         // log timezone offset implied by snap.now
  targetMs: null,      // pending seek target from the slider / markers
  following: false,
  followTimer: null,
  lastResult: null,    // { acceptedAtMs, term, payload, status }
  inFlight: null,      // { startedAt, acceptedAtMs?, voided }
  reqSeq: 0,
};

function parseOffsetMs(iso) {
  if (!iso) return 0;
  const m = /([+-])(\d{2}):(\d{2})$/.exec(iso);
  if (!m) return 0;
  const sign = m[1] === "-" ? -1 : 1;
  return sign * (parseInt(m[2], 10) * 60 + parseInt(m[3], 10)) * 60000;
}

function clockParts(ms) {
  const d = new Date(ms + drill.offsetMs);
  const p = (n, w = 2) => String(n).padStart(w, "0");
  return {
    time: `${p(d.getUTCHours())}:${p(d.getUTCMinutes())}:${p(d.getUTCSeconds())}.${p(d.getUTCMilliseconds(), 3)}`,
    short: `${p(d.getUTCHours())}:${p(d.getUTCMinutes())}:${p(d.getUTCSeconds())}`,
    date: `${d.getUTCFullYear()}-${p(d.getUTCMonth() + 1)}-${p(d.getUTCDate())}`,
  };
}

function drillNowMs() {
  const s = drill.snap;
  if (!s || !s.started) return null;
  if (!s.playing) return s.now_ms;
  return s.now_ms + Math.max(0, performance.now() - drill.anchorLocal);
}

function clearDrillResult() {
  drill.lastResult = null;
  drill.reqSeq += 1;
  drill.inFlight = null;
  resultMeta.textContent = "";
  resultsEl.innerHTML = "";
  drillVoidHint.style.display = "none";
  drillVoidHint.textContent = "";
  emptyHint.style.display = "block";
  emptyHint.textContent = "输入关键词后回车检索。首次检索会唤醒后端，稍等片刻。";
  renderDrillBanner();
}

function setDrillSnap(snap) {
  const previous = drill.snap;
  drill.snap = snap;
  drill.anchorLocal = performance.now();
  if (snap && snap.now) drill.offsetMs = parseOffsetMs(snap.now);
  // A fresh unanswered poll means a rewind may have voided our request.
  if (previous && snap && snap.started && drill.inFlight) {
    if (snap.generation !== drill.inFlight.generation) {
      drill.inFlight.voided = true;
    }
  }
  renderDrill();
}

function renderDrill() {
  const s = drill.snap;
  const started = !!(s && s.started);
  const now = drillNowMs();

  $("drillStart").disabled = false;
  $("drillStart").textContent = started ? "重新开始" : "开始演练";
  $("drillToggle").disabled = !started;
  $("drillToggle").textContent = s && s.playing ? "暂停" : "继续";
  $("drillSearchBtn").disabled = !started;

  const badge = $("drillBadge");
  const label = $("drillStateText");
  if (!started) {
    badge.className = "badge";
    label.textContent = "未开始";
    $("drillClock").className = "drill-clock";
    $("drillClock").firstChild.nodeValue = "--:--:--.000";
    $("drillOffset").textContent = "按真实经过时间推进";
    $("drillSub").textContent = "启动演练后，时钟从首条日志的时刻开始推进。";
    $("drillRange").disabled = true;
    $("drillRewind").disabled = true;
    $("drillGen").textContent = "";
    $("drillMin").textContent = "—";
    $("drillMax").textContent = "—";
    $("drillMarkers").innerHTML = '<span class="hint">启动演练后显示关键阶段。</span>';
    $("drillMarkers").dataset.signature = "empty";
    return;
  }

  badge.className = `badge ${s.playing ? "ready" : "starting"}`;
  label.textContent = s.playing ? "播放中（真实时间）" : "已暂停";
  const parts = clockParts(now);
  $("drillClock").className = `drill-clock${s.playing ? "" : " frozen"}`;
  $("drillClock").firstChild.nodeValue = parts.time;
  $("drillOffset").textContent = `${s.playing ? "真实时间 ×1" : "已冻结"} · ${s.now.slice(-6)}`;
  $("drillSub").textContent =
    `${parts.date} · 日志区间 ${s.first_at} → ${s.last_at} · 生成 ${s.generation}` +
    (s.voided ? " · 已发生过回拨" : "");
  $("drillGen").textContent = s.voided
    ? `回拨作废：累计 ${s.rewinds.reduce((n, r) => n + r.voided, 0)} 次在途检索`
    : "";

  const range = $("drillRange");
  range.disabled = false;
  range.min = "0";
  range.max = String(Math.max(1, s.last_at_ms - s.first_at_ms));
  if (drill.targetMs === null) {
    range.value = String(Math.max(0, now - s.first_at_ms));
  }
  $("drillMin").innerHTML = `起点 <b>${s.first_at.slice(11, 19)}</b>`;
  $("drillMax").innerHTML = `终点 <b>${s.last_at.slice(11, 19)}</b>`;
  $("drillRewind").disabled = drill.targetMs === null;

  const markers = s.markers || [];
  renderMarkers(markers, now);
  renderDrillBanner();
}

function renderMarkers(markers, now) {
  const host = $("drillMarkers");
  if (!markers.length) {
    if (host.dataset.signature !== "empty") {
      host.innerHTML = '<span class="hint">日志中没有识别到演练阶段标记。</span>';
      host.dataset.signature = "empty";
    }
    return;
  }
  const signature = markers.map((m) => m.at_ms).join(",");
  if (host.dataset.signature !== signature) {
    host.innerHTML = markers.map((m, i) => {
      return `<button class="marker" data-marker="${i}" title="回拨或快进到 ${esc(m.at)}">` +
        `<span class="dot">○</span> ${esc(m.label)}<span class="t">${esc(m.at.slice(11, 19))}</span></button>`;
    }).join("");
    host.dataset.signature = signature;
    host.querySelectorAll("[data-marker]").forEach((el) => {
      el.addEventListener("click", () => {
        const m = markers[parseInt(el.dataset.marker, 10)];
        setDrillTarget(m.at_ms);
      });
    });
  }
  host.querySelectorAll("[data-marker]").forEach((el) => {
    const m = markers[parseInt(el.dataset.marker, 10)];
    el.classList.toggle("past", m.at_ms <= now);
    el.querySelector(".dot").textContent = m.at_ms <= now ? "●" : "○";
  });
}

function setDrillTarget(atMs) {
  const s = drill.snap;
  if (!s || !s.started) return;
  drill.targetMs = Math.max(s.first_at_ms, Math.min(s.last_at_ms, Math.round(atMs)));
  const range = $("drillRange");
  range.value = String(drill.targetMs - s.first_at_ms);
  $("drillRewind").disabled = false;
  const now = drillNowMs();
  const verb = drill.targetMs < now ? "回拨" : "快进";
  $("drillSub").textContent =
    `待${verb}到 ${clockParts(drill.targetMs).time}（确认后才生效）`;
}

function renderDrillBanner() {
  const box = $("drillSearchBanner");
  const r = drill.lastResult;
  if (drill.inFlight) {
    const stamp = drill.inFlight.acceptedAtMs
      ? `已受理于 <b>${clockParts(drill.inFlight.acceptedAtMs).time}</b>`
      : "受理中";
    box.innerHTML = `<div class="drill-banner">演练检索进行中 · ${stamp} · 暂停/继续不会改变该时点</div>`;
    return;
  }
  if (r && r.status === "voided") {
    box.innerHTML =
      `<div class="drill-banner void"><b>结果作废</b> — 检索受理于 ` +
      `<b>${clockParts(r.acceptedAtMs).time}</b>，此后演练时钟被回拨，旧结果已丢弃。</div>`;
    return;
  }
  if (r && r.status === "ok") {
    box.innerHTML =
      `<div class="drill-banner">本次演练检索 <b>「${esc(r.term || "（全部）")}」</b> ` +
      `检索时点 <b>${esc(r.acceptedAt)}</b> — 只包含该时刻及之前的日志。</div>`;
    return;
  }
  box.innerHTML = "";
}

async function fetchDrill() {
  try {
    const resp = await fetch("/api/drill", { cache: "no-store" });
    if (!resp.ok) return;
    setDrillSnap(await resp.json());
  } catch (_) { /* transient */ }
}

function clearRenderedResults(message) {
  resultMeta.textContent = "";
  resultsEl.innerHTML = "";
  emptyHint.style.display = "none";
  drillVoidHint.style.display = message ? "block" : "none";
  drillVoidHint.textContent = message || "";
}

function clearLaterResults(newNowMs) {
  const r = drill.lastResult;
  if (!r || r.acceptedAtMs === undefined || r.acceptedAtMs === null) return;
  if (r.acceptedAtMs <= newNowMs) return;
  // Whatever is on screen was sliced at an instant later than the new clock;
  // drop it rather than letting a stale answer masquerade as current.
  drill.lastResult = { ...r, status: "voided" };
  clearRenderedResults("回拨后已清除按较晚时点查出的结果。");
  clearError();
  renderDrillBanner();
}

function showDrillError(payload, acceptedAtMs, term) {
  if (payload.error === "drill_rewound") {
    drill.lastResult = { status: "voided", acceptedAtMs, term };
    clearRenderedResults("该检索已被回拨作废，请按当前时点重新检索。");
    clearError();
    renderDrillBanner();
    return;
  }
  showError(payload);
}

async function runDrillSearch(term, { manual = true } = {}) {
  const s = drill.snap;
  if (!s || !s.started) return;
  if (drill.inFlight) return;
  const limit = Math.max(1, Math.min(200, parseInt($("limit").value || "50", 10)));
  const timeout = Math.max(100, Math.min(60000, parseInt($("readyTimeout").value || "3000", 10)));
  if (manual) {
    drillVoidHint.style.display = "none";
    clearError();
  }
  const seq = ++drill.reqSeq;
  drill.inFlight = {
    startedAt: performance.now(),
    acceptedAtMs: s.now_ms,
    generation: s.generation,
    voided: false,
  };
  renderDrillBanner();
  try {
    const resp = await fetch("/api/drill/search", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ query: term, limit, ready_timeout_ms: timeout }),
    });
    const payload = await resp.json();
    if (seq !== drill.reqSeq) return; // superseded by a rewind/clear
    if (!resp.ok) {
      const accepted = payload.accepted_at_ms ?? drill.inFlight?.acceptedAtMs;
      showDrillError(payload, accepted, term);
      return;
    }
    if (drill.inFlight?.voided || payload.voided) {
      drill.lastResult = { status: "voided", acceptedAtMs: payload.drill.accepted_at_ms, term };
      renderDrillBanner();
      return;
    }
    drill.lastResult = {
      status: "ok",
      acceptedAtMs: payload.drill.accepted_at_ms,
      acceptedAt: payload.drill.accepted_at,
      term,
      payload,
    };
    renderResults(payload, term, payload.drill.accepted_at);
  } catch (err) {
    showError({ error: "network_error", detail: String(err) });
  } finally {
    if (seq === drill.reqSeq) drill.inFlight = null;
    renderDrillBanner();
    fetchDrill();
  }
}

$("drillStart").addEventListener("click", async () => {
  const btn = $("drillStart");
  btn.disabled = true;
  try {
    const resp = await fetch("/api/drill/start", { method: "POST" });
    const payload = await resp.json();
    if (!resp.ok) {
      showError(payload);
      return;
    }
    clearDrillResult();
    drill.targetMs = null;
    drill.offsetMs = parseOffsetMs(payload.now);
    setDrillSnap(payload);
    if (drill.following) startFollowing();
  } catch (err) {
    showError({ error: "network_error", detail: String(err) });
  } finally {
    btn.disabled = false;
    renderDrill();
  }
});

$("drillToggle").addEventListener("click", async () => {
  const s = drill.snap;
  if (!s || !s.started) return;
  const btn = $("drillToggle");
  btn.disabled = true;
  try {
    const resp = await fetch("/api/drill/control", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ action: s.playing ? "pause" : "resume" }),
    });
    const payload = await resp.json();
    if (!resp.ok) {
      showError(payload);
      return;
    }
    drill.targetMs = null;
    drill.offsetMs = parseOffsetMs(payload.now);
    setDrillSnap(payload);
    if (payload.started && drill.following && payload.playing) startFollowing();
  } catch (err) {
    showError({ error: "network_error", detail: String(err) });
  } finally {
    btn.disabled = false;
    renderDrill();
  }
});
$("drillRange").addEventListener("input", (e) => {
  if (!drill.snap || !drill.snap.started) return;
  setDrillTarget(drill.snap.first_at_ms + parseInt(e.target.value, 10));
});

$("drillRewind").addEventListener("click", async () => {
  if (drill.targetMs === null || !drill.snap) return;
  const target = drill.targetMs;
  const targetText = clockParts(target).time;
  const ok = window.confirm(
    `确认回拨到 ${targetText}？\n\n` +
    "回拨后，尚未返回的旧演练检索会全部作废，本页按较晚时点查出的结果也会被清除。"
  );
  if (!ok) return;
  const btn = $("drillRewind");
  btn.disabled = true;
  const pending = drill.inFlight;
  try {
    const resp = await fetch("/api/drill/control", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ action: "seek", at_ms: target }),
    });
    const payload = await resp.json();
    if (!resp.ok) {
      showError(payload);
      return;
    }
    // Any search this page issued is now void; drop it locally too.
    drill.reqSeq += 1;
    drill.inFlight = null;
    drill.targetMs = null;
    drill.offsetMs = parseOffsetMs(payload.now);
    clearLaterResults(payload.now_ms);
    if (pending) {
      // The in-flight search was accepted at a later instant and is now
      // void; its response will be ignored when it arrives.
      drill.lastResult = {
        status: "voided",
        acceptedAtMs: pending.acceptedAtMs,
        term: $("drillQuery").value.trim(),
      };
    }
    setDrillSnap(payload);
  } catch (err) {
    showError({ error: "network_error", detail: String(err) });
  } finally {
    btn.disabled = false;
    renderDrill();
  }
});

$("drillSearchForm").addEventListener("submit", (event) => {
  event.preventDefault();
  runDrillSearch($("drillQuery").value.trim());
});

$("drillFollow").addEventListener("change", (e) => {
  drill.following = e.target.checked;
  if (drill.following) startFollowing();
  else stopFollowing();
});

function startFollowing() {
  stopFollowing();
  const tick = async () => {
    if (!drill.following) return;
    if (drill.snap && drill.snap.started && drill.snap.playing && !drill.inFlight) {
      await runDrillSearch($("drillQuery").value.trim(), { manual: false });
    }
    if (drill.following) drill.followTimer = setTimeout(tick, 1500);
  };
  drill.followTimer = setTimeout(tick, 1500);
}

function stopFollowing() {
  if (drill.followTimer) clearTimeout(drill.followTimer);
  drill.followTimer = null;
}

loadSettings();
refreshStatus();
fetchDrill();
setInterval(refreshStatus, 800);
setInterval(fetchDrill, 500);
setInterval(() => { if (drill.snap && drill.snap.started) renderDrill(); }, 100);


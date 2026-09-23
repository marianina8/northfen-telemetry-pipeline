// Northfen console: polls the run view while a scenario streams, draws one
// chart per sensor, and renders the anomaly feed. No dependencies.
(() => {
  "use strict";
  const base = document.body.dataset.base || "";
  const $ = (s, el = document) => el.querySelector(s);
  const esc = (s) => String(s ?? "").replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
  const human = (s) => ({ log_only: "log only", open_ticket: "open ticket", page_oncall: "page on-call" }[s] || String(s || "").replace(/_/g, " "));
  const svgNS = "http://www.w3.org/2000/svg";

  async function api(path, opts = {}) {
    const init = { credentials: "same-origin", ...opts };
    if (opts.body) init.headers = { "Content-Type": "application/json" };
    const r = await fetch(base + path, init);
    const j = await r.json().catch(() => ({}));
    if (r.status === 401) { location.href = base + "/login?next=" + encodeURIComponent(location.pathname.slice(base.length) + location.search); throw new Error("sign in required"); }
    if (!r.ok) { const e = new Error(j.error || r.statusText); e.status = r.status; e.body = j; throw e; }
    return j;
  }

  const actorInput = () => $("#actor");
  const actor = () => (actorInput()?.value || "engineer").trim() || "engineer";

  // ---- alert detail page -------------------------------------------------
  const ackPanel = $("#ack-panel");
  if (ackPanel) {
    ackPanel.addEventListener("click", async (ev) => {
      const b = ev.target.closest("[data-ack]");
      if (!b) return;
      const err = $("#ack-error");
      err.hidden = true;
      try {
        await api(`/api/alerts/${encodeURIComponent(ackPanel.dataset.alert)}/ack`, {
          method: "POST", body: JSON.stringify({ action: b.dataset.ack, note: $("#ack-note").value, name: actor() }),
        });
        location.reload();
      } catch (e) { err.textContent = e.message; err.hidden = false; }
    });
    return;
  }

  const root = $("#console");
  if (!root) return;

  // ---- console -------------------------------------------------------------
  const groups = [
    ["Normal operation: should stay quiet", ["baseline", "noisy_normal"]],
    ["Process problems: should flag", ["gradual_drift", "spike", "correlated_drift"]],
    ["Broken sensors: fault, no model call", ["sensor_fault"]],
    ["Edge case", ["threshold_boundary"]],
  ];
  let scenarios = [];
  let current = null;       // run id being shown
  let timer = null;
  let seen = new Set();     // alert ids already shown (for the arrival animation)
  let cards = new Map();    // alert id -> last rendered signature
  let lastView = null;

  const sel = $("#scenario"), runBtn = $("#run-btn"), desc = $("#scenario-desc"), runErr = $("#run-error");

  function describe() {
    const sc = scenarios.find((s) => s.name === sel.value);
    desc.textContent = sc ? `${sc.tool} · ~${sc.seconds}s of streaming. ${sc.description}` : "";
  }

  async function loadScenarios() {
    const { scenarios: list } = await api("/api/scenarios");
    scenarios = list;
    sel.innerHTML = "";
    for (const [label, cats] of groups) {
      const og = document.createElement("optgroup");
      og.label = label;
      for (const s of list.filter((x) => cats.includes(x.category))) {
        const o = document.createElement("option");
        o.value = s.name;
        o.textContent = s.title;
        og.appendChild(o);
      }
      if (og.children.length) sel.appendChild(og);
    }
    sel.value = "cmp07-correlated-drift";
    sel.disabled = false;
    runBtn.disabled = false;
    describe();
  }

  async function loadRuns() {
    const { runs, max_runs } = await api("/api/runs");
    const box = $("#runs");
    $("#runs-left").textContent = max_runs ? `· ${Math.max(0, max_runs - runs.length)} of ${max_runs} runs left` : "";
    if (!runs.length) { box.innerHTML = '<p class="muted small">No runs yet.</p>'; return runs; }
    box.innerHTML = runs.map((r) => `<a href="?run=${encodeURIComponent(r.id)}" data-run="${esc(r.id)}" class="${r.id === current ? "current" : ""}">${esc(r.title)} <span>· ${new Date(r.started_at).toLocaleTimeString()}</span></a>`).join("");
    return runs;
  }

  $("#runs").addEventListener("click", (ev) => {
    const a = ev.target.closest("a[data-run]");
    if (!a) return;
    ev.preventDefault();
    show(a.dataset.run, true);
  });

  sel.addEventListener("change", describe);
  runBtn.addEventListener("click", async () => {
    runErr.hidden = true;
    runBtn.disabled = true;
    try {
      const { run } = await api("/api/runs", { method: "POST", body: JSON.stringify({ scenario: sel.value }) });
      await show(run.id, true);
      loadRuns();
    } catch (e) {
      runErr.textContent = e.message;
      runErr.hidden = false;
      runBtn.disabled = false;
      if (e.status === 409 && e.body?.run) show(e.body.run.id, true);
    }
  });

  async function show(runId, push) {
    if (current !== runId) {
      current = runId;
      seen = new Set();
      cards = new Map();
      $("#charts").innerHTML = "";
      $("#alerts").innerHTML = "";
      document.querySelectorAll("#runs a").forEach((a) => a.classList.toggle("current", a.dataset.run === runId));
    }
    if (push) history.replaceState(null, "", `?run=${encodeURIComponent(runId)}`);
    clearTimeout(timer);
    await poll();
  }

  async function poll() {
    if (!current) return;
    const id = current;
    let v;
    try {
      v = await api(`/api/runs/${encodeURIComponent(id)}`);
    } catch (e) {
      if (id !== current) return;
      status(`<span>Couldn't load the run: ${esc(e.message)}. Retrying…</span>`);
      timer = setTimeout(poll, 3000);
      return;
    }
    if (id !== current) return;
    lastView = v;
    render(v);
    const live = v.streaming || v.settling;
    runBtn.disabled = live;
    if (live) timer = setTimeout(poll, 900);
    else loadRuns();
  }

  function status(html, spin) {
    $("#status-line").innerHTML = (spin ? '<span class="spinner" aria-hidden="true"></span>' : "") + html;
  }

  function render(v) {
    const r = v.run;
    for (const el of document.querySelectorAll("[data-k]")) el.textContent = (v.counts[el.dataset.k] || 0).toLocaleString();
    const done = Math.min(1, (v.tick + 1) / r.ticks);
    $("#progress-bar").style.width = `${(done * 100).toFixed(1)}%`;
    const pending = v.alerts.some((a) => a.explain_state === "pending");
    $("#st-stream").classList.toggle("active", v.streaming);
    $("#st-detect").classList.toggle("active", v.streaming);
    $("#st-explain").classList.toggle("active", pending);
    $("#st-dispatch").classList.toggle("active", !v.streaming && !pending && v.counts.actions > 0);
    $("#tool-title").textContent = `${v.tool} · ${human(v.tool_type)} · ${v.line}`;

    if (v.streaming) {
      const left = Math.max(0, Math.round((r.ticks - 1 - v.tick) * r.tick_seconds));
      status(`Streaming <b>${esc(r.title)}</b>: tick ${Math.max(0, v.tick + 1)} of ${r.ticks} · about ${left}s left${pending ? " · a window has been flagged, and the model will be called once correlated sensors settle" : ""}`, true);
    } else if (v.settling) {
      status("Stream finished. Waiting for the explain step (one model call per alert)…", true);
    } else {
      const n = v.alerts.length;
      status(n ? `Run complete: ${n} alert${n > 1 ? "s" : ""}, ${v.counts.model_calls || 0} model call${v.counts.model_calls === 1 ? "" : "s"}.` :
        `Run complete: <b>nothing flagged</b>. Every window stayed inside this tool's learned normal, and no model call was made.`);
    }

    const box = $("#charts");
    for (const s of v.sensors) {
      let card = box.querySelector(`[data-sensor="${CSS.escape(s.id)}"]`);
      if (!card) {
        card = document.createElement("article");
        card.className = "sensor";
        card.dataset.sensor = s.id;
        card.innerHTML = `<div class="sensor-head"><b>${esc(s.id)}</b><span class="meta">${esc(human(s.type))} · ${esc(s.unit)}</span><span class="state"></span><span class="now"></span></div><div class="chart"></div>`;
        box.appendChild(card);
      }
      const st = card.querySelector(".state");
      const flag = s.flags[s.flags.length - 1];
      st.className = `state ${s.state}`;
      st.textContent = { warmup: "◌ learning normal", normal: "● normal", anomaly: `▲ anomaly: ${flag?.rule || ""}`, sensor_fault: `✕ sensor fault: ${flag?.rule || ""}` }[s.state] || s.state;
      const last = s.points[s.points.length - 1];
      card.querySelector(".now").textContent = last ? (last.v == null ? "no reading" : `${fmt(last.v, s.decimals)} ${s.unit}${last.z != null ? ` · z ${last.z.toFixed(2)}` : ""}`) : "";
      drawChart(card.querySelector(".chart"), s, r.ticks, v.detector);
    }
    renderAlerts(v);
  }

  const fmt = (x, d) => Number(x).toFixed(Math.min(4, Math.max(0, d)));

  // ---- charts ----------------------------------------------------------------
  function el(name, attrs, parent) {
    const e = document.createElementNS(svgNS, name);
    for (const k in attrs) e.setAttribute(k, attrs[k]);
    if (parent) parent.appendChild(e);
    return e;
  }

  function drawChart(host, s, ticks, det) {
    const W = Math.max(280, host.clientWidth), H = 150;
    const L = 52, R = 10, T = 14, stripH = 6, axisH = 16;
    const plotB = H - axisH - stripH - 6;
    const x = (t) => L + (t / Math.max(1, ticks - 1)) * (W - L - R);
    const pts = s.points;
    let lo = Infinity, hi = -Infinity;
    for (const p of pts) {
      if (p.v != null) { lo = Math.min(lo, p.v); hi = Math.max(hi, p.v); }
      if (p.s > 0 && p.z != null) { lo = Math.min(lo, p.b - det.sustained_z * p.s); hi = Math.max(hi, p.b + det.sustained_z * p.s); }
    }
    if (!isFinite(lo)) { lo = 0; hi = 1; }
    if (hi - lo < 1e-9) { lo -= 1; hi += 1; }
    const pad = (hi - lo) * 0.08; lo -= pad; hi += pad;
    const y = (v) => T + (1 - (v - lo) / (hi - lo)) * (plotB - T);

    const svg = el("svg", { viewBox: `0 0 ${W} ${H}`, width: W, height: H, role: "img",
      "aria-label": `${s.id}: ${pts.length} readings, state ${human(s.state)}${s.flags.length ? `, first flag ${s.flags[0].rule} at tick ${s.flags[0].tick}` : ""}` });
    const pid = `hatch-${s.id}`;
    const defs = el("defs", {}, svg);
    const pat = el("pattern", { id: pid, width: 6, height: 6, patternUnits: "userSpaceOnUse", patternTransform: "rotate(45)" }, defs);
    el("line", { x1: 0, y1: 0, x2: 0, y2: 6, stroke: "var(--win-warm)", "stroke-width": 2 }, pat);
    const pidF = `hatchf-${s.id}`;
    const patF = el("pattern", { id: pidF, width: 6, height: 6, patternUnits: "userSpaceOnUse", patternTransform: "rotate(135)" }, defs);
    el("line", { x1: 0, y1: 0, x2: 0, y2: 6, stroke: "var(--serious)", "stroke-width": 2, opacity: 0.55 }, patF);

    // grid + y labels
    for (let i = 0; i <= 2; i++) {
      const v = lo + ((hi - lo) * i) / 2, yy = y(v);
      el("line", { x1: L, x2: W - R, y1: yy, y2: yy, stroke: "var(--grid)", "stroke-width": 1 }, svg);
      el("text", { x: L - 6, y: yy + 3, "text-anchor": "end", class: "ax" }, svg).textContent = fmt(v, Math.min(3, s.decimals));
    }
    // warm-up region
    const warmEnd = det.warmup - 0.5;
    el("rect", { x: x(0), y: T, width: Math.max(0, x(warmEnd) - x(0)), height: plotB - T, fill: `url(#${pid})`, opacity: 0.55 }, svg);
    el("text", { x: x(0) + 4, y: T + 10, class: "ax" }, svg).textContent = "learning normal";

    // learned-normal band (baseline ± sustained_z·σ) and baseline
    const scored = pts.filter((p) => p.s > 0 && p.z != null);
    if (scored.length > 1) {
      const up = scored.map((p) => `${x(p.t)},${y(p.b + det.sustained_z * p.s)}`);
      const dn = scored.map((p) => `${x(p.t)},${y(p.b - det.sustained_z * p.s)}`).reverse();
      el("polygon", { points: up.concat(dn).join(" "), fill: "var(--band)" }, svg);
      el("polyline", { points: scored.map((p) => `${x(p.t)},${y(p.b)}`).join(" "), fill: "none", stroke: "var(--base)", "stroke-width": 1, "stroke-dasharray": "3 3" }, svg);
    }
    // missing readings
    let run0 = null;
    const gaps = [];
    pts.forEach((p, i) => {
      if (p.v == null && run0 == null) run0 = p.t;
      if ((p.v != null || i === pts.length - 1) && run0 != null) { gaps.push([run0, p.v == null ? p.t : pts[i - 1].t]); run0 = null; }
    });
    for (const [a, b] of gaps) {
      el("rect", { x: x(a - 0.5), y: T, width: Math.max(2, x(b + 0.5) - x(a - 0.5)), height: plotB - T, fill: `url(#${pidF})` }, svg);
      el("text", { x: x(a - 0.5) + 3, y: plotB - 4, class: "ann" }, svg).textContent = "no data";
    }
    // the readings (gaps break the line)
    const seg = (filter, attrs) => {
      let d = "", pen = false;
      for (const p of pts) {
        if (p.v == null || !filter(p)) { pen = false; continue; }
        d += `${pen ? "L" : "M"}${x(p.t).toFixed(1)},${y(p.v).toFixed(1)}`;
        pen = true;
      }
      if (d) el("path", { d, fill: "none", "stroke-linejoin": "round", "stroke-linecap": "round", ...attrs }, svg);
    };
    seg(() => true, { stroke: "var(--series)", "stroke-width": 2 });
    const isAnom = (p) => p.r && p.r.some((r) => r === "spike" || r === "sustained" || r === "drift");
    const isFault = (p) => p.r && p.r.some((r) => r === "stuck");
    seg(isAnom, { stroke: "var(--critical)", "stroke-width": 2.5 });
    seg(isFault, { stroke: "var(--serious)", "stroke-width": 3 });
    for (const p of pts) if (isAnom(p) && p.r.includes("spike") && p.v != null) el("circle", { cx: x(p.t), cy: y(p.v), r: 3.5, fill: "var(--critical)", stroke: "var(--panel)", "stroke-width": 2 }, svg);
    // first-flag marker
    for (const f of s.flags) {
      const xx = x(f.tick);
      el("line", { x1: xx, x2: xx, y1: T, y2: plotB, stroke: f.kind === "sensor_fault" ? "var(--serious)" : "var(--critical)", "stroke-width": 1, "stroke-dasharray": "2 2" }, svg);
      const t = el("text", { x: xx + 4, y: T + 10, class: "ann" }, svg);
      t.textContent = `${f.kind === "sensor_fault" ? "✕" : "▲"} ${f.rule} @ tick ${f.tick}`;
      if (xx > W - 110) { t.setAttribute("x", xx - 4); t.setAttribute("text-anchor", "end"); }
    }
    // scored-window strip
    const sy = plotB + 4;
    for (const w of s.windows) {
      const x0 = x(w.start_tick - 0.5) + 1, x1 = x(w.end_tick + 0.5) - 1;
      const fill = { warmup: `url(#${pid})`, normal: "var(--win-normal)", flagged: "var(--critical)", fault: "var(--serious)" }[w.status] || "var(--win-normal)";
      const r = el("rect", { x: x0, y: sy, width: Math.max(1, x1 - x0), height: stripH, rx: 2, fill }, svg);
      if (w.status === "warmup") r.setAttribute("stroke", "var(--win-warm)");
    }
    // x axis
    for (const t of [0, Math.round((ticks - 1) / 2), ticks - 1]) {
      el("text", { x: x(t), y: H - 2, "text-anchor": t === 0 ? "start" : t === ticks - 1 ? "end" : "middle", class: "ax" }, svg).textContent = t === 0 ? "tick 0" : `${t}`;
    }
    // hover layer
    const cross = el("line", { y1: T, y2: plotB, stroke: "var(--ink2)", "stroke-width": 1, opacity: 0 }, svg);
    const hit = el("rect", { x: L, y: 0, width: W - L - R, height: H, fill: "transparent" }, svg);
    const tip = $("#tip");
    const byTick = new Map(pts.map((p) => [p.t, p]));
    hit.addEventListener("mousemove", (ev) => {
      const box = svg.getBoundingClientRect();
      const px = ((ev.clientX - box.left) / box.width) * W;
      const t = Math.round(((px - L) / (W - L - R)) * (ticks - 1));
      const p = byTick.get(t);
      if (!p) { tip.hidden = true; cross.setAttribute("opacity", 0); return; }
      cross.setAttribute("x1", x(t)); cross.setAttribute("x2", x(t)); cross.setAttribute("opacity", 0.5);
      let html = `<b>tick ${t}</b> · `;
      if (p.v == null) html += "no reading (sensor silent)";
      else html += `${fmt(p.v, s.decimals)} ${esc(s.unit)}`;
      if (p.z != null) html += `<br>z = ${p.z.toFixed(2)} · normal band ${fmt(p.b - det.sustained_z * p.s, s.decimals + 1)}–${fmt(p.b + det.sustained_z * p.s, s.decimals + 1)}`;
      else if (p.v != null) html += "<br>warm-up: learning this tool's normal";
      if (p.r && p.r.length) html += `<br>rule active: <b>${p.r.map(esc).join(", ")}</b>`;
      tip.innerHTML = html;
      tip.hidden = false;
      const tx = Math.min(window.innerWidth - tip.offsetWidth - 8, ev.clientX + 14);
      tip.style.left = `${tx}px`;
      tip.style.top = `${ev.clientY + 14}px`;
    });
    hit.addEventListener("mouseleave", () => { tip.hidden = true; cross.setAttribute("opacity", 0); });
    host.replaceChildren(svg);
  }

  let resizeT;
  window.addEventListener("resize", () => { clearTimeout(resizeT); resizeT = setTimeout(() => lastView && render(lastView), 150); });

  // ---- anomaly feed --------------------------------------------------------------
  function renderAlerts(v) {
    const box = $("#alerts");
    if (!v.alerts.length) {
      if (!box.querySelector(".muted")) box.innerHTML = `<p class="muted small">${v.streaming ? "Nothing flagged yet. Normal scenarios should stay empty here, and that's the false-positive check." : "Nothing flagged in this run."}</p>`;
      return;
    }
    box.querySelector("p.muted")?.remove();
    // newest first
    const list = v.alerts; // oldest first; each new card goes on top
    for (const a of list) {
      const sig = JSON.stringify([a.explain_state, a.status, a.sensors, a.action, a.last_event]);
      let card = box.querySelector(`[data-alert="${CSS.escape(a.id)}"]`);
      if (card && cards.get(a.id) === sig) continue;
      if (card && card.contains(document.activeElement) && document.activeElement.tagName === "INPUT") continue;
      const fresh = !seen.has(a.id);
      seen.add(a.id);
      cards.set(a.id, sig);
      const html = alertHTML(a, v);
      if (card) card.outerHTML = html;
      else box.insertAdjacentHTML("afterbegin", html);
      card = box.querySelector(`[data-alert="${CSS.escape(a.id)}"]`);
      if (fresh) card.classList.add("fresh");
    }
  }

  function step(state, text) {
    const ic = { done: "✓", busy: '<span class="spinner" aria-hidden="true"></span>', wait: "○", skip: "–" }[state];
    return `<li class="${state === "skip" ? "done" : state}"><span class="ic">${ic}</span><span>${text}</span></li>`;
  }

  function alertHTML(a, v) {
    const flags = a.flags.map((f) => `${esc(f.sensor_id)} <b>${esc(f.rule)}</b> @${f.tick} (z ${f.z.toFixed(2)})`).join(", ");
    const e = a.explanation;
    const steps = [step("done", `<b>Detected</b> by statistics: ${flags}`)];
    if (a.flags.length > 1) steps.push(step("done", `<b>Correlated:</b> ${a.flags.length} sensors on ${esc(a.equipment_id)} grouped into one incident`));
    if (a.kind === "sensor_fault") steps.push(step("skip", "<b>Explain skipped:</b> the sensor itself looks broken, so there's no model call"));
    else if (a.explain_state === "pending") steps.push(step("busy", "<b>Explaining</b> after a short settle so correlated sensors are included: one bounded model call"));
    else if (a.explain_state === "failed") steps.push(step("done", `<b>No explanation</b> (${esc(a.explain_error)}), so it goes to a human`));
    else steps.push(step("done", `<b>Explained</b> by ${esc(e?.model || "the model")}`));
    steps.push(a.decision ? step("done", `<b>Dispatched:</b> ${esc(human(a.decision.action))} <span class="muted">(rule <code>${esc(a.decision.rule)}</code>)</span>`) : step("wait", "<b>Dispatch</b> waits for the explanation"));
    const humanDone = a.status !== "open";
    steps.push(step(humanDone ? "done" : "wait", humanDone ? `<b>Human:</b> ${esc(a.status)} by ${esc(a.last_event?.actor || "")}${a.last_event?.detail ? ` · "${esc(a.last_event.detail)}"` : ""}` : "<b>Human:</b> waiting for the on-call engineer"));

    let body = "";
    if (e) {
      const conf = Math.round((e.confidence || 0) * 100);
      body += `<p class="explanation">${esc(e.explanation)}</p>
        <div class="causes">${(e.likely_causes || []).map((c, i) => `<span class="chip">${i + 1}. ${esc(human(c))}</span>`).join("")}</div>
        <div class="small muted">Check first</div><ol class="checks">${(e.recommended_checks || []).map((c) => `<li>${esc(c)}</li>`).join("")}</ol>
        <div class="conf"><span>confidence ${(e.confidence || 0).toFixed(2)}</span><span class="bar" title="below 0.60 goes to a human"><i style="width:${conf}%"></i></span><span class="muted">below 0.60 goes to a human</span></div>`;
    }
    if (a.decision) {
      body += `<div class="dispatch">${esc(a.decision.reason)}${(a.actions || []).map((x) => `<br>→ ${esc(x.detail)}`).join("")}</div>`;
    }
    const closed = a.status === "dismissed";
    const buttons = closed ? "" : `<div class="btns">
        <button type="button" data-act="acknowledge" data-id="${esc(a.id)}">Acknowledge</button>
        <button type="button" class="secondary" data-act="escalate" data-id="${esc(a.id)}">Escalate</button>
        <button type="button" class="secondary" data-act="dismiss-open" data-id="${esc(a.id)}">Dismiss…</button>
      </div>
      <div class="note-row" hidden><input maxlength="500" placeholder="Why dismiss? (goes in the audit trail)" aria-label="Dismiss note"><button type="button" data-act="dismiss" data-id="${esc(a.id)}">Dismiss</button></div>
      <p class="error-text small" hidden></p>`;
    return `<article class="alert k-${esc(a.kind)}" data-alert="${esc(a.id)}">
      <div class="alert-top"><span class="id">${esc(a.id)}</span><span class="tool">${esc(a.equipment_id)} · ${a.flags.length} sensor${a.flags.length > 1 ? "s" : ""}</span>
        <span class="chips">${a.severity ? `<span class="chip sev-${esc(a.severity)}">${esc(a.severity)} severity</span>` : a.kind === "sensor_fault" ? '<span class="chip">sensor fault</span>' : ""}${a.action ? `<span class="chip act-${esc(a.action)}">${esc(human(a.action))}</span>` : ""}${a.status !== "open" ? `<span class="chip st-${esc(a.status)}">${esc(a.status)}</span>` : ""}</span></div>
      <ol class="steps">${steps.join("")}</ol>
      ${body}
      ${buttons}
      <div class="more"><a href="${base}/alerts/${encodeURIComponent(a.id)}">Full audit trail and exactly what the model saw →</a></div>
    </article>`;
  }

  $("#alerts").addEventListener("click", async (ev) => {
    const b = ev.target.closest("button[data-act]");
    if (!b) return;
    const card = b.closest(".alert");
    const err = card.querySelector(".error-text");
    err.hidden = true;
    if (b.dataset.act === "dismiss-open") {
      const row = card.querySelector(".note-row");
      row.hidden = false;
      row.querySelector("input").focus();
      return;
    }
    const note = b.dataset.act === "dismiss" ? card.querySelector(".note-row input").value : "";
    b.disabled = true;
    try {
      await api(`/api/alerts/${encodeURIComponent(b.dataset.id)}/ack`, { method: "POST", body: JSON.stringify({ action: b.dataset.act, note, name: actor() }) });
      document.activeElement?.blur();
      cards.delete(b.dataset.id);
      await poll();
    } catch (e) {
      err.textContent = e.message;
      err.hidden = false;
      b.disabled = false;
    }
  });

  // ---- start ------------------------------------------------------------------------
  (async () => {
    try { await loadScenarios(); } catch (e) { runErr.textContent = `Couldn't load scenarios: ${e.message}`; runErr.hidden = false; }
    const runs = await loadRuns().catch(() => []);
    const want = root.dataset.run || new URLSearchParams(location.search).get("run") || runs[0]?.id;
    if (want) show(want, false);
  })();
})();

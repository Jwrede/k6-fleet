"use strict";
const $ = id => document.getElementById(id);
const storage = {
  get(k, d) { try { const v = localStorage.getItem("k6fleet." + k); return v === null ? d : JSON.parse(v); } catch (e) { return d; } },
  set(k, v) { try { localStorage.setItem("k6fleet." + k, JSON.stringify(v)); } catch (e) { /* the page works without it */ } },
};
const session = {
  get() { try { return sessionStorage.getItem("k6fleet.token") || ""; } catch (e) { return ""; } },
  set(v) { try { sessionStorage.setItem("k6fleet.token", v); } catch (e) { /* asked again next time */ } },
};

const fmtMs = v => v >= 1000 ? (v / 1000).toFixed(2) + " s" : v >= 100 ? v.toFixed(0) + " ms" : v >= 1 ? v.toFixed(1) + " ms" : v.toFixed(2) + " ms";
const fixed = n => v => v.toFixed(n);
const fmtPct = v => (100 * v).toFixed(2) + " %";
const fmtInt = v => Math.round(v).toLocaleString("en-US");
const fmtBytes = v => !v ? "-" : v >= 1 << 30 ? (v / (1 << 30)).toFixed(2) + " GiB" : (v / (1 << 20)).toFixed(0) + " MiB";
const fmtNum = v => Number.isInteger(v) ? String(v) : v.toPrecision(4);
const fmtClock = ms => { const s = Math.floor(ms / 1000); return clock(s); };

// A table that keeps its sort order and visible columns across refreshes.
class Table {
  constructor(id, columns, opts = {}) {
    this.el = $(id);
    this.id = id;
    this.columns = columns;
    this.opts = opts;
    this.sort = storage.get("sort." + id, opts.sort || null);
    this.hidden = new Set(storage.get("hidden." + id, columns.filter(c => c.off).map(c => c.key)));
    if (opts.selector) this.selector($(opts.selector));
    this.rows = [];
  }
  selector(box) {
    for (const c of this.columns) {
      const l = document.createElement("label"), i = document.createElement("input");
      i.type = "checkbox";
      i.checked = !this.hidden.has(c.key);
      i.onchange = () => {
        if (i.checked) this.hidden.delete(c.key); else this.hidden.add(c.key);
        storage.set("hidden." + this.id, [...this.hidden]);
        this.render();
      };
      l.append(i, " " + c.title);
      box.append(l);
    }
  }
  set(rows) { this.rows = rows; this.render(); }
  sorted() {
    const fixedRows = this.rows.filter(r => r.isTotal), rows = this.rows.filter(r => !r.isTotal);
    if (this.sort) {
      const {key, dir} = this.sort;
      rows.sort((a, b) => {
        const x = a[key], y = b[key];
        return dir * (typeof x === "string" ? String(x).localeCompare(String(y)) : x - y);
      });
    }
    return rows.concat(fixedRows);
  }
  render() {
    const cols = this.columns.filter(c => !this.hidden.has(c.key));
    const head = document.createElement("tr");
    for (const c of cols) {
      const th = document.createElement("th");
      th.textContent = c.title;
      if (c.num) th.className = "n";
      th.dataset.key = c.key;
      if (this.sort && this.sort.key === c.key) th.setAttribute("aria-sort", this.sort.dir > 0 ? "ascending" : "descending");
      th.onclick = () => {
        const dir = this.sort && this.sort.key === c.key ? -this.sort.dir : (c.num ? -1 : 1);
        this.sort = {key: c.key, dir};
        storage.set("sort." + this.id, this.sort);
        this.render();
      };
      head.append(th);
    }
    const body = this.sorted().map(r => {
      const tr = document.createElement("tr");
      if (r.isTotal) tr.className = "total";
      for (const c of cols) {
        const td = document.createElement("td");
        const v = r[c.key];
        td.textContent = v === undefined || v === null || v === "" ? "" : c.fmt ? c.fmt(v, r) : v;
        td.className = [c.num ? "n" : "", c.cls ? (typeof c.cls === "function" ? c.cls(v, r) : c.cls) : ""].join(" ").trim();
        tr.append(td);
      }
      return tr;
    });
    this.el.replaceChildren(head, ...body);
    if (this.opts.empty) { $(this.opts.empty).hidden = this.rows.length > 0; this.el.hidden = !this.rows.length; }
  }
}

const tables = {
  stats: new Table("stats", [
    {key: "method", title: "Type"},
    {key: "name", title: "Name", cls: "name"},
    {key: "requests", title: "# Requests", num: true, fmt: fmtInt},
    {key: "fails", title: "# Fails", num: true, fmt: fmtInt, cls: v => v ? "bad" : ""},
    {key: "med_ms", title: "Median", num: true, fmt: fmtMs},
    {key: "p90_ms", title: "p(90)", num: true, fmt: fmtMs, off: true},
    {key: "p95_ms", title: "p(95)", num: true, fmt: fmtMs},
    {key: "p99_ms", title: "p(99)", num: true, fmt: fmtMs},
    {key: "avg_ms", title: "Average", num: true, fmt: fmtMs},
    {key: "min_ms", title: "Min", num: true, fmt: fmtMs},
    {key: "max_ms", title: "Max", num: true, fmt: fmtMs},
    {key: "rps", title: "Current RPS", num: true, fmt: fixed(1)},
    {key: "fails_per_s", title: "Current failures/s", num: true, fmt: fixed(1)},
    {key: "share", title: "Share", num: true, fmt: v => v.toFixed(1) + " %", off: true},
  ], {selector: "stats-cols", empty: "stats-empty"}),
  failures: new Table("failures", [
    {key: "count", title: "# Occurrences", num: true, fmt: fmtInt},
    {key: "method", title: "Method"},
    {key: "name", title: "Name", cls: "name"},
    {key: "status", title: "Status"},
    {key: "error", title: "Error", cls: "name"},
  ], {empty: "failures-empty", sort: {key: "count", dir: -1}}),
  errors: new Table("errors", [
    {key: "count", title: "# Occurrences", num: true, fmt: fmtInt},
    {key: "agents", title: "Agents", fmt: v => v.join(", ")},
    {key: "message", title: "Message", cls: "pre"},
  ], {empty: "errors-empty", sort: {key: "count", dir: -1}}),
  thresholds: new Table("thresholds", [
    {key: "ok", title: "", fmt: v => v ? "ok" : "FAIL", cls: v => v ? "ok" : "bad"},
    {key: "series", title: "Series", cls: "name"},
    {key: "source", title: "Threshold"},
    {key: "actual", title: "Actual", num: true, fmt: fmtNum},
  ], {empty: "thresholds-empty"}),
  checks: new Table("checks", [
    {key: "label", title: "Check", cls: "name"},
    {key: "passes", title: "Passed", num: true, fmt: fmtInt},
    {key: "fails", title: "Failed", num: true, fmt: fmtInt, cls: v => v ? "bad" : ""},
  ], {empty: "checks-empty"}),
  agents: new Table("agents", [
    {key: "name", title: "Agent", cls: "name"},
    {key: "host", title: "Host"},
    {key: "state", title: "State", cls: v => v === "lost" ? "bad" : ""},
    {key: "segment", title: "Segment"},
    {key: "vus", title: "VUs", num: true, fmt: fmtInt},
    {key: "iterations", title: "Iterations", num: true, fmt: fmtInt},
    {key: "requests", title: "Requests", num: true, fmt: fmtInt},
    {key: "cpu", title: "CPU", num: true, fmt: v => v.toFixed(0) + " %", cls: v => v >= 90 ? "bad" : ""},
    {key: "cpu_max", title: "CPU max", num: true, fmt: v => v.toFixed(0) + " %"},
    {key: "mem_bytes", title: "k6 memory", num: true, fmt: fmtBytes},
    {key: "lag_ms", title: "Lag", num: true, fmt: v => v + " ms"},
    {key: "clock_offset_ms", title: "Clock offset", num: true, fmt: v => v + " ms", off: true},
  ], {}),
};

const charts = [
  new LineChart($("charts"), "Requests per second", [{key: "reqs", label: "RPS"}, {key: "fails", label: "Failures/s"}], v => v >= 100 ? v.toFixed(0) : String(+v.toFixed(1))),
  new LineChart($("charts"), "Response time", [{key: "p50_ms", label: "p(50)"}, {key: "p95_ms", label: "p(95)"}, {key: "p99_ms", label: "p(99)"}], fmtMs),
  new LineChart($("charts"), "VUs", [{key: "vus", label: "VUs"}], v => String(Math.round(v))),
  new LineChart($("charts"), "Highest agent CPU", [{key: "cpu_max", label: "CPU"}], v => v.toFixed(0) + " %"),
];

const tabs = [
  {id: "stats", title: "Statistics"}, {id: "charts", title: "Charts"}, {id: "failures", title: "Failures"},
  {id: "errors", title: "Errors"}, {id: "thresholds", title: "Thresholds"}, {id: "agents", title: "Agents"},
  {id: "logs", title: "Logs"}, {id: "downloads", title: "Downloads"},
];
for (const t of tabs) {
  t.button = document.createElement("button");
  t.button.type = "button";
  t.button.setAttribute("role", "tab");
  t.button.id = "tabbtn-" + t.id;
  t.label = document.createTextNode(t.title);
  t.badge = document.createElement("span");
  t.badge.className = "badge";
  t.badge.hidden = true;
  t.button.append(t.label, t.badge);
  t.button.onclick = () => { location.hash = t.id; };
  $("tabs").append(t.button);
}
function showTab() {
  const want = location.hash.slice(1);
  const current = tabs.find(t => t.id === want) || tabs[0];
  for (const t of tabs) {
    t.button.setAttribute("aria-selected", t === current);
    $("tab-" + t.id).hidden = t !== current;
  }
  if (current.id === "charts") charts.forEach(c => c.draw());
  if (current.id === "logs") { renderLogs(); scrollLog(); }
}
window.addEventListener("hashchange", showTab);
window.addEventListener("resize", () => charts.forEach(c => c.draw()));
function badge(id, n) {
  const t = tabs.find(t => t.id === id);
  t.badge.textContent = n > 999 ? "999+" : n;
  t.badge.hidden = !n;
}

const downloads = [
  ["report.html", "Report", "self-contained HTML page"],
  ["summary.json", "Summary", "merged results as JSON"],
  ["requests.csv", "Request statistics", "one row per method and name"],
  ["failures.csv", "Failures", "one row per kind of failed request"],
  ["errors.csv", "Errors", "grouped error messages"],
  ["timeline.csv", "Timeline", "one row per second"],
  ["logs.txt", "Logs", "the newest 2000 lines"],
];
for (const [file, title, text] of downloads) {
  const li = document.createElement("li"), a = document.createElement("a"), s = document.createElement("span");
  a.href = "download/" + file;
  a.textContent = title;
  s.textContent = text;
  li.append(a, s);
  $("downloads").append(li);
}

// Logs
let logLines = [], logSeq = 0;
const logSources = new Set();
function renderLogs() {
  const level = $("log-level").value, source = $("log-source").value, q = $("log-search").value.toLowerCase();
  const pass = l => {
    if (level === "error" && l.level !== "error" && l.level !== "fatal") return false;
    if (level === "warning" && !["warning", "error", "fatal"].includes(l.level)) return false;
    if (source && l.source !== source) return false;
    return !q || l.text.toLowerCase().includes(q);
  };
  const rows = logLines.filter(pass).map(l => {
    const d = document.createElement("div"), t = document.createElement("span"), s = document.createElement("span"), x = document.createElement("span");
    d.className = l.level;
    t.className = "t";
    t.textContent = l.time.slice(11, 19) + " ";
    s.className = "s";
    s.textContent = l.source + " ";
    x.className = "x";
    x.textContent = l.text;
    d.append(t, s, x);
    return d;
  });
  $("log").replaceChildren(...rows);
}
function scrollLog() { if ($("log-follow").checked) $("log").scrollTop = $("log").scrollHeight; }
for (const id of ["log-level", "log-source", "log-search"]) $(id).oninput = renderLogs;
async function pollLogs() {
  const r = await fetch("api/logs?since=" + logSeq, {cache: "no-store"});
  const b = await r.json();
  logSeq = b.seq;
  if (!b.lines.length) return;
  logLines = logLines.concat(b.lines).slice(-2000);
  for (const l of b.lines) {
    if (logSources.has(l.source)) continue;
    logSources.add(l.source);
    const o = document.createElement("option");
    o.value = o.textContent = l.source;
    $("log-source").append(o);
  }
  if (!$("tab-logs").hidden) { renderLogs(); scrollLog(); }
}

// State
const timeline = [];
let state = null, lost = false, run = -1, autoOpened = false;
const active = ["connecting", "initialising", "setup", "running", "stopping", "teardown"];

function say(text) {
  $("message").textContent = text;
  $("message").hidden = !text;
}
let shownError = "";
function render() {
  const s = state, finished = !active.includes(s.phase);
  document.title = (s.script ? s.script + " - " : "") + s.phase + " - k6-fleet";
  $("script").textContent = [s.script, s.k6_version && "k6 " + s.k6_version].filter(Boolean).join(", ");
  $("m-status").textContent = lost ? "not reachable" : s.phase + (s.exit_code === undefined ? "" : " (exit " + s.exit_code + ")");
  $("m-status").className = s.phase === "failed" || lost ? "bad" : "";
  $("m-time").textContent = s.elapsed_ms ? fmtClock(s.elapsed_ms) + (s.planned && s.planned !== "0s" ? " of " + s.planned : "") : "-";
  $("m-vus").textContent = s.phase === "idle" ? "-" : fmtInt(s.vus) + (s.target_vus && s.phase === "running" ? " / " + fmtInt(s.target_vus) : "");
  $("m-agents").textContent = s.agents.length;
  $("m-rps").textContent = s.rps.toFixed(1);
  $("m-fail").textContent = fmtPct(s.fail_ratio);
  $("m-fail").className = s.fail_ratio > 0 ? "bad" : "";
  $("new").hidden = !finished;
  $("new").textContent = s.phase === "idle" ? "Start" : "New test";
  $("stop").hidden = !["connecting", "initialising", "setup", "running"].includes(s.phase);
  $("edit").hidden = !(s.phase === "running" && s.adjustable);
  $("reset").hidden = s.phase !== "running";

  const msg = s.error || s.aborted || "";
  if (msg !== shownError) { shownError = msg; say(msg); }
  $("warnings").replaceChildren(...(s.warnings || []).map(w => { const p = document.createElement("p"); p.textContent = w; return p; }));

  const total = (s.requests || []).reduce((n, r) => r.name === "Aggregated" ? n : n + r.requests, 0);
  tables.stats.set((s.requests || []).map(r => ({...r, isTotal: r.name === "Aggregated" && r.method === "", share: total ? 100 * r.requests / total : 0})));
  tables.failures.set(s.failures || []);
  tables.errors.set(s.errors || []);
  tables.thresholds.set(s.thresholds || []);
  tables.checks.set((s.checks || []).map(c => ({...c, label: (c.group + " " + c.name).trim()})));
  tables.agents.set(s.agents);
  $("thresholds-h").textContent = finished ? "Thresholds" : "Thresholds (so far)";
  badge("failures", (s.failures || []).reduce((n, f) => n + f.count, 0));
  badge("errors", (s.errors || []).reduce((n, e) => n + e.count, 0));
  badge("thresholds", (s.thresholds || []).filter(t => !t.ok).length);
  if (!$("tab-charts").hidden) charts.forEach(c => c.setData(timeline));
  else charts.forEach(c => { c.data = timeline; });
}

async function poll() {
  try {
    const r = await fetch("api/state?since=" + timeline.length, {cache: "no-store"});
    const s = await r.json();
    if (s.run !== run) {
      if (run !== -1) { timeline.length = 0; logLines = []; $("log").replaceChildren(); charts.forEach(c => { c.zoom = null; }); }
      if (run !== -1) s.timeline = [];
      run = s.run;
    }
    timeline.push(...(s.timeline || []));
    state = s;
    lost = false;
    await pollLogs();
  } catch (e) {
    lost = state !== null;
  }
  if (state) {
    render();
    if (state.phase === "idle" && !autoOpened && !state.error) { autoOpened = true; openStart(); }
  }
  setTimeout(poll, 1000);
}

// Control
async function post(path, body) {
  const headers = {"X-K6-Fleet": "1"};
  const token = session.get();
  if (token) headers.Authorization = "Bearer " + token;
  if (body) headers["Content-Type"] = "application/json";
  try {
    const r = await fetch("api/" + path, {method: "POST", headers, body: body ? JSON.stringify(body) : undefined});
    return r.ok ? "" : (await r.text()).trim();
  } catch (e) {
    return String(e);
  }
}
async function act(path) {
  if (state.token_required && !session.get()) {
    const t = prompt("Control token");
    if (!t) return;
    session.set(t);
  }
  const err = await post(path);
  if (err) say(err);
}
$("stop").onclick = () => act("stop");
$("reset").onclick = () => act("reset");

function envRow(k = "", v = "") {
  const row = document.createElement("div"), a = document.createElement("input"), b = document.createElement("input"), x = document.createElement("button");
  row.className = "envrow";
  a.type = b.type = "text";
  a.placeholder = "NAME";
  a.setAttribute("aria-label", "Variable name");
  b.placeholder = "value";
  b.setAttribute("aria-label", "Variable value");
  a.value = k;
  b.value = v;
  x.type = "button";
  x.textContent = "x";
  x.setAttribute("aria-label", "Remove variable");
  x.onclick = () => row.remove();
  row.append(a, b, x);
  $("f-env").append(row);
}
$("f-env-add").onclick = () => envRow();

function openStart() {
  const d = state.defaults, last = storage.get("form", {});
  $("form-script").textContent = state.script + ", " + state.agents.length + " agents";
  $("f-vus").placeholder = d.max_vus ? "from the script: " + d.max_vus : "from the script";
  $("f-vus").value = last.vus || "";
  $("f-duration").value = last.duration || "";
  $("f-vus-hint").textContent = d.live_vus
    ? "Set a number to run the externally-controlled executor, which lets you change VUs during the run. Leave it empty to run the scenarios in the script."
    : "Set a number to replace the scenarios in the script. k6 2.x cannot change VUs during a run.";
  $("f-duration-hint").textContent = "Empty means " + (d.duration ? d.duration + " from the script" : "the script's duration") + ". " +
    (d.live_vus ? "If VUs are set, empty means until you press Stop." : "VUs need a duration.");
  $("f-env").replaceChildren();
  for (const [k, v] of Object.entries(last.env || {})) envRow(k, v);
  $("f-token-row").hidden = !state.token_required;
  $("f-token").value = session.get();
  $("f-error").hidden = true;
  $("start-dialog").showModal();
}
$("new").onclick = openStart;
$("f-cancel").onclick = () => $("start-dialog").close();
$("start-form").onsubmit = async e => {
  e.preventDefault();
  const env = {};
  for (const row of $("f-env").children) {
    const [k, v] = row.querySelectorAll("input");
    if (k.value.trim()) env[k.value.trim()] = v.value;
  }
  const form = {vus: Number($("f-vus").value) || 0, duration: $("f-duration").value.trim(), env};
  if (state.token_required) session.set($("f-token").value);
  const err = await post("start", form);
  if (err) {
    $("f-error").textContent = err;
    $("f-error").hidden = false;
    return;
  }
  storage.set("form", form);
  say("");
  $("start-dialog").close();
};

$("edit").onclick = () => {
  $("e-vus").value = state.target_vus || Math.round(state.vus);
  $("e-error").hidden = true;
  $("edit-dialog").showModal();
};
$("e-cancel").onclick = () => $("edit-dialog").close();
$("edit-form").onsubmit = async e => {
  e.preventDefault();
  if (state.token_required && !session.get()) {
    const t = prompt("Control token");
    if (t) session.set(t);
  }
  const err = await post("vus", {vus: Number($("e-vus").value)});
  if (err) {
    $("e-error").textContent = err;
    $("e-error").hidden = false;
    return;
  }
  $("edit-dialog").close();
};

function setTheme(t) {
  document.documentElement.dataset.theme = t;
  storage.set("theme", t);
  charts.forEach(c => c.draw());
}
$("theme").onclick = () => setTheme(document.documentElement.dataset.theme === "dark" ? "light" : "dark");
document.documentElement.dataset.theme = storage.get("theme", matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light");

showTab();
poll();

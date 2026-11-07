"use strict";
// A small line chart in SVG: axes, tooltip, legend toggles, drag to zoom. The
// page has to work without external assets, so no chart library is used.
const SVG = "http://www.w3.org/2000/svg";
const PAD = {l: 58, r: 22, t: 8, b: 20};

function svgEl(name, attrs, cls) {
  const e = document.createElementNS(SVG, name);
  for (const k in attrs) e.setAttribute(k, attrs[k]);
  if (cls) e.setAttribute("class", cls);
  return e;
}
function niceStep(range, ticks) {
  const raw = range / ticks, mag = Math.pow(10, Math.floor(Math.log10(raw))), f = raw / mag;
  return (f <= 1 ? 1 : f <= 2 ? 2 : f <= 5 ? 5 : 10) * mag;
}
function clock(sec) {
  const h = Math.floor(sec / 3600), m = Math.floor(sec / 60) % 60, s = sec % 60;
  return (h ? h + ":" + String(m).padStart(2, "0") : m) + ":" + String(s).padStart(2, "0");
}

class LineChart {
  constructor(parent, title, lines, fmt) {
    this.lines = lines.map((l, i) => ({...l, cls: "c" + i, on: true}));
    this.fmt = fmt;
    this.data = [];
    this.zoom = null;
    this.root = document.createElement("div");
    this.root.className = "chart";
    const h = document.createElement("h3");
    h.textContent = title;
    const legend = document.createElement("div");
    legend.className = "legend";
    for (const l of this.lines) {
      const b = document.createElement("button");
      b.type = "button";
      const i = document.createElement("i");
      i.className = l.cls;
      b.append(i, l.label);
      b.onclick = () => { l.on = !l.on; b.classList.toggle("off", !l.on); this.draw(); };
      legend.append(b);
    }
    this.svg = svgEl("svg", {role: "img", "aria-label": title});
    this.tip = document.createElement("div");
    this.tip.className = "tip";
    this.tip.hidden = true;
    this.root.append(h, legend, this.svg, this.tip);
    parent.append(this.root);
    this.svg.addEventListener("pointermove", e => this.hover(e));
    this.svg.addEventListener("pointerleave", () => { this.tip.hidden = true; this.clearCross(); });
    this.svg.addEventListener("pointerdown", e => { this.dragFrom = this.svgX(e); this.svg.setPointerCapture(e.pointerId); });
    this.svg.addEventListener("pointerup", e => this.endDrag(e));
    this.svg.addEventListener("dblclick", () => { this.zoom = null; this.draw(); });
  }
  svgX(e) { return e.clientX - this.svg.getBoundingClientRect().left; }
  visible() {
    if (!this.zoom) return this.data;
    return this.data.filter(p => p.second >= this.zoom[0] && p.second <= this.zoom[1]);
  }
  // Keeps the peak of each pixel column, so a short spike stays visible.
  thin(points, key, width) {
    if (points.length <= width) return points;
    const out = [], size = points.length / width;
    for (let i = 0; i < width; i++) {
      let best = points[Math.floor(i * size)];
      for (let j = Math.floor(i * size); j < Math.min(points.length, Math.floor((i + 1) * size)); j++) {
        if (points[j][key] > best[key]) best = points[j];
      }
      out.push(best);
    }
    return out;
  }
  setData(points) { this.data = points; if (!this.tip.hidden) return; this.draw(); }
  draw() {
    const box = this.svg.getBoundingClientRect();
    const W = box.width || 800, H = box.height || 210;
    this.W = W; this.H = H;
    this.svg.setAttribute("viewBox", `0 0 ${W} ${H}`);
    this.svg.replaceChildren();
    const pts = this.visible();
    if (pts.length < 1) return;
    const shown = this.lines.filter(l => l.on);
    const peak = Math.max(1e-9, ...shown.flatMap(l => pts.map(p => p[l.key])));
    const step = niceStep(peak, 4), top = Math.ceil(peak / step) * step;
    const x0 = pts[0].second, x1 = pts[pts.length - 1].second, span = Math.max(1, x1 - x0);
    const px = s => PAD.l + (W - PAD.l - PAD.r) * (s - x0) / span;
    const py = v => H - PAD.b - (H - PAD.t - PAD.b) * v / top;
    this.px = px; this.x0 = x0; this.span = span;
    for (let v = 0; v <= top + step / 2; v += step) {
      this.svg.append(svgEl("line", {x1: PAD.l, x2: W - PAD.r, y1: py(v), y2: py(v)}, "grid"));
      const t = svgEl("text", {x: PAD.l - 6, y: py(v) + 4, "text-anchor": "end"}, "axis");
      t.textContent = this.fmt(v);
      this.svg.append(t);
    }
    const xs = Math.max(1, Math.ceil(niceStep(span, Math.max(2, Math.floor(W / 110)))));
    for (let s = Math.ceil(x0 / xs) * xs; s <= x1; s += xs) {
      const t = svgEl("text", {x: px(s), y: H - 5, "text-anchor": "middle"}, "axis");
      t.textContent = clock(Math.round(s));
      this.svg.append(t);
    }
    for (const l of shown) {
      const line = this.thin(pts, l.key, Math.floor(W - PAD.l - PAD.r));
      this.svg.append(svgEl("polyline", {points: line.map(p => px(p.second).toFixed(1) + "," + py(p[l.key]).toFixed(1)).join(" ")}, l.cls));
    }
    this.cross = svgEl("line", {y1: PAD.t, y2: H - PAD.b, visibility: "hidden"}, "cross");
    this.sel = svgEl("rect", {y: PAD.t, height: H - PAD.t - PAD.b, width: 0, visibility: "hidden"}, "sel");
    this.svg.append(this.cross, this.sel);
  }
  clearCross() { if (this.cross) this.cross.setAttribute("visibility", "hidden"); this.draw(); }
  secondAt(x) { return this.x0 + this.span * (x - PAD.l) / (this.W - PAD.l - PAD.r); }
  hover(e) {
    const pts = this.visible();
    if (!pts.length || !this.cross) return;
    const x = this.svgX(e);
    if (this.dragFrom !== undefined) {
      this.sel.setAttribute("visibility", "visible");
      this.sel.setAttribute("x", Math.min(x, this.dragFrom));
      this.sel.setAttribute("width", Math.abs(x - this.dragFrom));
    }
    const sec = this.secondAt(x);
    let p = pts[0];
    for (const q of pts) if (Math.abs(q.second - sec) < Math.abs(p.second - sec)) p = q;
    this.cross.setAttribute("x1", this.px(p.second));
    this.cross.setAttribute("x2", this.px(p.second));
    this.cross.setAttribute("visibility", "visible");
    const rows = [document.createTextNode(clock(p.second))];
    for (const l of this.lines.filter(l => l.on)) {
      const d = document.createElement("div"), i = document.createElement("i");
      i.className = l.cls;
      d.append(i, l.label + ": " + this.fmt(p[l.key]));
      rows.push(d);
    }
    this.tip.replaceChildren(...rows);
    this.tip.hidden = false;
    const left = this.px(p.second) + 22;
    this.tip.style.left = (left + 170 > this.W ? left - 195 : left) + "px";
    this.tip.style.top = "34px";
  }
  endDrag(e) {
    const from = this.dragFrom, to = this.svgX(e);
    this.dragFrom = undefined;
    if (from !== undefined && Math.abs(to - from) > 6 && this.visible().length > 2) {
      // Whole seconds, so that a narrow selection still holds two points.
      const a = Math.floor(this.secondAt(Math.min(from, to))), b = Math.ceil(this.secondAt(Math.max(from, to)));
      this.zoom = [a, b];
      this.tip.hidden = true;
    }
    this.draw();
  }
}

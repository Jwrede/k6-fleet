// Drives the web page in a real browser over the DevTools protocol, without
// dependencies. Usage: node ui.mjs <page url> <chrome binary> <BASE for the form>
import { spawn } from 'node:child_process';
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

const [url, chromeBin, base] = process.argv.slice(2);
const shots = process.env.UI_SHOTS;
const profile = mkdtempSync(join(tmpdir(), 'k6fleet-ui-'));
const port = 19200 + Math.floor(Math.random() * 500);
const chrome = spawn(chromeBin, ['--headless=new', '--no-sandbox', '--disable-gpu', `--remote-debugging-port=${port}`,
  `--user-data-dir=${profile}`, '--window-size=1280,1000', 'about:blank'], { stdio: 'ignore' });
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
let failures = 0;
const ok = (cond, what) => { console.log(`${cond ? 'ok  ' : 'FAIL'} ${what}`); if (!cond) failures++; };

let ws;
for (let i = 0; i < 60 && !ws; i++) {
  try {
    const t = await (await fetch(`http://127.0.0.1:${port}/json`)).json();
    ws = new WebSocket(t.find((x) => x.type === 'page').webSocketDebuggerUrl);
  } catch { await sleep(250); }
}
await new Promise((r) => { ws.onopen = r; });
let id = 0;
const pending = new Map();
const problems = [];
ws.onmessage = (e) => {
  const m = JSON.parse(e.data);
  if (m.id && pending.has(m.id)) { pending.get(m.id)(m); pending.delete(m.id); }
  if (m.method === 'Runtime.exceptionThrown') problems.push(JSON.stringify(m.params.exceptionDetails).slice(0, 400));
  if (m.method === 'Log.entryAdded' && m.params.entry.level === 'error') problems.push(m.params.entry.text);
};
const send = (method, params = {}) => new Promise((r) => { const i = ++id; pending.set(i, r); ws.send(JSON.stringify({ id: i, method, params })); });
const ev = async (x) => {
  const r = await send('Runtime.evaluate', { expression: x, returnByValue: true, awaitPromise: true });
  if (r.result?.exceptionDetails) throw new Error(`${x}: ${JSON.stringify(r.result.exceptionDetails).slice(0, 300)}`);
  return r.result?.result?.value;
};
const until = async (x, what, ms = 30000) => {
  const end = Date.now() + ms;
  while (Date.now() < end) {
    if (await ev(x).catch(() => false)) { ok(true, what); return true; }
    await sleep(150);
  }
  ok(false, what);
  return false;
};
const shot = async (name) => {
  if (!shots) return;
  const r = await send('Page.captureScreenshot', { format: 'png' });
  writeFileSync(join(shots, `${name}.png`), Buffer.from(r.result.data, 'base64'));
};
const tab = (t) => ev(`document.getElementById('tabbtn-${t}').click()`);
const text = (sel) => `(document.querySelector(${JSON.stringify(sel)})?.textContent ?? '')`;
const mouse = (type, x, y, buttons = 0) => send('Input.dispatchMouseEvent', { type, x, y, button: buttons ? 'left' : 'none', buttons, clickCount: buttons ? 1 : 0 });

try {
  await send('Page.enable'); await send('Runtime.enable'); await send('Log.enable');
  await send('Page.navigate', { url });

  await until(`document.getElementById('start-dialog').open`, 'start form opens on an idle page');
  ok(await ev(`document.getElementById('f-vus').placeholder.includes('from the script')`), 'form shows the VUs of the script');
  await shot('form');
  const live = await ev(`state.defaults.live_vus`);
  await ev(`(() => { const d = document;
    d.getElementById('f-vus').value = '4';
    d.getElementById('f-duration').value = ${live ? "''" : "'60s'"};
    d.getElementById('f-env-add').click(); d.getElementById('f-env-add').click();
    const rows = d.querySelectorAll('.envrow');
    rows[0].querySelectorAll('input')[0].value = 'BASE'; rows[0].querySelectorAll('input')[1].value = ${JSON.stringify(base)};
    rows[1].querySelectorAll('input')[0].value = 'FAIL'; rows[1].querySelectorAll('input')[1].value = '1';
    d.getElementById('f-start').click(); })()`);
  await until(`!document.getElementById('start-dialog').open`, 'form closes after Start');
  await until(`${text('#m-status')}.startsWith('running')`, 'status shows running');
  await until(`${text('#m-vus')} === ${live ? "'4 / 4'" : "'4'"}`, 'monitor shows 4 VUs');
  await until(`[...document.querySelectorAll('#stats td.name')].some(t => t.textContent === 'hit')`, 'statistics list the request name');
  ok(await ev(`document.querySelector('#stats tr:last-child').classList.contains('total')`), 'total row comes last');
  await until(`parseFloat(${text('#m-rps')}) > 0`, 'monitor shows RPS');
  await until(`parseFloat(${text('#m-fail')}) > 0`, 'monitor shows failures');
  await shot('stats');

  await ev(`document.querySelector('#stats th[data-key=requests]').click()`);
  ok(await ev(`document.querySelector('#stats th[data-key=requests]').getAttribute('aria-sort') === 'descending'`), 'clicking a column sorts');
  await ev(`[...document.querySelectorAll('#stats-cols label')].find(l => l.textContent.includes('p(99)')).querySelector('input').click()`);
  ok(await ev(`!document.querySelector('#stats th[data-key=p99_ms]')`), 'a column can be hidden');
  await ev(`[...document.querySelectorAll('#stats-cols label')].find(l => l.textContent.includes('p(99)')).querySelector('input').click()`);

  await tab('charts');
  await until(`document.querySelectorAll('.chart polyline').length >= 7`, 'charts draw all lines');
  await sleep(4000);
  const rect = await ev(`(() => { const r = document.querySelector('.chart svg').getBoundingClientRect(); return {x: r.x, y: r.y, w: r.width, h: r.height}; })()`);
  const mx = rect.x + rect.w * 0.6, my = rect.y + 60;
  await mouse('mouseMoved', mx, my);
  await until(`!document.querySelector('.tip').hidden && document.querySelector('.tip').textContent.includes('RPS')`, 'tooltip shows values at the cursor');
  await shot('charts');
  await mouse('mouseMoved', rect.x + 5, rect.y - 40);
  const before = await ev(`document.querySelectorAll('.chart')[1].querySelectorAll('polyline').length`);
  await ev(`document.querySelectorAll('.chart')[1].querySelector('.legend button').click()`);
  ok(await ev(`document.querySelectorAll('.chart')[1].querySelectorAll('polyline').length`) === before - 1, 'legend hides a line');
  await ev(`document.querySelectorAll('.chart')[1].querySelector('.legend button').click()`);
  await send('Input.dispatchMouseEvent', { type: 'mouseMoved', x: rect.x + rect.w * 0.3, y: my });
  await send('Input.dispatchMouseEvent', { type: 'mousePressed', x: rect.x + rect.w * 0.3, y: my, button: 'left', buttons: 1, clickCount: 1 });
  await send('Input.dispatchMouseEvent', { type: 'mouseMoved', x: rect.x + rect.w * 0.7, y: my, button: 'left', buttons: 1 });
  await send('Input.dispatchMouseEvent', { type: 'mouseReleased', x: rect.x + rect.w * 0.7, y: my, button: 'left', buttons: 0, clickCount: 1 });
  await sleep(300);
  await mouse('mouseMoved', rect.x + 5, rect.y - 40);
  await sleep(1300);
  await until(`charts[0].zoom !== null && document.querySelector('.chart polyline').getAttribute('points').split(' ').length >= 2`, 'dragging zooms into the chart');

  await tab('failures');
  await until(`[...document.querySelectorAll('#failures td')].some(t => t.textContent === 'broken')`, 'failures list the request');
  ok(await ev(`!document.querySelector('#tabbtn-failures .badge').hidden`), 'failures tab shows a count');
  await tab('errors');
  await until(`[...document.querySelectorAll('#errors td')].some(t => t.textContent.includes('deliberate failure'))`, 'errors show the exception');
  await tab('thresholds');
  await until(`document.querySelectorAll('#thresholds tr').length >= 2`, 'thresholds are listed');
  await until(`[...document.querySelectorAll('#checks td')].some(t => t.textContent.includes('status is 200'))`, 'checks are listed');
  await tab('agents');
  await until(`document.querySelectorAll('#agents tr').length === 3 && [...document.querySelectorAll('#agents td')].some(t => t.textContent.includes('MiB'))`, 'agents show memory');
  await shot('agents');
  await tab('logs');
  await until(`document.getElementById('log').textContent.includes('first iteration done')`, 'logs show console.log output');
  await ev(`(() => { const s = document.getElementById('log-level'); s.value = 'error'; s.dispatchEvent(new Event('input')); })()`);
  ok(await ev(`!document.getElementById('log').textContent.includes('first iteration done')`), 'log level filter works');
  await shot('logs');
  await tab('downloads');
  const files = await ev(`Promise.all([...document.querySelectorAll('#downloads a')].map(a => fetch(a.href).then(r => r.status)))`);
  ok(files.length === 7 && files.every((c) => c === 200), `downloads answer (${files})`);

  await tab('stats');
  if (live) {
    await ev(`document.getElementById('edit').click()`);
    ok(await ev(`document.getElementById('e-vus').value === '4'`), 'edit form shows the current VUs');
    await ev(`document.getElementById('e-vus').value = '6'; document.querySelector('#edit-form button[type=submit]').click()`);
    await until(`${text('#m-vus')} === '6 / 6'`, 'monitor follows the new VUs');
  } else {
    ok(await ev(`document.getElementById('edit').hidden`), 'no Edit button without live VUs');
  }
  const count = () => ev(`Number(document.querySelector('#stats tr.total td:nth-child(3)').textContent.replaceAll(',', ''))`);
  const reqs = await count();
  await ev(`document.getElementById('reset').click()`);
  await until(`Number(document.querySelector('#stats tr.total td:nth-child(3)')?.textContent.replaceAll(',', '') ?? 0) < ${reqs}`, 'reset clears the statistics');

  await ev(`document.getElementById('theme').click()`);
  const dark = await ev(`document.documentElement.dataset.theme`);
  ok(dark === 'dark' || dark === 'light', 'theme toggle sets a theme');
  await sleep(200);
  await shot('theme');

  await ev(`document.getElementById('stop').click()`);
  await until(`${text('#m-status')}.startsWith('finished')`, 'status shows finished after Stop');
  ok(await ev(`${text('#m-status')}.includes('exit 105')`), 'exit code 105 is shown');
  ok(await ev(`!document.getElementById('new').hidden && document.getElementById('stop').hidden`), 'New test replaces Stop');
  await ev(`document.getElementById('new').click()`);
  ok(await ev(`document.getElementById('f-vus').value === '4' && document.querySelectorAll('.envrow').length === 2`), 'form remembers the last values');
  await ev(`document.getElementById('f-duration').value = '3s'; document.getElementById('f-start').click()`);
  await until(`state.run === 2 && state.phase !== 'finished'`, 'second run starts from the page');
  await until(`${text('#m-status')}.startsWith('finished (exit')`, 'second run ends by its duration');

  ok(problems.length === 0, `no script errors or CSP violations in the browser${problems.length ? ': ' + problems.join(' | ') : ''}`);
} catch (e) {
  ok(false, String(e));
} finally {
  ws.close();
  chrome.kill();
  await sleep(300);
  rmSync(profile, { recursive: true, force: true });
}
process.exit(failures ? 1 : 0);

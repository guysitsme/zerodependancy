/* ═══════════════════════════════════════════
   CHRONOS OPERATOR DASHBOARD — app.js
   Fully interactive, demo-mode simulation.
   Replace TCP calls with real WebSocket/fetch
   when the backend is ready.
════════════════════════════════════════════ */

'use strict';

// ─── State ───────────────────────────────────────────────────────────────────
const state = {
  connected: false,
  serverHost: 'localhost:9000',
  activeSeries: 'engine_temp',
  tailing: false,
  tailCount: 0,
  tailInterval: null,
  queryResults: [],
  queryMode: null, // 'raw' | 'hourly' | 'daily'
};

// Demo data store (simulates the backend)
const demoStore = {
  engine_temp: [],
  cpu_usage: [],
  mem_alloc: [],
};

// ─── DOM refs ────────────────────────────────────────────────────────────────
const $ = (id) => document.getElementById(id);

// Sidebar / connection
const connectBtn       = $('connectBtn');
const serverHostInput  = $('serverHost');
const statusDot        = $('statusDot');
const statusLabel      = $('statusLabel');
const globalSeriesSelect = $('globalSeriesSelect');

// Write
const writeSeriesInput    = $('writeSeries');
const writeValueInput     = $('writeValue');
const writeTimestampInput = $('writeTimestamp');
const nowBtn              = $('nowBtn');
const writeBtn            = $('writeBtn');
const writeResponse       = $('writeResponse');
const writeStatusBadge    = $('writeStatusBadge');

// Query
const querySeriesInput = $('querySeries');
const queryStartInput  = $('queryStart');
const queryEndInput    = $('queryEnd');
const queryBtn         = $('queryBtn');
const queryTierBadge   = $('queryTierBadge');
const queryTableWrap   = $('queryTableWrap');
const queryTableHead   = $('queryTableHead');
const queryTableBody   = $('queryTableBody');
const queryEmpty       = $('queryEmpty');
const chartEmpty       = $('chartEmpty');
const queryChart       = $('queryChart');
const chartContainer   = $('chartContainer');

// Tail
const tailStartBtn    = $('tailStartBtn');
const tailStopBtn     = $('tailStopBtn');
const tailClearBtn    = $('tailClearBtn');
const tailOutput      = $('tailOutput');
const tailSeriesLabel = $('tailSeriesLabel');
const tailCountEl     = $('tailCount');

// Benchmark
const runBenchmarkBtn = $('runBenchmarkBtn');
const rawSizeEl       = $('rawSize');
const compSizeEl      = $('compSize');
const compRatioEl     = $('compRatio');
const efficiencyBar   = $('efficiencyBar');
const efficiencyLabel = $('efficiencyLabel');

// Error log
const errorLog     = $('errorLog');
const errorMessage = $('errorMessage');
const errorClose   = $('errorClose');

// Nav
const navLinks = document.querySelectorAll('.nav-link');

// Mobile
const hamburger = $('hamburger');
const sidebar   = $('sidebar');

// ─── Utilities ───────────────────────────────────────────────────────────────
function nowEpoch() {
  return Math.floor(Date.now() / 1000);
}

function formatBytes(bytes) {
  if (bytes >= 1e9) return (bytes / 1e9).toFixed(1) + ' GB';
  if (bytes >= 1e6) return (bytes / 1e6).toFixed(1) + ' MB';
  if (bytes >= 1e3) return (bytes / 1e3).toFixed(1) + ' KB';
  return bytes + ' B';
}

function showError(msg) {
  errorMessage.textContent = msg;
  errorLog.style.display = 'flex';
}

function hideError() {
  errorLog.style.display = 'none';
}

function setConnectionState(connected) {
  state.connected = connected;
  statusDot.className = 'status-dot' + (connected ? ' connected' : '');
  statusLabel.textContent = connected
    ? `Connected: ${state.serverHost}`
    : 'Disconnected';
  connectBtn.textContent = connected ? 'Disconnect' : 'Connect';
}

// ─── Active series sync ───────────────────────────────────────────────────────
globalSeriesSelect.addEventListener('change', () => {
  state.activeSeries = globalSeriesSelect.value;
  writeSeriesInput.value = state.activeSeries;
  querySeriesInput.value = state.activeSeries;
});

// ─── Mobile sidebar ───────────────────────────────────────────────────────────
hamburger.addEventListener('click', () => {
  sidebar.classList.toggle('open');
});
document.addEventListener('click', (e) => {
  if (!sidebar.contains(e.target) && !hamburger.contains(e.target)) {
    sidebar.classList.remove('open');
  }
});

// ─── Smooth scroll navigation ────────────────────────────────────────────────
navLinks.forEach(link => {
  link.addEventListener('click', (e) => {
    e.preventDefault();
    navLinks.forEach(l => l.classList.remove('active'));
    link.classList.add('active');
    const target = document.querySelector(link.getAttribute('href'));
    if (target) target.scrollIntoView({ behavior: 'smooth', block: 'start' });
    sidebar.classList.remove('open');
  });
});

// ─── Connect / Disconnect ────────────────────────────────────────────────────
connectBtn.addEventListener('click', () => {
  state.serverHost = serverHostInput.value.trim() || 'localhost:9000';
  if (state.connected) {
    setConnectionState(false);
    if (state.tailing) stopTail();
  } else {
    // Demo: simulate connection handshake
    connectBtn.textContent = 'Connecting…';
    connectBtn.disabled = true;
    setTimeout(() => {
      connectBtn.disabled = false;
      setConnectionState(true);
    }, 600);
  }
});

// ─── Write ───────────────────────────────────────────────────────────────────
nowBtn.addEventListener('click', () => {
  writeTimestampInput.value = nowEpoch();
});

writeBtn.addEventListener('click', () => {
  const series = writeSeriesInput.value.trim();
  const value  = writeValueInput.value.trim();
  const ts     = writeTimestampInput.value.trim();

  // Validation
  if (!series || !/^[a-zA-Z0-9_]+$/.test(series)) {
    showError('Invalid series name. Use only [a-zA-Z0-9_].');
    setWriteStatus('error', 'ERR invalid series');
    return;
  }
  if (value === '' || isNaN(parseFloat(value))) {
    showError('Invalid value — must be a number.');
    setWriteStatus('error', 'ERR invalid value: not a number');
    return;
  }
  if (!ts || isNaN(parseInt(ts))) {
    showError('Invalid timestamp — must be a Unix epoch integer.');
    setWriteStatus('error', 'ERR invalid timestamp');
    return;
  }
  if (!state.connected) {
    showError('Not connected to server. Click "Connect" first.');
    return;
  }

  hideError();

  // Simulate TCP WRITE command + response
  const point = { ts: parseInt(ts), value: parseFloat(value) };
  if (!demoStore[series]) demoStore[series] = [];
  demoStore[series].push(point);
  demoStore[series].sort((a, b) => a.ts - b.ts);

  setWriteStatus('ok', 'OK');
  writeValueInput.value = '';
  writeTimestampInput.value = '';

  // If tailing this series, push to terminal
  if (state.tailing && series === state.activeSeries) {
    pushTailLine(point.ts, point.value);
  }
});

function setWriteStatus(type, text) {
  writeResponse.textContent = text;
  writeResponse.className = 'response-tag ' + type;
  writeStatusBadge.textContent = type === 'ok' ? 'OK' : 'ERR';
  writeStatusBadge.className = 'badge ' + type;
  setTimeout(() => {
    writeResponse.className = 'response-tag';
    writeResponse.textContent = '–';
    writeStatusBadge.textContent = 'READY';
    writeStatusBadge.className = 'badge';
  }, 3000);
}

// ─── Query ───────────────────────────────────────────────────────────────────
queryBtn.addEventListener('click', () => {
  const series = querySeriesInput.value.trim();
  const start  = parseInt(queryStartInput.value.trim());
  const end    = parseInt(queryEndInput.value.trim());

  if (!series || !/^[a-zA-Z0-9_]+$/.test(series)) {
    showError('ERR: Invalid series name.');
    return;
  }
  if (isNaN(start) || isNaN(end)) {
    showError('ERR: Start and End must be valid Unix epoch timestamps.');
    return;
  }
  if (start >= end) {
    showError('ERR: Start timestamp must be less than End timestamp.');
    return;
  }
  if (!state.connected) {
    showError('Not connected to server. Click "Connect" first.');
    return;
  }

  hideError();
  runQuery(series, start, end);
});

function runQuery(series, start, end) {
  const rangeWidth = end - start;
  const HOURLY_THRESHOLD = 3600 * 3;   // 3 hours
  const DAILY_THRESHOLD  = 86400 * 2;  // 2 days

  // Determine tier
  let tier, results;
  if (rangeWidth >= DAILY_THRESHOLD) {
    tier = 'daily';
    results = buildDailyRollup(series, start, end);
  } else if (rangeWidth >= HOURLY_THRESHOLD) {
    tier = 'hourly';
    results = buildHourlyRollup(series, start, end);
  } else {
    tier = 'raw';
    results = getRawPoints(series, start, end);
  }

  state.queryResults = results;
  state.queryMode = tier;
  renderQueryTierBadge(tier);
  renderQueryTable(results, tier);
  drawChart(results, tier);
}

function getRawPoints(series, start, end) {
  const data = demoStore[series] || generateDemoData(series, start, end);
  return data.filter(p => p.ts >= start && p.ts <= end);
}

function buildHourlyRollup(series, start, end) {
  const raw = getRawPoints(series, start, end);
  const buckets = {};
  raw.forEach(p => {
    const bucket = Math.floor(p.ts / 3600) * 3600;
    if (!buckets[bucket]) buckets[bucket] = { sum: 0, min: Infinity, max: -Infinity, count: 0 };
    buckets[bucket].sum += p.value;
    buckets[bucket].min = Math.min(buckets[bucket].min, p.value);
    buckets[bucket].max = Math.max(buckets[bucket].max, p.value);
    buckets[bucket].count++;
  });
  return Object.entries(buckets).map(([ws, b]) => ({
    window_start: parseInt(ws),
    avg: +(b.sum / b.count).toFixed(2),
    min: +b.min.toFixed(2),
    max: +b.max.toFixed(2),
    count: b.count,
  })).sort((a, b) => a.window_start - b.window_start);
}

function buildDailyRollup(series, start, end) {
  const raw = getRawPoints(series, start, end);
  const buckets = {};
  raw.forEach(p => {
    const bucket = Math.floor(p.ts / 86400) * 86400;
    if (!buckets[bucket]) buckets[bucket] = { sum: 0, min: Infinity, max: -Infinity, count: 0 };
    buckets[bucket].sum += p.value;
    buckets[bucket].min = Math.min(buckets[bucket].min, p.value);
    buckets[bucket].max = Math.max(buckets[bucket].max, p.value);
    buckets[bucket].count++;
  });
  return Object.entries(buckets).map(([ws, b]) => ({
    window_start: parseInt(ws),
    avg: +(b.sum / b.count).toFixed(2),
    min: +b.min.toFixed(2),
    max: +b.max.toFixed(2),
    count: b.count,
  })).sort((a, b) => a.window_start - b.window_start);
}

// Generate synthetic demo data so queries always return something
function generateDemoData(series, start, end) {
  const interval = 60; // 1 point per minute
  const baseValues = { engine_temp: 72, cpu_usage: 45, mem_alloc: 60 };
  const base = baseValues[series] || 50;
  const pts = [];
  for (let ts = start; ts <= end; ts += interval) {
    const noise = (Math.random() - 0.5) * 10;
    pts.push({ ts, value: +(base + noise).toFixed(2) });
  }
  if (!demoStore[series]) demoStore[series] = [];
  // Merge without duplicates
  const existing = new Set(demoStore[series].map(p => p.ts));
  pts.forEach(p => { if (!existing.has(p.ts)) demoStore[series].push(p); });
  demoStore[series].sort((a, b) => a.ts - b.ts);
  return pts;
}

function renderQueryTierBadge(tier) {
  const labels = { raw: 'RAW DATA', hourly: 'HOURLY ROLLUP', daily: 'DAILY ROLLUP' };
  queryTierBadge.textContent = labels[tier] || tier.toUpperCase();
  queryTierBadge.className = 'badge ' + tier;
  queryTierBadge.style.display = 'inline-block';
}

function renderQueryTable(results, tier) {
  queryEmpty.style.display = 'none';
  queryTableWrap.style.display = 'none';
  queryTableHead.innerHTML = '';
  queryTableBody.innerHTML = '';

  if (!results || results.length === 0) {
    queryEmpty.style.display = 'block';
    return;
  }

  // Header
  const isRollup = (tier === 'hourly' || tier === 'daily');
  const headers = isRollup
    ? ['window_start', 'avg', 'min', 'max', 'count']
    : ['timestamp', 'value'];

  const tr = document.createElement('tr');
  headers.forEach((h, i) => {
    const th = document.createElement('th');
    th.textContent = h;
    if (i > 0) th.className = 'num';
    tr.appendChild(th);
  });
  queryTableHead.appendChild(tr);

  // Rows
  results.forEach(row => {
    const tr = document.createElement('tr');
    if (isRollup) {
      [row.window_start, row.avg, row.min, row.max, row.count].forEach((v, i) => {
        const td = document.createElement('td');
        td.textContent = v;
        if (i > 0) td.className = 'num';
        tr.appendChild(td);
      });
    } else {
      [row.ts, row.value].forEach((v, i) => {
        const td = document.createElement('td');
        td.textContent = v;
        if (i > 0) td.className = 'num';
        tr.appendChild(td);
      });
    }
    queryTableBody.appendChild(tr);
  });

  queryTableWrap.style.display = 'block';
}

// ─── Chart (Canvas) ──────────────────────────────────────────────────────────
function drawChart(results, tier) {
  if (!results || results.length === 0) {
    chartEmpty.style.display = 'flex';
    queryChart.style.display = 'none';
    return;
  }

  chartEmpty.style.display = 'none';
  queryChart.style.display = 'block';

  const isRollup = (tier === 'hourly' || tier === 'daily');
  const values = isRollup ? results.map(r => r.avg) : results.map(r => r.value);

  const dpr = window.devicePixelRatio || 1;
  const W = chartContainer.clientWidth;
  const H = chartContainer.clientHeight;

  queryChart.width  = W * dpr;
  queryChart.height = H * dpr;
  queryChart.style.width  = W + 'px';
  queryChart.style.height = H + 'px';

  const ctx = queryChart.getContext('2d');
  ctx.scale(dpr, dpr);
  ctx.clearRect(0, 0, W, H);

  const PAD = { top: 16, right: 16, bottom: 28, left: 44 };
  const plotW = W - PAD.left - PAD.right;
  const plotH = H - PAD.top - PAD.bottom;

  const minVal = Math.min(...values);
  const maxVal = Math.max(...values);
  const valRange = maxVal - minVal || 1;

  // Gridlines
  ctx.strokeStyle = '#EBEBEB';
  ctx.lineWidth = 1;
  const gridLines = 4;
  for (let i = 0; i <= gridLines; i++) {
    const y = PAD.top + (i / gridLines) * plotH;
    ctx.beginPath();
    ctx.moveTo(PAD.left, y);
    ctx.lineTo(PAD.left + plotW, y);
    ctx.stroke();

    // Y label
    const label = (maxVal - (i / gridLines) * valRange).toFixed(1);
    ctx.fillStyle = '#747878';
    ctx.font = '10px JetBrains Mono, monospace';
    ctx.textAlign = 'right';
    ctx.fillText(label, PAD.left - 6, y + 4);
  }

  // Map points to canvas coords
  const pts = values.map((v, i) => ({
    x: PAD.left + (i / (values.length - 1 || 1)) * plotW,
    y: PAD.top + plotH - ((v - minVal) / valRange) * plotH,
  }));

  // Fill area
  ctx.beginPath();
  ctx.moveTo(pts[0].x, PAD.top + plotH);
  pts.forEach(p => ctx.lineTo(p.x, p.y));
  ctx.lineTo(pts[pts.length - 1].x, PAD.top + plotH);
  ctx.closePath();
  ctx.fillStyle = 'rgba(43,43,43,0.06)';
  ctx.fill();

  // Line
  ctx.beginPath();
  ctx.moveTo(pts[0].x, pts[0].y);
  for (let i = 1; i < pts.length; i++) {
    // Smooth curve
    const cpx = (pts[i - 1].x + pts[i].x) / 2;
    ctx.bezierCurveTo(cpx, pts[i - 1].y, cpx, pts[i].y, pts[i].x, pts[i].y);
  }
  ctx.strokeStyle = '#2B2B2B';
  ctx.lineWidth = 1.5;
  ctx.lineJoin = 'round';
  ctx.stroke();

  // Dots
  pts.forEach(p => {
    ctx.beginPath();
    ctx.arc(p.x, p.y, 2.5, 0, Math.PI * 2);
    ctx.fillStyle = '#2B2B2B';
    ctx.fill();
  });

  // X axis labels (first, middle, last)
  const tsArr = isRollup ? results.map(r => r.window_start) : results.map(r => r.ts);
  const xIndices = [0, Math.floor((tsArr.length - 1) / 2), tsArr.length - 1];
  xIndices.forEach(i => {
    if (i >= 0 && i < tsArr.length) {
      ctx.fillStyle = '#747878';
      ctx.font = '10px JetBrains Mono, monospace';
      ctx.textAlign = i === 0 ? 'left' : i === tsArr.length - 1 ? 'right' : 'center';
      ctx.fillText(tsArr[i], pts[i].x, H - 6);
    }
  });
}

// ─── Tail ─────────────────────────────────────────────────────────────────────
tailStartBtn.addEventListener('click', startTail);
tailStopBtn.addEventListener('click', stopTail);
tailClearBtn.addEventListener('click', () => {
  tailOutput.innerHTML = '<div class="terminal-placeholder">// Cleared</div>';
  state.tailCount = 0;
  tailCountEl.textContent = '0 points received';
});

function startTail() {
  if (!state.connected) {
    showError('Not connected to server. Click "Connect" first.');
    return;
  }
  state.tailing = true;
  state.tailCount = 0;
  const series = state.activeSeries;
  tailSeriesLabel.textContent = series;
  tailOutput.innerHTML = '';
  tailStartBtn.disabled = true;
  tailStopBtn.disabled = false;

  // Simulate live stream — push a new point every 1s
  const base = { engine_temp: 72, cpu_usage: 45, mem_alloc: 60 }[series] || 50;
  state.tailInterval = setInterval(() => {
    const ts = nowEpoch();
    const value = +(base + (Math.random() - 0.5) * 8).toFixed(2);
    pushTailLine(ts, value);
    // Also store it
    if (!demoStore[series]) demoStore[series] = [];
    demoStore[series].push({ ts, value });
  }, 1000);
}

function stopTail() {
  state.tailing = false;
  clearInterval(state.tailInterval);
  state.tailInterval = null;
  tailStartBtn.disabled = false;
  tailStopBtn.disabled = true;

  const line = document.createElement('div');
  line.className = 'terminal-line';
  line.style.opacity = '0.4';
  line.textContent = '// Tail stopped';
  tailOutput.appendChild(line);
}

function pushTailLine(ts, value) {
  state.tailCount++;
  tailCountEl.textContent = `${state.tailCount} point${state.tailCount !== 1 ? 's' : ''} received`;

  const line = document.createElement('div');
  line.className = 'terminal-line';
  line.innerHTML = `&gt; <span class="ts">${ts}</span>, <span class="val">${value}</span>`;
  tailOutput.appendChild(line);

  // Auto-scroll
  tailOutput.scrollTop = tailOutput.scrollHeight;

  // Keep max 200 lines
  while (tailOutput.children.length > 200) {
    tailOutput.removeChild(tailOutput.firstChild);
  }
}

// ─── Benchmark ────────────────────────────────────────────────────────────────
runBenchmarkBtn.addEventListener('click', runBenchmark);

function runBenchmark() {
  runBenchmarkBtn.textContent = 'Running…';
  runBenchmarkBtn.disabled = true;

  setTimeout(() => {
    // Simulate Gorilla compression on 24h of 1s data
    const pointCount = 86400;          // 24h × 1 point/sec
    const rawBytes   = pointCount * 16; // 8B ts + 8B float
    // Realistic Gorilla ratio: ~14x for smooth sensor data
    const ratio        = 13.8 + Math.random() * 1.2;
    const compBytes    = Math.round(rawBytes / ratio);
    const efficiency   = Math.round((1 - 1 / ratio) * 100);

    rawSizeEl.textContent   = formatBytes(rawBytes);
    compSizeEl.textContent  = formatBytes(compBytes);
    compRatioEl.textContent = ratio.toFixed(1) + 'x';
    efficiencyBar.style.width = efficiency + '%';
    efficiencyLabel.textContent = efficiency + '%';

    runBenchmarkBtn.textContent = 'Run';
    runBenchmarkBtn.disabled = false;
  }, 900);
}

// ─── Error close ─────────────────────────────────────────────────────────────
errorClose.addEventListener('click', hideError);

// ─── Init ────────────────────────────────────────────────────────────────────
function init() {
  // Seed timestamp inputs
  const now = nowEpoch();
  writeTimestampInput.value = now;
  queryEndInput.value = now;
  queryStartInput.value = now - 3600; // default: last 1 hour

  // Sync global series
  writeSeriesInput.value = state.activeSeries;
  querySeriesInput.value = state.activeSeries;

  // Auto-connect in demo mode
  setTimeout(() => setConnectionState(true), 400);
}

init();

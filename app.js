// ─── Chronos Operator Dashboard ──────────────────────────────────────────────
// Zero-dependency frontend: supports both Live Backend (via WebSocket)
// and fallback in-browser Demo Simulation mode.

// ─── State ───────────────────────────────────────────────────────────────────
const state = {
  connected: false,
  isLive: false,      // true when connected to real backend via WebSocket
  serverHost: 'localhost:9001',
  activeSeries: 'engine_temp',
  tailing: false,
  tailCount: 0,
  tailInterval: null,
  tailWS: null,       // dedicated WebSocket for tailing
  mainWS: null,       // main command WebSocket
  queryResults: [],
  queryMode: 'raw',
  pendingQueryCallback: null,
  queryBuffer: [],
};

// In-memory demo store (used when offline/demo mode)
const demoStore = {
  engine_temp: [],
  cpu_usage:   [],
  mem_alloc:   [],
};

// ─── DOM Elements ────────────────────────────────────────────────────────────
const globalSeriesSelect  = document.getElementById('globalSeriesSelect');
const serverHostInput     = document.getElementById('serverHost');
const connectBtn          = document.getElementById('connectBtn');
const statusDot           = document.getElementById('statusDot');
const statusLabel         = document.getElementById('statusLabel');
const sidebar             = document.getElementById('sidebar');
const hamburger           = document.getElementById('hamburger');
const navLinks            = document.querySelectorAll('.nav-link');

// Write panel
const writeSeriesInput    = document.getElementById('writeSeries');
const writeValueInput     = document.getElementById('writeValue');
const writeTimestampInput = document.getElementById('writeTimestamp');
const nowBtn              = document.getElementById('nowBtn');
const writeBtn            = document.getElementById('writeBtn');
const writeStatusBadge    = document.getElementById('writeStatusBadge');
const writeResponse       = document.getElementById('writeResponse');

// Query panel
const querySeriesInput    = document.getElementById('querySeries');
const queryStartInput     = document.getElementById('queryStart');
const queryEndInput       = document.getElementById('queryEnd');
const queryBtn            = document.getElementById('queryBtn');
const queryTierBadge      = document.getElementById('queryTierBadge');
const queryTableWrap      = document.getElementById('queryTableWrap');
const queryTableHead      = document.getElementById('queryTableHead');
const queryTableBody      = document.getElementById('queryTableBody');
const queryEmpty          = document.getElementById('queryEmpty');
const chartContainer      = document.getElementById('chartContainer');
const queryChart          = document.getElementById('queryChart');
const chartEmpty          = document.getElementById('chartEmpty');

// Tail panel
const tailStartBtn        = document.getElementById('tailStartBtn');
const tailStopBtn         = document.getElementById('tailStopBtn');
const tailClearBtn        = document.getElementById('tailClearBtn');
const tailSeriesLabel     = document.getElementById('tailSeriesLabel');
const tailCountEl         = document.getElementById('tailCount');
const tailOutput          = document.getElementById('tailOutput');

// Benchmark panel
const runBenchmarkBtn     = document.getElementById('runBenchmarkBtn');
const rawSizeEl           = document.getElementById('rawSize');
const compSizeEl          = document.getElementById('compSize');
const compRatioEl         = document.getElementById('compRatio');
const efficiencyBar       = document.getElementById('efficiencyBar');
const efficiencyLabel     = document.getElementById('efficiencyLabel');

// Error log
const errorLog            = document.getElementById('errorLog');
const errorMessage        = document.getElementById('errorMessage');
const errorClose          = document.getElementById('errorClose');

// ─── Helpers ─────────────────────────────────────────────────────────────────
function nowEpoch() {
  return Math.floor(Date.now() / 1000);
}

function formatBytes(bytes) {
  if (bytes < 1024) return bytes + ' B';
  if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(1) + ' KB';
  return (bytes / (1024 * 1024)).toFixed(2) + ' MB';
}

function showError(msg) {
  errorMessage.textContent = msg;
  errorLog.style.display = 'flex';
}

function hideError() {
  errorLog.style.display = 'none';
}

function setConnectionState(connected, isLive = false) {
  state.connected = connected;
  state.isLive = isLive;
  statusDot.className = 'status-dot' + (connected ? ' connected' : '');
  
  if (connected) {
    statusLabel.textContent = isLive 
      ? `Live: ${state.serverHost}` 
      : `Demo Mode (Simulated)`;
    connectBtn.textContent = 'Disconnect';
  } else {
    statusLabel.textContent = 'Disconnected';
    connectBtn.textContent = 'Connect';
  }
}

function getWebSocketUrl(hostInput) {
  let host = hostInput.trim();
  if (!host) host = 'localhost:9001';
  // Strip http:// or https:// or ws://
  host = host.replace(/^https?:\/\//i, '').replace(/^wss?:\/\//i, '');
  if (!host.includes('/')) {
    host = `${host}/ws`;
  }
  return `ws://${host}`;
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
  if (state.connected) {
    disconnectServer();
  } else {
    connectServer();
  }
});

function disconnectServer() {
  if (state.mainWS) {
    try { state.mainWS.close(); } catch (_) {}
    state.mainWS = null;
  }
  if (state.tailWS) {
    try { state.tailWS.close(); } catch (_) {}
    state.tailWS = null;
  }
  if (state.tailing) stopTail();
  setConnectionState(false, false);
}

function connectServer() {
  state.serverHost = serverHostInput.value.trim() || 'localhost:9001';
  const wsUrl = getWebSocketUrl(state.serverHost);

  connectBtn.textContent = 'Connecting…';
  connectBtn.disabled = true;

  try {
    const ws = new WebSocket(wsUrl);

    const timeout = setTimeout(() => {
      if (ws.readyState !== WebSocket.OPEN) {
        try { ws.close(); } catch (_) {}
        fallbackToDemo('Server connection timed out. Switched to Demo Mode.');
      }
    }, 2500);

    ws.onopen = () => {
      clearTimeout(timeout);
      connectBtn.disabled = false;
      state.mainWS = ws;
      setConnectionState(true, true);
      hideError();
    };

    ws.onmessage = (e) => {
      handleServerMessage(e.data);
    };

    ws.onerror = () => {
      clearTimeout(timeout);
      if (!state.connected) {
        fallbackToDemo(`Could not connect to ${wsUrl}. Running in Demo Mode.`);
      } else {
        showError('WebSocket connection error.');
      }
    };

    ws.onclose = () => {
      if (state.isLive) {
        disconnectServer();
        showError('Disconnected from backend server.');
      }
    };
  } catch (err) {
    fallbackToDemo(`Connection failed: ${err.message}. Running in Demo Mode.`);
  }
}

function fallbackToDemo(notice) {
  connectBtn.disabled = false;
  state.mainWS = null;
  setConnectionState(true, false);
  if (notice) {
    showError(notice);
    setTimeout(hideError, 4000);
  }
}

// ─── Central Server Message Handler ──────────────────────────────────────────
function handleServerMessage(data) {
  const lines = data.split('\n');

  for (const rawLine of lines) {
    const line = rawLine.trim();
    if (!line) continue;

    // Benchmark response: rawBytes,compBytes,ratio
    if (state.pendingBenchmark) {
      state.pendingBenchmark = false;
      const parts = line.split(',');
      if (parts.length === 3) {
        const rawBytes = parseInt(parts[0]);
        const compBytes = parseInt(parts[1]);
        const ratio = parseFloat(parts[2]);
        renderBenchmarkStats(rawBytes, compBytes, ratio);
        continue;
      }
    }

    // Query response buffering
    if (state.pendingQuery) {
      if (line === 'END') {
        state.pendingQuery = false;
        const buffered = state.queryBuffer;
        state.queryBuffer = [];
        processQueryLines(buffered);
      } else if (line.startsWith('ERR')) {
        state.pendingQuery = false;
        state.queryBuffer = [];
        showError(line);
      } else {
        state.queryBuffer.push(line);
      }
      continue;
    }

    // Write responses
    if (line === 'OK') {
      setWriteStatus('ok', 'OK');
      continue;
    }
    if (line.startsWith('ERR')) {
      setWriteStatus('error', line);
      showError(line);
      continue;
    }
  }
}

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

  if (state.isLive && state.mainWS && state.mainWS.readyState === WebSocket.OPEN) {
    // Send live WRITE command to backend
    state.mainWS.send(`WRITE ${series} ${ts} ${value}\n`);
    writeValueInput.value = '';
    writeTimestampInput.value = '';
  } else {
    // Demo mode: update local store
    const point = { ts: parseInt(ts), value: parseFloat(value) };
    if (!demoStore[series]) demoStore[series] = [];
    demoStore[series].push(point);
    demoStore[series].sort((a, b) => a.ts - b.ts);

    setWriteStatus('ok', 'OK');
    writeValueInput.value = '';
    writeTimestampInput.value = '';

    if (state.tailing && series === state.activeSeries) {
      pushTailLine(point.ts, point.value);
    }
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
  if (state.isLive && state.mainWS && state.mainWS.readyState === WebSocket.OPEN) {
    // Send live QUERY command over WebSocket
    state.pendingQuery = true;
    state.queryBuffer = [];
    state.lastQueryRange = { series, start, end };
    state.mainWS.send(`QUERY ${series} ${start} ${end}\n`);
  } else {
    // Demo query logic
    runDemoQuery(series, start, end);
  }
}

function processQueryLines(lines) {
  if (!lines || lines.length === 0) {
    renderQueryTierBadge('raw');
    renderQueryTable([], 'raw');
    drawChart([], 'raw');
    return;
  }

  // Detect format: raw (2 cols: ts,val) or rollup (5 cols: ws,avg,min,max,count)
  const firstCols = lines[0].split(',');
  const isRollup = (firstCols.length === 5);

  let tier = 'raw';
  let results = [];

  if (isRollup) {
    // Determine tier from range width
    const range = (state.lastQueryRange ? (state.lastQueryRange.end - state.lastQueryRange.start) : 0);
    tier = (range >= 86400 * 2) ? 'daily' : 'hourly';

    results = lines.map(l => {
      const p = l.split(',');
      return {
        window_start: parseInt(p[0]),
        avg: parseFloat(p[1]),
        min: parseFloat(p[2]),
        max: parseFloat(p[3]),
        count: parseInt(p[4]),
      };
    });
  } else {
    tier = 'raw';
    results = lines.map(l => {
      const p = l.split(',');
      return {
        ts: parseInt(p[0]),
        value: parseFloat(p[1]),
      };
    });
  }

  state.queryResults = results;
  state.queryMode = tier;
  renderQueryTierBadge(tier);
  renderQueryTable(results, tier);
  drawChart(results, tier);
}

function runDemoQuery(series, start, end) {
  const rangeWidth = end - start;
  const HOURLY_THRESHOLD = 3600 * 3;   // 3 hours
  const DAILY_THRESHOLD  = 86400 * 2;  // 2 days

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
  const W = chartContainer.clientWidth || 600;
  const H = chartContainer.clientHeight || 200;

  queryChart.width  = W * dpr;
  queryChart.height = H * dpr;
  queryChart.style.width  = W + 'px';
  queryChart.style.height = H + 'px';

  const ctx = queryChart.getContext('2d');
  ctx.scale(dpr, dpr);
  ctx.clearRect(0, 0, W, H);

  const PAD = { top: 16, right: 16, bottom: 28, left: 44 };
  const plotW = Math.max(10, W - PAD.left - PAD.right);
  const plotH = Math.max(10, H - PAD.top - PAD.bottom);

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

    const label = (maxVal - (i / gridLines) * valRange).toFixed(1);
    ctx.fillStyle = '#747878';
    ctx.font = '10px JetBrains Mono, monospace';
    ctx.textAlign = 'right';
    ctx.fillText(label, PAD.left - 6, y + 4);
  }

  // Map points to canvas coords
  const pts = values.map((v, i) => ({
    x: PAD.left + (i / Math.max(1, values.length - 1)) * plotW,
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

  // X axis labels
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

  if (state.isLive) {
    // Open dedicated live TAIL connection
    const wsUrl = getWebSocketUrl(state.serverHost);
    try {
      const ws = new WebSocket(wsUrl);
      state.tailWS = ws;

      ws.onopen = () => {
        ws.send(`TAIL ${series}\n`);
      };

      ws.onmessage = (e) => {
        const lines = e.data.split('\n');
        for (const l of lines) {
          const trimmed = l.trim();
          if (!trimmed || trimmed.startsWith('ERR')) continue;
          const [ts, val] = trimmed.split(',');
          if (ts && val) {
            pushTailLine(parseInt(ts), parseFloat(val));
          }
        }
      };

      ws.onclose = () => {
        if (state.tailing) stopTail();
      };
    } catch (err) {
      showError(`Tail connection failed: ${err.message}`);
      stopTail();
    }
  } else {
    // Demo stream simulation
    const base = { engine_temp: 72, cpu_usage: 45, mem_alloc: 60 }[series] || 50;
    state.tailInterval = setInterval(() => {
      const ts = nowEpoch();
      const value = +(base + (Math.random() - 0.5) * 8).toFixed(2);
      pushTailLine(ts, value);
      if (!demoStore[series]) demoStore[series] = [];
      demoStore[series].push({ ts, value });
    }, 1000);
  }
}

function stopTail() {
  state.tailing = false;
  if (state.tailWS) {
    try { state.tailWS.close(); } catch (_) {}
    state.tailWS = null;
  }
  if (state.tailInterval) {
    clearInterval(state.tailInterval);
    state.tailInterval = null;
  }
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

  tailOutput.scrollTop = tailOutput.scrollHeight;
  while (tailOutput.children.length > 200) {
    tailOutput.removeChild(tailOutput.firstChild);
  }
}

// ─── Benchmark ────────────────────────────────────────────────────────────────
runBenchmarkBtn.addEventListener('click', runBenchmark);

function runBenchmark() {
  runBenchmarkBtn.textContent = 'Running…';
  runBenchmarkBtn.disabled = true;

  if (state.isLive && state.mainWS && state.mainWS.readyState === WebSocket.OPEN) {
    state.pendingBenchmark = true;
    state.mainWS.send('BENCHMARK\n');
  } else {
    // Demo simulation
    setTimeout(() => {
      const pointCount = 86400;
      const rawBytes   = pointCount * 16;
      const ratio      = 13.8 + Math.random() * 1.2;
      const compBytes  = Math.round(rawBytes / ratio);
      renderBenchmarkStats(rawBytes, compBytes, ratio);
    }, 800);
  }
}

function renderBenchmarkStats(rawBytes, compBytes, ratio) {
  const efficiency = Math.min(100, Math.max(0, Math.round((1 - (compBytes / (rawBytes || 1))) * 100)));

  rawSizeEl.textContent   = formatBytes(rawBytes);
  compSizeEl.textContent  = formatBytes(compBytes);
  compRatioEl.textContent = (ratio ? ratio.toFixed(2) : (rawBytes / (compBytes || 1)).toFixed(2)) + 'x';
  efficiencyBar.style.width = efficiency + '%';
  efficiencyLabel.textContent = efficiency + '%';

  runBenchmarkBtn.textContent = 'Run';
  runBenchmarkBtn.disabled = false;
}

// ─── Error close ─────────────────────────────────────────────────────────────
errorClose.addEventListener('click', hideError);

// ─── Init ────────────────────────────────────────────────────────────────────
function init() {
  const now = nowEpoch();
  writeTimestampInput.value = now;
  queryEndInput.value = now;
  queryStartInput.value = now - 3600;

  writeSeriesInput.value = state.activeSeries;
  querySeriesInput.value = state.activeSeries;

  // Auto-connect to local backend or fallback to demo
  connectServer();
}

init();

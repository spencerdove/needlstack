/**
 * Needlstack — main.js
 * Client-side logic for the interactive stock visualization tool.
 */

// ── State ──────────────────────────────────────────────────────────────────

const state = {
  tickers: [],           // [{ticker, company_name, sector, industry}]
  cache: {},             // { TICKER: { prices: [...], financials: {...} } }
  activeTickers: [],     // ['AAPL', 'MSFT']
  activeMetrics: ['price'],
  dateRange: '1Y',
  granularity: 'D',      // 'D' | 'W' | 'M'
  loading: new Set(),
};

const METRICS = [
  { id: 'price',      label: 'Price',             source: 'prices',      field: 'adj_close',         yaxis: 'y1', unit: '$' },
  { id: 'revenue',    label: 'Revenue',            source: 'income',      field: 'revenue',           yaxis: 'y2', unit: '$B' },
  { id: 'net_income', label: 'Net Income',         source: 'income',      field: 'net_income',        yaxis: 'y2', unit: '$B' },
  { id: 'eps',        label: 'EPS',                source: 'income',      field: 'eps_diluted',       yaxis: 'y3', unit: '$' },
  { id: 'fcf',        label: 'Free Cash Flow',     source: 'cashflow',    field: 'free_cash_flow',    yaxis: 'y2', unit: '$B' },
  { id: 'debt',       label: 'Total Debt',         source: 'balance',     field: 'long_term_debt',    yaxis: 'y2', unit: '$B' },
  { id: 'equity',     label: 'Stockholders Equity',source: 'balance',     field: 'stockholders_equity',yaxis: 'y2', unit: '$B' },
];

const DATE_RANGES = ['1M', '3M', '6M', '1Y', '3Y', '5Y', 'MAX'];
const GRANULARITIES = [
  { id: 'D', label: 'Daily' },
  { id: 'W', label: 'Weekly' },
  { id: 'M', label: 'Monthly' },
];

const BASE_PATH = (() => {
  // Works whether served from root or from /needlstack/
  const path = window.location.pathname.replace(/\/[^/]*$/, '');
  return path || '';
})();

// ── Bootstrap ──────────────────────────────────────────────────────────────

document.addEventListener('DOMContentLoaded', async () => {
  buildControls();
  await fetchTickers();
  renderChart();
});

// ── Controls ───────────────────────────────────────────────────────────────

function buildControls() {
  const controls = document.getElementById('controls');

  // Row 1: search + chips
  const row1 = document.createElement('div');
  row1.className = 'controls-row';

  // Search wrapper
  const searchWrapper = document.createElement('div');
  searchWrapper.className = 'search-wrapper';

  const input = document.createElement('input');
  input.id = 'ticker-input';
  input.type = 'text';
  input.placeholder = 'Add ticker…';
  input.autocomplete = 'off';
  input.autocapitalize = 'characters';

  const dropdown = document.createElement('div');
  dropdown.id = 'ticker-dropdown';

  searchWrapper.append(input, dropdown);

  const chipContainer = document.createElement('div');
  chipContainer.id = 'chip-container';

  row1.append(searchWrapper, chipContainer);

  // Row 2: metrics + date range
  const row2 = document.createElement('div');
  row2.className = 'controls-row';

  // Metrics
  const metricsGroup = document.createElement('div');
  metricsGroup.className = 'metrics-group';
  const metricsLabel = document.createElement('span');
  metricsLabel.className = 'metrics-label';
  metricsLabel.textContent = 'Metrics';
  metricsGroup.appendChild(metricsLabel);

  for (const m of METRICS) {
    const btn = document.createElement('button');
    btn.className = 'metric-btn' + (state.activeMetrics.includes(m.id) ? ' active' : '');
    btn.textContent = m.label;
    btn.dataset.metric = m.id;
    btn.addEventListener('click', () => toggleMetric(m.id, btn));
    metricsGroup.appendChild(btn);
  }

  // Spacer
  const spacer = document.createElement('div');
  spacer.style.flex = '1';

  // Granularity (price only)
  const granGroup = document.createElement('div');
  granGroup.className = 'range-group';
  granGroup.id = 'gran-group';
  const granLabel = document.createElement('span');
  granLabel.className = 'metrics-label';
  granLabel.style.marginRight = '4px';
  granLabel.textContent = 'Bars';
  granGroup.appendChild(granLabel);
  for (const g of GRANULARITIES) {
    const btn = document.createElement('button');
    btn.className = 'range-btn' + (state.granularity === g.id ? ' active' : '');
    btn.textContent = g.label;
    btn.dataset.gran = g.id;
    btn.addEventListener('click', () => setGranularity(g.id, btn));
    granGroup.appendChild(btn);
  }

  // Date range
  const rangeGroup = document.createElement('div');
  rangeGroup.className = 'range-group';
  for (const r of DATE_RANGES) {
    const btn = document.createElement('button');
    btn.className = 'range-btn' + (state.dateRange === r ? ' active' : '');
    btn.textContent = r;
    btn.dataset.range = r;
    btn.addEventListener('click', () => setDateRange(r, btn));
    rangeGroup.appendChild(btn);
  }

  row2.append(metricsGroup, spacer, granGroup, rangeGroup);
  controls.append(row1, row2);

  // Wire up search
  let debounceTimer;
  input.addEventListener('input', () => {
    clearTimeout(debounceTimer);
    debounceTimer = setTimeout(() => updateDropdown(input.value.trim().toUpperCase()), 120);
  });

  input.addEventListener('keydown', (e) => {
    const items = dropdown.querySelectorAll('.dropdown-item');
    const focused = dropdown.querySelector('.focused');
    let idx = Array.from(items).indexOf(focused);

    if (e.key === 'ArrowDown') {
      e.preventDefault();
      idx = Math.min(idx + 1, items.length - 1);
      items.forEach(el => el.classList.remove('focused'));
      items[idx]?.classList.add('focused');
    } else if (e.key === 'ArrowUp') {
      e.preventDefault();
      idx = Math.max(idx - 1, 0);
      items.forEach(el => el.classList.remove('focused'));
      items[idx]?.classList.add('focused');
    } else if (e.key === 'Enter') {
      e.preventDefault();
      const target = focused || items[0];
      if (target) addTicker(target.dataset.ticker);
    } else if (e.key === 'Escape') {
      closeDropdown();
    }
  });

  document.addEventListener('click', (e) => {
    if (!searchWrapper.contains(e.target)) closeDropdown();
  });
}

// ── Ticker search ──────────────────────────────────────────────────────────

function updateDropdown(query) {
  const dropdown = document.getElementById('ticker-dropdown');
  dropdown.innerHTML = '';

  if (!query) {
    closeDropdown();
    return;
  }

  const matches = state.tickers
    .filter(t =>
      t.ticker.startsWith(query) ||
      t.company_name?.toLowerCase().includes(query.toLowerCase())
    )
    .filter(t => !state.activeTickers.includes(t.ticker))
    .slice(0, 10);

  if (matches.length === 0) {
    const empty = document.createElement('div');
    empty.className = 'dropdown-empty';
    empty.textContent = 'No results';
    dropdown.appendChild(empty);
  } else {
    for (const m of matches) {
      const item = document.createElement('div');
      item.className = 'dropdown-item';
      item.dataset.ticker = m.ticker;

      const tickerEl = document.createElement('span');
      tickerEl.className = 'dropdown-ticker';
      tickerEl.textContent = m.ticker;

      const nameEl = document.createElement('span');
      nameEl.className = 'dropdown-name';
      nameEl.textContent = m.company_name || '';

      item.append(tickerEl, nameEl);
      item.addEventListener('mousedown', (e) => {
        e.preventDefault();
        addTicker(m.ticker);
      });
      dropdown.appendChild(item);
    }
  }

  dropdown.classList.add('open');
}

function closeDropdown() {
  const dropdown = document.getElementById('ticker-dropdown');
  dropdown.classList.remove('open');
  dropdown.innerHTML = '';
}

async function addTicker(ticker) {
  const input = document.getElementById('ticker-input');
  input.value = '';
  closeDropdown();

  if (state.activeTickers.includes(ticker)) return;
  state.activeTickers.push(ticker);
  renderChips();
  await loadTickerData(ticker);
  renderChart();
}

function removeTicker(ticker) {
  state.activeTickers = state.activeTickers.filter(t => t !== ticker);
  renderChips();
  renderChart();
}

function renderChips() {
  const container = document.getElementById('chip-container');
  container.innerHTML = '';
  for (const ticker of state.activeTickers) {
    const chip = document.createElement('div');
    chip.className = 'chip';

    const label = document.createElement('span');
    label.textContent = ticker;

    const btn = document.createElement('button');
    btn.className = 'chip-remove';
    btn.textContent = '×';
    btn.title = `Remove ${ticker}`;
    btn.addEventListener('click', () => removeTicker(ticker));

    chip.append(label, btn);
    container.appendChild(chip);
  }
}

// ── Metric + date range ────────────────────────────────────────────────────

function toggleMetric(id, btn) {
  if (state.activeMetrics.includes(id)) {
    if (state.activeMetrics.length === 1) return; // keep at least one
    state.activeMetrics = state.activeMetrics.filter(m => m !== id);
    btn.classList.remove('active');
  } else {
    state.activeMetrics.push(id);
    btn.classList.add('active');
  }
  renderChart();
}

function setDateRange(range, btn) {
  state.dateRange = range;
  document.querySelectorAll('.range-btn').forEach(b => b.classList.remove('active'));
  btn.classList.add('active');
  renderChart();
}

function setGranularity(gran, btn) {
  state.granularity = gran;
  document.querySelectorAll('#gran-group .range-btn').forEach(b => b.classList.remove('active'));
  btn.classList.add('active');
  renderChart();
}

// ── Data fetching ──────────────────────────────────────────────────────────

async function fetchTickers() {
  try {
    const res = await fetch(`${BASE_PATH}/data/tickers.json`);
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    state.tickers = await res.json();
    setStatus(`${state.tickers.length} tickers loaded`);
  } catch (err) {
    setStatus(`Failed to load tickers: ${err.message}`);
    console.error(err);
  }
}

async function loadTickerData(ticker) {
  if (state.cache[ticker]) return;
  if (state.loading.has(ticker)) return;

  state.loading.add(ticker);
  showLoading(true);

  try {
    const [pricesRes, financialsRes] = await Promise.all([
      fetch(`${BASE_PATH}/data/prices/${ticker}.json`),
      fetch(`${BASE_PATH}/data/financials/${ticker}.json`),
    ]);

    const prices = pricesRes.ok ? await pricesRes.json() : [];
    const financials = financialsRes.ok ? await financialsRes.json() : {};

    state.cache[ticker] = { prices, financials };
    setStatus(`Loaded ${ticker} — ${prices.length} price points`);
  } catch (err) {
    console.error(`Failed to load ${ticker}:`, err);
    state.cache[ticker] = { prices: [], financials: {} };
  } finally {
    state.loading.delete(ticker);
    showLoading(false);
  }
}

// ── Date filtering ─────────────────────────────────────────────────────────

function cutoffDate(range) {
  const now = new Date();
  const map = {
    '1M': 1,
    '3M': 3,
    '6M': 6,
    '1Y': 12,
    '3Y': 36,
    '5Y': 60,
    'MAX': null,
  };
  const months = map[range];
  if (!months) return null;
  const d = new Date(now);
  d.setMonth(d.getMonth() - months);
  return d.toISOString().slice(0, 10);
}

function filterByDateRange(rows, dateField, range) {
  const cutoff = cutoffDate(range);
  if (!cutoff) return rows;
  return rows.filter(r => r[dateField] >= cutoff);
}

// ── OHLCV aggregation ──────────────────────────────────────────────────────

function aggregateOHLCV(rows, granularity) {
  if (granularity === 'D') return rows;

  const groups = new Map();
  for (const r of rows) {
    const d = new Date(r.date);
    let key;
    if (granularity === 'W') {
      // Monday of the ISO week
      const day = (d.getDay() + 6) % 7; // Mon=0 … Sun=6
      const mon = new Date(d);
      mon.setDate(d.getDate() - day);
      key = mon.toISOString().slice(0, 10);
    } else {
      // First of month
      key = r.date.slice(0, 7) + '-01';
    }
    if (!groups.has(key)) groups.set(key, []);
    groups.get(key).push(r);
  }

  return [...groups.entries()]
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([date, bars]) => ({
      date,
      open:      bars[0].open,
      high:      Math.max(...bars.map(b => b.high)),
      low:       Math.min(...bars.map(b => b.low)),
      close:     bars[bars.length - 1].close,
      adj_close: bars[bars.length - 1].adj_close,
      volume:    bars.reduce((s, b) => s + (b.volume || 0), 0),
    }));
}

// ── Chart rendering ────────────────────────────────────────────────────────

const TICKER_COLORS = [
  '#58a6ff', '#f78166', '#3fb950', '#d2a8ff',
  '#ffa657', '#79c0ff', '#ff7b72', '#56d364',
];

function renderChart() {
  const placeholder = document.getElementById('chart-placeholder');
  const chartDiv = document.getElementById('plotly-chart');

  if (state.activeTickers.length === 0) {
    placeholder.style.display = 'flex';
    if (window.Plotly && chartDiv._fullLayout) {
      Plotly.purge(chartDiv);
    }
    return;
  }

  if (!window.Plotly) {
    setStatus('Plotly failed to load — check network connection');
    return;
  }

  placeholder.style.display = 'none';

  const traces = [];
  const metricsInfo = METRICS.filter(m => state.activeMetrics.includes(m.id));
  const usedAxes = new Set(metricsInfo.map(m => m.yaxis));

  for (let ti = 0; ti < state.activeTickers.length; ti++) {
    const ticker = state.activeTickers[ti];
    const cached = state.cache[ticker];
    if (!cached) continue;

    const color = TICKER_COLORS[ti % TICKER_COLORS.length];
    const traceName = state.activeTickers.length > 1
      ? `${ticker} ${metricsInfo[0]?.label}`
      : metricsInfo[0]?.label;

    for (const metric of metricsInfo) {
      const yax = axisId(metric.yaxis, usedAxes);
      const multiTicker = state.activeTickers.length > 1;
      const name = multiTicker ? `${ticker} ${metric.label}` : metric.label;

      if (metric.source === 'prices') {
        // ── Candlestick ──────────────────────────────────────────────────
        const rangeFiltered = filterByDateRange(cached.prices, 'date', state.dateRange);
        const filtered = aggregateOHLCV(rangeFiltered, state.granularity);
        traces.push({
          type: 'candlestick',
          name,
          x:     filtered.map(r => r.date),
          open:  filtered.map(r => r.open),
          high:  filtered.map(r => r.high),
          low:   filtered.map(r => r.low),
          close: filtered.map(r => r.close),
          increasing: { line: { color: '#3fb950', width: 1 } },
          decreasing: { line: { color: '#f85149', width: 1 } },
          whiskerwidth: 0.4,
          yaxis: yax,
          hovertemplate:
            '<b>' + name + '</b><br>' +
            'O: %{open:.2f}  H: %{high:.2f}<br>' +
            'L: %{low:.2f}  C: %{close:.2f}<extra></extra>',
        });

      } else {
        // ── Line + markers (financials) ───────────────────────────────────
        let rows = [];
        if (metric.source === 'income')   rows = cached.financials?.income_statements || [];
        if (metric.source === 'balance')  rows = cached.financials?.balance_sheets || [];
        if (metric.source === 'cashflow') rows = cached.financials?.cash_flows || [];

        // Annual periods only to avoid quarterly clutter
        rows = rows.filter(r => r.period_type === 'A');
        const filtered = filterByDateRange(rows, 'period_end', state.dateRange);

        const xs = filtered.map(r => r.period_end);
        const ys = filtered.map(r => {
          const v = r[metric.field];
          return v != null ? +(v / 1e9).toFixed(3) : null;
        });

        traces.push({
          type: 'scatter',
          mode: 'lines+markers',
          name,
          x: xs,
          y: ys,
          line:   { color, width: 2 },
          marker: { color, size: 6 },
          yaxis: yax,
          hovertemplate:
            `<b>${name}</b><br>%{x}<br>%{y:.2f} ${metric.unit}<extra></extra>`,
        });
      }
    }
  }

  const layout = buildLayout(metricsInfo, usedAxes);

  const config = {
    responsive: true,
    displaylogo: false,
    modeBarButtonsToRemove: ['select2d', 'lasso2d', 'autoScale2d'],
    toImageButtonOptions: { filename: 'needlstack_chart' },
  };

  if (chartDiv._fullLayout) {
    Plotly.react(chartDiv, traces, layout, config);
  } else {
    Plotly.newPlot(chartDiv, traces, layout, config);
  }
  Plotly.Plots.resize(chartDiv);
}

// Map logical yaxis key (y1/y2/y3) to Plotly axis id, compressing unused axes
function axisId(yaxis, usedAxes) {
  const sorted = [...usedAxes].sort();
  const idx = sorted.indexOf(yaxis);
  if (idx === 0) return 'y';
  return `y${idx + 1}`;
}

function buildLayout(metricsInfo, usedAxes) {
  const sorted = [...usedAxes].sort();

  const axisBase = {
    gridcolor: '#21262d',
    linecolor: '#30363d',
    tickfont: { color: '#8b949e', size: 11 },
    titlefont: { color: '#8b949e', size: 11 },
    zerolinecolor: '#30363d',
  };

  // Build y-axis configs
  const axisConfigs = {};
  sorted.forEach((ykey, i) => {
    const metrics = metricsInfo.filter(m => m.yaxis === ykey);
    const unit = metrics[0]?.unit || '';
    const axisName = i === 0 ? 'yaxis' : `yaxis${i + 1}`;
    const side = i % 2 === 0 ? 'left' : 'right';
    axisConfigs[axisName] = {
      ...axisBase,
      title: unit,
      side,
      overlaying: i > 0 ? 'y' : undefined,
      showgrid: i === 0,
      tickformat: unit === '$B' ? ',.2f' : ',.2f',
    };
  });

  // Compute explicit x range so the view matches the date range selector exactly
  const today = new Date().toISOString().slice(0, 10);
  const rangeStart = cutoffDate(state.dateRange);
  const xRange = rangeStart ? [rangeStart, today] : undefined;

  return {
    paper_bgcolor: '#161b22',
    plot_bgcolor: '#0d1117',
    font: { family: '-apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif', color: '#e6edf3' },
    legend: {
      bgcolor: '#161b22',
      bordercolor: '#30363d',
      borderwidth: 1,
      font: { color: '#e6edf3', size: 11 },
    },
    margin: { l: 60, r: 60, t: 20, b: 60 },
    hovermode: 'closest',
    hoverlabel: {
      bgcolor: '#21262d',
      bordercolor: '#30363d',
      font: { color: '#e6edf3', size: 12 },
    },
    xaxis: {
      ...axisBase,
      type: 'date',
      range: xRange,
      rangeslider: { bgcolor: '#161b22', bordercolor: '#30363d', thickness: 0.06 },
    },
    ...axisConfigs,
  };
}

// ── Utility ────────────────────────────────────────────────────────────────

function showLoading(on) {
  const placeholder = document.getElementById('chart-placeholder');
  const spinner = placeholder.querySelector('.spinner');
  const msg = placeholder.querySelector('p');
  if (on && state.activeTickers.length > 0) {
    placeholder.style.display = 'flex';
    spinner.style.display = 'block';
    msg.textContent = 'Loading data…';
  } else {
    placeholder.style.display = 'none';
    spinner.style.display = 'none';
  }
}

function setStatus(text) {
  const bar = document.getElementById('status-bar');
  if (bar) bar.textContent = text;
}

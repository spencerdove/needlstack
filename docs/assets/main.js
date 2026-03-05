/**
 * Needlstack — main.js
 * Client-side logic for the interactive stock visualization tool.
 */

// ── State ──────────────────────────────────────────────────────────────────

const state = {
  tickers: [],           // [{ticker, company_name, sector, industry}]
  cache: {},             // { TICKER: { prices: [...], financials: {...}, ... } }
  activeTickers: [],     // ['AAPL', 'MSFT']
  activeMetrics: ['price'],
  dateRange: '1Y',
  granularity: 'D',      // 'D' | 'W' | 'M'
  loading: new Set(),
  activeTab: 'ownership', // 'ownership' | 'filings' | 'news' | 'social' | 'narratives' | 'chat'
  indexData: {},          // { SP500: [...], NDX100: [...] }
  narratives: [],         // [...]
};

const METRICS = [
  { id: 'price',      label: 'Price',               source: 'prices',      field: 'adj_close',          yaxis: 'y1', unit: '$' },
  { id: 'revenue',    label: 'Revenue',              source: 'income',      field: 'revenue',            yaxis: 'y2', unit: '$B' },
  { id: 'net_income', label: 'Net Income',           source: 'income',      field: 'net_income',         yaxis: 'y2', unit: '$B' },
  { id: 'eps',        label: 'EPS',                  source: 'income',      field: 'eps_diluted',        yaxis: 'y3', unit: '$' },
  { id: 'fcf',        label: 'Free Cash Flow',       source: 'cashflow',    field: 'free_cash_flow',     yaxis: 'y2', unit: '$B' },
  { id: 'debt',       label: 'Total Debt',           source: 'balance',     field: 'long_term_debt',     yaxis: 'y2', unit: '$B' },
  { id: 'equity',     label: 'Stockholders Equity',  source: 'balance',     field: 'stockholders_equity', yaxis: 'y2', unit: '$B' },
  { id: 'pe_ttm',     label: 'P/E TTM',              source: 'valuation',   field: 'pe_ttm',             yaxis: 'y3', unit: 'x' },
  { id: 'ev_ebitda',  label: 'EV/EBITDA',            source: 'valuation',   field: 'ev_ebitda',          yaxis: 'y3', unit: 'x' },
  { id: 'dividends',  label: 'Dividends',            source: 'corpactions', field: 'amount',             yaxis: 'y2', unit: '$' },
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

const DATA_BASE_URL = (window.location.hostname === 'localhost' || window.location.hostname === '127.0.0.1')
  ? `${BASE_PATH}/data`
  : 'https://data.needlstack.com';

// ── Bootstrap ──────────────────────────────────────────────────────────────

document.addEventListener('DOMContentLoaded', async () => {
  buildControls();
  buildTabBar();
  await Promise.all([fetchTickers(), fetchGlobalData()]);
  renderTabContent();
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

    // Index membership badges
    const badges = document.createElement('span');
    badges.className = 'chip-badges';
    const memberOf = Object.entries(state.indexData || {})
      .filter(([idx, tickers]) => Array.isArray(tickers) && tickers.includes(ticker))
      .map(([idx]) => idx.replace('SP', 'S&P '));
    for (const idx of memberOf.slice(0, 2)) {
      const badge = document.createElement('span');
      badge.className = 'index-badge';
      badge.textContent = idx;
      badges.appendChild(badge);
    }

    const btn = document.createElement('button');
    btn.className = 'chip-remove';
    btn.textContent = '×';
    btn.title = `Remove ${ticker}`;
    btn.addEventListener('click', () => removeTicker(ticker));

    chip.append(label, badges, btn);
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
    const res = await fetch(`${DATA_BASE_URL}/tickers.json`);
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    state.tickers = await res.json();
    setStatus(`${state.tickers.length} tickers loaded`);
  } catch (err) {
    setStatus(`Failed to load tickers: ${err.message}`);
    console.error(err);
  }
}

async function fetchGlobalData() {
  try {
    const [indexRes, narrativesRes] = await Promise.all([
      fetch(`${DATA_BASE_URL}/indexes.json`),
      fetch(`${DATA_BASE_URL}/narratives.json`),
    ]);
    state.indexData = indexRes.ok ? await indexRes.json() : {};
    state.narratives = narrativesRes.ok ? await narrativesRes.json() : [];
  } catch (err) {
    console.error('Failed to load global data:', err);
  }
}

async function loadTickerData(ticker) {
  if (state.cache[ticker]) return;
  if (state.loading.has(ticker)) return;

  state.loading.add(ticker);
  showLoading(true);

  try {
    const [pricesRes, financialsRes, metadataRes, corpActionsRes, profileRes, ownershipRes, sentimentRes, newsRes, socialRes] = await Promise.all([
      fetch(`${DATA_BASE_URL}/prices/${ticker}.json`),
      fetch(`${DATA_BASE_URL}/financials/${ticker}.json`),
      fetch(`${DATA_BASE_URL}/metadata/${ticker}.json`),
      fetch(`${DATA_BASE_URL}/corporate_actions/${ticker}.json`),
      fetch(`${DATA_BASE_URL}/profiles/${ticker}.json`),
      fetch(`${DATA_BASE_URL}/ownership/${ticker}.json`),
      fetch(`${DATA_BASE_URL}/sentiment/${ticker}.json`),
      fetch(`${DATA_BASE_URL}/news/${ticker}.json`),
      fetch(`${DATA_BASE_URL}/social/${ticker}.json`),
    ]);

    const prices = pricesRes.ok ? await pricesRes.json() : [];
    const financials = financialsRes.ok ? await financialsRes.json() : {};
    const metadata = metadataRes.ok ? await metadataRes.json() : {};
    const corporateActions = corpActionsRes.ok ? await corpActionsRes.json() : [];
    const profile = profileRes.ok ? await profileRes.json() : {};
    const ownership = ownershipRes.ok ? await ownershipRes.json() : { summary: null, top_holders: [] };
    const sentiment = sentimentRes.ok ? await sentimentRes.json() : [];
    const news = newsRes.ok ? await newsRes.json() : [];
    const social = socialRes.ok ? await socialRes.json() : { reddit: [], stocktwits: [] };

    state.cache[ticker] = { prices, financials, metadata, corporateActions, profile, ownership, sentiment, news, social };
    setStatus(`Loaded ${ticker} — ${prices.length} price points`);
  } catch (err) {
    console.error(`Failed to load ${ticker}:`, err);
    state.cache[ticker] = { prices: [], financials: {}, metadata: {}, corporateActions: [], profile: {}, ownership: { summary: null, top_holders: [] }, sentiment: [], news: [], social: { reddit: [], stocktwits: [] } };
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
    renderTabContent();
    return;
  }

  if (!window.Plotly) {
    setStatus('Plotly failed to load — check network connection');
    return;
  }

  placeholder.style.display = 'none';

  const traces = [];
  const shapes = [];
  const annotations = [];
  const metricsInfo = METRICS.filter(m => state.activeMetrics.includes(m.id));
  const usedAxes = new Set(metricsInfo.map(m => m.yaxis));

  for (let ti = 0; ti < state.activeTickers.length; ti++) {
    const ticker = state.activeTickers[ti];
    const cached = state.cache[ticker];
    if (!cached) continue;

    const color = TICKER_COLORS[ti % TICKER_COLORS.length];
    const multiTicker = state.activeTickers.length > 1;

    for (const metric of metricsInfo) {
      const yax = axisId(metric.yaxis, usedAxes);
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

        // ── Corporate action markers ──────────────────────────────────────
        const cutoff = cutoffDate(state.dateRange) || '';
        for (const action of (cached.corporateActions || [])) {
          if (action.action_type === 'split' && action.action_date >= cutoff) {
            shapes.push({
              type: 'line', xref: 'x', yref: 'paper',
              x0: action.action_date, x1: action.action_date, y0: 0, y1: 1,
              line: { color: '#ffa657', width: 1, dash: 'dot' }
            });
            annotations.push({
              x: action.action_date, xref: 'x', yref: 'paper', y: 1.02, showarrow: false,
              text: `${action.ratio}:1 split`, font: { color: '#ffa657', size: 10 }
            });
          }
          if (action.action_type === 'dividend' && action.action_date >= cutoff) {
            annotations.push({
              x: action.action_date, xref: 'x', yref: 'paper', y: -0.05, showarrow: false,
              text: `$${action.amount?.toFixed(2)}`, font: { color: '#3fb950', size: 9 }
            });
          }
        }

      } else if (metric.source === 'valuation') {
        // ── Valuation snapshots ───────────────────────────────────────────
        const rows = cached.financials?.valuation_snapshots || [];
        const filtered = filterByDateRange(rows, 'snapshot_date', state.dateRange);
        const xs = filtered.map(r => r.snapshot_date);
        const ys = filtered.map(r => r[metric.field]);
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

      } else if (metric.source === 'corpactions') {
        // ── Dividends as scatter ──────────────────────────────────────────
        const rows = (cached.corporateActions || []).filter(r => r.action_type === 'dividend');
        const filtered = filterByDateRange(rows, 'action_date', state.dateRange);
        const xs = filtered.map(r => r.action_date);
        const ys = filtered.map(r => r[metric.field]);
        traces.push({
          type: 'scatter',
          mode: 'markers',
          name,
          x: xs,
          y: ys,
          marker: { color, size: 8, symbol: 'diamond' },
          yaxis: yax,
          hovertemplate:
            `<b>${name}</b><br>%{x}<br>$%{y:.4f}<extra></extra>`,
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

  const layout = buildLayout(metricsInfo, usedAxes, { shapes, annotations });

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

  renderTabContent();
}

// Map logical yaxis key (y1/y2/y3) to Plotly axis id, compressing unused axes
function axisId(yaxis, usedAxes) {
  const sorted = [...usedAxes].sort();
  const idx = sorted.indexOf(yaxis);
  if (idx === 0) return 'y';
  return `y${idx + 1}`;
}

function buildLayout(metricsInfo, usedAxes, extra = {}) {
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
    shapes: extra.shapes || [],
    annotations: extra.annotations || [],
    ...axisConfigs,
  };
}

// ── Tab system ─────────────────────────────────────────────────────────────

function buildTabBar() {
  const tabBar = document.getElementById('tab-bar');
  tabBar.innerHTML = '';
  const tabs = [
    { id: 'ownership',  label: 'Ownership' },
    { id: 'filings',    label: 'Filings' },
    { id: 'news',       label: 'News' },
    { id: 'social',     label: 'Social' },
    { id: 'narratives', label: 'Narratives' },
    { id: 'chat',       label: 'AI Chat' },
  ];
  for (const tab of tabs) {
    const btn = document.createElement('button');
    btn.className = 'tab-btn' + (state.activeTab === tab.id ? ' active' : '');
    btn.textContent = tab.label;
    btn.dataset.tab = tab.id;
    btn.addEventListener('click', () => setActiveTab(tab.id));
    tabBar.appendChild(btn);
  }
}

function setActiveTab(tabId) {
  state.activeTab = tabId;
  document.querySelectorAll('.tab-btn').forEach(b =>
    b.classList.toggle('active', b.dataset.tab === tabId)
  );
  renderTabContent();
}

function renderTabContent() {
  const content = document.getElementById('tab-content');
  if (!content) return;

  if (state.activeTickers.length === 0) {
    content.innerHTML = '<p class="tab-empty">Select a ticker to see details</p>';
    return;
  }
  const ticker = state.activeTickers[0]; // primary ticker
  const cached = state.cache[ticker];
  if (!cached) { content.innerHTML = '<p class="tab-empty">Loading\u2026</p>'; return; }

  try {
    switch (state.activeTab) {
      case 'ownership':   renderOwnershipTab(content, ticker, cached); break;
      case 'filings':     renderFilingsTab(content, ticker, cached); break;
      case 'news':        renderNewsTab(content, ticker, cached); break;
      case 'social':      renderSocialTab(content, ticker, cached); break;
      case 'narratives':  renderNarrativesTab(content); break;
      case 'chat':        renderChatTab(content); break;
      default:            content.innerHTML = ''; break;
    }
  } catch (err) {
    console.error('Tab render error:', err);
    content.innerHTML = `<p class="tab-empty">Error rendering tab: ${err.message}</p>`;
  }
}

// ── Tab renderers ──────────────────────────────────────────────────────────

function renderOwnershipTab(content, ticker, cached) {
  const { ownership } = cached;
  if (!ownership || !ownership.summary) {
    content.innerHTML = `<p class="tab-empty">No institutional ownership data for ${ticker}</p>`;
    return;
  }
  const { summary, top_holders } = ownership;

  let html = `<div class="tab-section">
    <div class="ownership-summary">
      <div class="stat-card"><span class="stat-label">Institutions</span><span class="stat-value">${summary.total_institutions?.toLocaleString() || '\u2014'}</span></div>
      <div class="stat-card"><span class="stat-label">% Held</span><span class="stat-value">${summary.pct_outstanding_held?.toFixed(1) || '\u2014'}%</span></div>
      <div class="stat-card"><span class="stat-label">Net Change</span><span class="stat-value ${(summary.net_change_shares || 0) >= 0 ? 'positive' : 'negative'}">${formatShares(summary.net_change_shares)}</span></div>
      <div class="stat-card"><span class="stat-label">As of</span><span class="stat-value">${summary.report_date || '\u2014'}</span></div>
    </div>
    <table class="data-table">
      <thead><tr><th>Institution</th><th>Shares Held</th><th>Market Value</th><th>% Portfolio</th><th>Change</th></tr></thead>
      <tbody>`;

  for (const h of (top_holders || []).slice(0, 10)) {
    const changeClass = (h.change_shares || 0) >= 0 ? 'positive' : 'negative';
    html += `<tr>
      <td>${h.institution_name}</td>
      <td>${formatShares(h.shares_held)}</td>
      <td>$${formatLargeNum(h.market_value)}</td>
      <td>${h.pct_of_portfolio?.toFixed(2) || '\u2014'}%</td>
      <td class="${changeClass}">${formatShares(h.change_shares)}</td>
    </tr>`;
  }
  html += `</tbody></table></div>`;
  content.innerHTML = html;
}

function renderFilingsTab(content, ticker, cached) {
  const filings = cached.financials?.sec_filings || [];
  if (filings.length === 0) {
    content.innerHTML = `<p class="tab-empty">No recent SEC filings data for ${ticker}</p>`;
    return;
  }
  let html = `<div class="tab-section"><table class="data-table">
    <thead><tr><th>Filed</th><th>Form</th><th>Items</th><th>Link</th></tr></thead><tbody>`;
  for (const f of filings.slice(0, 20)) {
    const items = JSON.parse(f.items_reported || '[]');
    const badges = items.map(i => `<span class="item-badge">${i}</span>`).join(' ');
    html += `<tr>
      <td>${f.filed_date}</td>
      <td>${f.form_type}</td>
      <td>${badges || '\u2014'}</td>
      <td>${f.primary_doc_url ? `<a href="${f.primary_doc_url}" target="_blank" rel="noopener">SEC \u2197</a>` : '\u2014'}</td>
    </tr>`;
  }
  html += `</tbody></table></div>`;
  content.innerHTML = html;
}

function renderNewsTab(content, ticker, cached) {
  const articles = cached.news || [];
  if (articles.length === 0) {
    content.innerHTML = `<p class="tab-empty">No news data for ${ticker}. Run the RSS pipeline to collect articles.</p>`;
    return;
  }
  let html = `<div class="tab-section news-list">`;
  for (const a of articles.slice(0, 10)) {
    const sentimentClass = a.sentiment_label === 'bullish' ? 'bullish' : a.sentiment_label === 'bearish' ? 'bearish' : 'neutral';
    html += `<div class="news-item">
      <div class="news-meta">
        <span class="news-source">${a.source_id?.replace('_', ' ') || ''}</span>
        <span class="news-date">${a.published_at?.slice(0, 10) || ''}</span>
        <span class="sentiment-badge ${sentimentClass}">${a.sentiment_label || 'neutral'}</span>
        ${a.mention_in_title ? '<span class="title-badge">in title</span>' : ''}
      </div>
      <a class="news-title" href="${a.url}" target="_blank" rel="noopener">${a.title}</a>
    </div>`;
  }
  html += `</div>`;
  content.innerHTML = html;
}

function renderSocialTab(content, ticker, cached) {
  const social = cached.social || { reddit: [], stocktwits: [] };
  if (social.reddit.length === 0 && social.stocktwits.length === 0) {
    content.innerHTML = `<p class="tab-empty">No social data for ${ticker}. Run the social pipeline to collect data.</p>`;
    return;
  }

  const socialDiv = document.createElement('div');
  socialDiv.className = 'tab-section';
  content.innerHTML = '';
  content.appendChild(socialDiv);

  const redditTrace = {
    type: 'bar', name: 'Reddit mentions',
    x: social.reddit.map(r => r.date),
    y: social.reddit.map(r => r.mention_count),
    marker: { color: '#ff4500' }
  };
  const stwitsTrace = {
    type: 'bar', name: 'StockTwits',
    x: social.stocktwits.map(r => r.date),
    y: social.stocktwits.map(r => r.mention_count),
    marker: { color: '#40a9ff' }
  };

  const socialChartDiv = document.createElement('div');
  socialChartDiv.style.height = '200px';
  socialDiv.appendChild(socialChartDiv);

  Plotly.newPlot(socialChartDiv, [redditTrace, stwitsTrace], {
    paper_bgcolor: '#161b22', plot_bgcolor: '#0d1117',
    font: { color: '#e6edf3' },
    margin: { l: 50, r: 20, t: 20, b: 40 },
    legend: { bgcolor: '#161b22', font: { color: '#e6edf3' } },
    xaxis: { gridcolor: '#21262d', tickfont: { color: '#8b949e' } },
    yaxis: { gridcolor: '#21262d', tickfont: { color: '#8b949e' } },
    barmode: 'group',
  }, { responsive: true, displaylogo: false });
}

function renderNarrativesTab(content) {
  const narratives = state.narratives || [];
  if (narratives.length === 0) {
    content.innerHTML = `<p class="tab-empty">No narrative data yet. Run the daily_narrative.py script to generate signals.</p>`;
    return;
  }
  let html = `<div class="tab-section narratives-grid">`;
  for (const n of narratives) {
    const lastSignal = n.signals?.[n.signals.length - 1];
    const momentum = lastSignal?.momentum_score;
    const momentumClass = (momentum || 0) > 0 ? 'positive' : (momentum || 0) < 0 ? 'negative' : '';
    const momentumStr = momentum != null ? `${momentum > 0 ? '\u2191' : '\u2193'} ${Math.abs(momentum * 100).toFixed(0)}%` : '\u2014';
    const relatedTickers = (typeof n.related_tickers === 'string' ? JSON.parse(n.related_tickers) : n.related_tickers) || [];
    html += `<div class="narrative-card">
      <div class="narrative-header">
        <span class="narrative-name">${n.name}</span>
        <span class="narrative-momentum ${momentumClass}">${momentumStr}</span>
      </div>
      <p class="narrative-desc">${n.description}</p>
      <div class="narrative-tickers">
        ${relatedTickers.map(t => `<span class="narrative-ticker" onclick="addTicker('${t}')">${t}</span>`).join('')}
      </div>
    </div>`;
  }
  html += `</div>`;
  content.innerHTML = html;
}

function renderChatTab(content) {
  content.innerHTML = `
    <div class="tab-section chat-container">
      <div id="chat-messages"></div>
      <div class="chat-input-row">
        <input id="chat-input" type="text" placeholder="Ask about ${state.activeTickers.join(', ')}\u2026" />
        <button id="chat-send">Ask</button>
      </div>
      <p class="chat-hint">Powered by Claude. Requires api.needlstack.com to be deployed.</p>
    </div>`;

  document.getElementById('chat-send')?.addEventListener('click', sendChatMessage);
  document.getElementById('chat-input')?.addEventListener('keydown', (e) => {
    if (e.key === 'Enter') sendChatMessage();
  });
}

async function sendChatMessage() {
  const input = document.getElementById('chat-input');
  const messages = document.getElementById('chat-messages');
  if (!input || !messages) return;
  const text = input.value.trim();
  if (!text) return;
  input.value = '';

  // Append user message
  const userEl = document.createElement('div');
  userEl.className = 'chat-message user';
  userEl.textContent = text;
  messages.appendChild(userEl);

  // Append loading assistant message
  const assistantEl = document.createElement('div');
  assistantEl.className = 'chat-message assistant loading';
  assistantEl.textContent = '\u2026';
  messages.appendChild(assistantEl);
  messages.scrollTop = messages.scrollHeight;

  try {
    const API_BASE = 'https://api.needlstack.com';
    const res = await fetch(`${API_BASE}/chat`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ message: text, tickers: state.activeTickers }),
    });
    if (!res.ok) throw new Error(`API error ${res.status}`);
    const data = await res.json();
    assistantEl.className = 'chat-message assistant';
    assistantEl.textContent = data.response;
  } catch (err) {
    assistantEl.className = 'chat-message assistant error';
    assistantEl.textContent = `Error: ${err.message}. Make sure api.needlstack.com is deployed.`;
  }
  messages.scrollTop = messages.scrollHeight;
}

// ── Utility helpers ────────────────────────────────────────────────────────

function formatShares(n) {
  if (n == null) return '\u2014';
  const abs = Math.abs(n);
  const sign = n < 0 ? '-' : n > 0 ? '+' : '';
  if (abs >= 1e9) return sign + (abs / 1e9).toFixed(1) + 'B';
  if (abs >= 1e6) return sign + (abs / 1e6).toFixed(1) + 'M';
  if (abs >= 1e3) return sign + (abs / 1e3).toFixed(0) + 'K';
  return sign + abs.toFixed(0);
}

function formatLargeNum(n) {
  if (n == null) return '\u2014';
  if (n >= 1e12) return (n / 1e12).toFixed(2) + 'T';
  if (n >= 1e9)  return (n / 1e9).toFixed(1) + 'B';
  if (n >= 1e6)  return (n / 1e6).toFixed(0) + 'M';
  return n.toFixed(0);
}

function showLoading(on) {
  const placeholder = document.getElementById('chart-placeholder');
  const spinner = placeholder.querySelector('.spinner');
  const msg = placeholder.querySelector('p');
  if (on && state.activeTickers.length > 0) {
    placeholder.style.display = 'flex';
    spinner.style.display = 'block';
    msg.textContent = 'Loading data\u2026';
  } else {
    placeholder.style.display = 'none';
    spinner.style.display = 'none';
  }
}

function setStatus(text) {
  const bar = document.getElementById('status-bar');
  if (bar) bar.textContent = text;
}

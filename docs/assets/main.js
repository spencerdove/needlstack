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
  chartType: 'candlestick', // 'candlestick' | 'line'
  chartPeriodType: 'A',    // 'A' | 'Q' for financial metrics on chart
  loading: new Set(),
  activeTab: 'financials',
  activeTabTicker: null,  // for multi-ticker tab switching
  activeSection: 'data', // 'data' | 'portfolio' | 'watchlist' | 'health'
  indexData: {},
  narratives: [],
  // Portfolio
  portfolios: JSON.parse(localStorage.getItem('ns_portfolios') || '{}'),
  activePortfolio: localStorage.getItem('ns_activePortfolio') || '',
  _portfolioCreating: false,
  // Watchlist
  watchlists: JSON.parse(localStorage.getItem('ns_watchlists') || '{}'),
  activeWatchlist: localStorage.getItem('ns_activeWatchlist') || '',
  _watchlistCreating: false,
  _watchlistRenaming: false,
};

const METRIC_GROUPS = [
  { label: 'Price', metrics: [
    { id: 'price',      label: 'Price',     source: 'prices',      field: 'adj_close', yaxis: 'y1', unit: '$' },
    { id: 'dividends',  label: 'Dividends', source: 'corpactions', field: 'amount',    yaxis: 'y2', unit: '$' },
  ]},
  { label: 'Income', metrics: [
    { id: 'revenue',          label: 'Revenue',     source: 'income', field: 'revenue',          yaxis: 'y2', unit: '$B' },
    { id: 'net_income',       label: 'Net Income',  source: 'income', field: 'net_income',       yaxis: 'y2', unit: '$B' },
    { id: 'eps',              label: 'EPS',         source: 'income', field: 'eps_diluted',      yaxis: 'y3', unit: '$' },
    { id: 'gross_profit',     label: 'Gross Profit', source: 'income', field: 'gross_profit',    yaxis: 'y2', unit: '$B' },
    { id: 'operating_income', label: 'Op. Income',  source: 'income', field: 'operating_income', yaxis: 'y2', unit: '$B' },
    { id: 'ebit',             label: 'EBIT',        source: 'income', field: 'ebit',             yaxis: 'y2', unit: '$B' },
  ]},
  { label: 'Balance', metrics: [
    { id: 'cash',                label: 'Cash',        source: 'balance', field: 'cash',                 yaxis: 'y2', unit: '$B' },
    { id: 'debt',                label: 'Debt',        source: 'balance', field: 'long_term_debt',       yaxis: 'y2', unit: '$B' },
    { id: 'equity',              label: 'Equity',      source: 'balance', field: 'stockholders_equity',  yaxis: 'y2', unit: '$B' },
    { id: 'current_liabilities', label: 'Curr. Liab',  source: 'balance', field: 'current_liabilities',  yaxis: 'y2', unit: '$B' },
    { id: 'total_assets',        label: 'Total Assets', source: 'balance', field: 'total_assets',        yaxis: 'y2', unit: '$B' },
    { id: 'current_assets',      label: 'Curr. Assets', source: 'balance', field: 'current_assets',     yaxis: 'y2', unit: '$B' },
  ]},
  { label: 'Cash Flow', metrics: [
    { id: 'operating_cf', label: 'Op CF',  source: 'cashflow', field: 'operating_cf',   yaxis: 'y2', unit: '$B' },
    { id: 'capex',         label: 'CapEx', source: 'cashflow', field: 'capex',           yaxis: 'y2', unit: '$B' },
    { id: 'fcf',           label: 'FCF',   source: 'cashflow', field: 'free_cash_flow',  yaxis: 'y2', unit: '$B' },
  ]},
  { label: 'Ratios', metrics: [
    { id: 'pe_ttm',     label: 'P/E',        source: 'valuation', field: 'pe_ttm',     yaxis: 'y3', unit: 'x' },
    { id: 'ev_ebitda',  label: 'EV/EBITDA',   source: 'valuation', field: 'ev_ebitda',  yaxis: 'y3', unit: 'x' },
    { id: 'ps_ttm',     label: 'P/S',         source: 'valuation', field: 'ps_ttm',     yaxis: 'y3', unit: 'x' },
    { id: 'pb',          label: 'P/B',         source: 'valuation', field: 'pb',         yaxis: 'y3', unit: 'x' },
    { id: 'p_fcf',       label: 'P/FCF',       source: 'valuation', field: 'p_fcf',      yaxis: 'y3', unit: 'x' },
    { id: 'ev_revenue',  label: 'EV/Rev',      source: 'valuation', field: 'ev_revenue', yaxis: 'y3', unit: 'x' },
  ]},
];
const METRICS = METRIC_GROUPS.flatMap(g => g.metrics);

const DATE_RANGES = ['1M', '3M', '6M', '1Y', '3Y', '5Y', 'MAX'];
const GRANULARITIES = [
  { id: 'D', label: 'Daily' },
  { id: 'W', label: 'Weekly' },
  { id: 'M', label: 'Monthly' },
];

const BASE_PATH = (() => {
  const path = window.location.pathname.replace(/\/[^/]*$/, '');
  return path || '';
})();

const DATA_BASE_URL = (window.location.hostname === 'localhost' || window.location.hostname === '127.0.0.1')
  ? `${BASE_PATH}/data`
  : 'https://data.needlstack.com';

const LOCAL_DATA_URL = `${BASE_PATH}/data`;

// Plotly color constants
const CHART_PAPER_BG = '#0f131a';
const CHART_PLOT_BG = '#0a0e14';

// ── Bootstrap ──────────────────────────────────────────────────────────────

document.addEventListener('DOMContentLoaded', async () => {
  buildTopNav();
  buildControls();
  buildTabBar();
  await Promise.all([fetchTickers(), fetchGlobalData()]);
  renderTabContent();
  renderChart();
});

// ── Top Navigation ─────────────────────────────────────────────────────────

function buildTopNav() {
  const nav = document.getElementById('top-nav');
  if (!nav) return;
  const sections = [
    { id: 'data',      label: 'Data & Trends' },
    { id: 'portfolio', label: 'Portfolio Builder' },
    { id: 'watchlist', label: 'Watchlist Builder' },
    { id: 'health',    label: 'Data Health' },
  ];
  for (const s of sections) {
    const btn = document.createElement('button');
    btn.className = 'nav-btn' + (state.activeSection === s.id ? ' active' : '');
    btn.textContent = s.label;
    btn.dataset.section = s.id;
    btn.addEventListener('click', () => setActiveSection(s.id));
    nav.appendChild(btn);
  }
}

function setActiveSection(sectionId) {
  state.activeSection = sectionId;
  document.querySelectorAll('.nav-btn').forEach(b =>
    b.classList.toggle('active', b.dataset.section === sectionId)
  );
  // Show/hide sections
  document.querySelectorAll('.app-section').forEach(el => {
    el.style.display = 'none';
  });
  const target = document.getElementById(`section-${sectionId}`);
  if (target) {
    target.style.display = 'flex';
    renderSection(sectionId);
  }
}

function renderSection(sectionId) {
  switch (sectionId) {
    case 'data':
      renderChart();
      break;
    case 'portfolio':
      renderPortfolioSection();
      break;
    case 'watchlist':
      renderWatchlistSection();
      break;
    case 'health':
      renderHealthSection();
      break;
  }
}

// ── Controls ───────────────────────────────────────────────────────────────

function buildControls() {
  const controls = document.getElementById('controls');

  // Row 1: search + chips
  const row1 = document.createElement('div');
  row1.className = 'controls-row';

  const searchWrapper = document.createElement('div');
  searchWrapper.className = 'search-wrapper';

  const input = document.createElement('input');
  input.id = 'ticker-input';
  input.type = 'text';
  input.placeholder = 'Add ticker\u2026';
  input.autocomplete = 'off';
  input.autocapitalize = 'characters';

  const dropdown = document.createElement('div');
  dropdown.id = 'ticker-dropdown';

  searchWrapper.append(input, dropdown);

  const chipContainer = document.createElement('div');
  chipContainer.id = 'chip-container';

  row1.append(searchWrapper, chipContainer);

  // Metrics rows — one row per category
  const metricsRows = document.createElement('div');
  metricsRows.className = 'metrics-rows';
  metricsRows.id = 'metrics-rows';

  for (const group of METRIC_GROUPS) {
    const row = document.createElement('div');
    row.className = 'metrics-group';
    const label = document.createElement('span');
    label.className = 'metrics-label';
    label.textContent = group.label;
    row.appendChild(label);
    for (const m of group.metrics) {
      const btn = document.createElement('button');
      btn.className = 'metric-btn' + (state.activeMetrics.includes(m.id) ? ' active' : '');
      btn.textContent = m.label;
      btn.dataset.metric = m.id;
      btn.addEventListener('click', () => toggleMetric(m.id, btn));
      row.appendChild(btn);
    }
    metricsRows.appendChild(row);
  }

  // Row 2: chart controls (chart type + A/Q toggle + granularity + date range)
  const row2 = document.createElement('div');
  row2.className = 'controls-row';

  // Chart type toggle
  const chartTypeGroup = document.createElement('div');
  chartTypeGroup.className = 'chart-type-group';
  chartTypeGroup.id = 'chart-type-group';
  for (const ct of [{ id: 'candlestick', label: 'Candle' }, { id: 'line', label: 'Line' }]) {
    const btn = document.createElement('button');
    btn.className = 'chart-type-btn' + (state.chartType === ct.id ? ' active' : '');
    btn.textContent = ct.label;
    btn.dataset.chartType = ct.id;
    btn.addEventListener('click', () => setChartType(ct.id));
    chartTypeGroup.appendChild(btn);
  }

  // Annual/Quarterly toggle for financials on chart
  const periodGroup = document.createElement('div');
  periodGroup.className = 'chart-type-group';
  periodGroup.id = 'chart-period-group';
  for (const p of [{ id: 'A', label: 'Annual' }, { id: 'Q', label: 'Quarterly' }]) {
    const btn = document.createElement('button');
    btn.className = 'chart-type-btn' + (state.chartPeriodType === p.id ? ' active' : '');
    btn.textContent = p.label;
    btn.dataset.period = p.id;
    btn.addEventListener('click', () => setChartPeriodType(p.id));
    periodGroup.appendChild(btn);
  }

  const spacer = document.createElement('div');
  spacer.style.flex = '1';

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

  row2.append(chartTypeGroup, periodGroup, spacer, granGroup, rangeGroup);
  controls.append(row1, metricsRows, row2);

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

function setChartType(type) {
  state.chartType = type;
  document.querySelectorAll('#chart-type-group .chart-type-btn').forEach(b =>
    b.classList.toggle('active', b.dataset.chartType === type)
  );
  renderChart();
}

function setChartPeriodType(period) {
  state.chartPeriodType = period;
  document.querySelectorAll('#chart-period-group .chart-type-btn').forEach(b =>
    b.classList.toggle('active', b.dataset.period === period)
  );
  renderChart();
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
  if (input) input.value = '';
  closeDropdown();

  if (state.activeTickers.includes(ticker)) return;
  state.activeTickers.push(ticker);
  state.activeTabTicker = null;
  renderChips();
  await loadTickerData(ticker);
  renderCompanyHeader();
  renderChart();
}

function removeTicker(ticker) {
  state.activeTickers = state.activeTickers.filter(t => t !== ticker);
  if (state.activeTabTicker === ticker) state.activeTabTicker = null;
  renderChips();
  renderCompanyHeader();
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

    // Data availability dot
    const cached = state.cache[ticker];
    const dot = document.createElement('span');
    dot.className = 'chip-dot';
    if (cached) {
      const hasPrices = cached.prices.length > 0;
      const hasFinancials = Object.keys(cached.financials || {}).length > 0;
      dot.className += hasPrices && hasFinancials ? ' has-data' : ' partial-data';
      dot.title = hasPrices && hasFinancials ? 'Price + financial data available' : 'Partial data available';
    }

    // Index membership badges
    const badges = document.createElement('span');
    badges.className = 'chip-badges';
    const memberOf = Object.entries(state.indexData || {})
      .filter(([, tickers]) => Array.isArray(tickers) && tickers.includes(ticker))
      .map(([idx]) => idx.replace('SP', 'S&P '));
    for (const idx of memberOf.slice(0, 2)) {
      const badge = document.createElement('span');
      badge.className = 'index-badge';
      badge.textContent = idx;
      badges.appendChild(badge);
    }

    const btn = document.createElement('button');
    btn.className = 'chip-remove';
    btn.textContent = '\u00d7';
    btn.title = `Remove ${ticker}`;
    btn.addEventListener('click', () => removeTicker(ticker));

    chip.append(label, dot, badges, btn);
    container.appendChild(chip);
  }
}

// ── Company header ─────────────────────────────────────────────────────────

function renderCompanyHeader() {
  const container = document.getElementById('company-header');
  if (!container) return;

  if (state.activeTickers.length === 0) {
    container.innerHTML = '';
    return;
  }

  const ticker = state.activeTickers[0];
  const cached = state.cache[ticker];
  const info = state.tickers.find(t => t.ticker === ticker);
  if (!info) { container.innerHTML = ''; return; }

  const prices = cached?.prices || [];
  const latest = prices[prices.length - 1];
  const prev = prices[prices.length - 2];

  const price = latest?.adj_close ?? latest?.close;
  const prevPrice = prev?.adj_close ?? prev?.close;
  const change = price != null && prevPrice != null ? price - prevPrice : null;
  const changePct = change != null && prevPrice ? (change / prevPrice) * 100 : null;

  const meta = cached?.metadata || {};
  const marketCap = meta.market_cap || info.market_cap;

  const changeClass = change != null ? (change >= 0 ? 'positive' : 'negative') : '';
  const changeStr = change != null ? `${change >= 0 ? '+' : ''}${change.toFixed(2)} (${changePct.toFixed(2)}%)` : '\u2014';

  container.innerHTML = `<div class="company-card">
    <div class="company-info">
      <div class="company-name">${info.company_name || ticker}</div>
      <div class="company-meta">${info.sector || ''} ${info.industry ? '\u00b7 ' + info.industry : ''}</div>
    </div>
    <div class="company-stats">
      <div class="company-stat">
        <div class="company-stat-label">Price</div>
        <div class="company-stat-value">$${price != null ? price.toFixed(2) : '\u2014'}</div>
      </div>
      <div class="company-stat">
        <div class="company-stat-label">Change</div>
        <div class="company-stat-value ${changeClass}">${changeStr}</div>
      </div>
      ${marketCap ? `<div class="company-stat">
        <div class="company-stat-label">Market Cap</div>
        <div class="company-stat-value">$${formatLargeNum(marketCap)}</div>
      </div>` : ''}
    </div>
  </div>`;
}

// ── Metric + date range ────────────────────────────────────────────────────

function toggleMetric(id, btn) {
  if (state.activeMetrics.includes(id)) {
    if (state.activeMetrics.length === 1) return;
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
    // Fallback to local tickers.json if R2 is unavailable
    if (DATA_BASE_URL !== LOCAL_DATA_URL) {
      try {
        const res2 = await fetch(`${LOCAL_DATA_URL}/tickers.json`);
        if (res2.ok) {
          state.tickers = await res2.json();
          setStatus(`${state.tickers.length} tickers loaded (local)`);
          return;
        }
      } catch { /* ignore fallback error */ }
    }
    setStatus(`Failed to load tickers: ${err.message}`);
    console.error(err);
  }
}

async function fetchGlobalData() {
  try {
    const [indexRes, narrativesRes, coverageRes, validationRes] = await Promise.all([
      fetch(`${DATA_BASE_URL}/indexes.json`),
      fetch(`${DATA_BASE_URL}/narratives.json`),
      safeFetch(`${DATA_BASE_URL}/coverage.json`, null),
      safeFetch(`${DATA_BASE_URL}/validation.json`, null),
    ]);
    state.indexData = indexRes.ok ? await indexRes.json() : {};
    state.narratives = narrativesRes.ok ? await narrativesRes.json() : [];
    state.coverage = coverageRes;
    state.validation = validationRes;
  } catch (err) {
    console.error('Failed to load global data:', err);
  }
}

async function safeFetch(url, fallback) {
  try {
    const res = await fetch(url);
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    return await res.json();
  } catch {
    // Fall back to local path if R2 URL failed
    if (DATA_BASE_URL !== LOCAL_DATA_URL && url.startsWith(DATA_BASE_URL)) {
      try {
        const localUrl = url.replace(DATA_BASE_URL, LOCAL_DATA_URL);
        const res2 = await fetch(localUrl);
        if (res2.ok) return await res2.json();
      } catch { /* ignore */ }
    }
    return fallback;
  }
}

async function loadTickerData(ticker) {
  if (state.cache[ticker]) return;
  if (state.loading.has(ticker)) return;

  state.loading.add(ticker);
  showLoading(true);

  try {
    const [prices, financials, metadata, corporateActions, profile, ownership, sentiment, news, social, metrics] = await Promise.all([
      safeFetch(`${DATA_BASE_URL}/prices/${ticker}.json`, []),
      safeFetch(`${DATA_BASE_URL}/financials/${ticker}.json`, {}),
      safeFetch(`${DATA_BASE_URL}/metadata/${ticker}.json`, {}),
      safeFetch(`${DATA_BASE_URL}/corporate_actions/${ticker}.json`, []),
      safeFetch(`${DATA_BASE_URL}/profiles/${ticker}.json`, {}),
      safeFetch(`${DATA_BASE_URL}/ownership/${ticker}.json`, { summary: null, top_holders: [] }),
      safeFetch(`${DATA_BASE_URL}/sentiment/${ticker}.json`, []),
      safeFetch(`${DATA_BASE_URL}/news/${ticker}.json`, []),
      safeFetch(`${DATA_BASE_URL}/social/${ticker}.json`, { reddit: [], stocktwits: [] }),
      safeFetch(`${DATA_BASE_URL}/metrics/${ticker}.json`, { latest: null, history: [] }),
    ]);

    state.cache[ticker] = { prices, financials, metadata, corporateActions, profile, ownership, sentiment, news, social, metrics };
    setStatus(`Loaded ${ticker} \u2014 ${prices.length} price points`);
  } catch (err) {
    console.error(`Failed to load ${ticker}:`, err);
    state.cache[ticker] = { prices: [], financials: {}, metadata: {}, corporateActions: [], profile: {}, ownership: { summary: null, top_holders: [] }, sentiment: [], news: [], social: { reddit: [], stocktwits: [] }, metrics: { latest: null, history: [] } };
  } finally {
    state.loading.delete(ticker);
    showLoading(false);
  }
}

// ── Date filtering ─────────────────────────────────────────────────────────

function cutoffDate(range) {
  const now = new Date();
  const map = {
    '1M': 1, '3M': 3, '6M': 6, '1Y': 12, '3Y': 36, '5Y': 60, 'MAX': null,
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
      const day = (d.getDay() + 6) % 7;
      const mon = new Date(d);
      mon.setDate(d.getDate() - day);
      key = mon.toISOString().slice(0, 10);
    } else {
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
    setStatus('Plotly failed to load \u2014 check network connection');
    return;
  }

  placeholder.style.display = 'none';

  const traces = [];
  const shapes = [];
  const annotations = [];
  const metricsInfo = METRICS.filter(m => state.activeMetrics.includes(m.id));
  const usedAxes = new Set(metricsInfo.map(m => m.yaxis));

  const multiTicker = state.activeTickers.length > 1;
  // For price metric: determine effective chart type
  const effectiveChartType = (multiTicker && state.chartType === 'candlestick') ? 'line' : state.chartType;

  // Update chart mode note
  const existingNote = document.querySelector('.chart-mode-note');
  if (existingNote) existingNote.remove();
  if (multiTicker && state.chartType === 'candlestick') {
    const noteEl = document.createElement('span');
    noteEl.className = 'chart-mode-note';
    noteEl.textContent = 'Line mode for multi-ticker';
    const chartTypeGroup = document.getElementById('chart-type-group');
    if (chartTypeGroup) chartTypeGroup.parentNode.insertBefore(noteEl, chartTypeGroup.nextSibling);
  }

  for (let ti = 0; ti < state.activeTickers.length; ti++) {
    const ticker = state.activeTickers[ti];
    const cached = state.cache[ticker];
    if (!cached) continue;

    const color = TICKER_COLORS[ti % TICKER_COLORS.length];

    for (const metric of metricsInfo) {
      const yax = axisId(metric.yaxis, usedAxes);
      const name = multiTicker ? `${ticker} ${metric.label}` : metric.label;

      if (metric.source === 'prices') {
        const rangeFiltered = filterByDateRange(cached.prices, 'date', state.dateRange);
        const filtered = aggregateOHLCV(rangeFiltered, state.granularity);

        if (effectiveChartType === 'candlestick' && !multiTicker) {
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
          // Line chart
          traces.push({
            type: 'scatter',
            mode: 'lines',
            name,
            x: filtered.map(r => r.date),
            y: filtered.map(r => r.adj_close),
            line: { color, width: 2 },
            yaxis: yax,
            hovertemplate: `<b>${name}</b><br>%{x}<br>$%{y:.2f}<extra></extra>`,
          });
        }

        // Split/dividend annotations (only for single ticker or first ticker)
        if (ti === 0) {
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
        }

      } else if (metric.source === 'valuation') {
        const rows = cached.financials?.valuation_snapshots || [];
        const filtered = filterByDateRange(rows, 'snapshot_date', state.dateRange);
        traces.push({
          type: 'scatter', mode: 'lines+markers', name,
          x: filtered.map(r => r.snapshot_date),
          y: filtered.map(r => r[metric.field]),
          line: { color, width: 2 }, marker: { color, size: 6 },
          yaxis: yax,
          hovertemplate: `<b>${name}</b><br>%{x}<br>%{y:.2f} ${metric.unit}<extra></extra>`,
        });

      } else if (metric.source === 'corpactions') {
        const rows = (cached.corporateActions || []).filter(r => r.action_type === 'dividend');
        const filtered = filterByDateRange(rows, 'action_date', state.dateRange);
        traces.push({
          type: 'scatter', mode: 'markers', name,
          x: filtered.map(r => r.action_date),
          y: filtered.map(r => r[metric.field]),
          marker: { color, size: 8, symbol: 'diamond' },
          yaxis: yax,
          hovertemplate: `<b>${name}</b><br>%{x}<br>$%{y:.4f}<extra></extra>`,
        });

      } else {
        let rows = [];
        if (metric.source === 'income')   rows = cached.financials?.income_statements || [];
        if (metric.source === 'balance')  rows = cached.financials?.balance_sheets || [];
        if (metric.source === 'cashflow') rows = cached.financials?.cash_flows || [];

        rows = rows.filter(r => r.period_type === state.chartPeriodType);
        const filtered = filterByDateRange(rows, 'period_end', state.dateRange);

        traces.push({
          type: 'scatter', mode: 'lines+markers', name,
          x: filtered.map(r => r.period_end),
          y: filtered.map(r => {
            const v = r[metric.field];
            return v != null ? +(v / 1e9).toFixed(3) : null;
          }),
          line: { color, width: 2 }, marker: { color, size: 6 },
          yaxis: yax,
          hovertemplate: `<b>${name}</b><br>%{x}<br>%{y:.2f} ${metric.unit}<extra></extra>`,
        });
      }
    }
  }

  const layout = buildLayout(metricsInfo, usedAxes, { shapes, annotations });
  const config = {
    responsive: true, displaylogo: false,
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

function axisId(yaxis, usedAxes) {
  const sorted = [...usedAxes].sort();
  const idx = sorted.indexOf(yaxis);
  if (idx === 0) return 'y';
  return `y${idx + 1}`;
}

function buildLayout(metricsInfo, usedAxes, extra = {}) {
  const sorted = [...usedAxes].sort();

  const axisBase = {
    gridcolor: '#161b22', linecolor: '#1c2128',
    tickfont: { color: '#8b949e', size: 11 },
    titlefont: { color: '#8b949e', size: 11 },
    zerolinecolor: '#1c2128',
  };

  const axisConfigs = {};
  sorted.forEach((ykey, i) => {
    const metrics = metricsInfo.filter(m => m.yaxis === ykey);
    const unit = metrics[0]?.unit || '';
    const axisName = i === 0 ? 'yaxis' : `yaxis${i + 1}`;
    const side = i % 2 === 0 ? 'left' : 'right';
    axisConfigs[axisName] = {
      ...axisBase, title: unit, side,
      overlaying: i > 0 ? 'y' : undefined,
      showgrid: i === 0,
      tickformat: ',.2f',
    };
  });

  const today = new Date().toISOString().slice(0, 10);
  const rangeStart = cutoffDate(state.dateRange);
  const xRange = rangeStart ? [rangeStart, today] : undefined;

  return {
    paper_bgcolor: CHART_PAPER_BG, plot_bgcolor: CHART_PLOT_BG,
    font: { family: '-apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif', color: '#e6edf3' },
    legend: { bgcolor: CHART_PAPER_BG, bordercolor: '#1c2128', borderwidth: 1, font: { color: '#e6edf3', size: 11 } },
    margin: { l: 60, r: 60, t: 20, b: 60 },
    hovermode: 'closest',
    hoverlabel: { bgcolor: '#161b22', bordercolor: '#1c2128', font: { color: '#e6edf3', size: 12 } },
    xaxis: { ...axisBase, type: 'date', range: xRange, rangeslider: { visible: false } },
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
    { id: 'financials', label: 'Financials' },
    { id: 'metrics',    label: 'Metrics' },
    { id: 'valuation',  label: 'Valuation' },
    { id: 'ownership',  label: 'Ownership' },
    { id: 'news',       label: 'News' },
    { id: 'social',     label: 'Social' },
    { id: 'narratives', label: 'Narratives' },
    { id: 'filings',    label: 'Filings' },
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

  // Determine effective ticker for tab content
  const effectiveTicker = (state.activeTabTicker && state.activeTickers.includes(state.activeTabTicker))
    ? state.activeTabTicker
    : state.activeTickers[0];

  const cached = state.cache[effectiveTicker];
  if (!cached) { content.innerHTML = '<p class="tab-empty">Loading\u2026</p>'; return; }

  // Build tab ticker bar if multi-ticker
  let tickerBarHtml = '';
  if (state.activeTickers.length > 1) {
    tickerBarHtml = '<div class="tab-ticker-bar">';
    for (const t of state.activeTickers) {
      const activeClass = t === effectiveTicker ? ' active' : '';
      tickerBarHtml += `<button class="tab-ticker-pill${activeClass}" data-tab-ticker="${t}">${t}</button>`;
    }
    tickerBarHtml += '</div>';
  }

  try {
    // For narratives and chat, no per-ticker view needed
    if (state.activeTab === 'narratives') {
      renderNarrativesTab(content);
      return;
    }
    if (state.activeTab === 'chat') {
      renderChatTab(content);
      return;
    }

    // Render with ticker bar
    const wrapper = document.createElement('div');
    if (tickerBarHtml) {
      wrapper.innerHTML = tickerBarHtml;
      // Wire ticker pill clicks
      wrapper.querySelectorAll('.tab-ticker-pill').forEach(pill => {
        pill.addEventListener('click', () => {
          state.activeTabTicker = pill.dataset.tabTicker;
          renderTabContent();
        });
      });
    }
    content.innerHTML = '';
    content.appendChild(wrapper);

    const tabContent = document.createElement('div');
    content.appendChild(tabContent);

    switch (state.activeTab) {
      case 'financials':  renderFinancialsTab(tabContent, effectiveTicker, cached); break;
      case 'valuation':   renderValuationTab(tabContent, effectiveTicker, cached); break;
      case 'ownership':   renderOwnershipTab(tabContent, effectiveTicker, cached); break;
      case 'filings':     renderFilingsTab(tabContent, effectiveTicker, cached); break;
      case 'news':        renderNewsTab(tabContent, effectiveTicker, cached); break;
      case 'social':      renderSocialTab(tabContent, effectiveTicker, cached); break;
      case 'metrics':     renderMetricsTab(tabContent, effectiveTicker, cached); break;
      default:            tabContent.innerHTML = ''; break;
    }
  } catch (err) {
    console.error('Tab render error:', err);
    content.innerHTML = `<p class="tab-empty">Error rendering tab: ${err.message}</p>`;
  }
}

// ── Financials tab ─────────────────────────────────────────────────────────

function renderFinancialsTab(content, ticker, cached) {
  const fin = cached.financials || {};
  const hasIncome = (fin.income_statements || []).length > 0;
  const hasBalance = (fin.balance_sheets || []).length > 0;
  const hasCashflow = (fin.cash_flows || []).length > 0;

  if (!hasIncome && !hasBalance && !hasCashflow) {
    content.innerHTML = `<p class="tab-empty">No financial statement data available for ${ticker}</p>`;
    return;
  }

  // Preserve user's sub-selection
  if (!state._finView) state._finView = 'income';
  if (!state._finPeriod) state._finPeriod = 'A';

  const div = document.createElement('div');
  div.className = 'tab-section';

  // Controls
  const controls = document.createElement('div');
  controls.className = 'financials-controls';

  const views = [
    { id: 'income', label: 'Income Statement', has: hasIncome },
    { id: 'balance', label: 'Balance Sheet', has: hasBalance },
    { id: 'cashflow', label: 'Cash Flow', has: hasCashflow },
  ];
  for (const v of views) {
    const btn = document.createElement('button');
    btn.className = 'fin-toggle' + (state._finView === v.id ? ' active' : '') + (!v.has ? ' disabled' : '');
    btn.textContent = v.label;
    btn.disabled = !v.has;
    btn.addEventListener('click', () => {
      state._finView = v.id;
      renderFinancialsTab(content, ticker, cached);
    });
    controls.appendChild(btn);
  }

  // Period toggle
  const sep = document.createElement('span');
  sep.style.cssText = 'width:1px;height:20px;background:var(--border);margin:0 4px';
  controls.appendChild(sep);

  for (const p of [{ id: 'A', label: 'Annual' }, { id: 'Q', label: 'Quarterly' }]) {
    const btn = document.createElement('button');
    btn.className = 'fin-toggle' + (state._finPeriod === p.id ? ' active' : '');
    btn.textContent = p.label;
    btn.addEventListener('click', () => {
      state._finPeriod = p.id;
      renderFinancialsTab(content, ticker, cached);
    });
    controls.appendChild(btn);
  }

  div.appendChild(controls);

  // Get rows
  let rows = [];
  let fields = [];
  if (state._finView === 'income') {
    rows = fin.income_statements || [];
    fields = [
      { key: 'revenue', label: 'Revenue' },
      { key: 'cost_of_revenue', label: 'Cost of Revenue' },
      { key: 'gross_profit', label: 'Gross Profit' },
      { key: 'operating_income', label: 'Operating Income' },
      { key: 'pretax_income', label: 'Pretax Income' },
      { key: 'income_tax', label: 'Income Tax' },
      { key: 'net_income', label: 'Net Income' },
      { key: 'eps_diluted', label: 'EPS (Diluted)' },
      { key: 'interest_expense', label: 'Interest Expense' },
    ];
  } else if (state._finView === 'balance') {
    rows = fin.balance_sheets || [];
    fields = [
      { key: 'cash', label: 'Cash & Equivalents' },
      { key: 'current_assets', label: 'Current Assets' },
      { key: 'total_assets', label: 'Total Assets' },
      { key: 'current_liabilities', label: 'Current Liabilities' },
      { key: 'long_term_debt', label: 'Long-Term Debt' },
      { key: 'short_term_debt', label: 'Short-Term Debt' },
      { key: 'total_liabilities', label: 'Total Liabilities' },
      { key: 'stockholders_equity', label: 'Stockholders\u2019 Equity' },
      { key: 'goodwill', label: 'Goodwill' },
      { key: 'intangible_assets', label: 'Intangible Assets' },
      { key: 'accounts_receivable', label: 'Accounts Receivable' },
      { key: 'inventory', label: 'Inventory' },
    ];
  } else {
    rows = fin.cash_flows || [];
    fields = [
      { key: 'operating_cf', label: 'Operating Cash Flow' },
      { key: 'capex', label: 'Capital Expenditures' },
      { key: 'free_cash_flow', label: 'Free Cash Flow' },
      { key: 'investing_cf', label: 'Investing Cash Flow' },
      { key: 'financing_cf', label: 'Financing Cash Flow' },
      { key: 'dividends_paid', label: 'Dividends Paid' },
      { key: 'stock_repurchases', label: 'Stock Repurchases' },
      { key: 'depreciation_amortization', label: 'D&A' },
    ];
  }

  rows = rows.filter(r => r.period_type === state._finPeriod);
  rows.sort((a, b) => (b.period_end || '').localeCompare(a.period_end || ''));
  const periods = rows.slice(0, 10);

  if (periods.length === 0) {
    div.innerHTML += '<p class="tab-empty">No data for selected period type</p>';
    content.innerHTML = '';
    content.appendChild(div);
    return;
  }

  const wrap = document.createElement('div');
  wrap.className = 'financials-table-wrap';

  let html = '<table class="financials-table"><thead><tr><th>Item</th>';
  for (const p of periods) {
    const label = state._finPeriod === 'Q'
      ? p.period_end?.slice(0, 7)
      : p.period_end?.slice(0, 4);
    html += `<th>${label || '\u2014'}</th>`;
  }
  html += '</tr></thead><tbody>';

  for (const f of fields) {
    html += `<tr><td>${f.label}</td>`;
    for (const p of periods) {
      const v = p[f.key];
      html += `<td>${formatFinancialValue(v, f.key)}</td>`;
    }
    html += '</tr>';
  }

  html += '</tbody></table>';
  wrap.innerHTML = html;
  div.appendChild(wrap);

  content.innerHTML = '';
  content.appendChild(div);
}

function formatFinancialValue(v, key) {
  if (v == null) return '\u2014';
  if (key === 'eps_diluted') return '$' + v.toFixed(2);
  if (Math.abs(v) >= 1e9) return '$' + (v / 1e9).toFixed(1) + 'B';
  if (Math.abs(v) >= 1e6) return '$' + (v / 1e6).toFixed(0) + 'M';
  return '$' + v.toLocaleString();
}

// ── Valuation tab ──────────────────────────────────────────────────────────

function renderValuationTab(content, ticker, cached) {
  const snapshots = cached.financials?.valuation_snapshots || [];

  if (snapshots.length === 0) {
    content.innerHTML = `<p class="tab-empty">No valuation data available for ${ticker}</p>`;
    return;
  }

  content.innerHTML = '<div class="tab-section"><div class="valuation-charts" id="valuation-charts"></div></div>';

  const container = document.getElementById('valuation-charts');
  const multiples = [
    { field: 'pe_ttm', label: 'P/E (TTM)', color: '#58a6ff' },
    { field: 'ev_ebitda', label: 'EV/EBITDA', color: '#f78166' },
    { field: 'ps_ttm', label: 'P/S (TTM)', color: '#3fb950' },
    { field: 'pb', label: 'P/B', color: '#d2a8ff' },
    { field: 'ev_revenue', label: 'EV/Revenue', color: '#ffa657' },
    { field: 'p_fcf', label: 'P/FCF', color: '#79c0ff' },
  ];

  for (const m of multiples) {
    const xs = snapshots.map(s => s.snapshot_date);
    const ys = snapshots.map(s => s[m.field]);
    if (ys.every(y => y == null)) continue;

    const card = document.createElement('div');
    card.className = 'valuation-chart-card';
    card.innerHTML = `<div class="valuation-chart-title">${m.label}</div>`;
    const chartEl = document.createElement('div');
    chartEl.style.height = '180px';
    card.appendChild(chartEl);
    container.appendChild(card);

    Plotly.newPlot(chartEl, [{
      type: 'scatter', mode: 'lines', x: xs, y: ys,
      line: { color: m.color, width: 2 },
      fill: 'tozeroy', fillcolor: m.color + '15',
      hovertemplate: `${m.label}: %{y:.1f}x<br>%{x}<extra></extra>`,
    }], {
      paper_bgcolor: CHART_PAPER_BG, plot_bgcolor: CHART_PLOT_BG,
      font: { color: '#e6edf3', size: 10 },
      margin: { l: 40, r: 10, t: 5, b: 30 },
      xaxis: { gridcolor: '#161b22', tickfont: { color: '#8b949e', size: 10 } },
      yaxis: { gridcolor: '#161b22', tickfont: { color: '#8b949e', size: 10 }, ticksuffix: 'x' },
    }, { responsive: true, displayModeBar: false });
  }
}

// ── Tab renderers ──────────────────────────────────────────────────────────

function renderOwnershipTab(content, ticker, cached) {
  const { ownership } = cached;
  if (!ownership || !ownership.summary) {
    content.innerHTML = `<p class="tab-empty">No institutional ownership data available for ${ticker}</p>`;
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
    content.innerHTML = `<p class="tab-empty">No SEC filings data available for ${ticker}</p>`;
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
    content.innerHTML = `<p class="tab-empty">No news data available for ${ticker}</p>`;
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
    content.innerHTML = `<p class="tab-empty">No social data available for ${ticker}</p>`;
    return;
  }

  const socialDiv = document.createElement('div');
  socialDiv.className = 'tab-section';
  content.innerHTML = '';
  content.appendChild(socialDiv);

  const socialChartDiv = document.createElement('div');
  socialChartDiv.style.height = '200px';
  socialDiv.appendChild(socialChartDiv);

  Plotly.newPlot(socialChartDiv, [
    {
      type: 'bar', name: 'Reddit mentions',
      x: social.reddit.map(r => r.date), y: social.reddit.map(r => r.mention_count),
      marker: { color: '#ff4500' }
    },
    {
      type: 'bar', name: 'StockTwits',
      x: social.stocktwits.map(r => r.date), y: social.stocktwits.map(r => r.mention_count),
      marker: { color: '#40a9ff' }
    },
  ], {
    paper_bgcolor: CHART_PAPER_BG, plot_bgcolor: CHART_PLOT_BG,
    font: { color: '#e6edf3' },
    margin: { l: 50, r: 20, t: 20, b: 40 },
    legend: { bgcolor: CHART_PAPER_BG, font: { color: '#e6edf3' } },
    xaxis: { gridcolor: '#161b22', tickfont: { color: '#8b949e' } },
    yaxis: { gridcolor: '#161b22', tickfont: { color: '#8b949e' } },
    barmode: 'group',
  }, { responsive: true, displaylogo: false });
}

function renderNarrativesTab(content) {
  const narratives = state.narratives || [];
  if (narratives.length === 0) {
    content.innerHTML = `<p class="tab-empty">No narrative data available yet</p>`;
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

  const userEl = document.createElement('div');
  userEl.className = 'chat-message user';
  userEl.textContent = text;
  messages.appendChild(userEl);

  const assistantEl = document.createElement('div');
  assistantEl.className = 'chat-message assistant loading';
  assistantEl.textContent = '\u2026';
  messages.appendChild(assistantEl);
  messages.scrollTop = messages.scrollHeight;

  try {
    const res = await fetch('https://api.needlstack.com/chat', {
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

// ── Metrics tab ────────────────────────────────────────────────────────────

function fmtPct(v)  { return v != null ? (v * 100).toFixed(1) + '%' : '\u2014'; }
function fmtMult(v) { return v != null ? v.toFixed(1) + 'x' : '\u2014'; }
function fmtBn(v)   { return v != null ? '$' + (v / 1e9).toFixed(1) + 'B' : '\u2014'; }
function fmtNum(v)  { return v != null ? v.toFixed(2) : '\u2014'; }
function fmtDays(v) { return v != null ? Math.round(v) + 'd' : '\u2014'; }

function renderMetricsTab(content, ticker, cached) {
  const m = cached.metrics?.latest;
  const history = cached.metrics?.history || [];
  const fin = cached.financials || {};

  const latestVal = fin.valuation_snapshots?.[fin.valuation_snapshots.length - 1] || {};

  if (!m && !latestVal.pe_ttm) {
    content.innerHTML = `<p class="tab-empty">No metrics data available for ${ticker}</p>`;
    return;
  }

  const prev = history.length > 1 ? history[1] : null;
  function trend(key, higherIsBetter = true) {
    if (!prev || m == null) return '';
    const cur = m[key], p = prev[key];
    if (cur == null || p == null) return '';
    const up = cur > p;
    const cls = (up === higherIsBetter) ? 'trend-up' : 'trend-down';
    return `<span class="${cls}">${up ? '\u2191' : '\u2193'}</span>`;
  }

  function card(label, value, trendHtml = '') {
    return `<div class="metric-card"><span class="metric-label">${label}</span><span class="metric-value">${value}${trendHtml ? ' ' + trendHtml : ''}</span></div>`;
  }

  const ttmRevenue = (() => {
    const stmts = (fin.income_statements || []).filter(r => r.period_type === 'Q').slice(-4);
    if (stmts.length < 4) return null;
    return stmts.reduce((s, r) => s + (r.revenue || 0), 0);
  })();

  const sections = [
    {
      label: 'Profitability',
      cards: [
        card('Revenue (TTM)', fmtBn(ttmRevenue)),
        card('Gross Margin', fmtPct(m?.gross_margin), trend('gross_margin')),
        card('Operating Margin', fmtPct(m?.operating_margin), trend('operating_margin')),
        card('Net Margin', fmtPct(m?.net_margin), trend('net_margin')),
        card('EBITDA Margin', fmtPct(m?.ebitda_margin), trend('ebitda_margin')),
        card('Pretax Margin', fmtPct(m?.pretax_margin), trend('pretax_margin')),
      ],
    },
    {
      label: 'Growth',
      cards: [
        card('Revenue YoY', fmtPct(m?.revenue_yoy_growth), trend('revenue_yoy_growth')),
        card('Revenue QoQ', fmtPct(m?.revenue_qoq_growth), trend('revenue_qoq_growth')),
        card('Revenue 3Y CAGR', fmtPct(m?.revenue_3yr_cagr)),
        card('Revenue 5Y CAGR', fmtPct(m?.revenue_5yr_cagr)),
        card('EPS YoY', fmtPct(m?.eps_yoy_growth), trend('eps_yoy_growth')),
        card('EPS 3Y CAGR', fmtPct(m?.eps_3yr_cagr)),
        card('Op. Income YoY', fmtPct(m?.operating_income_yoy_growth), trend('operating_income_yoy_growth')),
        card('EBITDA YoY', fmtPct(m?.ebitda_yoy_growth), trend('ebitda_yoy_growth')),
        card('FCF YoY', fmtPct(m?.fcf_yoy_growth), trend('fcf_yoy_growth')),
      ],
    },
    {
      label: 'Cash Flow',
      cards: [
        card('OCF (TTM)', fmtBn(m?.ocf_ttm), trend('ocf_ttm')),
        card('FCF (TTM)', fmtBn(m?.fcf_ttm), trend('fcf_ttm')),
        card('EBITDA', fmtBn(m?.ebitda), trend('ebitda')),
        card('OCF Margin', fmtPct(m?.ocf_margin), trend('ocf_margin')),
        card('FCF Margin', fmtPct(m?.fcf_margin), trend('fcf_margin')),
        card('OCF/Share', fmtNum(m?.ocf_per_share), trend('ocf_per_share')),
        card('FCF/Share', fmtNum(m?.fcf_per_share), trend('fcf_per_share')),
        card('Cash Conv. Ratio', fmtNum(m?.cash_conversion_ratio), trend('cash_conversion_ratio')),
        card('CapEx/Revenue', fmtPct(m?.capex_to_revenue), trend('capex_to_revenue', false)),
      ],
    },
    {
      label: 'Returns',
      cards: [
        card('ROE', fmtPct(m?.roe), trend('roe')),
        card('ROA', fmtPct(m?.roa), trend('roa')),
        card('ROIC', fmtPct(m?.roic), trend('roic')),
        card('ROCE', fmtPct(m?.roce), trend('roce')),
      ],
    },
    {
      label: 'Liquidity',
      cards: [
        card('Current Ratio', fmtNum(m?.current_ratio), trend('current_ratio')),
        card('Quick Ratio', fmtNum(m?.quick_ratio), trend('quick_ratio')),
        card('Cash Ratio', fmtNum(m?.cash_ratio), trend('cash_ratio')),
        card('Working Capital', fmtBn(m?.working_capital), trend('working_capital')),
        card('Net Debt', fmtBn(m?.net_debt), trend('net_debt', false)),
      ],
    },
    {
      label: 'Leverage',
      cards: [
        card('Debt/Equity', fmtNum(m?.debt_to_equity), trend('debt_to_equity', false)),
        card('Debt/Assets', fmtNum(m?.debt_to_assets), trend('debt_to_assets', false)),
        card('Debt/Capital', fmtPct(m?.debt_to_capital), trend('debt_to_capital', false)),
        card('Equity Ratio', fmtPct(m?.equity_ratio), trend('equity_ratio')),
        card('Interest Coverage', fmtNum(m?.interest_coverage), trend('interest_coverage')),
        card('Net Debt/EBITDA', fmtNum(m?.net_debt_to_ebitda), trend('net_debt_to_ebitda', false)),
      ],
    },
    {
      label: 'Efficiency',
      cards: [
        card('Asset Turnover', fmtNum(m?.asset_turnover), trend('asset_turnover')),
        card('Inventory Turnover', fmtNum(m?.inventory_turnover), trend('inventory_turnover')),
        card('Receivables Turnover', fmtNum(m?.receivables_turnover), trend('receivables_turnover')),
        card('DSO', fmtDays(m?.dso), trend('dso', false)),
        card('DIO', fmtDays(m?.dio), trend('dio', false)),
        card('DPO', fmtDays(m?.dpo), trend('dpo')),
        card('CCC', fmtDays(m?.ccc), trend('ccc', false)),
      ],
    },
    {
      label: 'Per Share',
      cards: [
        card('Book Value/Share', fmtNum(m?.book_value_per_share), trend('book_value_per_share')),
        card('Tangible Book/Share', fmtNum(m?.tangible_book_value_per_share), trend('tangible_book_value_per_share')),
        card('OCF/Share', fmtNum(m?.ocf_per_share), trend('ocf_per_share')),
        card('FCF/Share', fmtNum(m?.fcf_per_share), trend('fcf_per_share')),
      ],
    },
    {
      label: 'Valuation',
      cards: [
        card('P/E TTM', fmtMult(m?.pe_ttm || latestVal.pe_ttm)),
        card('P/B', fmtMult(latestVal.pb)),
        card('P/S', fmtMult(latestVal.ps_ttm)),
        card('P/FCF', fmtMult(latestVal.p_fcf)),
        card('EV/EBITDA', fmtMult(m?.ev_ebitda || latestVal.ev_ebitda)),
        card('EV/EBIT', fmtMult(latestVal.ev_ebit)),
        card('EV/Revenue', fmtMult(latestVal.ev_revenue)),
      ],
    },
    {
      label: 'Shareholder Returns',
      cards: [
        card('Dividend Yield', fmtPct(m?.dividend_yield), trend('dividend_yield')),
        card('Payout Ratio', fmtPct(m?.dividend_payout_ratio), trend('dividend_payout_ratio', false)),
        card('Buyback Yield', fmtPct(m?.buyback_yield), trend('buyback_yield')),
        card('Shareholder Yield', fmtPct(m?.shareholder_yield), trend('shareholder_yield')),
      ],
    },
  ];

  let html = `<div class="tab-section metrics-dashboard">`;
  for (const section of sections) {
    html += `<div class="metric-category">${section.label}</div>`;
    html += `<div class="metrics-grid">${section.cards.join('')}</div>`;
  }
  html += `</div>`;
  content.innerHTML = html;
}

// ── Inline Ticker Search (reusable for Portfolio + Watchlist) ───────────────

function buildInlineTickerSearch(container, onSelect) {
  const wrapper = document.createElement('div');
  wrapper.className = 'inline-search-wrapper';

  const input = document.createElement('input');
  input.className = 'inline-search-input';
  input.type = 'text';
  input.placeholder = 'Ticker\u2026';
  input.autocomplete = 'off';
  input.autocapitalize = 'characters';

  const dropdown = document.createElement('div');
  dropdown.className = 'inline-search-dropdown';

  wrapper.append(input, dropdown);
  container.appendChild(wrapper);

  let debounceTimer;
  input.addEventListener('input', () => {
    clearTimeout(debounceTimer);
    debounceTimer = setTimeout(() => {
      const query = input.value.trim().toUpperCase();
      dropdown.innerHTML = '';
      if (!query) { dropdown.classList.remove('open'); return; }

      const matches = state.tickers
        .filter(t => t.ticker.startsWith(query) || t.company_name?.toLowerCase().includes(query.toLowerCase()))
        .slice(0, 8);

      if (matches.length === 0) {
        const empty = document.createElement('div');
        empty.className = 'dropdown-empty';
        empty.textContent = 'No results';
        dropdown.appendChild(empty);
      } else {
        for (const m of matches) {
          const item = document.createElement('div');
          item.className = 'dropdown-item';
          const tickerEl = document.createElement('span');
          tickerEl.className = 'dropdown-ticker';
          tickerEl.textContent = m.ticker;
          const nameEl = document.createElement('span');
          nameEl.className = 'dropdown-name';
          nameEl.textContent = m.company_name || '';
          item.append(tickerEl, nameEl);
          item.addEventListener('mousedown', (e) => {
            e.preventDefault();
            input.value = '';
            dropdown.classList.remove('open');
            dropdown.innerHTML = '';
            onSelect(m.ticker);
          });
          dropdown.appendChild(item);
        }
      }
      dropdown.classList.add('open');
    }, 120);
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
      if (target) {
        const tickerEl = target.querySelector('.dropdown-ticker');
        if (tickerEl) {
          input.value = '';
          dropdown.classList.remove('open');
          dropdown.innerHTML = '';
          onSelect(tickerEl.textContent);
        }
      }
    } else if (e.key === 'Escape') {
      dropdown.classList.remove('open');
      dropdown.innerHTML = '';
    }
  });

  // Close on outside click
  document.addEventListener('click', (e) => {
    if (!wrapper.contains(e.target)) {
      dropdown.classList.remove('open');
      dropdown.innerHTML = '';
    }
  });

  return { wrapper, input };
}

// ── Portfolio Builder ──────────────────────────────────────────────────────

function savePortfolios() {
  localStorage.setItem('ns_portfolios', JSON.stringify(state.portfolios));
  localStorage.setItem('ns_activePortfolio', state.activePortfolio);
}

// Migrate old portfolio format (weight → shares/purchasePrice/purchaseDate)
function migratePortfolioHoldings(portfolio) {
  for (const h of portfolio.holdings) {
    if (h.weight !== undefined && h.shares === undefined) {
      h.shares = h.weight;
      h.purchasePrice = 0;
      h.purchaseDate = '';
      delete h.weight;
    }
  }
}

function holdingCurrentValue(h) {
  const prices = state.cache[h.ticker]?.prices || [];
  const latest = prices[prices.length - 1];
  return (h.shares || 0) * (latest?.adj_close ?? latest?.close ?? 0);
}

function portfolioTotalValue(holdings) {
  return holdings.reduce((sum, h) => sum + holdingCurrentValue(h), 0);
}

function renderPortfolioSection() {
  const container = document.getElementById('section-portfolio');
  if (!container) return;

  const portfolioNames = Object.keys(state.portfolios);
  if (!state.activePortfolio && portfolioNames.length > 0) {
    state.activePortfolio = portfolioNames[0];
  }
  const portfolio = state.portfolios[state.activePortfolio] || { holdings: [] };
  migratePortfolioHoldings(portfolio);

  const section = document.createElement('div');
  section.className = 'portfolio-section';

  // Header
  const header = document.createElement('div');
  header.className = 'portfolio-header';

  const h2 = document.createElement('h2');
  h2.textContent = 'Portfolio Builder';
  header.appendChild(h2);

  if (state._portfolioCreating) {
    // Inline create form
    const form = document.createElement('div');
    form.className = 'inline-create-form';
    const nameInput = document.createElement('input');
    nameInput.type = 'text';
    nameInput.placeholder = 'Portfolio name\u2026';
    nameInput.autofocus = true;

    const createBtn = document.createElement('button');
    createBtn.className = 'btn-sm primary';
    createBtn.textContent = 'Create';

    const cancelBtn = document.createElement('button');
    cancelBtn.className = 'btn-sm';
    cancelBtn.textContent = 'Cancel';

    const doCreate = () => {
      const name = nameInput.value.trim();
      if (!name) return;
      state.portfolios[name] = { holdings: [] };
      state.activePortfolio = name;
      state._portfolioCreating = false;
      savePortfolios();
      renderPortfolioSection();
    };

    createBtn.addEventListener('click', doCreate);
    nameInput.addEventListener('keydown', (e) => {
      if (e.key === 'Enter') doCreate();
      if (e.key === 'Escape') { state._portfolioCreating = false; renderPortfolioSection(); }
    });
    cancelBtn.addEventListener('click', () => { state._portfolioCreating = false; renderPortfolioSection(); });

    form.append(nameInput, createBtn, cancelBtn);
    header.appendChild(form);
  } else {
    // Select + action buttons
    const select = document.createElement('select');
    select.className = 'portfolio-select';
    select.id = 'portfolio-select';
    if (portfolioNames.length === 0) {
      const opt = document.createElement('option');
      opt.value = '';
      opt.textContent = 'No portfolios';
      select.appendChild(opt);
    }
    for (const n of portfolioNames) {
      const opt = document.createElement('option');
      opt.value = n;
      opt.textContent = n;
      if (n === state.activePortfolio) opt.selected = true;
      select.appendChild(opt);
    }
    select.addEventListener('change', (e) => {
      state.activePortfolio = e.target.value;
      savePortfolios();
      renderPortfolioSection();
    });

    const actions = document.createElement('div');
    actions.className = 'portfolio-actions';

    const newBtn = document.createElement('button');
    newBtn.className = 'btn-sm primary';
    newBtn.textContent = 'New';
    newBtn.addEventListener('click', () => { state._portfolioCreating = true; renderPortfolioSection(); });

    const deleteBtn = document.createElement('button');
    deleteBtn.className = 'btn-sm danger';
    deleteBtn.textContent = 'Delete';
    deleteBtn.addEventListener('click', () => {
      if (!state.activePortfolio) return;
      if (!confirm(`Delete portfolio "${state.activePortfolio}"?`)) return;
      delete state.portfolios[state.activePortfolio];
      state.activePortfolio = Object.keys(state.portfolios)[0] || '';
      savePortfolios();
      renderPortfolioSection();
    });

    actions.append(newBtn, deleteBtn);
    header.append(select, actions);
  }

  section.appendChild(header);

  // Add holding form (4 fields with labels)
  if (state.activePortfolio && !state._portfolioCreating) {
    const addRow = document.createElement('div');
    addRow.className = 'portfolio-add-row';

    // Ticker field — select sets pending ticker, does NOT auto-add
    const tickerGroup = document.createElement('div');
    tickerGroup.className = 'field-group';
    const tickerLabel = document.createElement('label');
    tickerLabel.textContent = 'Ticker';
    tickerGroup.appendChild(tickerLabel);

    const tickerRow = document.createElement('div');
    tickerRow.style.cssText = 'display:flex;align-items:center;gap:6px';
    buildInlineTickerSearch(tickerRow, async (ticker) => {
      state._pendingPortfolioTicker = ticker;
      await loadTickerData(ticker);
      // Show selected chip
      const chipEl = document.getElementById('portfolio-pending-chip');
      if (chipEl) chipEl.innerHTML = `<span style="font-family:var(--mono);color:var(--accent);font-weight:600">${ticker}</span><button class="chip-remove" id="portfolio-pending-clear">&times;</button>`;
      const clearBtn = document.getElementById('portfolio-pending-clear');
      if (clearBtn) clearBtn.addEventListener('click', () => { state._pendingPortfolioTicker = null; chipEl.innerHTML = ''; document.getElementById('portfolio-date-info').innerHTML = ''; });
      // Clear date info
      const dateInfoEl = document.getElementById('portfolio-date-info');
      if (dateInfoEl) dateInfoEl.innerHTML = '';
    });
    const pendingChip = document.createElement('span');
    pendingChip.id = 'portfolio-pending-chip';
    pendingChip.style.cssText = 'display:inline-flex;align-items:center;gap:4px;background:var(--chip-bg);border:1px solid var(--chip-border);border-radius:16px;padding:2px 10px;min-height:28px';
    if (state._pendingPortfolioTicker) {
      pendingChip.innerHTML = `<span style="font-family:var(--mono);color:var(--accent);font-weight:600">${state._pendingPortfolioTicker}</span><button class="chip-remove" id="portfolio-pending-clear">&times;</button>`;
    }
    tickerRow.appendChild(pendingChip);
    tickerGroup.appendChild(tickerRow);
    addRow.appendChild(tickerGroup);

    // Purchase Date field
    const dateGroup = document.createElement('div');
    dateGroup.className = 'field-group';
    const dateLabel = document.createElement('label');
    dateLabel.textContent = 'Purchase Date';
    const dateInput = document.createElement('input');
    dateInput.type = 'date';
    dateInput.className = 'date-input';
    dateInput.id = 'portfolio-date';
    dateGroup.append(dateLabel, dateInput);
    const dateInfo = document.createElement('div');
    dateInfo.id = 'portfolio-date-info';
    dateInfo.className = 'portfolio-date-info';
    dateGroup.appendChild(dateInfo);
    addRow.appendChild(dateGroup);

    // Wire date change to show price info
    dateInput.addEventListener('change', () => {
      const ticker = state._pendingPortfolioTicker;
      const dateVal = dateInput.value;
      if (!ticker || !dateVal || !state.cache[ticker]) { dateInfo.innerHTML = ''; return; }
      const prices = state.cache[ticker].prices || [];
      const match = prices.find(p => p.date === dateVal);
      if (match) {
        dateInfo.innerHTML = `Open: <strong>$${match.open?.toFixed(2)}</strong> &middot; Close: <strong>$${(match.adj_close || match.close)?.toFixed(2)}</strong>`;
      } else {
        dateInfo.innerHTML = '<span style="color:var(--yellow)">No price data for this date</span>';
      }
    });

    // Shares field
    const sharesGroup = document.createElement('div');
    sharesGroup.className = 'field-group';
    const sharesLabel = document.createElement('label');
    sharesLabel.textContent = 'Shares';
    const sharesInput = document.createElement('input');
    sharesInput.type = 'number';
    sharesInput.className = 'shares-input';
    sharesInput.id = 'portfolio-shares';
    sharesInput.placeholder = '0';
    sharesInput.min = '0';
    sharesInput.step = 'any';
    sharesGroup.append(sharesLabel, sharesInput);
    addRow.appendChild(sharesGroup);

    // Price per Share field
    const priceGroup = document.createElement('div');
    priceGroup.className = 'field-group';
    const priceLabel = document.createElement('label');
    priceLabel.textContent = 'Price per Share';
    const priceInput = document.createElement('input');
    priceInput.type = 'number';
    priceInput.className = 'price-input';
    priceInput.id = 'portfolio-price';
    priceInput.placeholder = '$0.00';
    priceInput.min = '0';
    priceInput.step = '0.01';
    priceGroup.append(priceLabel, priceInput);
    addRow.appendChild(priceGroup);

    // Add to Portfolio button
    const addBtn = document.createElement('button');
    addBtn.className = 'btn-sm primary';
    addBtn.textContent = 'Add to Portfolio';
    addBtn.style.alignSelf = 'flex-end';
    addBtn.style.height = '32px';
    addBtn.addEventListener('click', async () => {
      const ticker = state._pendingPortfolioTicker;
      if (!ticker) { setStatus('Select a ticker first'); return; }
      const p = state.portfolios[state.activePortfolio];
      if (!p) return;
      if (p.holdings.find(h => h.ticker === ticker)) { setStatus(`${ticker} already in portfolio`); return; }
      const shares = parseFloat(sharesInput.value) || 0;
      if (shares <= 0) { setStatus('Enter number of shares'); return; }
      const purchaseDate = dateInput.value || '';
      const purchasePrice = parseFloat(priceInput.value) || 0;
      p.holdings.push({ ticker, purchaseDate, shares, purchasePrice });
      state._pendingPortfolioTicker = null;
      savePortfolios();
      await loadTickerData(ticker);
      renderPortfolioSection();
    });
    addRow.appendChild(addBtn);

    section.appendChild(addRow);

    // Holdings table
    if (portfolio.holdings.length > 0) {
      const grid = document.createElement('div');
      grid.className = 'portfolio-grid';

      const tableWrap = document.createElement('div');
      tableWrap.className = 'portfolio-table-wrap';
      const totalValue = portfolioTotalValue(portfolio.holdings);

      let tableHtml = `<table class="data-table" id="portfolio-table">
        <thead><tr><th>Ticker</th><th>Date</th><th class="num">Shares</th><th class="num">Cost Basis</th><th class="num">Current Value</th><th class="num">Gain/Loss</th><th class="num">Weight</th><th></th></tr></thead><tbody>`;

      for (const h of portfolio.holdings) {
        const info = state.tickers.find(t => t.ticker === h.ticker);
        const curVal = holdingCurrentValue(h);
        const costBasis = (h.shares || 0) * (h.purchasePrice || 0);
        const gainLoss = costBasis > 0 ? curVal - costBasis : null;
        const gainPct = costBasis > 0 ? ((curVal - costBasis) / costBasis * 100) : null;
        const weight = totalValue > 0 ? (curVal / totalValue * 100).toFixed(1) : '0.0';

        const gainClass = gainLoss != null ? (gainLoss >= 0 ? 'positive' : 'negative') : '';
        const gainStr = gainLoss != null
          ? `${gainLoss >= 0 ? '+' : ''}$${Math.abs(gainLoss).toLocaleString(undefined, {minimumFractionDigits: 2, maximumFractionDigits: 2})} (${gainPct >= 0 ? '+' : ''}${gainPct.toFixed(1)}%)`
          : '\u2014';

        tableHtml += `<tr>
          <td><span style="font-family:var(--mono);color:var(--accent)">${h.ticker}</span>${info?.sector ? `<br><span style="font-size:10px;color:var(--text-muted)">${info.sector}</span>` : ''}</td>
          <td>${h.purchaseDate || '\u2014'}</td>
          <td class="num">${h.shares || 0}</td>
          <td class="num">${costBasis > 0 ? '$' + costBasis.toLocaleString(undefined, {minimumFractionDigits: 2, maximumFractionDigits: 2}) : '\u2014'}</td>
          <td class="num">${curVal > 0 ? '$' + curVal.toLocaleString(undefined, {minimumFractionDigits: 2, maximumFractionDigits: 2}) : '\u2014'}</td>
          <td class="num ${gainClass}">${gainStr}</td>
          <td class="num">${weight}%</td>
          <td><button class="chip-remove" data-remove-ticker="${h.ticker}">\u00d7</button></td>
        </tr>`;
      }

      // Total row
      const totalCost = portfolio.holdings.reduce((s, h) => s + (h.shares || 0) * (h.purchasePrice || 0), 0);
      const totalGain = totalCost > 0 ? totalValue - totalCost : null;
      const totalGainPct = totalCost > 0 ? ((totalValue - totalCost) / totalCost * 100) : null;
      const totalGainClass = totalGain != null ? (totalGain >= 0 ? 'positive' : 'negative') : '';
      tableHtml += `<tr style="font-weight:600;border-top:1px solid var(--border-strong)">
        <td colspan="3">Total</td>
        <td class="num">${totalCost > 0 ? '$' + totalCost.toLocaleString(undefined, {minimumFractionDigits: 2, maximumFractionDigits: 2}) : '\u2014'}</td>
        <td class="num">${totalValue > 0 ? '$' + totalValue.toLocaleString(undefined, {minimumFractionDigits: 2, maximumFractionDigits: 2}) : '\u2014'}</td>
        <td class="num ${totalGainClass}">${totalGain != null ? `${totalGain >= 0 ? '+' : ''}$${Math.abs(totalGain).toLocaleString(undefined, {minimumFractionDigits: 2, maximumFractionDigits: 2})} (${totalGainPct >= 0 ? '+' : ''}${totalGainPct.toFixed(1)}%)` : '\u2014'}</td>
        <td class="num">100%</td>
        <td></td>
      </tr>`;

      tableHtml += `</tbody></table>`;
      tableWrap.innerHTML = tableHtml;
      grid.appendChild(tableWrap);

      // Sector allocation chart
      const pieCard = document.createElement('div');
      pieCard.className = 'portfolio-chart-card';
      pieCard.innerHTML = '<div class="portfolio-chart-title">Sector Allocation</div><div id="portfolio-pie" style="height:250px"></div>';
      grid.appendChild(pieCard);
      section.appendChild(grid);

      // Performance chart
      const perfCard = document.createElement('div');
      perfCard.className = 'portfolio-chart-card';
      perfCard.innerHTML = '<div class="portfolio-chart-title">Portfolio Performance (rebased to 100)</div><div id="portfolio-perf" style="height:250px"></div>';
      section.appendChild(perfCard);

      // Key stats
      const statsRow = document.createElement('div');
      statsRow.className = 'key-stats-row';
      const latestVals = portfolio.holdings.map(h => {
        const c = state.cache[h.ticker];
        const fin = c?.financials || {};
        const vs = fin.valuation_snapshots || [];
        const curVal = holdingCurrentValue(h);
        return { curVal, val: vs[vs.length - 1] || {} };
      });
      const wAvgPE = totalValue > 0
        ? latestVals.reduce((s, v) => s + (v.val.pe_ttm || 0) * v.curVal / totalValue, 0)
        : 0;
      statsRow.innerHTML = `
        <div class="stat-card"><span class="stat-label">Total Value</span><span class="stat-value">${totalValue > 0 ? '$' + totalValue.toLocaleString(undefined, {minimumFractionDigits: 2, maximumFractionDigits: 2}) : '\u2014'}</span></div>
        <div class="stat-card"><span class="stat-label">Total Gain/Loss</span><span class="stat-value ${totalGainClass}">${totalGain != null ? `${totalGain >= 0 ? '+' : ''}$${Math.abs(totalGain).toLocaleString(undefined, {minimumFractionDigits: 2, maximumFractionDigits: 2})}` : '\u2014'}</span></div>
        <div class="stat-card"><span class="stat-label">Wtd. Avg P/E</span><span class="stat-value">${wAvgPE > 0 ? wAvgPE.toFixed(1) + 'x' : '\u2014'}</span></div>`;
      section.appendChild(statsRow);
    }
  }

  container.innerHTML = '';
  container.appendChild(section);

  // Wire remove buttons
  container.querySelectorAll('[data-remove-ticker]').forEach(btn => {
    btn.addEventListener('click', () => removeFromPortfolio(btn.dataset.removeTicker));
  });

  // Focus the name input if creating
  if (state._portfolioCreating) {
    const nameInput = container.querySelector('.inline-create-form input');
    if (nameInput) setTimeout(() => nameInput.focus(), 0);
  }

  // Render charts after DOM is ready
  if (portfolio.holdings.length > 0) {
    setTimeout(() => renderPortfolioCharts(portfolio), 0);
  }
}

function removeFromPortfolio(ticker) {
  const p = state.portfolios[state.activePortfolio];
  if (!p) return;
  p.holdings = p.holdings.filter(h => h.ticker !== ticker);
  savePortfolios();
  renderPortfolioSection();
}

function renderPortfolioCharts(portfolio) {
  // Sector pie — use market-value weights
  const pieEl = document.getElementById('portfolio-pie');
  if (pieEl && window.Plotly) {
    const sectorWeights = {};
    const totalValue = portfolioTotalValue(portfolio.holdings) || 1;
    for (const h of portfolio.holdings) {
      const info = state.tickers.find(t => t.ticker === h.ticker);
      const sector = info?.sector || 'Unknown';
      sectorWeights[sector] = (sectorWeights[sector] || 0) + (holdingCurrentValue(h) / totalValue * 100);
    }
    Plotly.newPlot(pieEl, [{
      type: 'pie',
      labels: Object.keys(sectorWeights),
      values: Object.values(sectorWeights),
      hole: 0.4,
      textinfo: 'label+percent',
      textfont: { size: 11, color: '#e6edf3' },
      marker: { colors: TICKER_COLORS },
    }], {
      paper_bgcolor: CHART_PAPER_BG, plot_bgcolor: CHART_PLOT_BG,
      font: { color: '#e6edf3', size: 11 },
      margin: { l: 10, r: 10, t: 10, b: 10 },
      showlegend: false,
    }, { responsive: true, displayModeBar: false });
  }

  // Performance chart — use market-value weights
  const perfEl = document.getElementById('portfolio-perf');
  if (perfEl && window.Plotly) {
    const totalValue = portfolioTotalValue(portfolio.holdings) || 1;

    // Find common date range
    const allDates = new Set();
    for (const h of portfolio.holdings) {
      const prices = state.cache[h.ticker]?.prices || [];
      prices.forEach(p => allDates.add(p.date));
    }
    const dates = [...allDates].sort();
    if (dates.length === 0) return;

    // Build composite using market-value weights
    const composite = dates.map(date => {
      let val = 0;
      for (const h of portfolio.holdings) {
        const curVal = holdingCurrentValue(h);
        const w = curVal / totalValue;
        const prices = state.cache[h.ticker]?.prices || [];
        const first = prices[0];
        const row = prices.find(p => p.date === date);
        if (row && first) {
          const rebase = (row.adj_close || row.close) / (first.adj_close || first.close) * 100;
          val += rebase * w;
        }
      }
      return val;
    });

    Plotly.newPlot(perfEl, [{
      type: 'scatter', mode: 'lines',
      x: dates, y: composite,
      line: { color: '#58a6ff', width: 2 },
      fill: 'tozeroy', fillcolor: 'rgba(88,166,255,0.08)',
    }], {
      paper_bgcolor: CHART_PAPER_BG, plot_bgcolor: CHART_PLOT_BG,
      font: { color: '#e6edf3', size: 10 },
      margin: { l: 40, r: 10, t: 5, b: 30 },
      xaxis: { gridcolor: '#161b22', tickfont: { color: '#8b949e' } },
      yaxis: { gridcolor: '#161b22', tickfont: { color: '#8b949e' } },
    }, { responsive: true, displayModeBar: false });
  }
}

// ── Watchlist Builder ──────────────────────────────────────────────────────

function saveWatchlists() {
  localStorage.setItem('ns_watchlists', JSON.stringify(state.watchlists));
  localStorage.setItem('ns_activeWatchlist', state.activeWatchlist);
}

function renderWatchlistSection() {
  const container = document.getElementById('section-watchlist');
  if (!container) return;

  const watchlistNames = Object.keys(state.watchlists);
  if (!state.activeWatchlist && watchlistNames.length > 0) {
    state.activeWatchlist = watchlistNames[0];
  }
  // Migrate old formats to per-ticker date model: { tickers: [{ ticker, addedDate }] }
  for (const [name, val] of Object.entries(state.watchlists)) {
    if (Array.isArray(val)) {
      // Very old format: plain array of ticker strings
      state.watchlists[name] = { tickers: val.map(t => ({ ticker: t, addedDate: '' })) };
    } else if (val && Array.isArray(val.tickers) && val.tickers.length > 0 && typeof val.tickers[0] === 'string') {
      // Old format: { tickers: ['AAPL'], startDate: '...' }
      const defaultDate = val.startDate || '';
      state.watchlists[name] = { tickers: val.tickers.map(t => ({ ticker: t, addedDate: defaultDate })) };
    }
  }
  const watchlistObj = state.watchlists[state.activeWatchlist] || { tickers: [] };
  const watchlist = watchlistObj.tickers || [];

  if (!state._watchSort) state._watchSort = 'name';

  const section = document.createElement('div');
  section.className = 'watchlist-section';

  // Header
  const header = document.createElement('div');
  header.className = 'watchlist-header';

  const h2 = document.createElement('h2');
  h2.textContent = 'Watchlist Builder';
  header.appendChild(h2);

  if (state._watchlistCreating) {
    // Inline create form
    const form = document.createElement('div');
    form.className = 'inline-create-form';
    const nameInput = document.createElement('input');
    nameInput.type = 'text';
    nameInput.placeholder = 'Watchlist name\u2026';

    const createBtn = document.createElement('button');
    createBtn.className = 'btn-sm primary';
    createBtn.textContent = 'Create';

    const cancelBtn = document.createElement('button');
    cancelBtn.className = 'btn-sm';
    cancelBtn.textContent = 'Cancel';

    const doCreate = () => {
      const name = nameInput.value.trim();
      if (!name) return;
      state.watchlists[name] = { tickers: [], startDate: '' };
      state.activeWatchlist = name;
      state._watchlistCreating = false;
      saveWatchlists();
      renderWatchlistSection();
    };

    createBtn.addEventListener('click', doCreate);
    nameInput.addEventListener('keydown', (e) => {
      if (e.key === 'Enter') doCreate();
      if (e.key === 'Escape') { state._watchlistCreating = false; renderWatchlistSection(); }
    });
    cancelBtn.addEventListener('click', () => { state._watchlistCreating = false; renderWatchlistSection(); });

    form.append(nameInput, createBtn, cancelBtn);
    header.appendChild(form);
  } else if (state._watchlistRenaming) {
    // Inline rename form
    const form = document.createElement('div');
    form.className = 'inline-rename-form';
    const nameInput = document.createElement('input');
    nameInput.type = 'text';
    nameInput.value = state.activeWatchlist;

    const confirmBtn = document.createElement('button');
    confirmBtn.className = 'btn-sm primary';
    confirmBtn.textContent = 'Save';

    const cancelBtn = document.createElement('button');
    cancelBtn.className = 'btn-sm';
    cancelBtn.textContent = 'Cancel';

    const doRename = () => {
      const name = nameInput.value.trim();
      if (!name || name === state.activeWatchlist) { state._watchlistRenaming = false; renderWatchlistSection(); return; }
      state.watchlists[name] = state.watchlists[state.activeWatchlist];
      delete state.watchlists[state.activeWatchlist];
      state.activeWatchlist = name;
      state._watchlistRenaming = false;
      saveWatchlists();
      renderWatchlistSection();
    };

    confirmBtn.addEventListener('click', doRename);
    nameInput.addEventListener('keydown', (e) => {
      if (e.key === 'Enter') doRename();
      if (e.key === 'Escape') { state._watchlistRenaming = false; renderWatchlistSection(); }
    });
    cancelBtn.addEventListener('click', () => { state._watchlistRenaming = false; renderWatchlistSection(); });

    form.append(nameInput, confirmBtn, cancelBtn);
    header.appendChild(form);
  } else {
    // Select + action buttons
    const select = document.createElement('select');
    select.className = 'portfolio-select';
    if (watchlistNames.length === 0) {
      const opt = document.createElement('option');
      opt.value = '';
      opt.textContent = 'No watchlists';
      select.appendChild(opt);
    }
    for (const n of watchlistNames) {
      const opt = document.createElement('option');
      opt.value = n;
      opt.textContent = n;
      if (n === state.activeWatchlist) opt.selected = true;
      select.appendChild(opt);
    }
    select.addEventListener('change', (e) => {
      state.activeWatchlist = e.target.value;
      saveWatchlists();
      renderWatchlistSection();
    });

    const actions = document.createElement('div');
    actions.className = 'portfolio-actions';

    const newBtn = document.createElement('button');
    newBtn.className = 'btn-sm primary';
    newBtn.textContent = 'New';
    newBtn.addEventListener('click', () => { state._watchlistCreating = true; renderWatchlistSection(); });

    const renameBtn = document.createElement('button');
    renameBtn.className = 'btn-sm';
    renameBtn.textContent = 'Rename';
    renameBtn.addEventListener('click', () => {
      if (!state.activeWatchlist) return;
      state._watchlistRenaming = true;
      renderWatchlistSection();
    });

    const deleteBtn = document.createElement('button');
    deleteBtn.className = 'btn-sm danger';
    deleteBtn.textContent = 'Delete';
    deleteBtn.addEventListener('click', () => {
      if (!state.activeWatchlist) return;
      if (!confirm(`Delete watchlist "${state.activeWatchlist}"?`)) return;
      delete state.watchlists[state.activeWatchlist];
      state.activeWatchlist = Object.keys(state.watchlists)[0] || '';
      saveWatchlists();
      renderWatchlistSection();
    });

    actions.append(newBtn, renameBtn, deleteBtn);
    header.append(select, actions);

    // Sort buttons
    const sortDiv = document.createElement('div');
    sortDiv.className = 'watchlist-sort';
    const sortLabel = document.createElement('span');
    sortLabel.className = 'watchlist-sort-label';
    sortLabel.textContent = 'Sort:';
    sortDiv.appendChild(sortLabel);
    for (const s of ['name', 'change', 'sector']) {
      const btn = document.createElement('button');
      btn.className = 'btn-sm' + (state._watchSort === s ? ' primary' : '');
      btn.textContent = s.charAt(0).toUpperCase() + s.slice(1);
      btn.addEventListener('click', () => { state._watchSort = s; renderWatchlistSection(); });
      sortDiv.appendChild(btn);
    }
    header.appendChild(sortDiv);
  }

  section.appendChild(header);

  // Subtitle
  if (!state._watchlistCreating && !state._watchlistRenaming) {
    const subtitle = document.createElement('p');
    subtitle.className = 'watchlist-subtitle';
    subtitle.textContent = 'Track tickers and monitor price changes';
    section.appendChild(subtitle);
  }

  // Add ticker row (with inline search)
  if (state.activeWatchlist && !state._watchlistCreating && !state._watchlistRenaming) {
    const addSection = document.createElement('div');
    addSection.className = 'watchlist-add-section';
    const addLabel = document.createElement('span');
    addLabel.className = 'watchlist-add-label';
    addLabel.textContent = 'Add to Watchlist';
    addSection.appendChild(addLabel);

    const addRow = document.createElement('div');
    addRow.className = 'portfolio-add-row';

    buildInlineTickerSearch(addRow, async (ticker) => {
      const found = state.tickers.find(t => t.ticker === ticker);
      if (!found) { setStatus(`Ticker ${ticker} not found`); return; }
      const wlObj = state.watchlists[state.activeWatchlist];
      if (!wlObj) return;
      if (wlObj.tickers.some(t => t.ticker === ticker)) { setStatus(`${ticker} already in watchlist`); return; }
      wlObj.tickers.push({ ticker, addedDate: new Date().toISOString().slice(0, 10) });
      saveWatchlists();
      await loadTickerData(ticker);
      renderWatchlistSection();
    });

    addSection.appendChild(addRow);
    section.appendChild(addSection);

    // Empty state
    if (watchlist.length === 0) {
      const emptyDiv = document.createElement('div');
      emptyDiv.className = 'watchlist-empty';
      emptyDiv.innerHTML = '<div class="watchlist-empty-icon">&#x1F4C8;</div><div>Add tickers above to start tracking price changes</div>';
      section.appendChild(emptyDiv);
    }

    // Load data for watchlist tickers that aren't cached
    const tickersToLoad = watchlist.filter(entry => !state.cache[entry.ticker]);
    if (tickersToLoad.length > 0) {
      Promise.all(tickersToLoad.map(entry => loadTickerData(entry.ticker))).then(() => {
        renderWatchlistSection();
      });
    }

    // Sort tickers
    let sorted = [...watchlist].map(entry => {
      const ticker = entry.ticker;
      const addedDate = entry.addedDate || '';
      const info = state.tickers.find(t => t.ticker === ticker) || {};
      const cached = state.cache[ticker];
      const prices = cached?.prices || [];
      const latest = prices[prices.length - 1];
      const price = latest?.adj_close ?? latest?.close;
      // % change from addedDate (or day-over-day if no date)
      let change = 0;
      if (addedDate && prices.length > 0) {
        const startRow = prices.find(p => p.date >= addedDate) || prices[0];
        const startPrice = startRow?.adj_close ?? startRow?.close;
        if (startPrice && price) change = (price - startPrice) / startPrice * 100;
      } else {
        const prev = prices[prices.length - 2];
        const prevPrice = prev?.adj_close ?? prev?.close;
        if (price != null && prevPrice != null) change = (price - prevPrice) / prevPrice * 100;
      }
      return { ticker, addedDate, info, prices, price, change };
    });

    if (state._watchSort === 'change') sorted.sort((a, b) => b.change - a.change);
    else if (state._watchSort === 'sector') sorted.sort((a, b) => (a.info.sector || '').localeCompare(b.info.sector || ''));
    else sorted.sort((a, b) => a.ticker.localeCompare(b.ticker));

    const cardsDiv = document.createElement('div');
    cardsDiv.className = 'watchlist-cards';

    for (const item of sorted) {
      const changeClass = item.change >= 0 ? 'positive' : 'negative';
      const changeStr = item.price != null ? `${item.change >= 0 ? '+' : ''}${item.change.toFixed(2)}%` : '\u2014';

      const card = document.createElement('div');
      card.className = 'watchlist-card';
      card.dataset.ticker = item.ticker;
      card.innerHTML = `
        <button class="watchlist-card-remove" data-remove-wl="${item.ticker}">\u00d7</button>
        <div class="watchlist-card-top">
          <span class="watchlist-card-ticker">${item.ticker}</span>
          <span class="watchlist-card-price">${item.price != null ? '$' + item.price.toFixed(2) : '\u2014'}</span>
        </div>
        <div class="watchlist-card-name">${item.info.company_name || ''}</div>
        <div class="watchlist-card-bottom">
          <span class="watchlist-card-change ${changeClass}">${changeStr}</span>
          <div class="watchlist-sparkline"><canvas id="spark-${item.ticker}"></canvas></div>
          ${item.addedDate ? `<span class="sector-badge">${item.addedDate}</span>` : ''}
          ${item.info.sector ? `<span class="sector-badge">${item.info.sector}</span>` : ''}
        </div>`;
      cardsDiv.appendChild(card);
    }

    section.appendChild(cardsDiv);

    // Watchlist charts (if we have tickers)
    if (watchlist.length > 0) {
      // Blended return chart
      const blendedCard = document.createElement('div');
      blendedCard.className = 'portfolio-chart-card';
      blendedCard.innerHTML = '<div class="portfolio-chart-title">Blended Watchlist Return</div><div id="watchlist-blended" style="height:220px"></div>';
      section.appendChild(blendedCard);

      // Individual ticker chart
      const individualCard = document.createElement('div');
      individualCard.className = 'portfolio-chart-card';
      individualCard.innerHTML = '<div class="portfolio-chart-title">Individual Ticker Returns</div><div id="watchlist-individual" style="height:250px"></div>';
      section.appendChild(individualCard);
    }
  }

  container.innerHTML = '';
  container.appendChild(section);

  // Wire remove buttons
  container.querySelectorAll('[data-remove-wl]').forEach(btn => {
    btn.addEventListener('click', (e) => {
      e.stopPropagation();
      removeFromWatchlist(btn.dataset.removeWl);
    });
  });

  // Click to navigate
  container.querySelectorAll('.watchlist-card').forEach(card => {
    card.addEventListener('click', () => {
      const ticker = card.dataset.ticker;
      if (!state.activeTickers.includes(ticker)) {
        state.activeTickers.push(ticker);
        renderChips();
        loadTickerData(ticker).then(() => {
          renderCompanyHeader();
          renderChart();
        });
      }
      setActiveSection('data');
    });
  });

  // Focus create/rename input
  if (state._watchlistCreating || state._watchlistRenaming) {
    const input = container.querySelector('.inline-create-form input, .inline-rename-form input');
    if (input) setTimeout(() => { input.focus(); input.select(); }, 0);
  }

  // Draw sparklines + watchlist charts
  setTimeout(() => {
    for (const entry of (watchlist || [])) {
      const ticker = entry.ticker;
      const canvas = document.getElementById(`spark-${ticker}`);
      if (!canvas) continue;
      const prices = state.cache[ticker]?.prices || [];
      drawSparkline(canvas, prices.slice(-30).map(p => p.adj_close || p.close));
    }
    renderWatchlistCharts(watchlist);
  }, 0);
}

function removeFromWatchlist(ticker) {
  const wlObj = state.watchlists[state.activeWatchlist];
  if (!wlObj) return;
  wlObj.tickers = (wlObj.tickers || []).filter(t => t.ticker !== ticker);
  saveWatchlists();
  renderWatchlistSection();
}

function drawSparkline(canvas, data) {
  if (!data.length) return;
  const ctx = canvas.getContext('2d');
  const w = canvas.parentElement.clientWidth;
  const h = canvas.parentElement.clientHeight;
  canvas.width = w * 2;
  canvas.height = h * 2;
  ctx.scale(2, 2);

  const min = Math.min(...data);
  const max = Math.max(...data);
  const range = max - min || 1;
  const pad = 2;

  const isUp = data[data.length - 1] >= data[0];
  ctx.strokeStyle = isUp ? '#3fb950' : '#f85149';
  ctx.lineWidth = 1.5;
  ctx.beginPath();
  data.forEach((v, i) => {
    const x = pad + (i / (data.length - 1)) * (w - pad * 2);
    const y = pad + (1 - (v - min) / range) * (h - pad * 2);
    i === 0 ? ctx.moveTo(x, y) : ctx.lineTo(x, y);
  });
  ctx.stroke();
}

function renderWatchlistCharts(entries) {
  if (!entries || entries.length === 0 || !window.Plotly) return;

  // Gather price data and compute returns using per-ticker addedDate
  const tickerReturns = [];
  for (let ti = 0; ti < entries.length; ti++) {
    const entry = entries[ti];
    const ticker = entry.ticker;
    const addedDate = entry.addedDate || '';
    let prices = state.cache[ticker]?.prices || [];
    if (addedDate) prices = prices.filter(p => p.date >= addedDate);
    if (prices.length < 2) continue;
    const basePrice = prices[0].adj_close || prices[0].close;
    const returns = prices.map(p => ({
      date: p.date,
      ret: ((p.adj_close || p.close) - basePrice) / basePrice * 100,
    }));
    tickerReturns.push({ ticker, addedDate, returns, color: TICKER_COLORS[ti % TICKER_COLORS.length] });
  }

  if (tickerReturns.length === 0) return;

  // Blended chart — equal-weight average, using latest addedDate as common start
  const blendedEl = document.getElementById('watchlist-blended');
  if (blendedEl) {
    // For the blended chart, use the latest addedDate as common start
    const commonStart = tickerReturns
      .map(tr => tr.addedDate)
      .filter(Boolean)
      .sort()
      .pop() || '';

    // Re-compute returns from common start for blended view
    const blendedReturns = tickerReturns.map(tr => {
      const ticker = tr.ticker;
      let prices = state.cache[ticker]?.prices || [];
      if (commonStart) prices = prices.filter(p => p.date >= commonStart);
      if (prices.length < 2) return null;
      const base = prices[0].adj_close || prices[0].close;
      return { returns: prices.map(p => ({ date: p.date, ret: ((p.adj_close || p.close) - base) / base * 100 })) };
    }).filter(Boolean);

    const allDates = new Set();
    blendedReturns.forEach(tr => tr.returns.forEach(r => allDates.add(r.date)));
    const dates = [...allDates].sort();
    const blendedY = dates.map(date => {
      let sum = 0, count = 0;
      for (const tr of blendedReturns) {
        const row = tr.returns.find(r => r.date === date);
        if (row) { sum += row.ret; count++; }
      }
      return count > 0 ? sum / count : null;
    });

    Plotly.newPlot(blendedEl, [{
      type: 'scatter', mode: 'lines',
      x: dates, y: blendedY,
      line: { color: '#58a6ff', width: 2 },
      fill: 'tozeroy', fillcolor: 'rgba(88,166,255,0.08)',
      hovertemplate: 'Blended: %{y:.1f}%<br>%{x}<extra></extra>',
    }], {
      paper_bgcolor: CHART_PAPER_BG, plot_bgcolor: CHART_PLOT_BG,
      font: { color: '#e6edf3', size: 10 },
      margin: { l: 50, r: 10, t: 5, b: 30 },
      xaxis: { gridcolor: '#161b22', tickfont: { color: '#8b949e' } },
      yaxis: { gridcolor: '#161b22', tickfont: { color: '#8b949e' }, ticksuffix: '%' },
    }, { responsive: true, displayModeBar: false });
  }

  // Individual ticker chart
  const indivEl = document.getElementById('watchlist-individual');
  if (indivEl) {
    const traces = tickerReturns.map(tr => ({
      type: 'scatter', mode: 'lines',
      name: tr.ticker,
      x: tr.returns.map(r => r.date),
      y: tr.returns.map(r => r.ret),
      line: { color: tr.color, width: 2 },
      hovertemplate: `${tr.ticker}: %{y:.1f}%<br>%{x}<extra></extra>`,
    }));

    Plotly.newPlot(indivEl, traces, {
      paper_bgcolor: CHART_PAPER_BG, plot_bgcolor: CHART_PLOT_BG,
      font: { color: '#e6edf3', size: 10 },
      margin: { l: 50, r: 10, t: 5, b: 30 },
      legend: { bgcolor: CHART_PAPER_BG, font: { color: '#e6edf3', size: 11 } },
      xaxis: { gridcolor: '#161b22', tickfont: { color: '#8b949e' } },
      yaxis: { gridcolor: '#161b22', tickfont: { color: '#8b949e' }, ticksuffix: '%' },
    }, { responsive: true, displayModeBar: false });
  }
}

// ── Data Health Dashboard ──────────────────────────────────────────────────

async function renderHealthSection() {
  const container = document.getElementById('section-health');
  if (!container) return;

  const coverage = state.coverage;

  // If no coverage.json, fall back to probe-based approach
  if (!coverage || !coverage.categories) {
    container.innerHTML = `<div class="health-section">
      <h2>Data Health Dashboard</h2>
      <p style="color:var(--text-muted);font-size:13px">No coverage data available. Run <code>python3 scripts/export_coverage.py</code> to generate.</p>
    </div>`;
    return;
  }

  const totalTickers = coverage.total_tickers || 0;
  const equityTickers = coverage.equity_tickers || totalTickers;
  const etfTickers = coverage.etf_tickers || 0;
  const cats = coverage.categories || [];
  const avgCoverage = cats.length > 0
    ? (cats.reduce((s, c) => s + (c.pct_coverage || 0), 0) / cats.length).toFixed(1)
    : 0;

  let html = `<div class="health-section">
    <h2>Data Health Dashboard</h2>
    <p style="color:var(--text-muted);font-size:13px;margin-bottom:4px">
      ${avgCoverage}% average coverage across ${cats.length} data categories.
      ${totalTickers.toLocaleString()} tickers in universe (${equityTickers.toLocaleString()} equity, ${etfTickers.toLocaleString()} ETF).
      ${coverage.generated_at ? 'Updated ' + coverage.generated_at.slice(0, 16).replace('T', ' ') + ' UTC' : ''}
    </p>
    <div class="health-grid">`;

  for (const cat of cats) {
    const pct = cat.pct_coverage || 0;
    const barColor = pct > 80 ? 'var(--green)' : pct > 40 ? 'var(--yellow)' : 'var(--danger)';
    const badgeClass = cat.latest_date ? getFreshness(cat.latest_date) : (pct > 0 ? 'fresh' : 'missing');
    const badgeText = badgeClass === 'fresh' ? 'Fresh' : badgeClass === 'stale' ? 'Stale' : 'Missing';
    const count = cat.tickers_with_data || 0;
    const denom = cat.denominator || totalTickers;
    const denomLabel = cat.denominator_label || 'all';
    const denomSuffix = denomLabel === 'equity' ? ' equities' : denomLabel === 'equity+etf' ? ' equity+ETF' : ' tickers';
    const missing = cat.sample_missing || [];

    let dateRange = '';
    if (cat.earliest_date || cat.latest_date) {
      dateRange = '<br>';
      if (cat.earliest_date) dateRange += `Earliest: <strong>${cat.earliest_date}</strong>`;
      if (cat.earliest_date && cat.latest_date) dateRange += ' | ';
      if (cat.latest_date) dateRange += `Latest: <strong>${cat.latest_date}</strong>`;
    }

    html += `<div class="health-card">
      <div class="health-card-header">
        <span class="health-card-title">${cat.name}</span>
        <span class="health-badge ${badgeClass}">${badgeText}</span>
      </div>
      <div class="health-card-detail">
        <strong>${count.toLocaleString()}</strong> / ${denom.toLocaleString()}${denomSuffix}
        ${dateRange}
      </div>
      <div class="health-progress">
        <div class="health-progress-fill" style="width:${Math.min(pct, 100)}%;background:${barColor}"></div>
      </div>
      <div style="font-size:11px;color:var(--text-muted)">${pct.toFixed(1)}% coverage</div>
      ${missing.length > 0 ? `<details style="margin-top:4px"><summary style="font-size:11px;color:var(--text-muted);cursor:pointer">View gaps (${missing.length})</summary><div style="font-size:11px;color:var(--text-muted);margin-top:4px;max-height:120px;overflow-y:auto;font-family:var(--mono)">${missing.slice(0, 50).join(', ')}${missing.length > 50 ? '...' : ''}</div></details>` : ''}
    </div>`;
  }

  html += `</div>`;

  // Data Validation section
  const validation = state.validation;
  if (validation && validation.runs && validation.runs.length > 0) {
    html += `<div style="margin-top:12px"><div class="metric-category">Data Validation</div>`;

    const runs = validation.runs;
    const latestRun = validation.latest_run;

    // Run selector dropdown
    html += `<div style="margin-bottom:12px;display:flex;align-items:center;gap:8px">
      <label style="font-size:12px;color:var(--text-muted)">Run:</label>
      <select id="validation-run-select" class="portfolio-select" style="height:30px;font-size:12px">
        ${runs.map(r => `<option value="${r.run_id}"${r.run_id === (latestRun?.run_id) ? ' selected' : ''}>${(r.triggered_at || '').slice(0, 16).replace('T', ' ')} (${r.n_tickers || 0} tickers)</option>`).join('')}
      </select>
    </div>`;

    if (latestRun) {
      const summary = latestRun.summary || {};
      const passRate = summary.pass_rate || 0;
      const passRatePct = (passRate * 100).toFixed(1);
      const passBarColor = passRate > 0.9 ? 'var(--green)' : passRate > 0.7 ? 'var(--yellow)' : 'var(--danger)';

      html += `<div class="health-grid">
        <div class="health-card">
          <div class="health-card-header">
            <span class="health-card-title">Overall Pass Rate</span>
            <span class="health-badge ${passRate > 0.9 ? 'fresh' : passRate > 0.7 ? 'stale' : 'missing'}">${passRatePct}%</span>
          </div>
          <div class="health-card-detail">
            <strong>${(summary.passed || 0).toLocaleString()}</strong> / ${(summary.total_tests || 0).toLocaleString()} tests passed
          </div>
          <div class="health-progress">
            <div class="health-progress-fill" style="width:${passRatePct}%;background:${passBarColor}"></div>
          </div>
        </div>`;

      // Avg score card
      const avgScore = runs[0]?.avg_score;
      if (avgScore != null) {
        const scorePct = (avgScore * 100).toFixed(1);
        html += `<div class="health-card">
          <div class="health-card-header">
            <span class="health-card-title">Avg Score</span>
            <span class="health-badge fresh">${scorePct}%</span>
          </div>
          <div class="health-card-detail">
            Average validation score across ${runs[0]?.n_tickers || 0} tickers
          </div>
        </div>`;
      }

      html += `</div>`;

      // Worst metrics table
      const worstMetrics = latestRun.worst_metrics || [];
      if (worstMetrics.length > 0) {
        html += `<div style="margin-top:12px">
          <div style="font-size:12px;font-weight:600;color:var(--text-muted);margin-bottom:6px">Top Failing Metrics</div>
          <table class="data-table">
            <thead><tr><th>Metric</th><th class="num">Failures</th><th class="num">Avg % Diff</th></tr></thead>
            <tbody>`;
        for (const m of worstMetrics.slice(0, 5)) {
          html += `<tr>
            <td>${m.metric_name}</td>
            <td class="num">${m.fail_count}</td>
            <td class="num">${m.avg_pct_diff != null ? m.avg_pct_diff + '%' : '\u2014'}</td>
          </tr>`;
        }
        html += `</tbody></table></div>`;
      }

      // Per-ticker breakdown (expandable)
      const byTicker = latestRun.by_ticker || [];
      if (byTicker.length > 0) {
        html += `<details style="margin-top:12px">
          <summary style="font-size:12px;color:var(--text-muted);cursor:pointer;font-weight:600">Per-Ticker Scores (${byTicker.length} tickers)</summary>
          <div style="max-height:300px;overflow-y:auto;margin-top:8px">
          <table class="data-table">
            <thead><tr><th>Ticker</th><th class="num">Score</th><th class="num">Passed</th><th class="num">Total</th></tr></thead>
            <tbody>`;
        for (const t of byTicker) {
          const score = t.overall_score != null ? (t.overall_score * 100).toFixed(1) + '%' : '\u2014';
          const scoreClass = t.overall_score > 0.9 ? 'positive' : t.overall_score < 0.7 ? 'negative' : '';
          html += `<tr>
            <td>${t.ticker}</td>
            <td class="num ${scoreClass}">${score}</td>
            <td class="num">${t.n_passed || 0}</td>
            <td class="num">${t.n_total || 0}</td>
          </tr>`;
        }
        html += `</tbody></table></div></details>`;
      }
    }

    html += `</div>`;
  }

  // Global data checks
  html += `<div style="margin-top:12px"><div class="metric-category">Global Data Files</div><div class="health-grid">`;
  const globalChecks = [
    { name: 'Tickers Index', ok: state.tickers.length > 0, detail: `${state.tickers.length} tickers` },
    { name: 'Index Data', ok: Object.keys(state.indexData).length > 0, detail: `${Object.keys(state.indexData).length} indexes` },
    { name: 'Narratives', ok: state.narratives.length > 0, detail: `${state.narratives.length} narratives` },
    { name: 'Coverage', ok: true, detail: `Generated ${coverage.generated_at?.slice(0, 10) || 'unknown'}` },
    { name: 'Validation', ok: !!state.validation, detail: state.validation ? `${(state.validation.runs || []).length} runs` : 'Not available' },
  ];
  for (const g of globalChecks) {
    html += `<div class="health-card">
      <div class="health-card-header">
        <span class="health-card-title">${g.name}</span>
        <span class="health-badge ${g.ok ? 'fresh' : 'missing'}">${g.ok ? 'OK' : 'Missing'}</span>
      </div>
      <div class="health-card-detail">${g.detail}</div>
    </div>`;
  }
  html += `</div></div>`;

  html += `</div>`;
  container.innerHTML = html;
}

function getFreshness(dateStr) {
  if (!dateStr) return 'missing';
  const d = new Date(dateStr);
  const hoursAgo = (Date.now() - d.getTime()) / (1000 * 60 * 60);
  if (hoursAgo < 48) return 'fresh';
  if (hoursAgo < 7 * 24) return 'stale';
  return 'fresh'; // for non-daily data like financials, older is still OK
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

/* All chart text (titles, legends, ticks, tooltips) uses the body font.
   TF2 Build smears at these small sizes, so charts use TF2 Secondary. */
if (window.Chart) {
  Chart.defaults.font.family = "'TF2 Secondary', 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif";
}

/* Chart titles sit above the plot and need to stay readable, so they run
   larger than the default and drop a size on narrow screens. */
function chartTitleFont() {
  return {
    family: "'TF2 Secondary', 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif",
    size: window.innerWidth < 700 ? 16 : 20,
    weight: 'bold'
  };
}

/* Helper utilities */
function createThrottledFunction(fn, delay) {
  let last = 0;
  let timeoutId;
  return (...args) => {
    const now = Date.now();
    clearTimeout(timeoutId);
    if (now - last >= delay) {
      last = now;
      fn(...args);
    } else {
      timeoutId = setTimeout(() => {
        last = now;
        fn(...args);
      }, delay);
    }
  };
}

// Cache of server options for the ALL filter (persist across filtered views)
let cachedAllServerOptions = [];

/* Populate Server Filter dropdown */
function populateServerDropdown(data) {
  try {
    const sel = document.getElementById('ServerFilter');
    if (!sel) return;
    const prev = sel.value || 'ALL';
    const makeOpt = (val, label) => {
      const opt = document.createElement('option');
      opt.value = val;
      opt.textContent = label;
      return opt;
    };
    const top = Array.isArray(data.serverRanking) ? data.serverRanking : [];

    // Build a normalized list from current data (exclude 'Other')
    const normalizedCurrent = top
      .filter(s => s && s.id && s.id !== 'Other')
      .map(s => ({ value: String(s.id), text: `${s.label} (${s.pop})` }));

    // If we're on ALL, update the cache from current data
    if (prev === 'ALL') {
      cachedAllServerOptions = normalizedCurrent.slice();
    }

    // Use cached list unless empty (fallback to current normalized)
    let sourceList = cachedAllServerOptions.length ? cachedAllServerOptions.slice() : normalizedCurrent.slice();

    // Ensure currently selected server stays in the list (if not ALL)
    if (prev !== 'ALL' && prev) {
      const exists = sourceList.some(o => o.value === prev);
      if (!exists) {
        const found = normalizedCurrent.find(o => o.value === prev);
        sourceList.push(found || { value: prev, text: prev });
      }
    }

    // Rebuild options
    sel.textContent = '';
    sel.appendChild(makeOpt('ALL', 'ALL'));
    sourceList.forEach(o => sel.appendChild(makeOpt(o.value, o.text)));

    // Restore selection if present, else ALL
    const values = Array.from(sel.options).map(o => o.value);
    sel.value = values.includes(prev) ? prev : 'ALL';
  } catch (e) {
    // Non-fatal
    console.debug('Failed to populate server dropdown:', e);
  }
}
function setAllButtonsDisabled(disabled) {
  document.querySelectorAll("button").forEach(btn => btn.disabled = disabled);
}

// Current fetch abort controller (for cancellation)
let currentFetchController = null;
let loadingStartTime = null;
let loadingTimerInterval = null;

// Update the loading overlay with elapsed time
function updateLoadingTimer() {
  if (!loadingStartTime) return;
  const elapsed = Math.floor((Date.now() - loadingStartTime) / 1000);
  // Use existing overlay-text element
  const textEl = document.querySelector('#chartOverlay .overlay-text');
  if (textEl) {
    textEl.textContent = `Loading... ${elapsed}s`;
  }
}

// Show loading overlay with timer
function showLoading() {
  const overlay = document.getElementById('chartOverlay');
  if (overlay) {
    overlay.style.display = 'flex';

    // Update existing text element
    const textEl = overlay.querySelector('.overlay-text');
    if (textEl) {
      textEl.textContent = 'Loading... 0s';
    }

    // Add cancel button if not present (only element we add)
    let cancelBtn = document.getElementById('cancelLoadingBtn');
    if (!cancelBtn) {
      cancelBtn = document.createElement('button');
      cancelBtn.id = 'cancelLoadingBtn';
      cancelBtn.textContent = 'Cancel';
      cancelBtn.className = 'cancel-btn';
      cancelBtn.onclick = cancelCurrentFetch;
      overlay.appendChild(cancelBtn);
    }
    cancelBtn.style.display = 'block';
  }
  loadingStartTime = Date.now();
  loadingTimerInterval = setInterval(updateLoadingTimer, 1000);
}

// Hide loading overlay
function hideLoading() {
  const overlay = document.getElementById('chartOverlay');
  if (overlay) overlay.style.display = 'none';
  if (loadingTimerInterval) {
    clearInterval(loadingTimerInterval);
    loadingTimerInterval = null;
  }
  loadingStartTime = null;
}

// Cancel current fetch request
function cancelCurrentFetch() {
  if (currentFetchController) {
    currentFetchController.abort();
    currentFetchController = null;
  }
  hideLoading();
  const refreshBtn = document.getElementById('refreshChart');
  if (refreshBtn) refreshBtn.disabled = false;
}

/* Fetch wrapper with timeout */
const FETCH_TIMEOUT_MS = 60000; // 60 second timeout

async function doFetch(url, options = {}) {
  // Create abort controller for this request
  currentFetchController = new AbortController();
  const signal = currentFetchController.signal;

  // Create timeout that will abort the request
  const timeoutId = setTimeout(() => {
    if (currentFetchController) {
      currentFetchController.abort();
    }
  }, FETCH_TIMEOUT_MS);

  try {
    const res = await fetch(url, { ...options, signal });
    clearTimeout(timeoutId);

    if (res.status === 429) {
      const d = await res.json().catch(() => ({}));
      const cd = d.cooldown || 1;
      alert(`Rate limited: cool-down ${cd}s`);
      setAllButtonsDisabled(true);
      setTimeout(() => setAllButtonsDisabled(false), cd * 1000);
      throw new Error('Rate limited');
    }
    if (!res.ok) {
      const d = await res.json().catch(() => ({}));
      throw new Error(d.error || `HTTP ${res.status}`);
    }
    return res.json();
  } catch (err) {
    clearTimeout(timeoutId);

    if (err.name === 'AbortError') {
      // Check if it was a timeout or user cancellation
      const elapsed = loadingStartTime ? Math.floor((Date.now() - loadingStartTime) / 1000) : 0;
      if (elapsed >= FETCH_TIMEOUT_MS / 1000 - 1) {
        throw new Error(`Request timed out after ${FETCH_TIMEOUT_MS / 1000} seconds. The server may be generating chart data - try again in a moment.`);
      } else {
        throw new Error('Request cancelled');
      }
    }
    throw err;
  } finally {
    currentFetchController = null;
  }
}



/* CSV status */
async function checkCSVStatus() {
  try {
    const d = await doFetch('/api/csv_status');
    document.getElementById('refreshChart').disabled = !(d.exists && !d.empty);
  } catch { }
}

/* Build the query string shared by the data and monthly endpoints */
function buildParams() {
  const formData = new FormData(document.getElementById('paramsForm'));
  const params = new URLSearchParams();
  for (const [key, value] of formData.entries()) {
    params.append(key, value);
  }
  return params;
}

/* Fetch chart data */
async function fetchData() {
  const form = document.querySelector('#paramsForm');
  if (!form) throw new Error('paramsForm not found');
  return await doFetch(`/api/data?${buildParams().toString()}`);
}

/* Debounced chart update */
const debouncedUpdate = createThrottledFunction(() => {
  updateChart(false);
}, 500);

/* Data freshness badge */
async function updateDataFreshness() {
  try {
    const res = await fetch('/api/data_freshness');
    const data = await res.json();
    const freshnessDiv = document.getElementById('dataFreshness');
    if (data && data.latest_scan) {
      freshnessDiv.textContent = `Latest data: ${data.latest_scan}`;
    } else {
      freshnessDiv.textContent = 'Latest data: Not available';
    }
  } catch (e) {
    const freshnessDiv = document.getElementById('dataFreshness');
    freshnessDiv.textContent = 'Latest data: Error';
  }
}

/* Scatter sparkle particles across a gold medal row. Positions and timings
   vary per particle (and per render) so they twinkle at different moments
   instead of all firing together in one spot. */
function addRankSparkles(li) {
  const PARTICLE_COUNT = 7;
  for (let i = 0; i < PARTICLE_COUNT; i++) {
    const spark = document.createElement('span');
    spark.className = 'medal-spark';
    spark.setAttribute('aria-hidden', 'true');
    spark.style.left = `${6 + Math.random() * 86}%`;
    spark.style.top = `${10 + Math.random() * 60}%`;
    const size = 7 + Math.random() * 7;
    spark.style.width = `${size.toFixed(1)}px`;
    spark.style.height = `${size.toFixed(1)}px`;
    spark.style.animationDelay = `-${(Math.random() * 3.6).toFixed(2)}s`;
    spark.style.animationDuration = `${(2.6 + Math.random() * 2).toFixed(2)}s`;
    li.appendChild(spark);
  }
}

/* Render all charts */
function renderChart(data) {
  console.log("Rendering charts with data:", data);

  // Update server filter options based on current data window
  populateServerDropdown(data);

  document.getElementById('chartCanvas').parentElement.classList.toggle('empty', !data.labels || data.labels.length === 0);
  document.getElementById('totalPlayersChart').parentElement.classList.toggle('empty', !data.dailyTotals || data.dailyTotals.length === 0);
  document.getElementById('snapshotChartCanvas').parentElement.classList.toggle('empty', !data.snapshotCounts || data.snapshotCounts.length === 0);

  const mapRankingEl = document.getElementById('mapRanking');
  mapRankingEl.textContent = '';
  (Array.isArray(data.ranking) ? data.ranking : []).forEach((m, idx) => {
    const li = document.createElement('li');
    const label = String(m.label || '');
    // gold / silver / bronze for the top three (never for the 'Other' bucket)
    if (idx < 3 && label.toLowerCase() !== 'other') {
      li.classList.add('rank-medal', `rank-${idx + 1}`);
      if (idx === 0) addRankSparkles(li);
    }

    li.appendChild(document.createTextNode(label));
    const pct = document.createElement('span');
    pct.className = 'rank-pct';
    pct.textContent = `(${m.pop}%)`;
    li.appendChild(pct);
    mapRankingEl.appendChild(li);
  });

  // Server ranking (values are average contribution numbers, not percentages)
  const serverRank = Array.isArray(data.serverRanking) ? data.serverRanking : [];
  const serverRankingEl = document.getElementById('serverRanking');
  serverRankingEl.textContent = '';
  let totalAvgPlayers = 0;
  serverRank.forEach((s, idx) => {
    const li = document.createElement('li');
    const label = String(s.label || '');
    if (idx < 3 && label.toLowerCase() !== 'other') {
      li.classList.add('rank-medal', `rank-${idx + 1}`);
      if (idx === 0) addRankSparkles(li);
    }
    li.appendChild(document.createTextNode(label));
    const pct = document.createElement('span');
    pct.className = 'rank-pct';
    pct.textContent = `(${s.pop})`;
    li.appendChild(pct);
    serverRankingEl.appendChild(li);
    totalAvgPlayers += parseFloat(s.pop) || 0;
  });

  // Display total average players
  const totalEl = document.getElementById('serverRankingTotal');
  if (totalEl) {
    totalEl.textContent = `Total Avg Players: ${totalAvgPlayers.toFixed(2)}`;
  }

  // Build datasets for the Total Players chart
  const serverDatasets = (data.totalPlayersServerDatasets && data.totalPlayersServerDatasets.length)
    ? data.totalPlayersServerDatasets
    : [{ label: 'Total Players', data: data.dailyTotals, borderColor: '#3498db', backgroundColor: 'rgba(52, 152, 219, 0.5)', fill: true }];

  // Stacked area for map share chart
  const mapDatasets = (Array.isArray(data.datasets) ? data.datasets : []).map(ds => ({
    ...ds,
    fill: true
  }));

  const appendedCount = Number(data.appendedMapsCount || 0);
  const titleCountText = appendedCount > 0 ? `Top ${data.shownMapsCount} + ${appendedCount} appended` : `Top ${data.shownMapsCount}`;

  const chartConfigs = [
    {
      canvasId: 'chartCanvas',
      chartVar: 'myChart_instance',
      type: 'line',
      title: `${titleCountText} Maps: Share of Daily Players (%)`,
      data: { labels: data.labels, datasets: mapDatasets },
      options: {
        plugins: {
          legend: { position: 'bottom', labels: { color: 'white' } },
          title: { display: true, text: `${titleCountText} Maps: Share of Daily Players (%)`, color: 'white' }
        },
        scales: {
          x: { stacked: true, title: { display: true, text: 'Date', color: 'white' } },
          y: { stacked: true, title: { display: true, text: 'Share of Daily Players (%)', color: 'white' }, ticks: { callback: v => v + '%' } }
        }
      }
    },
    {
      canvasId: 'totalPlayersChart',
      chartVar: 'totalPlayersChart_instance',
      type: 'line',
      title: 'Total Daily Players: Stacked by Server',
      data: { labels: data.labels, datasets: serverDatasets },
      options: {
        plugins: {
          legend: { position: 'bottom', labels: { color: 'white' } },
          title: { display: true, text: 'Total Daily Players: Stacked by Server', color: 'white' }
        },
        scales: {
          x: { stacked: true, title: { display: true, text: 'Date', color: 'white' } },
          y: { stacked: true, min: 0, title: { display: true, text: 'Average Players per Day', color: 'white' } }
        }
      }
    },
    {
      canvasId: 'snapshotChartCanvas',
      chartVar: 'snapshotChart_instance',
      type: 'bar',
      title: 'Snapshots per Day (unique snapshots)',
      data: { labels: data.labels, datasets: [{ label: 'Snapshots', data: data.snapshotCounts, backgroundColor: '#2ecc71' }] },
      options: {
        plugins: { legend: { display: false }, title: { display: true, text: 'Snapshots per Day (unique snapshots)', color: 'white' } },
        scales: { x: { title: { display: true, text: 'Date', color: 'white' } }, y: { title: { display: true, text: 'Snapshots', color: 'white' } } }
      }
    }
  ];

  chartConfigs.forEach(config => {
    const ctx = document.getElementById(config.canvasId).getContext('2d');
    if (window[config.chartVar]) {
      window[config.chartVar].destroy();
    }
    const chartOptions = {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { display: true, position: 'bottom', labels: { color: 'white' } },
        title: { display: true, text: config.title, color: 'white', font: chartTitleFont() },
        tooltip: {
          mode: 'index',
          intersect: false,
          callbacks: {
            title: function (context) {
              if (context.length > 0) {
                const date = new Date(context[0].label);
                return date.toLocaleString();
              }
              return '';
            }
          }
        }
      },
      scales: {
        x: {
          type: 'time',
          time: {
            unit: 'day',
            tooltipFormat: 'yyyy-MM-dd HH:mm',
            displayFormats: {
              day: 'yyyy-MM-dd'
            }
          },
          ticks: { color: 'white', source: 'auto' },
          grid: { color: 'rgba(255,255,255,0.2)' }
        },
        y: { beginAtZero: true, ticks: { color: 'white' }, grid: { color: 'rgba(255,255,255,0.2)' } }
      }
    };
    if (config.options && config.options.plugins && config.options.plugins.legend) {
      Object.assign(chartOptions.plugins.legend, config.options.plugins.legend);
    }
    if (config.options && config.options.plugins && config.options.plugins.title) {
      Object.assign(chartOptions.plugins.title, config.options.plugins.title);
    }
    if (config.options && config.options.scales && config.options.scales.y) {
      Object.assign(chartOptions.scales.y, config.options.scales.y);
    }
    if (config.options && config.options.scales && config.options.scales.x) {
      // Deep merge specifically for x scale to preserve time config if needed, 
      // though we set the base above.
      // For now, simple assign is safe as we want to override title but keep type=time
      const { title } = config.options.scales.x;
      if (title) {
        chartOptions.scales.x.title = title;
      }
    }
    window[config.chartVar] = new Chart(ctx, { type: config.type, data: config.data, options: chartOptions });
  });
}

/* Split the series so measured stretches and interpolated ones can be drawn
   (and filled) in different colours. Each interpolated run repeats the
   neighbouring measured points so the two lines stay visually joined. */
function splitMonthlySeries(points) {
  const measured = [];
  const interpolatedRuns = [];
  let run = null;
  points.forEach((p, i) => {
    if (!p.interpolated) {
      measured.push(p);
      if (run) {
        run.push(p);
        interpolatedRuns.push(run);
        run = null;
      }
    } else {
      if (!run) {
        run = [];
        if (i > 0) run.push(points[i - 1]); // anchor to the last measured point
      }
      run.push(p);
    }
  });
  if (run) interpolatedRuns.push(run);
  return { measured, interpolatedRuns };
}

/* Seasonal chart: average concurrent players per half-month for a calendar year */
function renderMonthlyChart(data) {
  const canvas = document.getElementById('monthlyChartCanvas');
  if (!canvas) return;

  const container = canvas.parentElement;
  const labels = Array.isArray(data && data.labels) ? data.labels : [];
  const averages = Array.isArray(data && data.averages) ? data.averages : [];
  const interpolated = Array.isArray(data && data.interpolated) ? data.interpolated : [];
  const points = labels.map((label, i) => ({
    x: label,
    y: averages[i],
    interpolated: !!interpolated[i]
  }));
  const year = data && data.year ? data.year : new Date().getFullYear();

  container.classList.toggle('empty', points.length === 0);
  if (points.length === 0) {
    if (window.monthlyChart_instance) {
      window.monthlyChart_instance.destroy();
      window.monthlyChart_instance = null;
    }
    return;
  }

  const ctx = canvas.getContext('2d');
  if (window.monthlyChart_instance) window.monthlyChart_instance.destroy();

  const { measured, interpolatedRuns } = splitMonthlySeries(points);

  const measuredDataset = {
    label: 'Measured',
    data: measured,
    borderColor: '#2ecc71',
    backgroundColor: 'rgba(46, 204, 113, 0.14)',
    borderWidth: 2,
    tension: 0.25,
    fill: true,
    pointRadius: 3.5,
    pointBackgroundColor: '#2ecc71',
    pointBorderColor: '#2ecc71'
  };
  // one dataset per interpolated run, so the area under it is tinted red
  // rather than inheriting the green of the measured series
  const interpolatedDatasets = interpolatedRuns.map((run) => ({
    label: 'Interpolated',
    data: run,
    borderColor: '#e74c3c',
    backgroundColor: 'rgba(231, 76, 60, 0.14)',
    borderWidth: 2,
    borderDash: [5, 4],
    tension: 0.25,
    fill: true,
    pointRadius: 2.5,
    pointBackgroundColor: '#e74c3c',
    pointBorderColor: '#e74c3c'
  }));

  window.monthlyChart_instance = new Chart(ctx, {
    type: 'line',
    data: { datasets: [measuredDataset, ...interpolatedDatasets] },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { display: false },
        title: { display: true, text: `Average Players per Month (${year})`, color: 'white', font: chartTitleFont() },
        tooltip: {
          callbacks: {
            title: (ctx2) => (ctx2.length ? new Date(ctx2[0].parsed.x).toLocaleDateString() : ''),
            label: (ctx2) => {
              const p = ctx2.raw || {};
              return p.interpolated
                ? `${ctx2.parsed.y} average players (interpolated)`
                : `${ctx2.parsed.y} average players`;
            }
          }
        }
      },
      scales: {
        x: {
          type: 'time',
          // fixed Jan..Dec axis so years are comparable even with gaps
          min: `${year}-01-01`,
          max: `${year}-12-31`,
          time: { unit: 'month', displayFormats: { month: 'MMM' } },
          ticks: { color: 'white', maxRotation: 0, autoSkip: true },
          grid: { color: 'rgba(255,255,255,0.2)' }
        },
        y: {
          beginAtZero: true,
          ticks: { color: 'white' },
          grid: { color: 'rgba(255,255,255,0.2)' },
          title: { display: true, text: 'Average Players', color: 'white' }
        }
      }
    }
  });
}

/* Date Range Logic */
function initDateRange(minDateStr, maxDateStr) {
  const startInput = document.getElementById('StartDateInput');
  const endInput = document.getElementById('EndDateInput');
  const daysHidden = document.getElementById('DaysToShow');

  if (!startInput || !endInput) return;

  // Function to calculate days between dates
  const countDays = (s, e) => {
    const d1 = new Date(s);
    const d2 = new Date(e);
    return Math.floor((d2 - d1) / (1000 * 60 * 60 * 24)) + 1;
  };

  // Set initial max date to today or max available
  // To be safe, let's use the current date as max if maxDateStr is far in the future
  // But strictly, we should just respect what's passed or default to today
  const today = new Date().toISOString().split('T')[0];
  const maxVal = maxDateStr || today;
  const minVal = minDateStr || null;

  // Constrain date inputs to valid data range
  startInput.max = maxVal;
  endInput.max = maxVal;
  if (minVal) {
    startInput.min = minVal;
    endInput.min = minVal;
  }

  // Initialize values if empty
  if (!endInput.value) {
    endInput.value = maxVal;
  }
  if (!startInput.value) {
    // Default to 7 days before end, but clamp to earliest available data
    const d = new Date(endInput.value);
    d.setDate(d.getDate() - 6);
    let calculatedStart = d.toISOString().split('T')[0];

    // Clamp to earliest available data to avoid skewed rankings from empty time buckets
    if (minDateStr && calculatedStart < minDateStr) {
      calculatedStart = minDateStr;
    }
    startInput.value = calculatedStart;
  }

  // Sync hidden field
  const syncDays = () => {
    if (startInput.value && endInput.value) {
      if (startInput.value > endInput.value) {
        // Swap if invalid
        const temp = startInput.value;
        startInput.value = endInput.value;
        endInput.value = temp;
      }
      const diff = countDays(startInput.value, endInput.value);
      if (daysHidden) daysHidden.value = Math.max(1, diff);

      // Also update the param used by the API (Start Date is the API parameter)
      // The API expects 'start_date' and 'days_to_show'. 
      // Our form has name="start_date" on startInput, so it matches.
    }
  };

  startInput.addEventListener('change', () => {
    syncDays();
    // Optional: Auto-refresh or wait for button
    // debouncedUpdate(); 
  });

  endInput.addEventListener('change', () => {
    syncDays();
    // debouncedUpdate();
  });

  // Initial sync
  syncDays();
}

/* Main chart update orchestrator */
const updateChart = createThrottledFunction(async (showLoadingOverlay = true) => {
  const refreshBtn = document.getElementById('refreshChart');

  if (showLoadingOverlay) showLoading();
  refreshBtn.disabled = true;

  try {
    await checkCSVStatus();
    const chartData = await fetchData();
    renderChart(chartData);

    // The seasonal chart follows the same filters as the view, so a failure
    // here must not break the main charts
    try {
      const monthly = await doFetch(`/api/monthly?${buildParams().toString()}`);
      renderMonthlyChart(monthly);
    } catch (monthlyError) {
      console.debug('Monthly averages unavailable:', monthlyError);
    }

    if (!chartData.labels.length) {
      alert('No chart data found for the selected parameters.');
    }
  } catch (error) {
    console.error('Failed to update chart:', error);
    // Don't alert for user cancellation
    if (error.message !== 'Request cancelled') {
      alert(error.message || 'An error occurred while fetching chart data.');
    }
  } finally {
    hideLoading();
    refreshBtn.disabled = false;
  }
}, 1000);

/* --- Main --- */
async function initialize() {
  // Attach event listeners
  // Do not update chart on parameter changes or form submit; only via Refresh button
  document.getElementById('paramsForm').addEventListener('submit', (e) => {
    e.preventDefault();
    // Intentionally not calling updateChart here
  });
  // Remove automatic refresh-on-input entirely per requirement
  // document.getElementById('paramsForm').addEventListener('input', ... )
  document.getElementById('refreshChart').addEventListener('click', () => updateChart(true));
  // Do not auto-refresh when changing the server filter; user must click Refresh

  // Initial page load
  await checkCSVStatus();

  // Use data_coverage to get min/max for date inputs
  let range = { start: null, end: null };
  try {
    const cov = await doFetch('/api/data_coverage');
    if (cov) {
      range.start = cov.start;
      range.end = cov.end;
    }
  } catch (e) { }

  initDateRange(range.start, range.end);

  await updateChart(true);
  await updateDataFreshness();
}

// (Old Start Date setter removed; superseded by timeline)

document.addEventListener('DOMContentLoaded', initialize);

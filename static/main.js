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
    function setAllButtonsDisabled(disabled) {
      document.querySelectorAll("button").forEach(btn => btn.disabled = disabled);
    }
    function sanitizeApiKey(key) {
      return key.replace(/[^a-zA-Z0-9-_]/g,'');
    }
    function hasValidApiKey(key) {
      return key && key.length >= 10;
    }

    /* Fetch wrapper */
    async function doFetch(url, options = {}) {
      options.headers = options.headers || {};
      const rawKey = document.getElementById('api_key').value || '';
      const key = sanitizeApiKey(rawKey);
      if (hasValidApiKey(key)) options.headers['X-API-KEY'] = key;

      const res = await fetch(url, options);
      if (res.status === 429) {
        const d = await res.json().catch(() => ({}));
        const cd = d.cooldown || 1;
        alert(`Rate limited — cool-down ${cd}s`);
        setAllButtonsDisabled(true);
        setTimeout(() => setAllButtonsDisabled(false), cd * 1000);
        throw new Error('Rate limited');
      }
      if (res.status === 401) {
        document.getElementById('apiKeyState').textContent = 'API Key State: Invalid';
        throw new Error('Unauthorized');
      }
      if (!res.ok) {
        const d = await res.json().catch(() => ({}));
        alert(d.error || `HTTP ${res.status}`);
        throw new Error(`HTTP error: ${res.status}`);
      }
      return res.json();
    }

    /* API-key UI */
    let statusInterval = null;
    async function validateApiKey(key) {
      if (!hasValidApiKey(key)) return false;
      try {
        const res = await fetch('/api/validate_key', { cache: 'no-store', headers: { 'X-API-KEY': key } });
        return res.ok;
      } catch { return false; }
    }
    function toggleExtraParams(show) {
      document.getElementById('fastWriteGroup').style.display = show ? 'flex' : 'none';
      document.getElementById('runtimeMinutesGroup').style.display = show ? 'flex' : 'none';
    }
    async function checkApiKeyState() {
      const elem = document.getElementById('api_key');
      const raw = elem.value;
      const key = sanitizeApiKey(raw);
      if (raw !== key) elem.value = key;
      if (!hasValidApiKey(key)) {
        document.getElementById('apiKeyState').textContent =
          'API Key State: Not supplied — chart-only mode';
        document.getElementById('startScan').style.display = 'none';
        document.getElementById('stopScan').style.display = 'none';
        toggleExtraParams(false);
        if (statusInterval) clearInterval(statusInterval), statusInterval = null;
        return;
      }
      const valid = await validateApiKey(key);
      if (!valid) {
        document.getElementById('apiKeyState').textContent = 'API Key State: Invalid';
        document.getElementById('startScan').style.display = 'none';
        document.getElementById('stopScan').style.display = 'none';
        toggleExtraParams(false);
        if (statusInterval) clearInterval(statusInterval), statusInterval = null;
        return;
      }
      document.getElementById('apiKeyState').textContent = 'API Key State: Valid';
      document.getElementById('startScan').style.display = 'inline-block';
      document.getElementById('stopScan').style.display = 'inline-block';
      toggleExtraParams(true);
      if (!statusInterval) {
        statusInterval = setInterval(() => refreshStatus().catch(() => {}), 1000);
      }
    }

    /* Connection heartbeat */
    async function updateConnectionStatus() {
      const el = document.getElementById('connectionStatus');
      try {
        const d = await fetch('/api/heartbeat', { cache: 'no-store' })
          .then(r => { if (!r.ok) throw ''; return r.json(); });
        el.textContent = 'Connected';
        el.classList.replace('disconnected', 'connected');
        document.getElementById('requestsLeft').textContent =
          `Requests Left: ${d.requests_left}`;
      } catch {
        el.textContent = 'No server found';
        el.classList.replace('connected', 'disconnected');
      }
    }

    /* CSV status */
    async function checkCSVStatus() {
      try {
        const d = await doFetch('/api/csv_status');
        document.getElementById('refreshChart').disabled = !(d.exists && !d.empty);
      } catch {}
    }

    /* Scan controls */
    async function startScanning() {
      const key = sanitizeApiKey(document.getElementById('api_key').value);
      if (!hasValidApiKey(key)) { alert('API key must be at least 10 characters.'); return; }
      try {
        const res = await doFetch('/api/start_scan', { method: 'POST' });
        alert(res.status || res.error);
      } catch {}
    }
    async function stopScanning() {
      const key = sanitizeApiKey(document.getElementById('api_key').value);
      if (!hasValidApiKey(key)) { alert('API key must be at least 10 characters.'); return; }
      try {
        const res = await doFetch('/api/stop_scan', { method: 'POST' });
        alert(res.status || res.error);
      } catch {}
    }

    /* Status polling */
    async function refreshStatus() {
      try {
        const s = await doFetch('/api/status');
        document.getElementById('scanningStatus').textContent = `Scanning Status: ${s.scanning_status || 'Idle'}`;
        document.getElementById('scanningMode').textContent = `Scanning Mode: ${s.scanning_mode || 'None'}`;
        document.getElementById('currentIP').textContent = `Current IP: ${s.current_scanned_ip || 'None'}`;
        document.getElementById('lastError').textContent = `Last Error: ${s.last_error || 'None'}`;
        document.getElementById('errorCount').textContent = `Error Count: ${s.error_count || 0}`;
      } catch {}
    }

    /* Fetch chart data */
    async function fetchData() {
      const form = document.querySelector('#paramsForm');
      if (!form) throw new Error('paramsForm not found');
      const formData = new FormData(document.getElementById('paramsForm'));
      const params = new URLSearchParams();
      for (const [key, value] of formData.entries()) {
        params.append(key, value);
      }
      return await doFetch(`/api/data?${params.toString()}`);
    }

    /* Debounced chart update */
    const debouncedUpdate = createThrottledFunction(() => {
      updateChart(false);
    }, 500);

    /* Render all charts */
    function renderChart(data) {
      console.log("Rendering charts with data:", data);

      document.getElementById('chartCanvas').parentElement.classList.toggle('empty', !data.labels || data.labels.length === 0);
      document.getElementById('totalPlayersChart').parentElement.classList.toggle('empty', !data.dailyTotals || data.dailyTotals.length === 0);
      document.getElementById('snapshotChartCanvas').parentElement.classList.toggle('empty', !data.snapshotCounts || data.snapshotCounts.length === 0);

      document.getElementById('mapRanking').innerHTML = data.ranking.map((m, idx) => {
        const medal = idx === 0 ? '🥇' : idx === 1 ? '🥈' : idx === 2 ? '🥉' : '';
        return `<li>${medal}${m.label}<span class="rank-pct">(${m.pop}%)</span></li>`;
      }).join('');

      const chartConfigs = [
        {
          canvasId: 'chartCanvas',
          chartVar: 'myChart_instance',
          type: 'line',
          title: `Top ${data.shownMapsCount} Maps (share per day %)`,
          data: { labels: data.labels, datasets: data.datasets },
          options: { plugins: { legend: { position: 'bottom', labels: { color: 'white' } } }, scales: { y: { ticks: { callback: v => v + '%' } } } }
        },
        {
          canvasId: 'totalPlayersChart',
          chartVar: 'totalPlayersChart_instance',
          type: 'line',
          title: 'Total Daily Players (filtered)',
          data: { labels: data.labels, datasets: [{ label: 'Total Players', data: data.dailyTotals, borderColor: '#3498db', backgroundColor: 'rgba(52, 152, 219, 0.5)', fill: true }] },
          options: { plugins: { legend: { display: false } } }
        },
        {
          canvasId: 'snapshotChartCanvas',
          chartVar: 'snapshotChart_instance',
          type: 'bar',
          title: 'Snapshots per Day',
          data: { labels: data.labels, datasets: [{ label: 'Snapshots per Day', data: data.snapshotCounts, backgroundColor: '#2ecc71' }] },
          options: { plugins: { legend: { display: false } } }
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
            title: { display: true, text: config.title, color: 'white' }
          },
          scales: {
            x: { ticks: { color: 'white' }, grid: { color: 'rgba(255,255,255,0.2)' } },
            y: { beginAtZero: true, ticks: { color: 'white' }, grid: { color: 'rgba(255,255,255,0.2)' } }
          },
          ...config.options
        };
        if (config.options && config.options.plugins && config.options.plugins.legend) {
          Object.assign(chartOptions.plugins.legend, config.options.plugins.legend);
        }
        if (config.options && config.options.scales && config.options.scales.y) {
          Object.assign(chartOptions.scales.y, config.options.scales.y);
        }
        window[config.chartVar] = new Chart(ctx, { type: config.type, data: config.data, options: chartOptions });
      });

      const avgCount = data.averageDailyPlayerCount.toFixed(1);
      document.getElementById('playerCountDisplay').textContent = `Average player count (${document.getElementById('OnlyMapsContaining').value}): ${avgCount}`;
    }

    /* Date picker */
    async function initializeDatePicker() {
      const dateInput = document.getElementById('Start_Date');
      try {
        const data = await doFetch('/api/date_range');
        if (data.min_date && data.max_date) {
          dateInput.min = data.min_date;
          dateInput.max = data.max_date;
          dateInput.value = data.min_date; // Default to the earliest date
        }
      } catch (error) {
        console.error('Failed to initialize date picker:', error);
      }
    }

    /* Main chart update orchestrator */
    const updateChart = createThrottledFunction(async (showLoading = true) => {
      const loadingIndicator = document.getElementById('loadingIndicator');
      const chartContainers = document.querySelectorAll('.chart-container');
      const playerCount = document.getElementById('playerCountDisplay');
      const refreshBtn = document.getElementById('refreshChart');

      if (showLoading) {
        loadingIndicator.style.display = 'block';
        chartContainers.forEach(c => c.style.display = 'none');
        playerCount.style.display = 'none';
      }
      refreshBtn.disabled = true;

      try {
        await checkCSVStatus();
        const chartData = await fetchData();
        renderChart(chartData);

        if (!chartData.labels.length) {
          alert('No chart data found for the selected parameters.');
        }
      } catch (error) {
        console.error('Failed to update chart:', error);
        alert('An error occurred while fetching chart data. Please check the console for details.');
      } finally {
        if (showLoading) {
          loadingIndicator.style.display = 'none';
          chartContainers.forEach(c => c.style.display = 'block');
          playerCount.style.display = 'block';
        }
        refreshBtn.disabled = false;
      }
    }, 1000);

    /* --- Main --- */
    async function initialize() {
      // Attach event listeners
      document.getElementById('paramsForm').addEventListener('submit', (e) => {
        e.preventDefault();
        updateChart(true);
      });
      document.getElementById('paramsForm').addEventListener('input', debouncedUpdate);
      document.getElementById('refreshChart').addEventListener('click', () => updateChart(true));
      document.getElementById('startScan').addEventListener('click', startScanning);
      document.getElementById('stopScan').addEventListener('click', stopScanning);
      document.getElementById('refreshStatus').addEventListener('click', refreshStatus);
      document.getElementById('api_key').addEventListener('input', () => checkApiKeyState().catch(() => {}));

      // Initial page load
      updateConnectionStatus();
      await checkApiKeyState();
      await checkCSVStatus();
      await refreshStatus();
      await initializeDatePicker();
      await updateChart(true);
    }

    document.addEventListener('DOMContentLoaded', initialize);

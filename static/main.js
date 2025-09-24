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
          .filter(s => s && s.label && s.label !== 'Other')
          .map(s => ({ value: String(s.label), text: `${s.label} (${s.pop})` }));

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
    function toggleExtraParams(show) {
      document.getElementById('fastWriteGroup').style.display = show ? 'flex' : 'none';
      document.getElementById('runtimeMinutesGroup').style.display = show ? 'flex' : 'none';
    }
    async function checkApiKeyState() {
      const elem = document.getElementById('api_key');
      const raw = elem.value;
      const key = sanitizeApiKey(raw);
      if (raw !== key) elem.value = key;

      // We'll just rely on the client-side check for showing extra params
      // to avoid a 404 on a non-existent endpoint. The backend still validates.
      const valid = hasValidApiKey(key);
      toggleExtraParams(valid);
    }

    /* CSV status */
    async function checkCSVStatus() {
      try {
        const d = await doFetch('/api/csv_status');
        document.getElementById('refreshChart').disabled = !(d.exists && !d.empty);
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
        const medal = idx === 0 ? '🥇' : idx === 1 ? '🥈' : idx === 2 ? '🥉' : '';
        if (medal) {
          li.appendChild(document.createTextNode(medal));
        }
        li.appendChild(document.createTextNode(String(m.label || '')));
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
      serverRank.forEach((s) => {
        const li = document.createElement('li');
        li.appendChild(document.createTextNode(String(s.label || '')));
        const pct = document.createElement('span');
        pct.className = 'rank-pct';
        pct.textContent = `(${s.pop})`;
        li.appendChild(pct);
        serverRankingEl.appendChild(li);
      });

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
          title: `${titleCountText} Maps — Share of Daily Players (%)`,
          data: { labels: data.labels, datasets: mapDatasets },
          options: {
            plugins: {
              legend: { position: 'bottom', labels: { color: 'white' } },
              title: { display: true, text: `${titleCountText} Maps — Share of Daily Players (%)`, color: 'white' }
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
          title: 'Total Daily Players — Stacked by Server',
          data: { labels: data.labels, datasets: serverDatasets },
          options: {
            plugins: {
              legend: { position: 'bottom', labels: { color: 'white' } },
              title: { display: true, text: 'Total Daily Players — Stacked by Server', color: 'white' }
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
            title: { display: true, text: config.title, color: 'white' }
          },
          scales: {
            x: { ticks: { color: 'white' }, grid: { color: 'rgba(255,255,255,0.2)' } },
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
          Object.assign(chartOptions.scales.x, config.options.scales.x);
        }
        window[config.chartVar] = new Chart(ctx, { type: config.type, data: config.data, options: chartOptions });
      });
    }

    /* Timeline date range */
    const timelineState = {
      dates: [],        // array of 'YYYY-MM-DD'
      presentSet: new Set(),
      startIdx: 0,
      endIdx: 0,
      pxPerDay: 6,
      activeHandle: null // 'start' | 'end' | null
    };

    function ymdToDate(s) {
      const [y, m, d] = s.split('-').map(Number);
      return new Date(y, m - 1, d);
    }
    function dateToYmd(d) {
      const y = d.getFullYear();
      const m = String(d.getMonth() + 1).padStart(2, '0');
      const da = String(d.getDate()).padStart(2, '0');
      return `${y}-${m}-${da}`;
    }
    function enumerateDatesInclusive(startStr, endStr) {
      const dates = [];
      if (!startStr || !endStr) return dates;
      let d = ymdToDate(startStr);
      const end = ymdToDate(endStr);
      while (d <= end) {
        dates.push(dateToYmd(d));
        d = new Date(d.getFullYear(), d.getMonth(), d.getDate() + 1);
      }
      return dates;
    }

    function updateHiddenDateFields() {
      // Map timeline selection to form fields: Start_Date + DaysToShow
      const startYmd = timelineState.dates[timelineState.startIdx];
      const endYmd = timelineState.dates[timelineState.endIdx];
      const dayCount = timelineState.endIdx - timelineState.startIdx + 1;
      const startInput = document.getElementById('Start_Date');
      const daysInput = document.getElementById('DaysToShow');
      if (startInput) startInput.value = startYmd;
      if (daysInput) daysInput.value = Math.max(1, dayCount);
      updateCountLabel();
    }

    function updateTimelineSelectionUI() {
      const sel = document.getElementById('timelineSelection');
      const total = Math.max(1, timelineState.dates.length - 1);
      const leftPct = (timelineState.startIdx / total) * 100;
      const rightPct = (timelineState.endIdx / total) * 100;
      sel.style.left = leftPct + '%';
      sel.style.width = Math.max(0, rightPct - leftPct) + '%';
    }

    function updateBracketMarkers() {
      const total = Math.max(1, timelineState.dates.length - 1);
      const bStart = document.getElementById('timelineBracketStart');
      const bEnd = document.getElementById('timelineBracketEnd');
      if (!bStart || !bEnd) return;
      const leftPct = (timelineState.startIdx / total) * 100;
      const rightPct = (timelineState.endIdx / total) * 100;
      bStart.style.left = leftPct + '%';
      bEnd.style.left = rightPct + '%';
    }

    function updateVisibleWindowLabels() {
      if (timelineState.dates.length === 0) return;
      const startLbl = document.getElementById('timelineStartLabel');
      const endLbl = document.getElementById('timelineEndLabel');
      if (startLbl) startLbl.textContent = timelineState.dates[timelineState.startIdx] || '';
      if (endLbl) endLbl.textContent = timelineState.dates[timelineState.endIdx] || '';
    }

    function updateCountLabel() {
      const dayCount = timelineState.endIdx - timelineState.startIdx + 1;
      const lbl = document.getElementById('timelineCountLabel');
      if (lbl) lbl.textContent = `${dayCount} ${dayCount === 1 ? 'day' : 'days'}`;
    }

    function updateCoverageBackground() {
      // Build contiguous segments across dates as green (present) or red (absent)
      const total = Math.max(1, timelineState.dates.length - 1);
      const covEl = document.getElementById('timelineCoverage');
      if (timelineState.dates.length === 0) {
        covEl.style.background = 'transparent';
        return;
      }
      if (timelineState.dates.length === 1) {
        const only = timelineState.dates[0];
        const color = timelineState.presentSet.has(only) ? '#f39c12' : '#c0392b';
        covEl.style.background = color;
        return;
      }
      const segs = [];
      let i = 0;
      while (i < timelineState.dates.length) {
        const present = timelineState.presentSet.has(timelineState.dates[i]);
        const start = i;
        i++;
        while (i < timelineState.dates.length && timelineState.presentSet.has(timelineState.dates[i]) === present) i++;
        const end = i - 1;
        const startPct = (start / total) * 100;
        const endPct = (end / total) * 100;
        const color = present ? '#f39c12' : '#c0392b';
        segs.push(`${color} ${startPct}%, ${color} ${endPct}%`);
      }
      const gradient = `linear-gradient(to right, ${segs.join(', ')})`;
      covEl.style.background = gradient;
    }

    async function initializeTimeline() {
      try {
        const cov = await doFetch('/api/data_coverage');
        const container = document.getElementById('timelineContainer');
        if (!cov || !cov.start || !cov.end) {
          container.style.display = 'none';
          return;
        }
        timelineState.dates = enumerateDatesInclusive(cov.start, cov.end);
        timelineState.presentSet = new Set(cov.present_dates || []);

        // Set scrollable inner width proportional to number of days
        const scrollEl = document.getElementById('timelineScroll');
        const innerEl = document.getElementById('timelineInner');
        const dCount = Math.max(1, timelineState.dates.length);
        const computeScale = (preserveLeftIdx = null) => {
          const viewport = scrollEl.clientWidth || 600;
          const targetDaysVisible = Math.max(1, Math.min(30, dCount)); // ~one month in view
          const oldPx = timelineState.pxPerDay;
          const leftIdx = preserveLeftIdx !== null ? preserveLeftIdx : Math.floor(scrollEl.scrollLeft / Math.max(1, oldPx));
          const pxPerDay = viewport / targetDaysVisible;
          timelineState.pxPerDay = pxPerDay;
          const innerWidth = Math.min(dCount * pxPerDay, 200000);
          innerEl.style.width = innerWidth + 'px';
          // preserve left index position
          const newScrollLeft = Math.max(0, Math.min(innerWidth - viewport, leftIdx * pxPerDay));
          scrollEl.scrollLeft = newScrollLeft;
          updateVisibleWindowLabels();
        };
        // initial scale
        computeScale();

        const startRange = document.getElementById('timelineStart');
        const endRange = document.getElementById('timelineEnd');
        const maxIdx = Math.max(1, timelineState.dates.length - 1);
        startRange.min = '0';
        startRange.max = String(maxIdx);
        endRange.min = '0';
        endRange.max = String(maxIdx);

        // Default selection: last 7 days if possible
        timelineState.endIdx = maxIdx;
        timelineState.startIdx = Math.max(0, maxIdx - 6);
        startRange.value = String(timelineState.startIdx);
        endRange.value = String(timelineState.endIdx);

        updateCoverageBackground();
        updateTimelineSelectionUI();
        updateHiddenDateFields();
        updateVisibleWindowLabels();
        updateBracketMarkers();

        scrollEl.addEventListener('scroll', () => {
          updateVisibleWindowLabels();
          updateBracketMarkers();
        });
        window.addEventListener('resize', () => {
          const preserveLeftIdx = Math.floor(scrollEl.scrollLeft / Math.max(1, timelineState.pxPerDay));
          const viewport = scrollEl.clientWidth || 600;
          // recompute scale to keep ~one month visible
          const dCount2 = Math.max(1, timelineState.dates.length);
          const targetDaysVisible = Math.max(1, Math.min(30, dCount2));
          // reuse computeScale with preserved left index
          const oldPx = timelineState.pxPerDay;
          (function(){
            const pxPerDay = viewport / targetDaysVisible;
            timelineState.pxPerDay = pxPerDay;
            const innerWidth = Math.min(dCount2 * pxPerDay, 200000);
            innerEl.style.width = innerWidth + 'px';
            const newScrollLeft = Math.max(0, Math.min(innerWidth - viewport, preserveLeftIdx * pxPerDay));
            scrollEl.scrollLeft = newScrollLeft;
            updateVisibleWindowLabels();
            updateBracketMarkers();
          })();
        });

        // Determine which thumb is closer to the pointer and bring it to front
        const bringNearestHandleToFront = (clientX) => {
          const rect = scrollEl.getBoundingClientRect();
          const localX = clientX - rect.left; // px within scroll viewport
          const pointerIdx = Math.max(0, Math.min(timelineState.dates.length - 1,
            Math.floor((scrollEl.scrollLeft + localX) / timelineState.pxPerDay)));
          const dStart = Math.abs(pointerIdx - timelineState.startIdx);
          const dEnd = Math.abs(pointerIdx - timelineState.endIdx);
          const startEl = document.getElementById('timelineStart');
          const endEl = document.getElementById('timelineEnd');
          if (!startEl || !endEl) return;
          if (dStart <= dEnd) {
            startEl.style.zIndex = '5';
            endEl.style.zIndex = '4';
          } else {
            startEl.style.zIndex = '4';
            endEl.style.zIndex = '5';
          }
        };

        const pointerHandler = (ev) => {
          const cx = ev.touches && ev.touches.length ? ev.touches[0].clientX : ev.clientX;
          if (typeof cx === 'number') bringNearestHandleToFront(cx);
        };

        // Update active handle on pointer activity over scroll area and sliders
        // Capturing handlers fire before the event reaches the inputs, so the nearest
        // thumb is brought to front for the initial click/touch.
        scrollEl.addEventListener('pointerdown', pointerHandler, { capture: true });
        scrollEl.addEventListener('mousedown', pointerHandler, { capture: true });
        scrollEl.addEventListener('touchstart', pointerHandler, { passive: true, capture: true });
        // Keep updating as the pointer moves across the area
        scrollEl.addEventListener('mousemove', pointerHandler);
        scrollEl.addEventListener('pointermove', pointerHandler);
        document.getElementById('timelineStart').addEventListener('mousemove', pointerHandler);
        document.getElementById('timelineEnd').addEventListener('mousemove', pointerHandler);

        // Track active handle for auto-scroll logic
        startRange.addEventListener('pointerdown', () => { timelineState.activeHandle = 'start'; });
        endRange.addEventListener('pointerdown',   () => { timelineState.activeHandle = 'end'; });
        startRange.addEventListener('mousedown',   () => { timelineState.activeHandle = 'start'; });
        endRange.addEventListener('mousedown',     () => { timelineState.activeHandle = 'end'; });
        startRange.addEventListener('touchstart',  () => { timelineState.activeHandle = 'start'; }, { passive: true });
        endRange.addEventListener('touchstart',    () => { timelineState.activeHandle = 'end'; },   { passive: true });
        const clearActive = () => { timelineState.activeHandle = null; };
        window.addEventListener('pointerup', clearActive);
        window.addEventListener('mouseup', clearActive);
        window.addEventListener('touchend', clearActive, { passive: true });

        const getVisibleIdxRange = () => {
          const leftPx = scrollEl.scrollLeft;
          const rightPx = leftPx + scrollEl.clientWidth;
          const leftIdx = Math.max(0, Math.min(timelineState.dates.length - 1, Math.floor(leftPx / Math.max(1, timelineState.pxPerDay))));
          const rightIdx = Math.max(0, Math.min(timelineState.dates.length - 1, Math.floor((rightPx - 1) / Math.max(1, timelineState.pxPerDay))));
          return { leftIdx, rightIdx };
        };

        const ensureHandleVisible = (idx) => {
          const { leftIdx, rightIdx } = getVisibleIdxRange();
          const margin = 2; // days of padding
          const viewport = scrollEl.clientWidth || 1;
          const innerWidth = parseFloat(getComputedStyle(innerEl).width) || (timelineState.dates.length * timelineState.pxPerDay);
          if (idx <= leftIdx + margin) {
            const targetLeftIdx = Math.max(0, idx - margin);
            scrollEl.scrollLeft = Math.max(0, Math.min(innerWidth - viewport, targetLeftIdx * timelineState.pxPerDay));
          } else if (idx >= rightIdx - margin) {
            const targetRightIdx = Math.min(timelineState.dates.length - 1, idx + margin);
            const newLeft = targetRightIdx * timelineState.pxPerDay - viewport + timelineState.pxPerDay;
            scrollEl.scrollLeft = Math.max(0, Math.min(innerWidth - viewport, newLeft));
          }
        };

        const clampAndSync = () => {
          let s = parseInt(startRange.value, 10);
          let e = parseInt(endRange.value, 10);
          if (s > e) {
            // Snap the one the user moved
            const active = document.activeElement === endRange ? 'end' : 'start';
            if (active === 'end') s = e; else e = s;
          }
          const maxIdx2 = Math.max(1, timelineState.dates.length - 1);
          s = Math.max(0, Math.min(maxIdx2, s));
          e = Math.max(0, Math.min(maxIdx2, e));
          timelineState.startIdx = s;
          timelineState.endIdx = e;
          startRange.value = String(s);
          endRange.value = String(e);
          updateTimelineSelectionUI();
          updateHiddenDateFields();
          updateVisibleWindowLabels();
          updateBracketMarkers();
          // Auto-scroll to keep the active handle in view
          const idx = timelineState.activeHandle === 'end' ? timelineState.endIdx : timelineState.startIdx;
          ensureHandleVisible(idx);
        };

        startRange.addEventListener('input', () => { clampAndSync(); updateCountLabel(); /* manual refresh required */ });
        endRange.addEventListener('input', () => { clampAndSync(); updateCountLabel(); /* manual refresh required */ });
      } catch (error) {
        console.error('Failed to initialize timeline:', error);
      }
    }

    /* Main chart update orchestrator */
    const updateChart = createThrottledFunction(async (showLoading = true) => {
      const overlay = document.getElementById('chartOverlay');
      const refreshBtn = document.getElementById('refreshChart');

      if (showLoading && overlay) overlay.style.display = 'flex';
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
        if (overlay) overlay.style.display = 'none';
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
      document.getElementById('api_key').addEventListener('input', () => checkApiKeyState().catch(() => {}));
      // Do not auto-refresh when changing the server filter; user must click Refresh

      // Initial page load
      await checkApiKeyState();
      await checkCSVStatus();
      await initializeTimeline();
      await updateChart(true);
      await updateDataFreshness();
    }

    // (Old Start Date setter removed; superseded by timeline)

document.addEventListener('DOMContentLoaded', initialize);

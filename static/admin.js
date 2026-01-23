let currentPage = 1;
let currentLimit = 50;
let totalPages = 1;

async function loadStats() {
    const tbody = document.getElementById('logTable');
    try {
        const query = new URLSearchParams({
            page: currentPage,
            limit: currentLimit,
            date: document.getElementById('dateSelect').value
        });

        console.log(`Fetching admin stats (page ${currentPage}, limit ${currentLimit})...`);
        const res = await fetch(`/api/admin/stats?${query.toString()}`);

        if (!res.ok) {
            const text = await res.text();
            tbody.innerHTML = `<tr><td colspan="4">Error: HTTP ${res.status}</td></tr>`;
            return;
        }

        const data = await res.json();

        // Update Summary Cards
        document.getElementById('totalRequests').textContent = data.total_requests || 0;
        document.getElementById('uniqueIps').textContent = data.unique_ips_today || 0;
        document.getElementById('currentDate').textContent = new Date().toLocaleDateString();

        const lastUpdateEl = document.getElementById('lastUpdate');
        if (lastUpdateEl) {
            lastUpdateEl.textContent = 'Last updated: ' + new Date().toLocaleTimeString();
        }

        // Update Pagination Controls
        totalPages = data.total_pages || 1;
        document.getElementById('pageIndicator').textContent = `Page ${data.page} of ${totalPages}`;
        document.getElementById('prevBtn').disabled = data.page <= 1;
        document.getElementById('nextBtn').disabled = data.page >= totalPages;

        if (!data.logs || data.logs.length === 0) {
            tbody.innerHTML = '<tr><td colspan="4">No requests found.</td></tr>';
            return;
        }

        // Render Rows
        tbody.innerHTML = data.logs.map(log => `
            <tr>
                <td>${log.timestamp}</td>
                <td>${log.ip}</td>
                <td><span class="endpoint-item">${log.endpoint}</span></td>
                <td style="word-break: break-all; font-family: monospace; font-size: 0.9em;">${log.full_path}</td>
            </tr>
        `).join('');

    } catch (err) {
        console.error('Error loading stats:', err);
        tbody.innerHTML = `<tr><td colspan="4">Error: ${err.message}</td></tr>`;
    }
}

document.addEventListener('DOMContentLoaded', () => {
    // Initial Load - Set default date to today
    const now = new Date();
    const localDate = now.toLocaleDateString('en-CA'); // YYYY-MM-DD format
    document.getElementById('dateSelect').value = localDate;

    loadStats();

    // Refresh Button
    const refreshBtn = document.querySelector('.refresh-btn');
    if (refreshBtn) {
        refreshBtn.addEventListener('click', loadStats);
    }

    // Date Selector
    document.getElementById('dateSelect').addEventListener('change', () => {
        currentPage = 1;
        loadStats();
    });

    // Limit Selector
    document.getElementById('limitSelect').addEventListener('change', (e) => {
        currentLimit = parseInt(e.target.value);
        currentPage = 1; // Reset to first page
        loadStats();
    });

    // Pagination Buttons
    document.getElementById('prevBtn').addEventListener('click', () => {
        if (currentPage > 1) {
            currentPage--;
            loadStats();
        }
    });

    document.getElementById('nextBtn').addEventListener('click', () => {
        if (currentPage < totalPages) {
            currentPage++;
            loadStats();
        }
    });

    // Auto-refresh every 30 seconds
    setInterval(loadStats, 30000);
});

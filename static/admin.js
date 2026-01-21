async function loadStats() {
    const tbody = document.getElementById('ipTable');
    try {
        console.log('Fetching admin stats...');
        const res = await fetch('/api/admin/stats');
        console.log('Response status:', res.status);

        if (!res.ok) {
            const text = await res.text();
            console.error('Error response:', text);
            tbody.innerHTML = `<tr><td colspan="3">Error: HTTP ${res.status}</td></tr>`;
            return;
        }

        const data = await res.json();
        console.log('Stats data:', data);

        document.getElementById('totalRequests').textContent = data.total_requests || 0;
        document.getElementById('uniqueIps').textContent = data.unique_ips || 0;
        document.getElementById('currentDate').textContent = data.date || 'N/A';

        const lastUpdateEl = document.getElementById('lastUpdate');
        if (lastUpdateEl) {
            lastUpdateEl.textContent = 'Last updated: ' + new Date().toLocaleTimeString();
        }

        if (!data.ip_breakdown || data.ip_breakdown.length === 0) {
            tbody.innerHTML = '<tr><td colspan="3">No requests recorded today. Visit the main dashboard first.</td></tr>';
            return;
        }

        tbody.innerHTML = data.ip_breakdown.map((ip, index) => {
            const endpoints = Object.entries(ip.endpoints)
                .map(([ep, count]) => `<span class="endpoint-item">${ep}: ${count}</span>`)
                .join('');

            const recentRows = (ip.recent_requests || []).map(req => `
                <tr class="detail-row">
                    <td>${req.timestamp}</td>
                    <td colspan="2" style="word-break: break-all; font-family: monospace;">${req.full_path}</td>
                </tr>
            `).join('');

            return `
    <tr class="summary-row" data-index="${index}">
      <td><span class="toggle-icon">▶</span> ${ip.ip}</td>
      <td>${ip.total_requests}</td>
      <td class="endpoint-list">${endpoints}</td>
    </tr>
    <tr id="details-${index}" class="details-container" style="display: none;">
        <td colspan="3" style="padding: 0;">
            <div style="padding: 1rem; background: #252525;">
                <h4 style="color: #f39c12; margin-bottom: 0.5rem;">Last 10 Requests</h4>
                <table style="width: 100%; background: #1e1e1e;">
                    <thead>
                        <tr>
                            <th style="width: 100px;">Time</th>
                            <th>Full Path</th>
                        </tr>
                    </thead>
                    <tbody>
                        ${recentRows || '<tr><td colspan="2">No details recorded yet.</td></tr>'}
                    </tbody>
                </table>
            </div>
        </td>
    </tr>
  `;
        }).join('');

        // Attach event listeners after creating elements to avoid CSP inline-script issues
        document.querySelectorAll('.summary-row').forEach(row => {
            row.addEventListener('click', function () {
                const index = this.getAttribute('data-index');
                const detailsRow = document.getElementById(`details-${index}`);
                const icon = this.querySelector('.toggle-icon');

                if (detailsRow.style.display === 'none') {
                    detailsRow.style.display = 'table-row';
                    icon.textContent = '▼';
                } else {
                    detailsRow.style.display = 'none';
                    icon.textContent = '▶';
                }
            });
        });

    } catch (err) {
        console.error('Error loading stats:', err);
        tbody.innerHTML = `<tr><td colspan="3">Error: ${err.message}</td></tr>`;
    }
}

document.addEventListener('DOMContentLoaded', () => {
    // Load on page load
    loadStats();

    // Attach click handler to refresh button
    const refreshBtn = document.querySelector('.refresh-btn');
    if (refreshBtn) {
        refreshBtn.addEventListener('click', loadStats);
    }

    // Auto-refresh every 30 seconds
    setInterval(loadStats, 30000);
});

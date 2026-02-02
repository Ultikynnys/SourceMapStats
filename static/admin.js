let currentPage = 1;
let currentLimit = 50;
let totalPages = 1;

// HTML escape to prevent XSS in admin panel
function escapeHtml(text) {
    if (!text) return '';
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

// Block an IP via API
async function blockIP(ip, event) {
    event.stopPropagation(); // Don't toggle the row
    if (!confirm(`Block IP ${ip}?`)) return;

    try {
        const res = await fetch('/api/admin/block', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ ip: ip, reason: 'Blocked from admin panel' })
        });
        const data = await res.json();
        if (data.success) {
            alert(`✅ ${data.message}`);
            loadStats(); // Refresh
        } else {
            alert(`❌ ${data.error || data.message}`);
        }
    } catch (err) {
        alert(`Error: ${err.message}`);
    }
}

// Unblock an IP via API
async function unblockIP(ip, event) {
    event.stopPropagation(); // Don't toggle the row
    if (!confirm(`Unblock IP ${ip}?`)) return;

    try {
        const res = await fetch('/api/admin/unblock', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ ip: ip })
        });
        const data = await res.json();
        if (data.success) {
            alert(`✅ ${data.message}`);
            loadStats(); // Refresh
        } else {
            alert(`❌ ${data.error || data.message}`);
        }
    } catch (err) {
        alert(`Error: ${err.message}`);
    }
}

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

        if (!data.ip_breakdown || data.ip_breakdown.length === 0) {
            tbody.innerHTML = '<tr><td colspan="4">No requests found.</td></tr>';
            return;
        }

        // Render Rows
        tbody.innerHTML = data.ip_breakdown.map((ip, index) => {
            const endpoints = Object.entries(ip.endpoints)
                .map(([ep, count]) => `<span class="endpoint-item">${ep}: ${count}</span>`)
                .join('');

            const recentRows = (ip.recent_requests || []).map(req => `
                <tr class="detail-row">
                    <td>${req.timestamp}</td>
                    <td colspan="2" style="word-break: break-all; font-family: monospace;" class="${req.is_threat ? 'threat-path' : ''}">${escapeHtml(req.full_path)}${req.is_threat ? ` <span class="threat-type-tag">${req.threat_type}</span>` : ''}</td>
                </tr>
            `).join('');

            // Determine row class based on threat/blocked status
            let rowClass = 'summary-row';
            if (ip.is_blocked) {
                rowClass += ' blocked-row';
            } else if (ip.threat_detected) {
                rowClass += ' threat-row';
            }

            // Build badges
            let badges = '';
            if (ip.is_blocked) {
                badges += '<span class="blocked-badge">BLOCKED</span>';
            }
            if (ip.threat_detected) {
                badges += `<span class="threat-badge">THREAT (${ip.threat_count})</span>`;
                ip.threat_types.forEach(t => {
                    badges += `<span class="threat-type-tag">${t}</span>`;
                });
            }

            // Block/Unblock button
            let actionBtn = '';
            if (ip.is_blocked) {
                actionBtn = `<button class="unblock-btn" onclick="unblockIP('${ip.ip}', event)">Unblock</button>`;
            } else if (ip.threat_detected) {
                actionBtn = `<button class="block-btn" onclick="blockIP('${ip.ip}', event)">Block</button>`;
            }

            return `
    <tr class="${rowClass}" data-index="${index}">
      <td class="ip-cell">
        <span class="toggle-icon">▶</span> 
        ${ip.ip}
        ${badges}
        ${actionBtn}
      </td>
      <td>${ip.total_requests}</td>
      <td class="endpoint-list">${endpoints}</td>
    </tr>
    <tr id="details-${index}" class="details-container" style="display: none;">
        <td colspan="3" style="padding: 0;">
            <div style="padding: 1rem; background: #252525;">
                <h4 style="color: #f39c12; margin-bottom: 0.5rem;">Recent Requests</h4>
                <table style="width: 100%; background: #1e1e1e;">
                    <thead>
                        <tr>
                            <th style="width: 100px;">Time</th>
                            <th>Full Path</th>
                        </tr>
                    </thead>
                    <tbody>
                        ${recentRows || '<tr><td colspan="2">No details recorded.</td></tr>'}
                    </tbody>
                </table>
            </div>
        </td>
    </tr>
  `;
        }).join('');

        // Attach event listeners
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

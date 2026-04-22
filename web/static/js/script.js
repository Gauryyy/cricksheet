document.addEventListener('DOMContentLoaded', function() {
    init();
});

let distributionChart = null;
let trendsChart = null;
let teamChart = null;

function init() {
    loadFilterOptions();
    
    // Check which page we are on
    if (document.getElementById('kpi-matches')) {
        updateDashboard(); // Initial load for dashboard
        document.getElementById('apply-filters').addEventListener('click', updateDashboard);
    }
    
    if (document.getElementById('metric-users')) {
        loadAdminData();
        document.getElementById('btn-trigger-etl').addEventListener('click', triggerETL);
    }
}

// --- Dashboard Logic ---

function loadFilterOptions() {
    console.log("Loading filter options...");
    fetch('/api/filter_options')
        .then(res => {
            if (!res.ok) throw new Error(`HTTP error! status: ${res.status}`);
            return res.json();
        })
        .then(data => {
            console.log("Filter options loaded:", data);
            const teamSelect = document.getElementById('filter-team');
            const playerSelect = document.getElementById('filter-player');
            const venueSelect = document.getElementById('filter-venue');
            
            if (teamSelect) {
                data.teams.forEach(t => teamSelect.add(new Option(t, t)));
            }
            if (playerSelect) {
                data.players.forEach(p => playerSelect.add(new Option(p, p)));
            }
            if (venueSelect) {
                data.venues.forEach(v => venueSelect.add(new Option(v, v)));
            }
            
            if (document.getElementById('filter-date-start')) {
                document.getElementById('filter-date-start').value = data.date_range.min;
                document.getElementById('filter-date-end').value = data.date_range.max;
            }
        })
        .catch(err => {
            console.error("Failed to load filter options:", err);
        });
}

function getFilters() {
    const filters = {
        team: document.getElementById('filter-team')?.value || 'All',
        player: document.getElementById('filter-player')?.value || 'All',
        venue: document.getElementById('filter-venue')?.value || 'All',
        date_start: document.getElementById('filter-date-start')?.value,
        date_end: document.getElementById('filter-date-end')?.value,
        over_max: document.getElementById('filter-over-max')?.value
    };
    console.log("Current filters:", filters);
    return filters;
}

function updateDashboard() {
    const filters = getFilters();
    console.log("Updating dashboard with filters:", filters);
    
    // Fetch KPIs and Insights
    fetch('/api/dashboard_stats', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(filters)
    })
    .then(res => {
        if (!res.ok) throw new Error(`HTTP error! status: ${res.status}`);
        return res.json();
    })
    .then(data => {
        console.log("Dashboard stats received:", data);
        
        // Helper to safe update text
        const safeSetText = (id, val) => {
            const el = document.getElementById(id);
            if (el) el.innerText = val;
        };

        // Update KPIs
        safeSetText('kpi-matches', data.kpis.total_matches);
        safeSetText('kpi-runs', (data.kpis.total_runs || 0).toLocaleString());
        safeSetText('kpi-wickets', data.kpis.total_wickets);
        safeSetText('kpi-avg', data.kpis.avg_score);
        
        // Update Insights
        safeSetText('insight-wins-team', data.insights.most_wins_team);
        safeSetText('insight-wins-count', data.insights.most_wins_count);
        safeSetText('insight-high-score-team', data.insights.highest_score_team);
        safeSetText('insight-high-score', data.insights.highest_score);
        
        // Update Top Scorers Table
        const scorersBody = document.getElementById('table-top-scorers');
        if (scorersBody) {
            scorersBody.innerHTML = '';
            if (data.top_scorers && data.top_scorers.length > 0) {
                data.top_scorers.forEach(s => {
                    const row = document.createElement('tr');
                    row.className = 'hover:bg-surface-container-low transition-colors';
                    row.innerHTML = `
                        <td class="py-4 font-bold text-primary">${s.batter}</td>
                        <td class="py-4 text-right font-black text-secondary">${s.runs_total}</td>
                    `;
                    scorersBody.appendChild(row);
                });
            } else {
                scorersBody.innerHTML = '<tr><td colspan="2" class="py-4 text-center text-slate-400">No data available</td></tr>';
            }
        }
        
        // Update Match Log
        const matchesBody = document.getElementById('table-matches');
        if (matchesBody) {
            matchesBody.innerHTML = '';
            if (data.recent_matches && data.recent_matches.length > 0) {
                data.recent_matches.forEach(m => {
                    const row = document.createElement('tr');
                    row.className = 'hover:bg-surface-container-low transition-colors';
                    row.innerHTML = `
                        <td class="px-8 py-6">
                            <div class="flex flex-col">
                                <span class="text-sm font-bold">${m.team1} vs ${m.team2}</span>
                            </div>
                        </td>
                        <td class="px-8 py-6 text-sm text-on-surface-variant">${m.venue}</td>
                        <td class="px-8 py-6">
                            <span class="bg-primary/10 text-primary px-3 py-1 rounded-full text-[10px] font-bold">${m.winner} won</span>
                        </td>
                        <td class="px-8 py-6 text-sm text-on-surface-variant">${m.date}</td>
                    `;
                    matchesBody.appendChild(row);
                });
            } else {
                matchesBody.innerHTML = '<tr><td colspan="4" class="px-8 py-10 text-center text-slate-400">No recent matches found</td></tr>';
            }
        }
    })
    .catch(err => {
        console.error("Dashboard stats fetch failed:", err);
    });

    // Update charts
    renderDistributionChart(filters);
    renderTrendsChart(filters);
}

function renderDistributionChart(filters) {
    fetch('/api/charts/runs_distribution', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(filters)
    })
    .then(res => res.json())
    .then(data => {
        const canvas = document.getElementById('chart-distribution');
        if (!canvas) return;
        const ctx = canvas.getContext('2d');
        if (distributionChart) distributionChart.destroy();
        
        distributionChart = new Chart(ctx, {
            type: 'bar',
            data: {
                labels: Object.keys(data),
                datasets: [{
                    label: 'Runs',
                    data: Object.values(data),
                    backgroundColor: ['#16423c', '#2b5bb5', '#83aea6'],
                    borderRadius: 8
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: { legend: { display: false } },
                scales: { y: { beginAtZero: true } }
            }
        });
    })
    .catch(err => console.error("Distribution chart failed:", err));
}

function renderTrendsChart(filters) {
    fetch('/api/charts/player_trends', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(filters)
    })
    .then(res => res.json())
    .then(data => {
        const canvas = document.getElementById('chart-trends');
        if (!canvas) return;
        const ctx = canvas.getContext('2d');
        if (trendsChart) trendsChart.destroy();
        
        trendsChart = new Chart(ctx, {
            type: 'line',
            data: {
                labels: data.dates,
                datasets: [{
                    label: `${data.player} Runs`,
                    data: data.runs,
                    borderColor: '#2b5bb5',
                    backgroundColor: '#2b5bb522',
                    fill: true,
                    tension: 0.4
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: { legend: { position: 'bottom' } },
                scales: { 
                    x: { ticks: { maxRotation: 45, minRotation: 45 } },
                    y: { beginAtZero: true }
                }
            }
        });
    })
    .catch(err => console.error("Trends chart failed:", err));
}

// --- Admin Logic ---

function loadAdminData() {
    console.log("Loading admin data...");
    fetch('/api/admin/metrics')
        .then(res => res.json())
        .then(data => {
            const mUsers = document.getElementById('metric-users');
            if (mUsers) mUsers.innerText = data.total_users.toLocaleString();
            
            const mSessions = document.getElementById('metric-sessions');
            if (mSessions) mSessions.innerText = data.active_sessions.toLocaleString();
            
            const mQueries = document.getElementById('metric-queries');
            if (mQueries) mQueries.innerText = data.queries_24h;
            
            const mSize = document.getElementById('metric-data-size');
            if (mSize) mSize.innerText = data.data_size;
        });

    fetch('/api/admin/users')
        .then(res => res.json())
        .then(users => {
            const tbody = document.getElementById('table-users');
            if (tbody) {
                tbody.innerHTML = '';
                users.forEach(u => {
                    tbody.innerHTML += `
                        <tr class="bg-surface-container-low/40 hover:bg-surface-container-low transition-colors">
                            <td class="px-4 py-4 rounded-l-xl">
                                <div class="flex items-center gap-3">
                                    <div class="w-8 h-8 rounded-full bg-slate-200 flex items-center justify-center font-bold">${u.name[0]}</div>
                                    <div>
                                        <p class="text-sm font-bold">${u.name}</p>
                                        <p class="text-[10px] text-on-surface-variant">${u.email}</p>
                                    </div>
                                </div>
                            </td>
                            <td class="px-4 py-4 text-xs font-medium">${u.role}</td>
                            <td class="px-4 py-4 text-xs">${u.last_active}</td>
                            <td class="px-4 py-4 rounded-r-xl text-right">
                                <button onclick="this.classList.toggle('bg-green-500'); this.classList.toggle('bg-slate-300')" class="w-10 h-5 ${u.active ? 'bg-green-500' : 'bg-slate-300'} rounded-full relative transition-colors">
                                    <div class="absolute inset-y-0.5 left-0.5 w-4 h-4 bg-white rounded-full shadow-sm"></div>
                                </button>
                            </td>
                        </tr>
                    `;
                });
            }
        });
}

function triggerETL() {
    console.log("Triggering ETL pipeline...");
    const overlay = document.getElementById('etl-overlay');
    if (overlay) overlay.classList.remove('hidden');
    
    fetch('/api/admin/upload', { method: 'POST' })
        .then(res => res.json())
        .then(data => {
            if (overlay) overlay.classList.add('hidden');
            if (data.status === 'success') {
                alert('ETL Pipeline successfully re-ingested data!');
                loadAdminData();
            } else {
                alert('ETL Pipeline failed: ' + data.error);
            }
        })
        .catch(err => {
            if (overlay) overlay.classList.add('hidden');
            console.error("ETL trigger failed:", err);
            alert('Error triggering execution');
        });
}
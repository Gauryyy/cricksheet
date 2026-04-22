document.addEventListener('DOMContentLoaded', function() {
    initDashboard();
});

let distributionChart = null;
let trendsChart = null;

function logInteraction(action, metadata = {}) {
    fetch('/api/log_interaction', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ action, metadata })
    }).catch(err => console.warn("Log interaction failed:", err));
}

function initDashboard() {
    loadFilterOptions();
    updateDashboard();

    const applyBtn = document.getElementById('apply-filters');
    if (applyBtn) {
        applyBtn.addEventListener('click', updateDashboard);
    }

    const clearBtn = document.getElementById('clear-filters');
    if (clearBtn) {
        clearBtn.addEventListener('click', clearFilters);
    }

    const teamA = document.getElementById('filter-team');
    const teamB = document.getElementById('filter-team-b');
    if (teamA) teamA.addEventListener('change', () => reloadPlayers());
    if (teamB) teamB.addEventListener('change', () => reloadPlayers());
}

function reloadPlayers() {
    const teamA = document.getElementById('filter-team')?.value || 'All';
    const teamB = document.getElementById('filter-team-b')?.value || 'All';
    const playerSelect = document.getElementById('filter-player');

    fetch(`/api/filter_players?team_a=${encodeURIComponent(teamA)}&team_b=${encodeURIComponent(teamB)}`)
        .then(res => res.json())
        .then(players => {
            if (playerSelect) {
                const currentVal = playerSelect.value;
                playerSelect.innerHTML = '<option value="All">All Players</option>';
                players.forEach(p => playerSelect.add(new Option(p, p)));
                if (players.includes(currentVal)) playerSelect.value = currentVal;
            }
        });
}

function loadFilterOptions() {
    fetch('/api/filter_options')
        .then(res => {
            if (!res.ok) throw new Error(`HTTP error: ${res.status}`);
            return res.json();
        })
        .then(data => {
            const teamSelect = document.getElementById('filter-team');
            const teamSelectB = document.getElementById('filter-team-b');
            const venueSelect = document.getElementById('filter-venue');

            if (teamSelect && teamSelectB) {
                teamSelect.innerHTML = '<option value="All">Primary Team: All</option>';
                teamSelectB.innerHTML = '<option value="All">Vs Team: All (Global View)</option>';
                data.teams.forEach(t => {
                    teamSelect.add(new Option(t, t));
                    teamSelectB.add(new Option(t, t));
                });
            }

            reloadPlayers();

            if (venueSelect) {
                venueSelect.innerHTML = '<option value="All">All Venues</option>';
                data.venues.forEach(v => venueSelect.add(new Option(v, v)));
            }

            if (document.getElementById('filter-date-start')) {
                document.getElementById('filter-date-start').value = data.date_range.min || '';
                document.getElementById('filter-date-end').value = data.date_range.max || '';
            }
        })
        .catch(err => console.error("Failed to load filter options:", err));
}

function getFilters() {
    return {
        team: document.getElementById('filter-team')?.value || 'All',
        team_a: document.getElementById('filter-team')?.value || 'All',
        team_b: document.getElementById('filter-team-b')?.value || 'All',
        player: document.getElementById('filter-player')?.value || 'All',
        venue: document.getElementById('filter-venue')?.value || 'All',
        date_start: document.getElementById('filter-date-start')?.value,
        date_end: document.getElementById('filter-date-end')?.value,
        over_max: document.getElementById('filter-over-max')?.value
    };
}

function updateDashboard() {
    const filters = getFilters();
    updateActiveFiltersDisplay(filters);
    updateCardTitles(filters);
    logInteraction('DASHBOARD_UPDATE', filters);

    fetch('/api/dashboard_stats', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(filters)
    })
        .then(res => {
            if (!res.ok) throw new Error(`HTTP error: ${res.status}`);
            return res.json();
        })
        .then(data => {
            const safeSetText = (id, val, defaultVal = '-') => {
                const el = document.getElementById(id);
                if (el) el.innerText = val !== undefined && val !== null ? val : defaultVal;
            };

            safeSetText('kpi-matches', data.kpis.total_matches);
            safeSetText('kpi-runs', (data.kpis.total_runs || 0).toLocaleString());
            safeSetText('kpi-wickets', data.kpis.total_wickets);
            safeSetText('kpi-avg', data.kpis.avg_score);
            safeSetText('insight-wins-team', data.insights.most_wins_team, 'N/A');
            safeSetText('insight-wins-count', data.insights.most_wins_count, '0');
            safeSetText('insight-fifties', data.insights.fifties, '0');
            safeSetText('insight-centuries', data.insights.centuries, '0');

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
                            <td class="px-8 py-6 text-sm text-on-surface-variant">${m.venue || '-'}</td>
                            <td class="px-8 py-6">
                                <span class="bg-primary/10 text-primary px-3 py-1 rounded-full text-[10px] font-bold">${m.winner || 'Draw/NR'} won</span>
                            </td>
                            <td class="px-8 py-6 text-sm text-on-surface-variant">${m.date || '-'}</td>
                        `;
                        matchesBody.appendChild(row);
                    });
                } else {
                    matchesBody.innerHTML = '<tr><td colspan="4" class="px-8 py-10 text-center text-slate-400">No recent matches found</td></tr>';
                }
            }
        })
        .catch(err => console.error("Dashboard stats fetch failed:", err));

    renderDistributionChart(filters);
    renderTrendsChart(filters);
}

function renderDistributionChart(filters) {
    fetch('/api/charts/runs_distribution', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(filters)
    })
        .then(res => {
            if (!res.ok) throw new Error("Chart distribution API failed");
            return res.json();
        })
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

function clearFilters() {
    if (document.getElementById('filter-team')) document.getElementById('filter-team').value = 'All';
    if (document.getElementById('filter-team-b')) document.getElementById('filter-team-b').value = 'All';
    if (document.getElementById('filter-player')) document.getElementById('filter-player').value = 'All';
    if (document.getElementById('filter-venue')) document.getElementById('filter-venue').value = 'All';
    if (document.getElementById('filter-over-max')) {
        document.getElementById('filter-over-max').value = 50;
        document.getElementById('over-range-val').innerText = '50.0';
    }

    updateDashboard();
    logInteraction('FILTERS_CLEARED');
}

function updateCardTitles(filters) {
    const playerContext = filters.player !== 'All' ? filters.player : '';
    const venueContext = filters.venue !== 'All' ? `at ${filters.venue}` : '';
    let teamContext = 'Total';

    if (filters.team_a !== 'All' && filters.team_b !== 'All') {
        teamContext = `${filters.team_a} vs ${filters.team_b}`;
    } else if (filters.team_a !== 'All') {
        teamContext = filters.team_a;
    }

    const safeSetLabel = (id, text) => {
        const el = document.getElementById(id);
        if (el) el.innerText = text;
    };

    safeSetLabel('label-kpi-matches', `${teamContext} Matches`);
    safeSetLabel('label-kpi-runs', playerContext ? `${playerContext}'s Runs` : `${teamContext} Runs`);
    safeSetLabel('label-kpi-wickets', playerContext ? `${playerContext}'s Wickets` : `${teamContext} Wickets`);
    safeSetLabel('label-kpi-avg', `Avg Score ${venueContext}`.trim());
    safeSetLabel('label-table-scorers', playerContext ? `Stats for ${playerContext}` : `Top 5 Run Scorers ${venueContext}`.trim());
    safeSetLabel('label-chart-dist', playerContext ? `${playerContext}: Runs by Opposition` : `Runs Distribution: ${teamContext}`);
    safeSetLabel('label-chart-trends', playerContext ? `${playerContext} Scoring Trends` : `Team Trends: ${teamContext}`);
    safeSetLabel('label-table-matches', filters.team_b !== 'All' ? `H2H Log: ${filters.team_a} vs ${filters.team_b}` : `Recent Match Log`);
    safeSetLabel('label-insight-secondary', playerContext ? `${playerContext} Milestones` : `Tournament Milestones`);
}

function updateActiveFiltersDisplay(filters) {
    const container = document.getElementById('active-filters-chips');
    if (!container) return;

    container.innerHTML = '';
    const activeItems = [];

    if (filters.team_a && filters.team_a !== 'All' && filters.team_b && filters.team_b !== 'All') {
        activeItems.push({ label: 'H2H', value: `${filters.team_a} vs ${filters.team_b}` });
    } else if (filters.team_a && filters.team_a !== 'All') {
        activeItems.push({ label: 'Team', value: filters.team_a });
    }

    if (filters.venue && filters.venue !== 'All') activeItems.push({ label: 'Venue', value: filters.venue });
    if (filters.player && filters.player !== 'All') activeItems.push({ label: 'Player', value: filters.player });

    activeItems.forEach((item, index) => {
        const chip = document.createElement('div');
        chip.className = 'flex items-center gap-1.5';
        chip.innerHTML = `
            <span class="text-[10px] uppercase tracking-wider text-slate-400 font-bold">${item.label}:</span>
            <span class="text-xs font-bold text-secondary">${item.value}</span>
        `;
        container.appendChild(chip);

        if (index < activeItems.length - 1) {
            const sep = document.createElement('span');
            sep.className = 'material-symbols-outlined text-slate-300 text-sm';
            sep.innerText = 'chevron_right';
            container.appendChild(sep);
        }
    });
}

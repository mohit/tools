const App = {
    state: {
        view: 'dashboard',
        date: new Date(),
        goals: null,
        charts: {}
    },

    init: async function () {
        // Configure Chart.js defaults
        Chart.defaults.font.family = "'Azeret Mono', monospace";
        Chart.defaults.color = '#3D3935';

        this.bindEvents();
        await this.fetchGoals();

        // Restore view from localStorage or default to dashboard
        const savedView = localStorage.getItem('reflector_view') || 'dashboard';
        this.navigate(savedView);
    },

    bindEvents: function () {
        document.querySelectorAll('.nav-item').forEach(btn => {
            btn.addEventListener('click', (e) => {
                const view = e.target.dataset.view;
                if (view) this.navigate(view);
            });
        });

        const moreToggle = document.getElementById('more-menu-toggle');
        const moreContainer = document.getElementById('more-menu-container');
        const moreMenu = document.getElementById('more-menu');

        if (moreToggle && moreContainer && moreMenu) {
            moreToggle.addEventListener('click', (e) => {
                e.stopPropagation();
                const isOpen = moreContainer.classList.toggle('is-open');
                moreToggle.setAttribute('aria-expanded', String(isOpen));
            });

            document.addEventListener('click', (e) => {
                if (!moreContainer.contains(e.target)) {
                    this.closeMoreMenu();
                }
            });

            moreMenu.querySelectorAll('[data-view]').forEach(btn => {
                btn.addEventListener('click', () => this.closeMoreMenu());
            });
        }
    },

    fetchGoals: async function () {
        try {
            const res = await fetch('/api/goals');
            this.state.goals = await res.json();
        } catch (e) {
            console.error('Failed to fetch goals', e);
        }
    },

    getWeeklyTarget: function (metric) {
        if (!this.state.goals || !this.state.goals[metric]) return 0;
        const goal = this.state.goals[metric];
        if (goal.period === 'daily') return goal.target * 7;
        return goal.target;
    },

    navigate: function (view) {
        this.state.view = view;
        localStorage.setItem('reflector_view', view);

        document.querySelectorAll('.nav-item').forEach(b => b.classList.remove('active'));
        const activeBtn = document.querySelector(`.nav-item[data-view="${view}"]`);
        if (activeBtn) activeBtn.classList.add('active');

        const content = document.getElementById('main-content');

        // Cleanup charts
        Object.values(this.state.charts).forEach(c => c.destroy());
        this.state.charts = {};

        if (view === 'dashboard') this.loadDashboard();
        else if (view === 'analyze') this.loadAnalyze();
        else if (view === 'goals') this.loadGoals();
        else if (view === 'insights') this.loadInsights();
        else if (view === 'patterns') this.loadPatterns();
        else if (view === 'calendar') this.loadCalendar();
        else this.loadDashboard();

        this.closeMoreMenu();
    },
    closeMoreMenu: function () {
        const moreToggle = document.getElementById('more-menu-toggle');
        const moreContainer = document.getElementById('more-menu-container');
        if (moreContainer) {
            moreContainer.classList.remove('is-open');
        }
        if (moreToggle) {
            moreToggle.setAttribute('aria-expanded', 'false');
        }
    },

    loadDashboard: async function () {
        const container = document.getElementById('main-content');
        container.innerHTML = '<div class="loading">Loading dashboard...</div>';

        try {
            const dateStr = this.state.date.toISOString().split('T')[0];

            // Fetch Summary (Monthly)
            const summaryRes = await fetch(`/api/summary?period=month&date=${dateStr}`);
            const summaryData = await summaryRes.json();

            if (summaryData.error) throw new Error(summaryData.error);

            // Fetch Daily Data for Current Week (Monday - Sunday)
            const now = new Date(this.state.date);
            const day = now.getDay(); // 0 (Sun) - 6 (Sat)
            const diff = now.getDate() - day + (day === 0 ? -6 : 1); // Adjust to get Monday
            const weekStart = new Date(now);
            weekStart.setDate(diff);
            const weekEnd = new Date(weekStart);
            weekEnd.setDate(weekStart.getDate() + 6);

            const startStr = weekStart.toISOString().split('T')[0];
            const endStr = weekEnd.toISOString().split('T')[0];

            const dailyRes = await fetch(`/api/daily/${startStr}/${endStr}`);
            const dailyData = await dailyRes.json();

            // Fetch Insights for Dashboard
            const insightRes = await fetch(`/api/insights/${dateStr.split('-')[0]}/${dateStr.split('-')[1]}`);
            const insightData = await insightRes.json();

            // Merge insights into summaryData
            summaryData.insights = insightData;

            this.renderDashboard(summaryData, dailyData, container);
        } catch (e) {
            container.innerHTML = `<div class="insight-alert warning">Error loading dashboard: ${e.message}</div>`;
        }
    },

    renderDashboard: function (data, dailyData, container) {
        const current = data.current.stats;
        const prev = data.previous.stats;

        const stats = {
            steps: current.total_steps || 0,
            exercise: current.total_exercise_minutes || 0,
            sleep: current.avg_sleep_hours || 0, // We display avg sleep for the month
            hrv: current.avg_hrv || 0
        };

        // Monthly Goals = Daily Target * 30
        const getMonthlyTarget = (metric) => {
            if (!this.state.goals || !this.state.goals[metric]) return 0;
            const goal = this.state.goals[metric];
            return goal.target * 30; // Approximation for monthly goal
        };

        const sleepGoal = this.state.goals && this.state.goals.sleep_hours ? this.state.goals.sleep_hours.target : 7.5;
        const stepsGoal = getMonthlyTarget('steps') || 300000;
        const exerciseGoal = getMonthlyTarget('exercise_minutes') || 900;
        const hrvGoal = this.state.goals && this.state.goals.hrv ? this.state.goals.hrv.target : 50;

        // --- Fetch Insights Content (Highlights/Recs) --- 
        // We do this async inside render, which isn't ideal but fits current structure.
        // Better would be to fetch in loadDashboard and pass it in. 
        // For now, we'll placeholder it or let the loadDashboard handle it.
        // Actually, let's update loadDashboard to fetch insights too.

        const calendarDays = dailyData.metrics.map(d => ({
            date: d.date,
            sleepHours: d.sleep_hours ? d.sleep_hours.toFixed(1) : '-',
            intensity: d.steps > 15000 ? 4 : (d.steps > 10000 ? 3 : (d.steps > 5000 ? 2 : (d.steps > 0 ? 1 : 0))),
            hasWorkout: dailyData.workouts.some(w => w.start_time.startsWith(d.date))
        }));

        const html = `
            <div class="dashboard-grid">
                <!-- Hero Section -->
                <div class="section-full dashboard-hero">
                    <h1>Health Pulse</h1>
                    <p>${data.current.start_date} — ${data.current.end_date}</p>
                </div>

                <!-- Compact Metrics Squircles -->
                <div class="section-full">
                    ${Reflector.Components.MetricSquircles({
            metrics: [
                {
                    label: 'Steps',
                    value: Math.round(stats.steps),
                    metric: 'steps',
                    change: this.calcDelta(current.total_steps, prev.total_steps),
                    isPositiveGood: true
                },
                {
                    label: 'Exercise',
                    value: Math.round(stats.exercise),
                    metric: 'exercise',
                    change: this.calcDelta(current.total_exercise_minutes, prev.total_exercise_minutes),
                    isPositiveGood: true
                },
                {
                    label: 'Avg Sleep',
                    value: stats.sleep.toFixed(1) + 'h',
                    metric: 'sleep',
                    change: this.calcDelta(current.avg_sleep_hours, prev.avg_sleep_hours),
                    isPositiveGood: true
                },
                {
                    label: 'HRV',
                    value: Math.round(stats.hrv),
                    metric: 'hrv',
                    change: this.calcDelta(current.avg_hrv, prev.avg_hrv),
                    isPositiveGood: true
                }
            ]
        })}
                </div>
                
                <!-- Week Calendar -->
                <div class="section-half">
                    ${Reflector.Components.WeekCalendar({ days: calendarDays })}
                </div>

                <!-- Progress Rings -->
                <div class="section-half">
                    <div class="card" style="height: 100%;">
                        <div class="card-header"><h3 class="card-title">Monthly Goals</h3></div>
                        <div style="display: grid; grid-template-columns: repeat(2, 1fr); gap: var(--space-sm);">
                            ${Reflector.Components.GoalRing({
            label: 'Steps',
            current: stats.steps,
            target: stepsGoal,
            unit: '',
            color: 'var(--color-movement)'
        })}
                            ${Reflector.Components.GoalRing({
            label: 'Exercise',
            current: stats.exercise,
            target: exerciseGoal,
            unit: 'min',
            color: 'var(--color-heart)'
        })}
                            ${Reflector.Components.GoalRing({
            label: 'Avg Sleep',
            current: stats.sleep,
            target: sleepGoal,
            unit: 'h',
            color: 'var(--color-sleep)'
        })}
                            ${Reflector.Components.GoalRing({
            label: 'HRV',
            current: stats.hrv,
            target: hrvGoal,
            unit: 'ms',
            color: 'var(--color-recovery)'
        })}
                        </div>
                    </div>
                </div>

                <!-- Highlights & Lowlights (moved from Insights) -->
                <div class="section-full">
                     <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); gap: var(--space-sm);">
                        <div class="card">
                            <div class="card-header"><h3 class="card-title">Highlights</h3></div>
                            ${data.insights?.highlights?.length ? data.insights.highlights.map(h => `
                                <div class="insight-alert success">
                                    <strong style="font-size: 0.9rem;">${h.title}</strong>
                                    <div style="margin-top: 4px; color: var(--text-secondary); font-size: 0.8rem;">${h.description}</div>
                                </div>
                            `).join('') : '<p style="color:var(--text-muted); font-size: 0.85rem;">No major highlights yet.</p>'}
                        </div>
                        <div class="card">
                            <div class="card-header"><h3 class="card-title">Areas for Focus</h3></div>
                            ${data.insights?.lowlights?.length ? data.insights.lowlights.map(h => `
                                <div class="insight-alert warning">
                                    <strong style="font-size: 0.9rem;">${h.title}</strong>
                                    <div style="margin-top: 4px; color: var(--text-secondary); font-size: 0.8rem;">${h.description}</div>
                                </div>
                            `).join('') : '<p style="color:var(--text-muted); font-size: 0.85rem;">No major issues detected.</p>'}
                        </div>
                     </div>
                </div>

                <!-- Recommendations -->
                <div class="section-full">
                     <div class="card">
                        <div class="card-header"><h3 class="card-title">Recommendations</h3></div>
                        ${data.insights?.recommendations?.length ? data.insights.recommendations.map(h => `
                            <div class="insight-alert info">
                                <strong style="font-size: 0.9rem;">${h.title}</strong>
                                <div style="margin-top: 4px; color: var(--text-secondary); font-size: 0.8rem;">${h.description}</div>
                            </div>
                        `).join('') : '<p style="color:var(--text-muted); font-size: 0.85rem;">Keep up the good work!</p>'}
                     </div>
                </div>
            </div>
        `;

        container.innerHTML = html;
    },

    calcDelta: function (curr, prev) {
        if (!prev) return 0;
        return Math.round(((curr - prev) / prev) * 100);
    },

    loadAnalyze: async function () {
        const container = document.getElementById('main-content');
        container.innerHTML = `
            <div class="dashboard-grid">
                <div class="section-full">
                    <h2 style="font-size: 1.5rem; margin-bottom: 8px;">Monthly Analysis</h2>
                    <canvas id="trendChart" style="background: white; border-radius: 8px; padding: 12px; box-shadow: var(--shadow-sm); width: 100%; height: 220px;"></canvas>
                </div>
                
                 <div class="section-full" id="correlations-container">
                    <h3 style="font-size: 1.1rem; margin-bottom: 6px;">Correlations</h3>
                    <div class="loading">Loading correlations...</div>
                 </div>

                 <!-- Detected Patterns (Moved from Insights) -->
                 <div class="section-full" id="patterns-container">
                    <!-- Loaded dynamically -->
                 </div>
            </div>
        `;

        // Load 30 day data
        const end = new Date();
        const start = new Date();
        start.setDate(end.getDate() - 30);

        try {
            const res = await fetch(`/api/daily/${start.toISOString().split('T')[0]}/${end.toISOString().split('T')[0]}`);
            const data = await res.json();

            // Render Chart
            const ctx = document.getElementById('trendChart').getContext('2d');
            this.state.charts.trends = new Chart(ctx, {
                type: 'line',
                data: {
                    labels: data.metrics.map(d => d.date.slice(5)), // MM-DD
                    datasets: [
                        {
                            label: 'Steps',
                            data: data.metrics.map(d => d.steps),
                            borderColor: '#3B82F6',
                            yAxisID: 'y',
                            tension: 0.4
                        },
                        {
                            label: 'Sleep (hrs)',
                            data: data.metrics.map(d => d.sleep_hours),
                            borderColor: '#8B5CF6',
                            yAxisID: 'y1',
                            tension: 0.4
                        }
                    ]
                },
                options: {
                    responsive: true,
                    interaction: {
                        mode: 'index',
                        intersect: false,
                    },
                    scales: {
                        y: {
                            type: 'linear',
                            display: true,
                            position: 'left',
                            grid: { display: false }
                        },
                        y1: {
                            type: 'linear',
                            display: true,
                            position: 'right',
                            grid: { display: false }
                        },
                        x: {
                            grid: { display: false }
                        }
                    }
                }
            });

            // Load correlations
            const corrRes = await fetch(`/api/correlations/${start.toISOString().split('T')[0]}/${end.toISOString().split('T')[0]}`);
            const corrData = await corrRes.json();
            const corrContainer = document.getElementById('correlations-container');

            // Load patterns for Analyze view
            const patRes = await fetch(`/api/patterns/${start.toISOString().split('T')[0]}/${end.toISOString().split('T')[0]}`);
            const patData = await patRes.json();
            const patContainer = document.getElementById('patterns-container');

            if (patContainer) {
                // Reuse the pattern render logic from loadPatterns or simplify it here
                // Helper to render individual list items based on pattern type
                const renderPatternItem = (item) => {
                    if (item.type && item.count !== undefined) {
                        return `<li><strong>${item.type}</strong>: ${item.count} sessions</li>`;
                    }
                    if (item.length_days !== undefined) {
                        return `<li><strong>${item.length_days} days</strong>: ${item.start_date} → ${item.end_date}</li>`;
                    }
                    if (item.date) {
                        const details = item.issues ? item.issues.join(', ') : (item.notes || '');
                        if (details) {
                            return `<li><strong>${item.date}</strong>: ${details}</li>`;
                        }
                        return `<li><strong>${item.date}</strong></li>`;
                    }
                    return '';
                };

                const renderList = (title, list) => list && list.length ? `
                    <div class="card" style="margin-bottom: 20px;">
                        <div class="card-header"><h3 class="card-title">${title}</h3></div>
                        <ul class="pattern-list">
                            ${list.map(i => renderPatternItem(i)).join('')}
                        </ul>
                    </div>
                 ` : '';

                patContainer.innerHTML = `
                    <h3>Detected Patterns of Habit</h3>
                    <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 20px;">
                        ${renderList('Good Days', patData.good_days)}
                        ${renderList('Days to Watch', patData.bad_days)}
                        ${renderList('Step Streaks', patData.step_streaks)}
                        ${renderList('Workout Patterns', patData.workouts ? patData.workouts.workout_types : [])}
                    </div>
                 `;
            }

            corrContainer.innerHTML = Reflector.Components.CorrelationGrid({ correlations: corrData });
        } catch (e) {
            console.error(e);
        }
    },

    loadGoals: function () {
        const container = document.getElementById('main-content');
        const g = this.state.goals;

        if (!g) {
            container.innerHTML = "Loading goals...";
            return;
        }

        const renderSlider = (key, label, min, max, step) => {
            const val = g[key].target;
            return `
                <div class="card" style="margin-bottom: var(--space-md)">
                    <div style="display:flex; justify-content:space-between; margin-bottom:10px;">
                        <h3 class="card-title">${label}</h3>
                        <span id="val-${key}" style="font-weight:600">${val}</span>
                    </div>
                    <input type="range" min="${min}" max="${max}" step="${step}" value="${val}" 
                           style="width:100%"
                           oninput="document.getElementById('val-${key}').innerText = this.value"
                           onchange="App.updateGoal('${key}', this.value)">
                    <p style="font-size:0.8em; color:var(--text-secondary); margin-top:5px;">Daily Target</p>
                </div>
            `;
        };

        container.innerHTML = `
            <div style="max-width: 600px; margin: 0 auto;">
                <h2 style="margin-bottom: 20px;">Edit Goals</h2>
                ${renderSlider('steps', 'Daily Steps', 1000, 30000, 500)}
                ${renderSlider('exercise_minutes', 'Daily Exercise (min)', 10, 120, 5)}
                ${renderSlider('sleep_hours', 'Sleep Duration (hrs)', 5, 10, 0.5)}
                ${renderSlider('resting_hr', 'Resting Heart Rate (bpm)', 40, 100, 1)}
            </div>
        `;
    },

    loadInsights: async function () {
        const container = document.getElementById('main-content');
        container.innerHTML = '<div class="loading">Loading insights...</div>';

        try {
            const year = this.state.date.getFullYear();
            const month = this.state.date.getMonth() + 1;
            const res = await fetch(`/api/monthly/${year}/${month}`);
            const data = await res.json();

            const renderSection = (title, items, tone) => {
                if (!items || items.length === 0) {
                    return `
                        <div class="card">
                            <div class="card-header"><h3 class="card-title">${title}</h3></div>
                            <p style="color: var(--text-muted);">No insights yet for this period.</p>
                        </div>
                    `;
                }

                return `
                    <div class="card">
                        <div class="card-header"><h3 class="card-title">${title}</h3></div>
                        ${items.map(item => `
                            <div class="insight-alert ${tone}">
                                <strong>${item.title}</strong>
                                <div style="margin-top: 6px; color: var(--text-secondary);">${item.description}</div>
                            </div>
                        `).join('')}
                    </div>
                `;
            };

            container.innerHTML = `
                <div class="dashboard-grid">
                    <div class="section-full dashboard-hero">
                        <h1>Insights</h1>
                        <p>${data.year}-${String(data.month).padStart(2, '0')}</p>
                    </div>
                    <div class="section-full">
                        ${renderSection('Highlights', data.insights.highlights, 'success')}
                    </div>
                    <div class="section-full">
                        ${renderSection('Lowlights', data.insights.lowlights, 'warning')}
                    </div>
                    <div class="section-full">
                        ${renderSection('Patterns', data.insights.patterns, 'info')}
                    </div>
                    <div class="section-full">
                        ${renderSection('Recommendations', data.insights.recommendations, 'info')}
                    </div>
                </div>
            `;
        } catch (e) {
            container.innerHTML = `<div class="insight-alert warning">Error loading insights: ${e.message}</div>`;
        }
    },

    loadPatterns: async function () {
        const container = document.getElementById('main-content');
        container.innerHTML = '<div class="loading">Loading patterns...</div>';

        try {
            const end = new Date();
            const start = new Date();
            start.setDate(end.getDate() - 30);
            const startDate = start.toISOString().split('T')[0];
            const endDate = end.toISOString().split('T')[0];

            const res = await fetch(`/api/patterns/${startDate}/${endDate}`);
            const data = await res.json();

            const renderDayList = (title, days) => `
                <div class="card">
                    <div class="card-header"><h3 class="card-title">${title}</h3></div>
                    ${days.length === 0 ? '<p style="color: var(--text-muted);">No days found.</p>' : `
                        <ul class="pattern-list">
                            ${days.map(day => `
                                <li>
                                    <strong>${day.date}</strong>
                                    <span>${day.notes || (day.issues ? day.issues.join(', ') : '')}</span>
                                </li>
                            `).join('')}
                        </ul>
                    `}
                </div>
            `;

            const renderStreaks = (streaks) => `
                <div class="card">
                    <div class="card-header"><h3 class="card-title">Step Streaks</h3></div>
                    ${streaks.length === 0 ? '<p style="color: var(--text-muted);">No streaks detected.</p>' : `
                        <ul class="pattern-list">
                            ${streaks.map(streak => `
                                <li>
                                    <strong>${streak.length_days} days</strong>
                                    <span>${streak.start_date} → ${streak.end_date}</span>
                                </li>
                            `).join('')}
                        </ul>
                    `}
                </div>
            `;

            const renderWorkoutPatterns = (workouts) => `
                <div class="card">
                    <div class="card-header"><h3 class="card-title">Workout Patterns</h3></div>
                    ${workouts.workout_types.length === 0 ? '<p style="color: var(--text-muted);">No workout patterns detected.</p>' : `
                        <ul class="pattern-list">
                            ${workouts.workout_types.map(item => `
                                <li>
                                    <strong>${item.type}</strong>
                                    <span>${item.count} sessions</span>
                                </li>
                            `).join('')}
                        </ul>
                    `}
                </div>
            `;

            container.innerHTML = `
                <div class="dashboard-grid">
                    <div class="section-full dashboard-hero">
                        <h1>Patterns</h1>
                        <p>Last 30 days</p>
                    </div>
                    <div class="section-half">
                        ${renderDayList('Good Days', data.good_days || [])}
                    </div>
                    <div class="section-half">
                        ${renderDayList('Bad Days', data.bad_days || [])}
                    </div>
                    <div class="section-half">
                        ${renderStreaks(data.step_streaks || [])}
                    </div>
                    <div class="section-half">
                        ${renderWorkoutPatterns(data.workouts || { workout_types: [] })}
                    </div>
                </div>
            `;
        } catch (e) {
            container.innerHTML = `<div class="insight-alert warning">Error loading patterns: ${e.message}</div>`;
        }
    },

    loadCalendar: async function () {
        const container = document.getElementById('main-content');
        container.innerHTML = '<div class="loading">Loading calendar...</div>';

        try {
            const year = this.state.date.getFullYear();
            const monthIndex = this.state.date.getMonth();
            const startDate = new Date(year, monthIndex, 1);
            const endDate = new Date(year, monthIndex + 1, 0);
            const start = startDate.toISOString().split('T')[0];
            const end = endDate.toISOString().split('T')[0];

            const res = await fetch(`/api/daily/${start}/${end}`);
            const data = await res.json();

            container.innerHTML = `
                <div class="dashboard-grid">
                    <div class="section-full dashboard-hero">
                        <h1>Calendar</h1>
                        <p>${year}-${String(monthIndex + 1).padStart(2, '0')}</p>
                    </div>
                    <div class="section-full">
                        <div class="card">
                            <div class="card-header"><h3 class="card-title">Daily Summary</h3></div>
                            <div class="calendar-list">
                                ${data.metrics.map(day => `
                                    <div class="calendar-row">
                                        <div class="calendar-date">${day.date}</div>
                                        <div class="calendar-metric">
                                            <span>${day.steps?.toLocaleString() || 0} steps</span>
                                            <span>${day.sleep_hours ? day.sleep_hours.toFixed(1) : '-'}h sleep</span>
                                            <span>${day.exercise_minutes || 0} min exercise</span>
                                        </div>
                                    </div>
                                `).join('')}
                            </div>
                        </div>
                    </div>
                </div>
            `;
        } catch (e) {
            container.innerHTML = `<div class="insight-alert warning">Error loading calendar: ${e.message}</div>`;
        }
    },

    updateGoal: async function (metric, value) {
        try {
            const res = await fetch('/api/goals', {
                method: 'PUT',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ metric, target: value })
            });
            this.state.goals = await res.json();
        } catch (e) {
            console.error('Error updating goal', e);
        }
    }
};

document.addEventListener('DOMContentLoaded', () => {
    App.init();
});

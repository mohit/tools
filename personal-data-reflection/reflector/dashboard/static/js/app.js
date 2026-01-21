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
    },

    loadDashboard: async function () {
        const container = document.getElementById('main-content');
        container.innerHTML = '<div class="loading">Loading dashboard...</div>';

        try {
            const dateStr = this.state.date.toISOString().split('T')[0];

            // Fetch Summary
            const summaryRes = await fetch(`/api/summary?period=week&date=${dateStr}`);
            const summaryData = await summaryRes.json();

            if (summaryData.error) throw new Error(summaryData.error);

            // Fetch Daily Data for Calendar
            const start = summaryData.current.start_date;
            const end = summaryData.current.end_date;
            const dailyRes = await fetch(`/api/daily/${start}/${end}`);
            const dailyData = await dailyRes.json();

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
            sleep: current.avg_sleep_hours || 0, // We display avg sleep for the week
            hrv: current.avg_hrv || 0
        };

        // Sleep goal is usually daily, so we compare Avg Weekly Sleep to Daily Goal
        const sleepGoal = this.state.goals && this.state.goals.sleep_hours ? this.state.goals.sleep_hours.target : 7.5;

        // Steps/Exercise are cumulative totals
        const stepsGoal = this.getWeeklyTarget('steps') || 70000;
        const exerciseGoal = this.getWeeklyTarget('exercise_minutes') || 210;
        const hrvGoal = this.state.goals && this.state.goals.hrv ? this.state.goals.hrv.target : 50;

        // Prepare Calendar Data
        // Map daily metrics to calendar day format
        // Ensure we have 7 days even if data is missing? The WeekCalendar assumes we pass what we have.
        // But for visual consistency we might want to map to Mon-Sun.
        // For now, let's just pass the data we have.
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
                    change: this.calcDelta(current.total_steps, prev.total_steps)
                },
                {
                    label: 'Exercise',
                    value: Math.round(stats.exercise),
                    metric: 'exercise',
                    change: this.calcDelta(current.total_exercise_minutes, prev.total_exercise_minutes)
                },
                {
                    label: 'Avg Sleep',
                    value: stats.sleep.toFixed(1) + 'h',
                    metric: 'sleep',
                    change: this.calcDelta(current.avg_sleep_hours, prev.avg_sleep_hours)
                },
                {
                    label: 'HRV',
                    value: Math.round(stats.hrv),
                    metric: 'hrv',
                    change: this.calcDelta(current.avg_hrv, prev.avg_hrv)
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
                        <div class="card-header"><h3 class="card-title">Weekly Goals</h3></div>
                        <div style="display: grid; grid-template-columns: repeat(2, 1fr); gap: var(--space-md);">
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
                    <h2>Monthly Analysis</h2>
                    <canvas id="trendChart" style="background: white; border-radius: 12px; padding: 20px; box-shadow: var(--shadow-sm); width: 100%; height: 300px; margin-top: 20px;"></canvas>
                </div>
                 <div class="section-full" id="correlations-container">
                    <h3>Correlations</h3>
                    <div class="loading">Loading correlations...</div>
                 </div>
            </div>
        `;

        // Load 30 day data
        const end = new Date();
        const start = new Date();
        start.setDate(end.getDate() - 30);

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

        // Use the new CorrelationGrid component - no wrapper card
        corrContainer.innerHTML = `
            <h2 style="margin-bottom: var(--space-md); color: var(--color-brand); font-family: var(--font-header); font-size: 2.5rem; font-weight: 700;">Discovered Patterns</h2>
            ${Reflector.Components.CorrelationGrid({ correlations: corrData })}
        `;
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

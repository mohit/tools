const App = {
    state: {
        view: 'dashboard',
        date: new Date(),
        goals: null,
        charts: {}
    },

    /**
     * Format a date string as a relative time (e.g., "Today", "Yesterday", "3 days ago")
     * with the day name for context.
     */
    formatRelativeDate: function (dateStr) {
        const date = new Date(dateStr + 'T00:00:00'); // Parse as local date
        const today = new Date();
        today.setHours(0, 0, 0, 0);

        const diffTime = today - date;
        const diffDays = Math.round(diffTime / (1000 * 60 * 60 * 24));

        const dayNames = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];
        const dayName = dayNames[date.getDay()];

        // Format the month and day
        const monthNames = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
        const monthDay = `${monthNames[date.getMonth()]} ${date.getDate()}`;

        let relative;
        if (diffDays === 0) {
            relative = 'Today';
        } else if (diffDays === 1) {
            relative = 'Yesterday';
        } else if (diffDays < 7) {
            relative = `${diffDays} days ago`;
        } else if (diffDays < 14) {
            relative = '1 week ago';
        } else if (diffDays < 30) {
            const weeks = Math.floor(diffDays / 7);
            relative = `${weeks} weeks ago`;
        } else {
            relative = monthDay;
        }

        return { relative, dayName, monthDay, diffDays };
    },

    /**
     * Format a date range (e.g., for streaks) as relative with context
     */
    formatDateRange: function (startDateStr, endDateStr) {
        const start = this.formatRelativeDate(startDateStr);
        const end = this.formatRelativeDate(endDateStr);

        // If the end date is recent, show relative
        if (end.diffDays <= 7) {
            if (end.diffDays === 0) {
                return `${start.monthDay} → Today`;
            }
            return `${start.monthDay} → ${end.relative}`;
        }

        return `${start.monthDay} → ${end.monthDay}`;
    },

    init: async function () {
        // Configure Chart.js defaults
        Chart.defaults.font.family = "'Azeret Mono', monospace";
        Chart.defaults.color = '#3D3935';

        this.bindEvents();
        this.updateDateDisplay();
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

        // Date navigator
        const datePrev = document.getElementById('date-prev');
        const dateNext = document.getElementById('date-next');

        if (datePrev) {
            datePrev.addEventListener('click', () => this.navigateDate(-1));
        }
        if (dateNext) {
            dateNext.addEventListener('click', () => this.navigateDate(1));
        }
    },

    /**
     * Navigate the date by a given number of months
     */
    navigateDate: function (delta) {
        const current = this.state.date;
        const newDate = new Date(current.getFullYear(), current.getMonth() + delta, 1);
        this.state.date = newDate;
        this.updateDateDisplay();

        // Reload the current view with the new date
        this.navigate(this.state.view);
    },

    /**
     * Update the date display in the header
     */
    updateDateDisplay: function () {
        const display = document.getElementById('date-range-display');
        const nextBtn = document.getElementById('date-next');

        if (display) {
            const monthNames = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
            const month = monthNames[this.state.date.getMonth()];
            const year = this.state.date.getFullYear();
            display.textContent = `${month} ${year}`;
        }

        // Disable next button if we're at the current month
        if (nextBtn) {
            const now = new Date();
            const isCurrentMonth = this.state.date.getFullYear() === now.getFullYear()
                && this.state.date.getMonth() === now.getMonth();
            nextBtn.disabled = isCurrentMonth;
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

            // Fetch the entire month's daily data
            const selectedYear = this.state.date.getFullYear();
            const selectedMonth = this.state.date.getMonth();
            const today = new Date();
            const isCurrentMonth = selectedYear === today.getFullYear() && selectedMonth === today.getMonth();

            // Month date range
            const monthStart = new Date(selectedYear, selectedMonth, 1);
            const monthEnd = isCurrentMonth ? today : new Date(selectedYear, selectedMonth + 1, 0);

            const startStr = monthStart.toISOString().split('T')[0];
            const endStr = monthEnd.toISOString().split('T')[0];

            const dailyRes = await fetch(`/api/daily/${startStr}/${endStr}`);
            const dailyData = await dailyRes.json();

            // Fetch Insights for Dashboard
            const insightRes = await fetch(`/api/insights/${dateStr.split('-')[0]}/${dateStr.split('-')[1]}`);
            const insightData = await insightRes.json();

            // Merge insights into summaryData
            summaryData.insights = insightData;

            // Calculate the "focus week" (most recent week)
            // For current month: the week containing today
            // For past months: the last complete week of the month
            let focusWeekStart, focusWeekEnd;

            if (isCurrentMonth) {
                const dayOfWeek = today.getDay();
                const daysToSubtract = dayOfWeek === 0 ? 6 : dayOfWeek - 1;
                focusWeekStart = new Date(today);
                focusWeekStart.setDate(today.getDate() - daysToSubtract);
                focusWeekEnd = new Date(today);
            } else {
                // Find the last Sunday of the month
                const lastDay = new Date(selectedYear, selectedMonth + 1, 0);
                const dayOfWeek = lastDay.getDay();
                let lastSunday = new Date(lastDay);
                if (dayOfWeek !== 0) {
                    lastSunday.setDate(lastDay.getDate() - dayOfWeek);
                }
                focusWeekStart = new Date(lastSunday);
                focusWeekStart.setDate(lastSunday.getDate() - 6);
                focusWeekEnd = new Date(lastSunday);
            }

            // Pass month and focus week info for display
            summaryData.monthDateRange = {
                start: monthStart,
                end: monthEnd,
                startStr: startStr,
                endStr: endStr
            };
            summaryData.focusWeek = {
                start: focusWeekStart,
                end: focusWeekEnd,
                startStr: focusWeekStart.toISOString().split('T')[0],
                endStr: focusWeekEnd.toISOString().split('T')[0]
            };

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

        const periodStart = data.monthDateRange?.startStr || null;
        const periodEnd = data.monthDateRange?.endStr || null;

        // Goal totals should match the selected summary period length.
        const getMonthlyTarget = (metric) => {
            if (!this.state.goals || !this.state.goals[metric]) return 0;
            const goal = this.state.goals[metric];
            const math = window.Reflector?.DashboardMath;
            if (!math || typeof math.getGoalTargetForPeriod !== 'function') {
                return goal.target * 30;
            }
            return math.getGoalTargetForPeriod(goal.target, periodStart, periodEnd, 30);
        };

        const sleepGoal = this.state.goals && this.state.goals.sleep_hours ? this.state.goals.sleep_hours.target : 7.5;
        const stepsGoal = getMonthlyTarget('steps') || 300000;
        const exerciseGoal = getMonthlyTarget('exercise_minutes') || 900;
        const hrvGoal = this.state.goals && this.state.goals.hrv ? this.state.goals.hrv.target : 50;

        // Format the focus week date range for display
        const monthNames = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
        let focusWeekStr = '';
        if (data.focusWeek) {
            const start = data.focusWeek.start;
            const end = data.focusWeek.end;
            const startMonth = monthNames[start.getMonth()];
            const endMonth = monthNames[end.getMonth()];

            if (startMonth === endMonth) {
                focusWeekStr = `${startMonth} ${start.getDate()} - ${end.getDate()}`;
            } else {
                focusWeekStr = `${startMonth} ${start.getDate()} - ${endMonth} ${end.getDate()}`;
            }
        }

        // Prepare calendar days with focus week highlighting
        const focusStartStr = data.focusWeek?.startStr || '';
        const focusEndStr = data.focusWeek?.endStr || '';

        const calendarDays = dailyData.metrics.map(d => ({
            date: d.date,
            steps: d.steps || 0,
            sleepHours: d.sleep_hours ? d.sleep_hours.toFixed(1) : '-',
            exerciseMinutes: d.exercise_minutes || 0,
            intensity: d.steps > 15000 ? 4 : (d.steps > 10000 ? 3 : (d.steps > 5000 ? 2 : (d.steps > 0 ? 1 : 0))),
            hasWorkout: dailyData.workouts.some(w => w.start_time.startsWith(d.date)),
            isFocusWeek: d.date >= focusStartStr && d.date <= focusEndStr
        })).reverse(); // Reverse to show most recent first

        const html = `
            <div class="dashboard-grid">
                <!-- Left Column: Recommendations, Focus, Calendar -->
                <div class="section-half" style="display: flex; flex-direction: column; gap: var(--space-md);">
                    
                    <!-- Recommendations -->
                    ${data.insights?.recommendations?.length ? `
                    <div class="card">
                        <div class="card-header"><h3 class="card-title">Recommendations</h3></div>
                        ${data.insights.recommendations.map(h => `
                            <div class="insight-alert info">
                                <strong style="font-size: 0.9rem;">${h.title}</strong>
                                <div style="margin-top: 4px; color: var(--text-secondary); font-size: 0.8rem;">${h.description}</div>
                            </div>
                        `).join('')}
                    </div>` : ''}

                    <!-- Areas for Focus -->
                    <div class="card">
                        <div class="card-header"><h3 class="card-title">Areas for Focus</h3></div>
                        ${data.insights?.lowlights?.length ? data.insights.lowlights.map(h => `
                            <div class="insight-alert warning">
                                <strong style="font-size: 0.9rem;">${h.title}</strong>
                                <div style="margin-top: 4px; color: var(--text-secondary); font-size: 0.8rem;">${h.description}</div>
                            </div>
                        `).join('') : '<p style="color:var(--text-muted); font-size: 0.85rem;">No major issues detected.</p>'}
                    </div>

                    <!-- Calendar -->
                    <div style="flex: 1;">
                        ${Reflector.Components.MonthCalendar({
            days: calendarDays,
            focusWeekLabel: focusWeekStr
        })}
                    </div>
                </div>

                <!-- Right Column: Metrics, Goals -->
                <div class="section-half" style="display: flex; flex-direction: column; gap: var(--space-md); justify-content: space-between;">
                    
                    <!-- Top Level Metrics (Squircles) -->
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

                    <!-- Monthly Goals -->
                    <div class="card">
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

                <!-- Bottom Full Width: Highlights -->
                <div class="section-full">
                    <div class="card">
                        <div class="card-header"><h3 class="card-title">Highlights</h3></div>
                        <div style="display: grid; grid-template-columns: repeat(2, 1fr); gap: var(--space-sm);">
                            ${data.insights?.highlights?.length ? data.insights.highlights.map(h => `
                                <div class="insight-alert success" style="height: 100%;">
                                    <strong style="font-size: 0.9rem;">${h.title}</strong>
                                    <div style="margin-top: 4px; color: var(--text-secondary); font-size: 0.8rem;">${h.description}</div>
                                </div>
                            `).join('') : '<p style="color:var(--text-muted); font-size: 0.85rem; grid-column: span 2;">No major highlights yet.</p>'}
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

        // Use the selected month's date range
        const selectedYear = this.state.date.getFullYear();
        const selectedMonth = this.state.date.getMonth();
        const start = new Date(selectedYear, selectedMonth, 1);
        const end = new Date(selectedYear, selectedMonth + 1, 0); // Last day of month

        // If viewing current month, cap end date to today
        const today = new Date();
        if (selectedYear === today.getFullYear() && selectedMonth === today.getMonth()) {
            end.setTime(today.getTime());
        }

        const monthNames = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
        const monthLabel = `${monthNames[selectedMonth]} ${selectedYear}`;

        container.innerHTML = `
            <div class="dashboard-grid">
                <div class="section-full">
                    <h2 style="font-size: 1.5rem; margin-bottom: 8px;">Monthly Analysis</h2>
                    <p style="color: var(--text-muted); font-size: 0.9rem; margin-bottom: 12px;">${monthLabel}</p>
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
                // Helper to render individual list items based on pattern type
                const renderPatternItem = (item) => {
                    if (item.type && item.count !== undefined) {
                        return `<li><strong>${item.type}</strong><span>${item.count} sessions</span></li>`;
                    }
                    if (item.length_days !== undefined) {
                        const dateRange = this.formatDateRange(item.start_date, item.end_date);
                        return `<li><strong>${item.length_days} days</strong><span>${dateRange}</span></li>`;
                    }
                    if (item.date) {
                        const formatted = this.formatRelativeDate(item.date);
                        const dateDisplay = `<span class="pattern-date"><span class="relative">${formatted.relative}</span><span class="day-context">${formatted.dayName}, ${formatted.monthDay}</span></span>`;
                        const details = item.issues ? item.issues.join(', ') : (item.notes || '');
                        if (details) {
                            return `<li>${dateDisplay}<span class="pattern-details">${details}</span></li>`;
                        }
                        return `<li>${dateDisplay}</li>`;
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
            // Use the selected month's date range
            const selectedYear = this.state.date.getFullYear();
            const selectedMonth = this.state.date.getMonth();
            const start = new Date(selectedYear, selectedMonth, 1);
            const end = new Date(selectedYear, selectedMonth + 1, 0); // Last day of month

            // If viewing current month, cap end date to today
            const today = new Date();
            if (selectedYear === today.getFullYear() && selectedMonth === today.getMonth()) {
                end.setTime(today.getTime());
            }

            const startDate = start.toISOString().split('T')[0];
            const endDate = end.toISOString().split('T')[0];

            const monthNames = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'];
            const monthLabel = `${monthNames[selectedMonth]} ${selectedYear}`;

            const res = await fetch(`/api/patterns/${startDate}/${endDate}`);
            const data = await res.json();

            const renderDayList = (title, days) => `
                <div class="card">
                    <div class="card-header"><h3 class="card-title">${title}</h3></div>
                    ${days.length === 0 ? '<p style="color: var(--text-muted);">No days found.</p>' : `
                        <ul class="pattern-list">
                            ${days.map(day => {
                const formatted = this.formatRelativeDate(day.date);
                const dateDisplay = `<span class="pattern-date"><span class="relative">${formatted.relative}</span><span class="day-context">${formatted.dayName}, ${formatted.monthDay}</span></span>`;
                const details = day.notes || (day.issues ? day.issues.join(', ') : '');
                return `<li>${dateDisplay}${details ? `<span class="pattern-details">${details}</span>` : ''}</li>`;
            }).join('')}
                        </ul>
                    `}
                </div>
            `;

            const renderStreaks = (streaks) => `
                <div class="card">
                    <div class="card-header"><h3 class="card-title">Step Streaks</h3></div>
                    ${streaks.length === 0 ? '<p style="color: var(--text-muted);">No streaks detected.</p>' : `
                        <ul class="pattern-list">
                            ${streaks.map(streak => {
                const dateRange = this.formatDateRange(streak.start_date, streak.end_date);
                return `<li><strong>${streak.length_days} days</strong><span>${dateRange}</span></li>`;
            }).join('')}
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
                        <p>${monthLabel}</p>
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

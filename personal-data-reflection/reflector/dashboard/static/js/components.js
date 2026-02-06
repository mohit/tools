window.Reflector = window.Reflector || {};
window.Reflector.Components = {

    /**
     * Renders a Circular Goal Progress Ring
     * @param {Object} props
     * @param {string} props.label - e.g. "Steps"
     * @param {number} props.current - Current value
     * @param {number} props.target - Target value
     * @param {string} props.unit - e.g. "steps"
     * @param {string} props.color - CSS color string
     * @param {string} props.icon - SVG icon string
     */
    /**
     * Renders a Circular Goal Progress Ring with premium styling
     */
    GoalRing: function ({ label, current, target, unit, color, icon }) {
        const percent = Math.min(100, Math.max(0, (current / target) * 100));
        const radius = 32;
        const circumference = 2 * Math.PI * radius;
        const offset = circumference - (percent / 100) * circumference;

        return `
            <div class="correlation-squircle" style="align-items: center; justify-content: center; min-height: 140px; border-width: 3px; padding: 12px;">
                 <div style="position: relative; width: 80px; height: 80px; margin-bottom: 8px;">
                    <svg width="80" height="80" viewBox="0 0 80 80">
                        <!-- Background Circle -->
                        <circle cx="40" cy="40" r="${radius}" 
                            fill="none" 
                            stroke="rgba(0,0,0,0.08)" 
                            stroke-width="7" />
                        <!-- Progress Circle -->
                        <circle cx="40" cy="40" r="${radius}" 
                            fill="none" 
                            stroke="${color}" 
                            stroke-width="7" 
                            stroke-dasharray="${circumference}" 
                            stroke-dashoffset="${offset}"
                            stroke-linecap="round"
                            transform="rotate(-90 40 40)" />
                    </svg>
                    <div style="position: absolute; top: 50%; left: 50%; transform: translate(-50%, -50%); font-size: 1rem; font-weight: 700; color: var(--color-ink);">
                        ${Math.round(percent)}%
                    </div>
                </div>
                <h3 style="font-size: 0.95rem; font-weight: 700; color: var(--color-ink); margin-bottom: 2px;">${label}</h3>
                <div style="font-size: 0.75rem; color: var(--color-ink-subdued); font-weight: 500;">
                    ${current.toLocaleString()} / ${target.toLocaleString()} ${unit}
                </div>
            </div>
        `;
    },

    /**
     * Renders the Week Calendar Heatmap (legacy, kept for other views)
     * @param {Object} props
     * @param {Array} props.days - Array of day objects { date, score, intensity, hasWorkout, sleepHours }
     * @param {string} props.title - Optional title (default: "Recent Week")
     * @param {string} props.dateRange - Optional date range string (e.g., "Jan 13 - 19")
     */
    WeekCalendar: function ({ days, title, dateRange }) {
        const dayHeaders = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'];
        const displayTitle = title || 'Recent Week';

        const renderHeader = () => dayHeaders.map(d => `<div class="day-header">${d}</div>`).join('');

        const renderDays = () => days.map(day => {
            // Safe parsing of YYYY-MM-DD to get day number without timezone offset issues
            const dayNum = parseInt(day.date.split('-')[2], 10);
            const intensityClass = `intensity-${day.intensity || 0}`; // Ensure intensity calculation logic maps to 0-4

            // Map intensity directly to opacity for now as per CSS
            const opacityClass = day.intensity > 3 ? 'high' : (day.intensity > 1 ? 'med' : 'low');

            return `
                <div class="day-column">
                    <div class="day-cell ${opacityClass}" style="background-color: var(--color-movement);">
                        ${dayNum}
                        ${day.hasWorkout ? '<div class="dot-indicator"></div>' : ''}
                    </div>
                    <div style="font-size:0.7em; color:var(--text-muted);">${day.sleepHours}h</div>
                </div>
            `;
        }).join('');

        return `
            <div class="card" style="height: 100%;">
                <div class="card-header">
                   <h3 class="card-title">${displayTitle}</h3>
                   ${dateRange ? `<span style="font-size: 0.8rem; color: var(--text-muted);">${dateRange}</span>` : ''}
                </div>
                <div class="week-calendar">
                    ${renderHeader()}
                    ${renderDays()}
                </div>
            </div>
        `;
    },

    /**
     * Renders a Month Calendar grid view with focus week highlighted
     * @param {Object} props
     * @param {Array} props.days - Array of day objects with data
     * @param {string} props.focusWeekLabel - Label for the focus week (e.g., "Jan 13 - 19")
     */
    MonthCalendar: function ({ days, focusWeekLabel }) {
        const dayHeaders = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'];

        // Sort days ascending by date for calendar layout
        const sortedDays = [...days].sort((a, b) => a.date.localeCompare(b.date));

        // Build a map for quick lookup
        const dayMap = {};
        sortedDays.forEach(d => { dayMap[d.date] = d; });

        // Get the month/year from first day
        if (sortedDays.length === 0) {
            return `<div class="card"><p style="color: var(--text-muted);">No data available</p></div>`;
        }

        const firstDate = new Date(sortedDays[0].date + 'T00:00:00');
        const year = firstDate.getFullYear();
        const month = firstDate.getMonth();

        // First day of month and its weekday (0=Sun, convert to Mon=0)
        const firstOfMonth = new Date(year, month, 1);
        let startWeekday = firstOfMonth.getDay(); // 0=Sun
        startWeekday = startWeekday === 0 ? 6 : startWeekday - 1; // Convert to Mon=0

        // Last day of month
        const lastOfMonth = new Date(year, month + 1, 0);
        const daysInMonth = lastOfMonth.getDate();

        // Build calendar grid (6 weeks max)
        const weeks = [];
        let currentDay = 1;

        for (let week = 0; week < 6; week++) {
            const weekDays = [];
            let hasAnyDay = false;

            for (let dow = 0; dow < 7; dow++) {
                if ((week === 0 && dow < startWeekday) || currentDay > daysInMonth) {
                    // Empty cell
                    weekDays.push(null);
                } else {
                    const dateStr = `${year}-${String(month + 1).padStart(2, '0')}-${String(currentDay).padStart(2, '0')}`;
                    const dayData = dayMap[dateStr] || null;
                    weekDays.push({
                        day: currentDay,
                        date: dateStr,
                        data: dayData
                    });
                    hasAnyDay = true;
                    currentDay++;
                }
            }

            if (hasAnyDay) {
                weeks.push(weekDays);
            }
        }

        const renderCell = (cell) => {
            if (!cell) {
                return `<div class="cal-cell empty"></div>`;
            }

            const d = cell.data;
            const isFocus = d?.isFocusWeek || false;
            const hasData = d !== null;

            // Intensity for background
            const intensity = d?.intensity || 0;
            const intensityClass = intensity > 3 ? 'high' : (intensity > 1 ? 'med' : 'low');

            return `
                <div class="cal-cell ${isFocus ? 'focus' : ''} ${hasData ? 'has-data' : 'no-data'}">
                    <div class="cal-day-num">${cell.day}</div>
                    ${hasData ? `
                        <div class="cal-intensity ${intensityClass}"></div>
                        <div class="cal-sleep">${d.sleepHours}h</div>
                        ${d.hasWorkout ? '<div class="cal-workout-dot"></div>' : ''}
                        <div class="cal-popover">
                            <div class="popover-item"><strong>Steps:</strong> ${d.steps.toLocaleString()}</div>
                            <div class="popover-item"><strong>Exercise:</strong> ${d.exerciseMinutes}m</div>
                            <div class="popover-item"><strong>Sleep:</strong> ${d.sleepHours}h</div>
                        </div>
                    ` : ''}
                </div>
            `;
        };

        return `
            <div class="card month-calendar-card">
                <div class="card-header">
                   <h3 class="card-title">Daily Activity</h3>
                   ${focusWeekLabel ? `<span class="focus-week-label">Focus: ${focusWeekLabel}</span>` : ''}
                </div>
                <div class="cal-grid">
                    <div class="cal-header">
                        ${dayHeaders.map(d => `<div class="cal-header-cell">${d}</div>`).join('')}
                    </div>
                    ${weeks.map(week => `
                        <div class="cal-week">
                            ${week.map(cell => renderCell(cell)).join('')}
                        </div>
                    `).join('')}
                </div>
            </div>
        `;
    },

    /**
     * Renders a Comparison Card
     * @param {Object} props
     * @param {string} props.title - e.g. "vs Previous Month"
     * @param {Array} props.metrics - [{ label, delta, isPositiveGood }]
     */
    ComparisonCard: function ({ title, metrics }) {
        const renderRow = (m) => {
            const isUp = m.delta > 0;
            const isNeutral = m.delta === 0;
            const cssClass = isNeutral ? 'delta-neutral' : (
                (m.isPositiveGood && isUp) || (!m.isPositiveGood && !isUp) ? 'delta-up' : 'delta-down'
            );
            const symbol = isNeutral ? '→' : (isUp ? '↑' : '↓');

            return `
                <div class="metric-row">
                    <span class="metric-name">${m.label}</span>
                    <span class="metric-delta ${cssClass}">
                        ${symbol} ${Math.abs(m.delta)}%
                    </span>
                </div>
            `;
        };

        return `
            <div class="card comparison-card">
                 <div class="card-header">
                   <h3 class="card-title">${title}</h3> 
                </div>
                ${metrics.map(renderRow).join('')}
            </div>
        `;
    },

    /**
     * Renders a Highlight Alert
     */
    Highlight: function ({ text, type }) {
        return `
            <div class="insight-alert ${type}">
                ${text}
            </div>
        `;
    },

    /**
     * Renders compact metric squircles for key stats with PREMIUM styling
     */
    MetricSquircles: function ({ metrics }) {
        // We'll treat these as a grid of distinct cards now
        const renderSquircle = (m, index) => {
            const isGood = m.change > 0 ? (m.isPositiveGood !== false) : (m.change < 0 ? (m.isPositiveGood === false) : true);
            const isNeutral = m.change === 0;
            const statusClass = isNeutral ? 'neutral' : (isGood ? 'positive' : 'negative');
            const changeSymbol = m.change > 0 ? '↑' : (m.change < 0 ? '↓' : '→');

            // Map metrics to themes
            let theme = 'navy';
            if (m.metric == 'steps') theme = 'blue-grey';
            if (m.metric == 'exercise') theme = 'orange';
            if (m.metric == 'sleep') theme = 'red'; // as per correlations

            const getExplanation = (metric, change, isGood) => {
                if (change === 0) return 'Holding steady.';
                if (metric === 'steps') return isGood ? 'More movement than last month.' : 'Walking less than usual.';
                if (metric === 'exercise') return isGood ? 'Exercise frequency is up!' : 'Training has slowed down.';
                if (metric === 'sleep') return isGood ? 'You are sleeping more.' : 'Rest is down significantly.';
                if (metric === 'hrv') return isGood ? 'Recovery is improving.' : 'System strain is higher.';
                return isGood ? 'Trending positively.' : 'Needs attention.';
            };

            const explanation = getExplanation(m.metric, m.change, isGood);

            return `
                <div class="correlation-squircle" data-theme="${theme}" style="min-height: 140px; padding: 14px;">
                    <div style="font-size: 0.8rem; font-weight: 700; color: var(--color-ink-subdued); text-transform: uppercase; letter-spacing: 0.04em; margin-bottom: 4px;">
                        ${m.label}
                    </div>
                    
                    <div style="font-size: 2rem; font-weight: 800; color: var(--color-ink); line-height: 1.1; margin-bottom: 8px;">
                        ${typeof m.value === 'number' ? m.value.toLocaleString() : m.value}
                    </div>
                    
                    ${m.change !== undefined ? `
                        <div style="margin-top: auto; display: flex; flex-direction: column; gap: 4px;">
                            <div style="display: flex; align-items: center; gap: 6px; font-size: 0.85rem; font-weight: 600;">
                                <span class="metric-change ${statusClass}" style="padding: 2px 8px; border-radius: 12px; margin: 0;">
                                    ${changeSymbol} ${Math.abs(m.change)}%
                                </span>
                                <span style="color: var(--color-ink-faint); font-size: 0.75rem; font-weight: 400;">vs prev month</span>
                            </div>
                            <div style="font-size: 0.75rem; color: var(--color-ink-subdued); font-weight: 500; line-height: 1.2;">
                                ${explanation}
                            </div>
                        </div>
                    ` : ''}
                </div>
            `;
        };

        return `
            <div class="correlations-grid" style="margin-top: 0;">
                ${metrics.map((m, i) => renderSquircle(m, i)).join('')}
            </div>
        `;
    },

    /**
     * Renders compact correlation squircles in a grid with premium styling
     */
    CorrelationGrid: function ({ correlations }) {
        if (!correlations || correlations.length === 0) {
            return '<p style="color: var(--text-muted); text-align: center;">No strong correlations found</p>';
        }

        // Sort by absolute correlation value (strongest first)
        const sorted = [...correlations].sort((a, b) =>
            Math.abs(b.correlation) - Math.abs(a.correlation)
        );

        const themes = ['red', 'orange', 'peach', 'navy', 'blue-grey', 'pale-blue'];

        // Human-readable metric display names
        const metricDisplayNames = {
            'sleep_hours': 'Sleep',
            'steps': 'Steps',
            'active_energy_kcal': 'Calories Burned',
            'resting_heart_rate': 'Resting HR',
            'hrv_sdnn': 'Heart Rate Variability',
            'exercise_minutes': 'Exercise',
            'distance_km': 'Distance',
            'walking_hr': 'Walking HR',
            'body_mass_kg': 'Weight'
        };

        const renderSquircle = (corr, index) => {
            const coefficient = (corr.correlation > 0 ? '' : '') + corr.correlation.toFixed(2);

            // Cycle through themes
            const theme = themes[index % themes.length];
            const isFeatured = index === 6; // Featured card (Meditation Minutes in mockup)

            // Get friendly metric names with fallback to formatted field name
            const formatName = (name) => {
                if (metricDisplayNames[name]) {
                    return metricDisplayNames[name];
                }
                // Fallback: convert snake_case to Title Case
                return name.replace(/_/g, ' ')
                    .split(' ')
                    .map(w => w.charAt(0).toUpperCase() + w.slice(1))
                    .join(' ');
            };

            const nameA = formatName(corr.metric_a);
            const nameB = formatName(corr.metric_b);

            // Arrow direction SVG
            const isPositive = corr.correlation > 0;
            const arrowSvg = isPositive ?
                `<svg class="correlation-arrow-svg" viewBox="0 0 24 24"><path d="M5 12h14M13 5l7 7-7 7" stroke="currentColor"/></svg>` :
                `<svg class="correlation-arrow-svg" viewBox="0 0 24 24" style="transform: rotate(45deg)"><path d="M19 5L5 19M5 9v10h10" stroke="currentColor"/></svg>`;

            return `
                <div class="correlation-squircle ${isFeatured ? 'featured' : ''}" data-theme="${theme}">
                    <div class="correlation-coefficient">${coefficient}</div>
                    <div class="correlation-metrics-container" style="${isFeatured ? 'width: 50%;' : ''}">
                        <div class="correlation-metric-top">${nameA}</div>
                        <div class="correlation-arrow" style="color: var(--corr-${theme})">
                            ${arrowSvg}
                        </div>
                        <div class="correlation-metric-bottom">${nameB}</div>
                    </div>
                    <div class="correlation-description" style="${isFeatured ? 'width: 50%;' : ''}">${corr.description || ''}</div>
                </div>
            `;
        };

        return `
            <div class="correlations-grid">
                ${sorted.map((c, i) => renderSquircle(c, i)).join('')}
            </div>
        `;
    }
};

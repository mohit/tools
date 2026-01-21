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
    GoalRing: function ({ label, current, target, unit, color, icon }) {
        const percent = Math.min(100, Math.max(0, (current / target) * 100));
        const radius = 45;
        const circumference = 2 * Math.PI * radius;
        const offset = circumference - (percent / 100) * circumference;

        return `
            <div class="card goal-card">
                <div class="ring-container">
                    <svg width="120" height="120" viewBox="0 0 120 120">
                        <!-- Background Circle -->
                        <circle cx="60" cy="60" r="${radius}" 
                            fill="none" 
                            stroke="rgba(0,0,0,0.08)" 
                            stroke-width="10" />
                        <!-- Progress Circle -->
                        <circle cx="60" cy="60" r="${radius}" 
                            fill="none" 
                            stroke="${color}" 
                            stroke-width="10" 
                            stroke-dasharray="${circumference}" 
                            stroke-dashoffset="${offset}"
                            stroke-linecap="round"
                            transform="rotate(-90 60 60)" />
                    </svg>
                    <div class="ring-value" style="font-size: 1.75rem;">
                        ${Math.round(percent)}<span style="font-size:0.5em; opacity:0.7">%</span>
                    </div>
                </div>
                <h3 class="card-title">${label}</h3>
                <div class="goal-stats">
                    <span>${current.toLocaleString()}</span>
                    <span>/</span>
                    <span>${target.toLocaleString()} ${unit}</span>
                </div>
            </div>
        `;
    },

    /**
     * Renders the Week Calendar Heatmap
     * @param {Object} props
     * @param {Array} props.days - Array of day objects { date, score, intensity, hasWorkout, sleepHours }
     */
    WeekCalendar: function ({ days }) {
        const dayHeaders = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'];

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
                   <h3 class="card-title">This Week</h3> 
                </div>
                <div class="week-calendar">
                    ${renderHeader()}
                    ${renderDays()}
                </div>
            </div>
        `;
    },

    /**
     * Renders a Comparison Card
     * @param {Object} props
     * @param {string} props.title - e.g. "vs Last Week"
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
     * Renders compact metric squircles for key stats
     * @param {Object} props
     * @param {Array} props.metrics - Array of { label, value, unit, change, metric }
     */
    MetricSquircles: function ({ metrics }) {
        const renderSquircle = (m) => {
            const changeClass = m.change > 0 ? 'positive' : (m.change < 0 ? 'negative' : '');
            const changeSymbol = m.change > 0 ? '↑' : (m.change < 0 ? '↓' : '→');

            return `
                <div class="metric-squircle" data-metric="${m.metric}">
                    <div class="metric-value">${typeof m.value === 'number' ? m.value.toLocaleString() : m.value}</div>
                    <div class="metric-label">${m.label}</div>
                    ${m.change !== undefined ? `
                        <div class="metric-change ${changeClass}">
                            ${changeSymbol} ${Math.abs(m.change)}%
                        </div>
                    ` : ''}
                </div>
            `;
        };

        return `
            <div class="metrics-squircles">
                ${metrics.map(renderSquircle).join('')}
            </div>
        `;
    },

    /**
     * Renders compact correlation squircles in a grid
     * @param {Object} props
     * @param {Array} props.correlations - Array of correlation objects
     * Each correlation: { metric_a, metric_b, correlation, description, strength }
     */
    CorrelationGrid: function ({ correlations }) {
        if (!correlations || correlations.length === 0) {
            return '<p style="color: var(--text-muted); text-align: center;">No strong correlations found</p>';
        }

        // Sort by absolute correlation value (strongest first)
        const sorted = [...correlations].sort((a, b) =>
            Math.abs(b.correlation) - Math.abs(a.correlation)
        );

        const renderSquircle = (corr) => {
            const absCorr = Math.abs(corr.correlation);
            const coefficient = corr.correlation.toFixed(2);

            // Determine strength for styling
            const strength = absCorr >= 0.7 ? 'strong' : (absCorr >= 0.5 ? 'moderate' : 'weak');

            // Short metric names
            const nameA = corr.metric_a.replace('_', ' ').split(' ')
                .map(w => w.charAt(0).toUpperCase() + w.slice(1)).join(' ');
            const nameB = corr.metric_b.replace('_', ' ').split(' ')
                .map(w => w.charAt(0).toUpperCase() + w.slice(1)).join(' ');

            // Determine arrow direction
            const arrow = corr.correlation > 0 ? '→' : '⤓';

            // Determine arrow color matching the primary metric
            let arrowColor = 'var(--color-brand)';
            if (corr.metric_a.includes('sleep')) arrowColor = 'var(--color-sleep)';
            else if (corr.metric_a.includes('step') || corr.metric_a.includes('movement') || corr.metric_a.includes('run')) arrowColor = 'var(--color-movement)';
            else if (corr.metric_a.includes('heart') || corr.metric_a.includes('hrv')) arrowColor = 'var(--color-recovery)';
            else if (corr.metric_a.includes('exercise') || corr.metric_a.includes('active') || corr.metric_a.includes('energy')) arrowColor = 'var(--color-heart)';

            return `
                <div class="correlation-squircle" data-strength="${strength}">
                    <div class="correlation-coefficient">${coefficient}</div>
                    <div class="correlation-metrics">${nameA}</div>
                    <div class="correlation-arrow" style="color: ${arrowColor}">${arrow}</div>
                    <div class="correlation-metrics">${nameB}</div>
                    <div class="correlation-description">${corr.description || ''}</div>
                </div>
            `;
        };

        return `
            <div class="correlations-grid">
                ${sorted.map(renderSquircle).join('')}
            </div>
        `;
    }
};

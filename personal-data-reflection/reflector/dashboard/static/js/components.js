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
                            stroke="rgba(0,0,0,0.1)" 
                            stroke-width="8" />
                        <!-- Progress Circle -->
                        <circle cx="60" cy="60" r="${radius}" 
                            fill="none" 
                            stroke="${color}" 
                            stroke-width="8" 
                            stroke-dasharray="${circumference}" 
                            stroke-dashoffset="${offset}"
                            stroke-linecap="round"
                            transform="rotate(-90 60 60)" />
                    </svg>
                    <div class="ring-value">
                        ${Math.round(percent)}<span style="font-size:0.6em">%</span>
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
            <div class="card">
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
    }
};

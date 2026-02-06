(function () {
    function parseDate(value) {
        if (value instanceof Date) return value;
        if (typeof value === 'string') return new Date(value + 'T00:00:00');
        return null;
    }

    function getPeriodDayCount(startDate, endDate) {
        const start = parseDate(startDate);
        const end = parseDate(endDate);
        if (!start || !end || Number.isNaN(start.getTime()) || Number.isNaN(end.getTime()) || end < start) {
            return 0;
        }

        const msPerDay = 24 * 60 * 60 * 1000;
        return Math.floor((end - start) / msPerDay) + 1;
    }

    function getGoalTargetForPeriod(dailyTarget, startDate, endDate, fallbackDays) {
        const target = Number(dailyTarget);
        if (!Number.isFinite(target) || target <= 0) {
            return 0;
        }

        const periodDays = getPeriodDayCount(startDate, endDate);
        const days = periodDays > 0 ? periodDays : (fallbackDays || 30);
        return target * days;
    }

    const api = {
        getPeriodDayCount,
        getGoalTargetForPeriod
    };

    if (typeof module !== 'undefined' && module.exports) {
        module.exports = api;
    }

    if (typeof window !== 'undefined') {
        window.Reflector = window.Reflector || {};
        window.Reflector.DashboardMath = api;
    }
})();

const test = require('node:test');
const assert = require('node:assert/strict');

const {
    getPeriodDayCount,
    getGoalTargetForPeriod
} = require('../reflector/dashboard/static/js/dashboard-math.js');

test('getPeriodDayCount counts both start and end date', () => {
    assert.equal(getPeriodDayCount('2026-02-01', '2026-02-28'), 28);
    assert.equal(getPeriodDayCount('2026-01-01', '2026-01-31'), 31);
});

test('getGoalTargetForPeriod scales daily target by period length', () => {
    assert.equal(getGoalTargetForPeriod(10000, '2026-02-01', '2026-02-15', 30), 150000);
    assert.equal(getGoalTargetForPeriod(30, '2026-01-01', '2026-01-31', 30), 930);
});

test('getGoalTargetForPeriod uses fallback when dates are invalid', () => {
    assert.equal(getGoalTargetForPeriod(100, null, null, 30), 3000);
});

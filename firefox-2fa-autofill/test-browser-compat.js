const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

function loadBackground(promiseAPI, failRead = false) {
  let onMessage;
  const writes = [];
  const sent = [];
  const errors = [];
  const api = {
    runtime: { onMessage: { addListener(fn) { onMessage = fn; } } },
    tabs: {
      onRemoved: { addListener() {} },
      sendMessage(tab, message, callback) {
        sent.push({ tab, message });
        if (callback) callback();
        else return Promise.resolve();
      },
    },
    storage: { local: {
      get(key, callback) {
        const value = { recentCodes: [{ code: '111111', timestamp: Date.now(), source: 'test' }] };
        if (callback) {
          if (failRead) api.runtime.lastError = { message: 'Unavailable' };
          callback(value);
          delete api.runtime.lastError;
        } else return failRead ? Promise.reject(new Error('Unavailable')) : Promise.resolve(value);
      },
      set(value, callback) {
        writes.push(value);
        if (callback) callback();
        else return Promise.resolve();
      },
    } },
  };
  const context = {
    [promiseAPI ? 'browser' : 'chrome']: api,
    console: { log() {}, error(...args) { errors.push(args); } },
    setInterval() {},
  };
  vm.runInNewContext(fs.readFileSync(path.join(__dirname, 'background.js'), 'utf8'), context);
  return { onMessage: (...args) => onMessage(...args), writes, sent, errors };
}

for (const promiseAPI of [false, true]) {
  test(`${promiseAPI ? 'Promise' : 'callback'} API restores, persists and sends codes`, async () => {
    const app = loadBackground(promiseAPI);
    await new Promise(setImmediate);
    let response;
    app.onMessage({ type: 'GET_ALL_CODES' }, {}, value => { response = value; });
    assert.equal(response.codes[0].code, '111111');
    // Empty stored state to exercise pending-tab delivery.
    const blank = loadBackground(promiseAPI, true);
    await new Promise(setImmediate);
    blank.onMessage({ type: 'REQUEST_CODE' }, { tab: { id: 12 } }, () => {});
    blank.onMessage({ type: 'NEW_CODE', code: '222222', timestamp: Date.now() }, {}, () => {});
    await new Promise(setImmediate);
    assert.equal(blank.writes[0].recentCodes[0].code, '222222');
    assert.equal(blank.sent[0].tab, 12);
    assert.equal(blank.sent[0].message.code, '222222');
    assert.equal(blank.errors.length, 1);
  });
}

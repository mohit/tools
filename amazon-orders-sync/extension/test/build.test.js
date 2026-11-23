const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");

const ROOT = path.join(__dirname, "..");

test("manifests reference only files that exist, in dependency order", async () => {
  const { buildManifest, BACKGROUND_SCRIPTS, CONTENT_SCRIPTS } = await import(path.join(ROOT, "scripts", "build.mjs"));
  for (const f of [...BACKGROUND_SCRIPTS, ...CONTENT_SCRIPTS]) {
    assert.ok(fs.existsSync(path.join(ROOT, "src", f)), `${f} missing`);
  }
  for (const list of [BACKGROUND_SCRIPTS, CONTENT_SCRIPTS]) {
    assert.equal(list[0], "shared/browser.js");
    assert.ok(list.indexOf("shared/util.js") < list.indexOf("shared/constants.js"));
  }
  const chrome = buildManifest("chrome");
  const safari = buildManifest("safari");
  assert.equal(chrome.background.service_worker, "background/sw.js");
  assert.equal(chrome.background.scripts, undefined);
  assert.deepEqual(safari.background.scripts, BACKGROUND_SCRIPTS);
  assert.equal(safari.background.persistent, false);
  assert.equal(safari.background.service_worker, undefined);
  // Everything except background and browser_specific_settings is identical.
  const strip = ({ background, browser_specific_settings, minimum_chrome_version, ...rest }) => rest;
  assert.deepEqual(strip(chrome), strip(safari));
  assert.deepEqual(chrome.content_scripts[0].js, CONTENT_SCRIPTS);
  for (const page of [chrome.action.default_popup, chrome.options_ui.page]) {
    assert.ok(fs.existsSync(path.join(ROOT, "src", page)), `${page} missing`);
  }
});

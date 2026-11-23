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
  // safari-web-extension-converter flags these as unsupported in Safari MV3.
  assert.equal("persistent" in safari.background, false);
  assert.equal("open_in_tab" in safari.options_ui, false);
  assert.equal(chrome.options_ui.open_in_tab, true);
  assert.equal(safari.background.service_worker, undefined);
  // Everything except background, options_ui flags and browser_specific_settings is identical.
  const strip = ({ background, browser_specific_settings, minimum_chrome_version, options_ui, ...rest }) => rest;
  assert.deepEqual(strip(chrome), strip(safari));
  assert.equal(chrome.options_ui.page, safari.options_ui.page);
  assert.deepEqual(chrome.content_scripts[0].js, CONTENT_SCRIPTS);
  for (const page of [chrome.action.default_popup, chrome.options_ui.page]) {
    assert.ok(fs.existsSync(path.join(ROOT, "src", page)), `${page} missing`);
  }
});

test("safari-xcode puts the extension bundle id under the app's", async () => {
  const { fixBundleIds, BUNDLE_ID } = await import(path.join(ROOT, "scripts", "safari-xcode.mjs"));
  // What safari-web-extension-converter in Xcode 26/27 emits for --app-name "Amazon Orders Sync".
  const pbx = [
    'PRODUCT_BUNDLE_IDENTIFIER = "com.mohit.tools.amazon-orders-sync.Extension";',
    'PRODUCT_BUNDLE_IDENTIFIER = "com.mohit.tools.Amazon-Orders-Sync";',
    "PRODUCT_BUNDLE_IDENTIFIER = com.mohit.tools.Amazon-Orders-Sync;",
  ].join("\n");
  const ids = [...fixBundleIds(pbx).matchAll(/PRODUCT_BUNDLE_IDENTIFIER = "([^"]+)";/g)].map((m) => m[1]);
  assert.deepEqual(ids, [`${BUNDLE_ID}.Extension`, BUNDLE_ID, BUNDLE_ID]);
});

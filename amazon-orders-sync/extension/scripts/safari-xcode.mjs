#!/usr/bin/env node
// Wrap dist/safari in a macOS Xcode app project (dist/xcode) and optionally compile it.
//
//   node scripts/safari-xcode.mjs           # build:safari + convert + fix bundle ids
//   node scripts/safari-xcode.mjs --build   # …and xcodebuild Debug, ad-hoc signed ("Sign to Run Locally")
//
// Why the patch: safari-web-extension-converter (Xcode 26/27) derives the *app* bundle id from
// the app name ("com.mohit.tools.Amazon-Orders-Sync") while the extension keeps the requested id
// plus ".Extension". The extension id is then not prefixed by the app id and xcodebuild fails
// with "Embedded binary's bundle identifier is not prefixed with the parent app's".

import { execFileSync } from "node:child_process";
import { existsSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = join(dirname(fileURLToPath(import.meta.url)), "..");
export const APP_NAME = "Amazon Orders Sync";
export const BUNDLE_ID = "com.mohit.tools.amazon-orders-sync";
const XCODE_DIR = join(ROOT, "dist", "xcode");
const PROJECT_DIR = join(XCODE_DIR, APP_NAME);
const PBXPROJ = join(PROJECT_DIR, `${APP_NAME}.xcodeproj`, "project.pbxproj");
const DERIVED = join(XCODE_DIR, "build");

/** Force every target's bundle id under BUNDLE_ID (app = BUNDLE_ID, extension = BUNDLE_ID.Extension). */
export function fixBundleIds(pbx, bundleId = BUNDLE_ID) {
  return pbx.replace(/PRODUCT_BUNDLE_IDENTIFIER = "?([^";]+)"?;/g, (_m, id) => {
    const next = /\.Extension$/.test(id) ? `${bundleId}.Extension` : bundleId;
    return `PRODUCT_BUNDLE_IDENTIFIER = "${next}";`;
  });
}

function run(cmd, args, opts = {}) {
  console.log(`$ ${cmd} ${args.map((a) => (/\s/.test(a) ? JSON.stringify(a) : a)).join(" ")}`);
  return execFileSync(cmd, args, { stdio: "inherit", cwd: ROOT, ...opts });
}

function main() {
  const compile = process.argv.includes("--build");
  run("node", ["scripts/build.mjs", "safari"]);
  run("xcrun", [
    "safari-web-extension-converter", "dist/safari",
    "--project-location", "dist/xcode",
    "--app-name", APP_NAME,
    "--bundle-identifier", BUNDLE_ID,
    "--macos-only", "--no-open", "--no-prompt", "--force",
  ]);
  if (!existsSync(PBXPROJ)) throw new Error(`converter did not produce ${PBXPROJ}`);
  writeFileSync(PBXPROJ, fixBundleIds(readFileSync(PBXPROJ, "utf8")));
  console.log(`bundle ids → ${BUNDLE_ID} / ${BUNDLE_ID}.Extension`);
  if (!compile) return;
  run("xcodebuild", [
    "-project", `${APP_NAME}.xcodeproj`, "-scheme", APP_NAME, "-configuration", "Debug",
    "-derivedDataPath", DERIVED,
    "CODE_SIGN_IDENTITY=-", "CODE_SIGN_STYLE=Manual", "DEVELOPMENT_TEAM=",
    "build", "-quiet",
  ], { cwd: PROJECT_DIR });
  console.log(`app → ${join(DERIVED, "Build", "Products", "Debug", `${APP_NAME}.app`)}`);
}

if (import.meta.url === `file://${process.argv[1]}`) main();

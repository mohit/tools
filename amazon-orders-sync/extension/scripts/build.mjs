#!/usr/bin/env node
// Assemble browser-specific builds from the single shared source tree.
//
//   node scripts/build.mjs chrome|safari|all
//
// Output: dist/chrome and dist/safari. The only per-browser differences are the manifest's
// `background` entry (Chrome: service worker that importScripts() the shared modules;
// Safari: non-persistent background page listing the same files) and browser_specific_settings.
// No bundler: every source file is copied verbatim, so what you debug is what you wrote.

import { cpSync, mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import zlib from "node:zlib";

const ROOT = join(dirname(fileURLToPath(import.meta.url)), "..");
const SRC = join(ROOT, "src");
const DIST = join(ROOT, "dist");

// Load order matters: each module registers on globalThis.AOS and later ones read earlier ones.
export const BACKGROUND_SCRIPTS = [
  "shared/browser.js",
  "shared/util.js",
  "shared/constants.js",
  "shared/outbox.js",
  "shared/client.js",
  "shared/plan.js",
  "background/background.js",
];

export const CONTENT_SCRIPTS = [
  "shared/browser.js",
  "shared/util.js",
  "shared/constants.js",
  "shared/parsers.js",
  "shared/crawler.js",
  "content/content.js",
];

const readJson = (p) => JSON.parse(readFileSync(p, "utf8"));

function merge(base, override) {
  const out = { ...base };
  for (const [k, v] of Object.entries(override)) {
    out[k] = v && typeof v === "object" && !Array.isArray(v) && base[k] && typeof base[k] === "object"
      ? merge(base[k], v)
      : v;
  }
  return out;
}

export function buildManifest(target) {
  const pkg = readJson(join(ROOT, "package.json"));
  const manifest = merge(readJson(join(ROOT, "manifest", "base.json")), readJson(join(ROOT, "manifest", `${target}.json`)));
  manifest.version = pkg.version;
  manifest.content_scripts = manifest.content_scripts.map((cs) => ({ ...cs, js: CONTENT_SCRIPTS }));
  if (target === "safari") manifest.background = { ...manifest.background, scripts: BACKGROUND_SCRIPTS };
  return manifest;
}

// --- icons: drawn in code so the repo carries no binaries ---------------------------------

const CRC_TABLE = Array.from({ length: 256 }, (_, n) => {
  let c = n;
  for (let k = 0; k < 8; k++) c = c & 1 ? 0xedb88320 ^ (c >>> 1) : c >>> 1;
  return c >>> 0;
});

function crc32(buf) {
  let c = 0xffffffff;
  for (const b of buf) c = CRC_TABLE[(c ^ b) & 0xff] ^ (c >>> 8);
  return (c ^ 0xffffffff) >>> 0;
}

function chunk(type, data) {
  const len = Buffer.alloc(4);
  len.writeUInt32BE(data.length);
  const body = Buffer.concat([Buffer.from(type, "ascii"), data]);
  const crc = Buffer.alloc(4);
  crc.writeUInt32BE(crc32(body));
  return Buffer.concat([len, body, crc]);
}

function png(size, pixel) {
  const raw = Buffer.alloc((size * 4 + 1) * size);
  for (let y = 0; y < size; y++) {
    raw[y * (size * 4 + 1)] = 0;
    for (let x = 0; x < size; x++) {
      const [r, g, b, a] = pixel(x / size, y / size);
      const i = y * (size * 4 + 1) + 1 + x * 4;
      raw[i] = r;
      raw[i + 1] = g;
      raw[i + 2] = b;
      raw[i + 3] = a;
    }
  }
  const ihdr = Buffer.alloc(13);
  ihdr.writeUInt32BE(size, 0);
  ihdr.writeUInt32BE(size, 4);
  ihdr[8] = 8; // bit depth
  ihdr[9] = 6; // RGBA
  return Buffer.concat([
    Buffer.from([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a]),
    chunk("IHDR", ihdr),
    chunk("IDAT", zlib.deflateSync(raw)),
    chunk("IEND", Buffer.alloc(0)),
  ]);
}

// A parcel (tan box with a tape stripe) on a dark rounded tile, with a small sync dot.
function iconPixel(u, v) {
  const r = 0.2;
  const dx = Math.max(0, Math.abs(u - 0.5) - (0.5 - r));
  const dy = Math.max(0, Math.abs(v - 0.5) - (0.5 - r));
  if (dx * dx + dy * dy > r * r) return [0, 0, 0, 0];
  const inBox = u > 0.22 && u < 0.78 && v > 0.3 && v < 0.8;
  const lid = u > 0.18 && u < 0.82 && v > 0.24 && v < 0.36;
  const tape = Math.abs(u - 0.5) < 0.06 && v > 0.24 && v < 0.8;
  const dot = (u - 0.78) ** 2 + (v - 0.22) ** 2 < 0.012;
  if (dot) return [76, 175, 80, 255];
  if (tape && (inBox || lid)) return [181, 130, 64, 255];
  if (lid) return [240, 200, 130, 255];
  if (inBox) return [222, 176, 108, 255];
  return [35, 47, 62, 255];
}

function writeIcons(outDir) {
  mkdirSync(join(outDir, "icons"), { recursive: true });
  for (const size of [16, 32, 48, 128]) {
    writeFileSync(join(outDir, "icons", `icon-${size}.png`), png(size, iconPixel));
  }
}

// --- build ------------------------------------------------------------------------------

function build(target) {
  const out = join(DIST, target);
  rmSync(out, { recursive: true, force: true });
  mkdirSync(out, { recursive: true });
  cpSync(SRC, out, { recursive: true });
  writeIcons(out);
  if (target === "chrome") {
    const imports = BACKGROUND_SCRIPTS.map((p) => JSON.stringify(`/${p}`)).join(",\n  ");
    writeFileSync(
      join(out, "background", "sw.js"),
      `// Generated by scripts/build.mjs — Chrome MV3 service worker entry.\nimportScripts(\n  ${imports}\n);\n`,
    );
  }
  writeFileSync(join(out, "manifest.json"), `${JSON.stringify(buildManifest(target), null, 2)}\n`);
  console.log(`built ${target} → ${out}`);
}

if (import.meta.url === `file://${process.argv[1]}`) {
  const arg = process.argv[2] || "all";
  const targets = arg === "all" ? ["chrome", "safari"] : [arg];
  for (const t of targets) {
    if (!["chrome", "safari"].includes(t)) {
      console.error(`unknown target ${t}; use chrome, safari or all`);
      process.exit(2);
    }
    build(t);
  }
}

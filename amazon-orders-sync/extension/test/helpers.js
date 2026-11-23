const fs = require("node:fs");
const path = require("node:path");
const { JSDOM } = require("jsdom");

const FIXTURES = path.join(__dirname, "fixtures");
const SHARED = path.join(__dirname, "..", "src", "shared");

function fixture(name) {
  return fs.readFileSync(path.join(FIXTURES, name), "utf8");
}

function parseHtml(html, url = "https://www.amazon.com/") {
  return new JSDOM(html, { url }).window.document;
}

function load(name) {
  return require(path.join(SHARED, name));
}

module.exports = { fixture, parseHtml, load, FIXTURES };

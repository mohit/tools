const test = require('node:test');
const assert = require('node:assert/strict');

// Copy patterns from content-google-voice.js
const CODE_PATTERNS = [
  /(?:code|verification|verify|otp|pin)[:\s]+(\d{4,8})\b/i,
  /\b(\d{4,8})\s+is\s+your\s+(?:code|verification|otp|pin)/i,
  /(?:enter|use|input)[:\s]+(\d{4,8})\b/i,
  /\bG-(\d{4,8})\b/i,
  /your\s+(?:verification\s+)?code\s+is\s+(\d{4,8})\b/i,
];

function extractCode(text) {
  for (const pattern of CODE_PATTERNS) {
    const match = text.match(pattern);
    if (match) return match[1];
  }
  return null;
}

test('extracts "code is 123456"', () => {
  assert.equal(extractCode('Your code is 123456'), '123456');
});

test('extracts "code: 789012"', () => {
  assert.equal(extractCode('verification code: 789012'), '789012');
});

test('extracts "123456 is your code"', () => {
  assert.equal(extractCode('123456 is your verification code'), '123456');
});

test('extracts Google G- format', () => {
  assert.equal(extractCode('G-482910 is your verification code'), '482910');
});

test('extracts "enter 5678"', () => {
  assert.equal(extractCode('Please enter 5678 to verify'), '5678');
});

test('rejects plain numbers without context', () => {
  assert.equal(extractCode('I have 123456 apples'), null);
});

test('rejects short codes', () => {
  assert.equal(extractCode('code: 123'), null);
});

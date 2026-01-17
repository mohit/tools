// Cross-browser compatibility: use browserAPI.* or chrome.*
const browserAPI = typeof browser !== 'undefined' ? browser : chrome;

// Background script - stores codes and coordinates between Google Voice and websites

let recentCodes = []; // { code, timestamp, source }
const MAX_CODES = 10;
const CODE_EXPIRY = 15 * 60 * 1000; // 15 minutes

let pendingTabs = new Set();

// Listen for messages from content scripts
browserAPI.runtime.onMessage.addListener((message, sender, sendResponse) => {

  if (message.type === 'NEW_CODE') {
    // Received a new 2FA code from Google Voice
    const { code, source, timestamp } = message;

    console.log('[2FA Autofill] New code received:', code, 'from:', source);

    // Check if this code already exists
    const existingIndex = recentCodes.findIndex(c => c.code === code);
    if (existingIndex >= 0) {
      // Update timestamp and move to front
      recentCodes.splice(existingIndex, 1);
    }

    // Add to front of list
    recentCodes.unshift({
      code,
      source: source || extractSourceFromMessage(message.messageText),
      timestamp: timestamp || Date.now()
    });

    // Keep only recent codes
    recentCodes = recentCodes.slice(0, MAX_CODES);

    // Clean up expired codes
    cleanupExpiredCodes();

    // Notify all tabs waiting for a code
    notifyPendingTabs(code);

    // Persist to storage
    browserAPI.storage.local.set({ recentCodes });

    sendResponse({ success: true });
  }

  if (message.type === 'REQUEST_CODE') {
    // A website is requesting the latest code
    const tabId = sender.tab?.id;

    cleanupExpiredCodes();

    if (recentCodes.length > 0) {
      const latest = recentCodes[0];
      sendResponse({
        code: latest.code,
        timestamp: latest.timestamp,
        source: latest.source
      });
    } else {
      // Register this tab as waiting for a code
      if (tabId) {
        pendingTabs.add(tabId);
      }
      sendResponse({ code: null, waiting: true });
    }
  }

  if (message.type === 'GET_ALL_CODES') {
    // Popup or content script requesting all recent codes
    cleanupExpiredCodes();
    sendResponse({ codes: recentCodes });
  }

  if (message.type === 'CODE_FILLED') {
    // Code was successfully filled
    console.log('[2FA Autofill] Code filled:', message.code);
    sendResponse({ success: true });
  }

  if (message.type === 'GET_STATUS') {
    // Legacy: popup requesting status
    cleanupExpiredCodes();
    const latest = recentCodes[0];
    sendResponse({
      hasCode: recentCodes.length > 0,
      code: latest?.code,
      timestamp: latest?.timestamp,
      source: latest?.source,
      pendingTabs: pendingTabs.size
    });
  }

  return true; // Keep message channel open for async response
});

// Try to extract source/sender from message text
function extractSourceFromMessage(text) {
  if (!text) return null;

  // Common patterns for service names in 2FA messages
  const patterns = [
    /^(\w+):/,                           // "ServiceName: Your code..."
    /from\s+(\w+)/i,                     // "...from Google"
    /(\w+)\s+verification/i,             // "Google verification code"
    /(\w+)\s+code/i,                     // "Amazon code is..."
  ];

  for (const pattern of patterns) {
    const match = text.match(pattern);
    if (match && match[1].length > 1 && match[1].length < 20) {
      return match[1];
    }
  }

  return null;
}

// Remove codes older than 15 minutes
function cleanupExpiredCodes() {
  const now = Date.now();
  recentCodes = recentCodes.filter(c => (now - c.timestamp) < CODE_EXPIRY);
}

// Notify all pending tabs that a new code is available
async function notifyPendingTabs(code) {
  const latest = recentCodes[0];

  for (const tabId of pendingTabs) {
    try {
      await browserAPI.tabs.sendMessage(tabId, {
        type: 'CODE_AVAILABLE',
        code: latest.code,
        timestamp: latest.timestamp,
        source: latest.source
      });
    } catch (e) {
      // Tab might be closed, remove from pending
      pendingTabs.delete(tabId);
    }
  }
  pendingTabs.clear();
}

// Clean up when tabs are closed
browserAPI.tabs.onRemoved.addListener((tabId) => {
  pendingTabs.delete(tabId);
});

// Load stored codes on startup
browserAPI.storage.local.get('recentCodes').then(result => {
  if (result.recentCodes) {
    recentCodes = result.recentCodes;
    cleanupExpiredCodes();
  }
});

console.log('[2FA Autofill] Background script loaded');

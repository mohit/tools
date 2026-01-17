// Cross-browser compatibility: use browserAPI.* or chrome.*
const browserAPI = typeof browser !== 'undefined' ? browser : chrome;

// Content script for Google Voice - monitors for new SMS messages with 2FA codes

console.log('[2FA Autofill] Google Voice content script loaded');

// Patterns that REQUIRE a keyword context to match
const CODE_PATTERNS = [
  // "code is 123456" or "code: 123456"
  /(?:code|verification|verify|otp|pin)[:\s]+(\d{4,8})\b/i,
  // "123456 is your code"
  /\b(\d{4,8})\s+is\s+your\s+(?:code|verification|otp|pin)/i,
  // "enter 123456" or "use 123456"
  /(?:enter|use|input)[:\s]+(\d{4,8})\b/i,
  // G-123456 format (Google)
  /\bG-(\d{4,8})\b/i,
  // "Your code is 123456"
  /your\s+(?:verification\s+)?code\s+is\s+(\d{4,8})\b/i,
];

// Keywords that MUST be present for a message to be considered a 2FA message
const REQUIRED_KEYWORDS = [
  'verification', 'verify', 'code', 'otp', 'one-time', 'one time',
  'security code', 'confirm', 'login', 'sign in', 'authenticate',
  '2fa', 'two-factor', 'mfa', 'passcode'
];

// Patterns to EXCLUDE (phone numbers, dates, etc.)
const EXCLUDE_PATTERNS = [
  /\(\d{3}\)\s*\d{3}-\d{4}/, // (555) 123-4567
  /\d{3}-\d{3}-\d{4}/,       // 555-123-4567
  /\d{3}\.\d{3}\.\d{4}/,     // 555.123.4567
  /\b\d{5}-\d{4}\b/,         // ZIP+4
  /\$[\d,]+\.\d{2}/,         // Currency
  /\b(19|20)\d{2}\b/,        // Years
];

let lastSentCode = null;
let lastSentTime = 0;

// Parse a time string like "3:55 PM" or "Thu" or "Jan 15" into a timestamp
function parseMessageTime(timeStr) {
  if (!timeStr) return Date.now();

  timeStr = timeStr.trim();
  const now = new Date();

  // Match "3:55 PM" or "3:55 AM" format (today's message)
  const timeMatch = timeStr.match(/^(\d{1,2}):(\d{2})\s*(AM|PM)?$/i);
  if (timeMatch) {
    let hours = parseInt(timeMatch[1], 10);
    const minutes = parseInt(timeMatch[2], 10);
    const ampm = timeMatch[3]?.toUpperCase();

    if (ampm === 'PM' && hours !== 12) hours += 12;
    if (ampm === 'AM' && hours === 12) hours = 0;

    const messageDate = new Date(now);
    messageDate.setHours(hours, minutes, 0, 0);

    // If the time is in the future, it's probably from yesterday
    if (messageDate > now) {
      messageDate.setDate(messageDate.getDate() - 1);
    }

    return messageDate.getTime();
  }

  // Match day names like "Thu", "Wed" (within the last week)
  const dayMatch = timeStr.match(/^(Mon|Tue|Wed|Thu|Fri|Sat|Sun)/i);
  if (dayMatch) {
    const days = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];
    const targetDay = days.findIndex(d => d.toLowerCase() === dayMatch[1].toLowerCase());
    const currentDay = now.getDay();

    let daysAgo = currentDay - targetDay;
    if (daysAgo <= 0) daysAgo += 7;

    const messageDate = new Date(now);
    messageDate.setDate(messageDate.getDate() - daysAgo);
    messageDate.setHours(12, 0, 0, 0); // Assume midday

    return messageDate.getTime();
  }

  // Match "Jan 15" or "Dec 3" format
  const dateMatch = timeStr.match(/^(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+(\d{1,2})$/i);
  if (dateMatch) {
    const months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
    const month = months.findIndex(m => m.toLowerCase() === dateMatch[1].toLowerCase());
    const day = parseInt(dateMatch[2], 10);

    const messageDate = new Date(now.getFullYear(), month, day, 12, 0, 0, 0);

    // If date is in the future, it's from last year
    if (messageDate > now) {
      messageDate.setFullYear(messageDate.getFullYear() - 1);
    }

    return messageDate.getTime();
  }

  // Fallback to now
  return Date.now();
}

// Extract timestamp from a list item or message element
function getMessageTimestamp(element) {
  if (!element) return Date.now();

  // Look for time elements in Google Voice
  const timeSelectors = [
    '[data-e2e-timestamp]',
    'time',
    '.timestamp',
    '.message-time',
    '.time',
    'gv-relative-time',
  ];

  for (const selector of timeSelectors) {
    const timeEl = element.querySelector(selector);
    if (timeEl) {
      // Try datetime attribute first
      const datetime = timeEl.getAttribute('datetime');
      if (datetime) {
        const parsed = Date.parse(datetime);
        if (!isNaN(parsed)) return parsed;
      }

      // Try text content
      const timeText = timeEl.textContent?.trim();
      if (timeText) {
        return parseMessageTime(timeText);
      }
    }
  }

  // Look for any text that looks like a time in the element
  const allText = element.textContent || '';
  const timePattern = /\b(\d{1,2}:\d{2}\s*(?:AM|PM)?)\b/i;
  const match = allText.match(timePattern);
  if (match) {
    return parseMessageTime(match[1]);
  }

  return Date.now();
}

// Known service names that send 2FA codes
const KNOWN_SERVICES = [
  'google', 'amazon', 'apple', 'microsoft', 'facebook', 'meta', 'twitter', 'x',
  'paypal', 'venmo', 'chase', 'bank of america', 'wells fargo', 'citi', 'citibank',
  'uber', 'lyft', 'doordash', 'instacart', 'grubhub',
  'slack', 'discord', 'zoom', 'dropbox', 'github', 'gitlab', 'linkedin',
  'instagram', 'whatsapp', 'signal', 'telegram',
  'coinbase', 'robinhood', 'fidelity', 'schwab', 'vanguard', 'etrade',
  'netflix', 'hulu', 'spotify', 'adobe', 'steam',
  'adp', 'workday', 'okta', 'duo', 'authy',
  'square', 'stripe', 'shopify', 'etsy', 'ebay',
  'airbnb', 'expedia', 'united', 'delta', 'american airlines', 'southwest',
  'att', 'at&t', 'verizon', 't-mobile', 'comcast', 'xfinity',
  'marcus', 'capital one', 'amex', 'american express', 'discover',
  'usaa', 'navy federal', 'pnc', 'td bank', 'us bank', 'truist', 'ally',
  // Credit unions often have patterns like "XX CU" or "XX FCU"
  'sf fire cu', 'fire cu', 'credit union',
];

// Pattern to detect credit union names (e.g., "SF Fire CU", "Navy FCU")
function extractCreditUnionName(text) {
  // Match patterns like "SF Fire CU", "Navy FCU", "X Credit Union"
  const cuPatterns = [
    /\b([A-Z][A-Za-z\s]{1,20})\s+(?:CU|FCU|Credit\s+Union)\b/i,
    /\b([A-Z]{2,5})\s+(?:CU|FCU)\b/, // "SF CU", "NFCU"
  ];

  for (const pattern of cuPatterns) {
    const match = text.match(pattern);
    if (match) {
      return match[0].trim(); // Return the full match including "CU"
    }
  }
  return null;
}

// Common words to filter out from source extraction
const COMMON_WORDS = [
  'your', 'the', 'this', 'that', 'use', 'enter', 'code', 'is', 'are', 'was',
  'verification', 'verify', 'security', 'never', 'share', 'anyone', 'call',
  'with', 'for', 'not', 'did', 'will', 'can', 'may', 'our', 'please', 'do',
  'one', 'time', 'otp', 'pin', 'password', 'login', 'sign', 'account'
];

// Extract source/sender from message text
function extractSourceFromText(text) {
  if (!text) return null;
  const lowerText = text.toLowerCase();

  // Check for credit union names first (special pattern)
  const cuName = extractCreditUnionName(text);
  if (cuName) {
    return cuName;
  }

  // Check for known service names (most reliable)
  for (const service of KNOWN_SERVICES) {
    if (lowerText.includes(service)) {
      // Capitalize first letter of each word
      return service.split(' ').map(w => w.charAt(0).toUpperCase() + w.slice(1)).join(' ');
    }
  }

  // Look for company/organization patterns
  const orgPatterns = [
    // "SF Fire CU" or similar abbreviations
    /\b([A-Z]{2,}(?:\s+[A-Z][a-z]+)*(?:\s+(?:CU|FCU|Bank|Inc|Corp|LLC))?)\b/,
    // "Company Name:" at start
    /^([A-Z][A-Za-z0-9]+(?:\s+[A-Z][A-Za-z0-9]+)*):/,
    // "[CompanyName]" in brackets
    /\[([A-Za-z0-9\s]+)\]/,
    // "from CompanyName"
    /from\s+([A-Z][A-Za-z0-9]+(?:\s+[A-Z][A-Za-z0-9]+)*)/i,
  ];

  for (const pattern of orgPatterns) {
    const match = text.match(pattern);
    if (match && match[1]) {
      const candidate = match[1].trim();
      // Must be 2-30 chars and not a common word
      if (candidate.length >= 2 && candidate.length <= 30) {
        const lowerCandidate = candidate.toLowerCase();
        // Check it's not just common words
        const words = lowerCandidate.split(/\s+/);
        const hasNonCommonWord = words.some(w => !COMMON_WORDS.includes(w) && w.length > 2);
        if (hasNonCommonWord) {
          return candidate;
        }
      }
    }
  }

  return null;
}

// Extract 2FA code from message text - requires keyword context
function extractCode(text) {
  if (!text || text.length > 500) return null; // Skip very long text blocks

  const lowerText = text.toLowerCase();

  // MUST have a 2FA-related keyword
  const hasRequiredKeyword = REQUIRED_KEYWORDS.some(kw => lowerText.includes(kw));
  if (!hasRequiredKeyword) {
    return null;
  }

  // Check for excluded patterns (phone numbers, etc.)
  for (const pattern of EXCLUDE_PATTERNS) {
    if (pattern.test(text)) {
      // If the text contains a phone number, be extra careful
      // Only continue if we have very strong 2FA indicators
      if (!lowerText.includes('verification code') && !lowerText.includes('your code')) {
        return null;
      }
    }
  }

  // Try each pattern
  for (const pattern of CODE_PATTERNS) {
    const match = text.match(pattern);
    if (match) {
      const code = match[1];
      // Validate: must be 4-8 digits
      if (code.length >= 4 && code.length <= 8) {
        return code;
      }
    }
  }

  return null;
}

// Check if this looks like a real message preview (not a phone number or UI element)
function isValidMessageText(text) {
  if (!text) return false;
  text = text.trim();

  // Too short or too long
  if (text.length < 10 || text.length > 300) return false;

  // Looks like just a phone number
  if (/^[\d\s\-\(\)\.]+$/.test(text)) return false;

  // Looks like just a time/date
  if (/^\d{1,2}:\d{2}\s*(AM|PM)?$/i.test(text)) return false;

  return true;
}

// Get contact name from a list item element
function getContactFromListItem(listItem) {
  if (!listItem) return null;

  // Try various selectors for contact/sender name in Google Voice
  const contactSelectors = [
    '[data-e2e-contact-name]',
    '[data-caller-id]',
    '.contact-name',
    '.sender-name',
    '.participant-name',
    // Google Voice often uses specific class patterns
    'h3', // Contact names are often in h3
    '[role="heading"]',
  ];

  for (const selector of contactSelectors) {
    const el = listItem.querySelector(selector);
    if (el) {
      const name = el.textContent?.trim();
      // Filter out things that look like times or numbers only
      if (name && name.length > 1 && !/^\d{1,2}:\d{2}/.test(name) && !/^[\d\s]+$/.test(name)) {
        return name;
      }
    }
  }

  // Try to get from the first text that looks like a name (not a phone number format, not a time)
  const textNodes = listItem.querySelectorAll('span, div');
  for (const node of textNodes) {
    const text = node.textContent?.trim();
    if (text && text.length >= 2 && text.length <= 30) {
      // Skip if it looks like a time, phone, or message preview
      if (!/^\d{1,2}:\d{2}/.test(text) &&
          !/^\(\d{3}\)/.test(text) &&
          !/^\d{3}-\d{3}/.test(text) &&
          !/verification|code|otp/i.test(text)) {
        return text;
      }
    }
  }

  return null;
}

// Get the most recent message from the conversation list
function getMostRecentMessage() {
  // Target the message list items in the sidebar/main list
  // Google Voice structure: list items with contact info and message preview

  // Try to find message preview elements specifically
  const previewSelectors = [
    // Message preview text in list
    'gv-thread-item .preview-text',
    'gv-message-list-item .message-preview',
    '[data-thread-id] .preview',
    // More generic but still targeted
    '.thread-preview',
    '.message-snippet',
  ];

  for (const selector of previewSelectors) {
    const elements = document.querySelectorAll(selector);
    if (elements.length > 0) {
      const text = elements[0].textContent;
      const listItem = elements[0].closest('[role="listitem"], [role="row"], gv-thread-item');
      const contact = getContactFromListItem(listItem);
      const timestamp = getMessageTimestamp(listItem);
      return { text, contact, timestamp };
    }
  }

  // Fallback: look for the first few list items that contain message-like text
  // Only check top 5 messages to avoid picking up old codes
  const listItems = document.querySelectorAll('[role="listitem"], [role="row"]');
  const maxToCheck = Math.min(listItems.length, 5);

  for (let i = 0; i < maxToCheck; i++) {
    const text = listItems[i].textContent;
    if (isValidMessageText(text) && extractCode(text)) {
      const contact = getContactFromListItem(listItems[i]);
      const timestamp = getMessageTimestamp(listItems[i]);
      return { text, contact, timestamp };
    }
  }

  return null;
}

// Get current conversation contact name (when a conversation is open)
function getCurrentConversationContact() {
  // When viewing a conversation, the contact name is usually in a header
  const headerSelectors = [
    'gv-conversation-header [data-e2e-contact-name]',
    '.conversation-header .contact-name',
    'gv-contact-pill',
    '[data-e2e-conversation-title]',
    'h1', // Often the contact name is in h1 when conversation is open
  ];

  for (const selector of headerSelectors) {
    const el = document.querySelector(selector);
    if (el) {
      const name = el.textContent?.trim();
      if (name && name.length > 1 && name.length < 50) {
        return name;
      }
    }
  }

  return null;
}

// Scan the currently open conversation for messages
function scanOpenConversation() {
  // When a conversation is open, messages appear in a different container
  const messageSelectors = [
    'gv-text-message-item',
    '[data-message-id]',
    '.message-content',
    '.text-msg',
  ];

  const messages = [];
  for (const selector of messageSelectors) {
    const elements = document.querySelectorAll(selector);
    // Only check the last 5 messages in an open conversation
    const startIdx = Math.max(0, elements.length - 5);
    for (let i = startIdx; i < elements.length; i++) {
      const text = elements[i].textContent;
      if (isValidMessageText(text)) {
        messages.push({ text, element: elements[i] });
      }
    }
  }

  if (messages.length > 0) {
    const lastMsg = messages[messages.length - 1];
    const contact = getCurrentConversationContact();
    const timestamp = getMessageTimestamp(lastMsg.element);
    return { text: lastMsg.text, contact, timestamp };
  }

  return null;
}

// Main scan function
function scanForCode() {
  let messageData = null;
  let messageText = null;
  let contactName = null;
  let messageTimestamp = null;

  // Try open conversation first
  messageData = scanOpenConversation();
  if (messageData) {
    messageText = messageData.text;
    contactName = messageData.contact;
    messageTimestamp = messageData.timestamp;
  }

  // Fall back to message list preview
  if (!messageText) {
    messageData = getMostRecentMessage();
    if (messageData) {
      messageText = messageData.text;
      contactName = messageData.contact;
      messageTimestamp = messageData.timestamp;
    }
  }

  if (!messageText) {
    // Last resort: scan visible text more carefully
    // Only check first 50 lines to avoid old messages
    const body = document.body.innerText;
    const lines = body.split('\n').filter(line => isValidMessageText(line)).slice(0, 50);

    for (const line of lines) {
      const code = extractCode(line);
      if (code) {
        messageText = line;
        messageTimestamp = Date.now(); // Can't determine actual time in fallback
        break;
      }
    }
  }

  if (!messageText) return;

  const code = extractCode(messageText);
  if (!code) return;

  // Debounce: don't send the same code within 30 seconds
  if (code === lastSentCode && (Date.now() - lastSentTime) < 30000) {
    return;
  }

  // Determine the source - prefer extracted service name, fall back to contact
  let source = extractSourceFromText(messageText);
  if (!source && contactName) {
    source = contactName;
  }

  // Use message delivery time, not detection time
  const timestamp = messageTimestamp || Date.now();

  console.log('[2FA Autofill] Found code:', code, 'source:', source, 'time:', new Date(timestamp).toLocaleTimeString());

  lastSentCode = code;
  lastSentTime = Date.now();

  browserAPI.runtime.sendMessage({
    type: 'NEW_CODE',
    code: code,
    source: source,
    messageText: messageText,
    timestamp: timestamp
  }).catch(err => console.error('[2FA Autofill] Error sending code:', err));
}

// Watch for new messages via MutationObserver
function setupObserver() {
  let scanTimeout = null;

  const observer = new MutationObserver((mutations) => {
    // Debounce scans
    if (scanTimeout) clearTimeout(scanTimeout);
    scanTimeout = setTimeout(scanForCode, 300);
  });

  observer.observe(document.body, {
    childList: true,
    subtree: true,
    characterData: true
  });

  console.log('[2FA Autofill] Observer active');
}

// Initialize
function init() {
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
    return;
  }

  console.log('[2FA Autofill] Initializing Google Voice monitor');

  // Initial scan after page load
  setTimeout(scanForCode, 2000);

  // Set up observer
  setupObserver();

  // Periodic scan as backup (every 10 seconds)
  setInterval(scanForCode, 10000);
}

init();

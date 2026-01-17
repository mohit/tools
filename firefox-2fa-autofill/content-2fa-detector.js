// Cross-browser compatibility: use browserAPI.* or chrome.*
const browserAPI = typeof browser !== 'undefined' ? browser : chrome;

// Content script for detecting 2FA input fields and showing code suggestions

console.log('[2FA Autofill] 2FA detector loaded on:', window.location.hostname);

let suggestionBox = null;
let currentField = null;
let availableCodes = []; // { code, timestamp, isNew }

// Attributes/patterns that indicate a 2FA/OTP input field
const OTP_INDICATORS = {
  names: [
    'otp', 'code', 'verification', 'verify', 'token', 'pin', 'mfa', '2fa',
    'one-time', 'onetime', 'passcode', 'security-code', 'auth-code',
    'sms-code', 'totp', 'tfa'
  ],
  autocomplete: ['one-time-code', 'otp'],
  classPatterns: [
    /otp/i, /code/i, /verify/i, /token/i, /pin/i, /mfa/i, /2fa/i,
    /digit/i, /passcode/i
  ]
};

// Check if an input element looks like a 2FA field
function is2FAField(input) {
  if (!input || input.tagName !== 'INPUT') return false;
  if (input.type === 'hidden' || input.disabled || input.readOnly) return false;
  if (input.value && input.value.length >= 4) return false;

  const inputType = input.type?.toLowerCase();
  const inputMode = input.inputMode?.toLowerCase();
  const autocomplete = input.autocomplete?.toLowerCase() || '';

  if (OTP_INDICATORS.autocomplete.some(ac => autocomplete.includes(ac))) {
    return true;
  }

  const name = (input.name || '').toLowerCase();
  const id = (input.id || '').toLowerCase();
  const placeholder = (input.placeholder || '').toLowerCase();
  const ariaLabel = (input.getAttribute('aria-label') || '').toLowerCase();
  const textToCheck = `${name} ${id} ${placeholder} ${ariaLabel}`;

  if (OTP_INDICATORS.names.some(indicator => textToCheck.includes(indicator))) {
    return true;
  }

  const className = input.className || '';
  if (OTP_INDICATORS.classPatterns.some(pattern => pattern.test(className))) {
    return true;
  }

  const maxLength = parseInt(input.maxLength, 10);
  if (maxLength >= 4 && maxLength <= 8) {
    if (inputType === 'tel' || inputType === 'number' || inputMode === 'numeric') {
      return true;
    }
  }

  const parent = input.closest('form, div, section');
  if (parent) {
    const parentText = parent.textContent?.toLowerCase() || '';
    const verificationKeywords = [
      'verification code', 'enter code', 'enter the code',
      'security code', 'one-time', 'otp', '2-step', 'two-step',
      'sent to your phone', 'sent a code', 'text message'
    ];
    if (verificationKeywords.some(kw => parentText.includes(kw))) {
      if (inputType === 'text' || inputType === 'tel' || inputType === 'number' || !inputType) {
        return true;
      }
    }
  }

  return false;
}

// Find all 2FA input fields on the page
function find2FAFields() {
  const inputs = document.querySelectorAll('input');
  const otpFields = [];
  inputs.forEach(input => {
    if (is2FAField(input)) {
      otpFields.push(input);
    }
  });
  return otpFields;
}

// Create the suggestion popup UI
function createSuggestionBox() {
  if (suggestionBox) return suggestionBox;

  suggestionBox = document.createElement('div');
  suggestionBox.id = 'gv-2fa-suggestions';
  suggestionBox.innerHTML = `
    <style>
      #gv-2fa-suggestions {
        position: absolute;
        z-index: 999999;
        background: white;
        border: 1px solid #ddd;
        border-radius: 8px;
        box-shadow: 0 4px 12px rgba(0,0,0,0.15);
        padding: 8px;
        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
        font-size: 13px;
        min-width: 200px;
        display: none;
      }
      #gv-2fa-suggestions .header {
        display: flex;
        justify-content: space-between;
        align-items: center;
        margin-bottom: 6px;
        padding-bottom: 6px;
        border-bottom: 1px solid #eee;
      }
      #gv-2fa-suggestions .title {
        font-weight: 600;
        color: #333;
        font-size: 11px;
        text-transform: uppercase;
        letter-spacing: 0.5px;
      }
      #gv-2fa-suggestions .close-btn {
        background: none;
        border: none;
        cursor: pointer;
        color: #999;
        font-size: 16px;
        padding: 0 4px;
      }
      #gv-2fa-suggestions .close-btn:hover {
        color: #333;
      }
      #gv-2fa-suggestions .code-item {
        display: flex;
        align-items: center;
        padding: 8px 10px;
        margin: 4px 0;
        background: #f8f9fa;
        border-radius: 6px;
        cursor: pointer;
        transition: background 0.15s;
      }
      #gv-2fa-suggestions .code-item:hover {
        background: #e8f0fe;
      }
      #gv-2fa-suggestions .code-item.new-code {
        background: #e8f5e9;
        border: 1px solid #4caf50;
      }
      #gv-2fa-suggestions .code-item.new-code:hover {
        background: #c8e6c9;
      }
      #gv-2fa-suggestions .code-value {
        font-family: 'SF Mono', Monaco, monospace;
        font-size: 18px;
        font-weight: bold;
        color: #1a73e8;
        letter-spacing: 2px;
        flex-grow: 1;
      }
      #gv-2fa-suggestions .code-item.new-code .code-value {
        color: #2e7d32;
      }
      #gv-2fa-suggestions .code-meta {
        margin-left: auto;
        text-align: right;
      }
      #gv-2fa-suggestions .code-source {
        font-size: 10px;
        color: #666;
        max-width: 80px;
        overflow: hidden;
        text-overflow: ellipsis;
        white-space: nowrap;
      }
      #gv-2fa-suggestions .code-badge {
        font-size: 10px;
        padding: 2px 6px;
        border-radius: 10px;
        background: #1a73e8;
        color: white;
      }
      #gv-2fa-suggestions .code-item.new-code .code-badge {
        background: #4caf50;
      }
      #gv-2fa-suggestions .waiting {
        color: #666;
        font-size: 12px;
        padding: 8px;
        text-align: center;
      }
      #gv-2fa-suggestions .waiting .spinner {
        display: inline-block;
        width: 12px;
        height: 12px;
        border: 2px solid #ddd;
        border-top-color: #1a73e8;
        border-radius: 50%;
        animation: gv-spin 1s linear infinite;
        margin-right: 6px;
        vertical-align: middle;
      }
      @keyframes gv-spin {
        to { transform: rotate(360deg); }
      }
    </style>
    <div class="header">
      <span class="title">Google Voice Codes</span>
      <button class="close-btn" title="Close">&times;</button>
    </div>
    <div class="codes-list"></div>
  `;

  document.body.appendChild(suggestionBox);

  // Close button handler
  suggestionBox.querySelector('.close-btn').addEventListener('click', () => {
    hideSuggestionBox();
  });

  return suggestionBox;
}

// Position the suggestion box near a field
function positionSuggestionBox(field) {
  if (!suggestionBox || !field) return;

  const rect = field.getBoundingClientRect();
  const scrollTop = window.scrollY || document.documentElement.scrollTop;
  const scrollLeft = window.scrollX || document.documentElement.scrollLeft;

  suggestionBox.style.top = (rect.bottom + scrollTop + 4) + 'px';
  suggestionBox.style.left = (rect.left + scrollLeft) + 'px';
}

// Show the suggestion box
function showSuggestionBox(field) {
  createSuggestionBox();
  currentField = field;
  positionSuggestionBox(field);
  updateSuggestionBoxContent();
  suggestionBox.style.display = 'block';
}

// Hide the suggestion box
function hideSuggestionBox() {
  if (suggestionBox) {
    suggestionBox.style.display = 'none';
  }
  currentField = null;
}

// Update the content of the suggestion box
function updateSuggestionBoxContent() {
  if (!suggestionBox) return;

  const codesList = suggestionBox.querySelector('.codes-list');

  if (availableCodes.length === 0) {
    codesList.innerHTML = `
      <div class="waiting">
        <span class="spinner"></span>
        Waiting for code from Google Voice...
      </div>
    `;
    return;
  }

  codesList.innerHTML = availableCodes.map((item, index) => `
    <div class="code-item ${item.isNew ? 'new-code' : ''}" data-code="${item.code}" data-index="${index}">
      <span class="code-value">${item.code}</span>
      <div class="code-meta">
        ${item.source ? `<div class="code-source">${escapeHtml(item.source)}</div>` : ''}
        <span class="code-badge">${item.isNew ? 'NEW' : getTimeAgo(item.timestamp)}</span>
      </div>
    </div>
  `).join('');

  // Add click handlers
  codesList.querySelectorAll('.code-item').forEach(el => {
    el.addEventListener('click', () => {
      const code = el.dataset.code;
      if (currentField) {
        fillCode(currentField, code);
        hideSuggestionBox();
        notifyCodeFilled(code);
      }
    });
  });
}

// Get human-readable time ago
function getTimeAgo(timestamp) {
  if (!timestamp) return '';
  const seconds = Math.floor((Date.now() - timestamp) / 1000);
  if (seconds < 10) return 'now';
  if (seconds < 60) return `${seconds}s`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m`;
  return `${Math.floor(seconds / 3600)}h`;
}

// Escape HTML to prevent XSS
function escapeHtml(text) {
  const div = document.createElement('div');
  div.textContent = text;
  return div.innerHTML;
}

// Fill a code into a field
function fillCode(field, code) {
  if (!field || !code) return false;

  field.focus();

  const maxLength = parseInt(field.maxLength, 10) || 10;

  // Single field for full code
  if (maxLength >= code.length || isNaN(maxLength)) {
    field.value = code;
    field.dispatchEvent(new Event('input', { bubbles: true }));
    field.dispatchEvent(new Event('change', { bubbles: true }));
    field.dispatchEvent(new KeyboardEvent('keyup', { bubbles: true }));
    console.log('[2FA Autofill] Filled code into field');
    return true;
  }

  // Split input (one digit per field)
  if (maxLength === 1) {
    return fillSplitCode(field, code);
  }

  return false;
}

// Handle split OTP inputs
function fillSplitCode(firstField, code) {
  const parent = firstField.closest('form, div');
  if (!parent) return false;

  const inputs = parent.querySelectorAll('input[maxlength="1"]');
  if (inputs.length === 0) return false;

  let filled = 0;
  inputs.forEach((input, index) => {
    if (index < code.length) {
      input.value = code[index];
      input.dispatchEvent(new Event('input', { bubbles: true }));
      input.dispatchEvent(new Event('change', { bubbles: true }));
      filled++;
    }
  });

  console.log('[2FA Autofill] Filled', filled, 'split input fields');
  return filled > 0;
}

// Request current code from background
async function requestCode() {
  try {
    const response = await browserAPI.runtime.sendMessage({ type: 'GET_ALL_CODES' });
    if (response?.codes) {
      // Add all codes (they'll be deduplicated by addCode)
      for (const codeData of response.codes) {
        addCode(codeData.code, codeData.timestamp, false, codeData.source);
      }
    }
  } catch (err) {
    console.error('[2FA Autofill] Error requesting code:', err);
  }
}

// Add a code to the available codes list
function addCode(code, timestamp, isNew, source = null) {
  // Check if code already exists
  const existingIndex = availableCodes.findIndex(c => c.code === code);
  if (existingIndex >= 0) {
    // Update existing code's isNew status if it's newer
    if (isNew) {
      availableCodes[existingIndex].isNew = true;
      availableCodes[existingIndex].timestamp = timestamp;
      if (source) availableCodes[existingIndex].source = source;
      // Move to top
      const item = availableCodes.splice(existingIndex, 1)[0];
      availableCodes.unshift(item);
    }
  } else {
    // Add new code at the top
    availableCodes.unshift({ code, timestamp, isNew, source });
    // Keep only last 5 codes
    if (availableCodes.length > 5) {
      availableCodes.pop();
    }
  }

  updateSuggestionBoxContent();
}

// Notify background that code was filled
async function notifyCodeFilled(code) {
  try {
    await browserAPI.runtime.sendMessage({ type: 'CODE_FILLED', code });
    // Mark the code as not new anymore
    const item = availableCodes.find(c => c.code === code);
    if (item) item.isNew = false;
  } catch (err) {
    console.error('[2FA Autofill] Error notifying code filled:', err);
  }
}

// Listen for new codes from background
browserAPI.runtime.onMessage.addListener((message) => {
  if (message.type === 'CODE_AVAILABLE' && message.code) {
    console.log('[2FA Autofill] New code received:', message.code, 'from:', message.source);
    addCode(message.code, message.timestamp || Date.now(), true, message.source);
  }
});

// Main scan function
async function scanForFields() {
  const fields = find2FAFields();

  if (fields.length === 0) {
    hideSuggestionBox();
    return;
  }

  console.log('[2FA Autofill] Found', fields.length, '2FA field(s)');

  const field = fields[0];

  // Show suggestion box if not already showing for this field
  if (currentField !== field) {
    // Request any existing code first
    await requestCode();
    showSuggestionBox(field);
  }
}

// Watch for field focus to show suggestions
function setupFocusListeners() {
  document.addEventListener('focusin', (e) => {
    if (is2FAField(e.target)) {
      requestCode();
      showSuggestionBox(e.target);
    }
  });

  document.addEventListener('focusout', (e) => {
    // Delay hiding to allow clicking on suggestion
    setTimeout(() => {
      if (!suggestionBox?.contains(document.activeElement) &&
          !is2FAField(document.activeElement)) {
        // Don't hide if we have codes - keep it visible
        // hideSuggestionBox();
      }
    }, 200);
  });
}

// Watch for dynamically added fields
function setupObserver() {
  const observer = new MutationObserver(() => {
    clearTimeout(window._2faScanTimeout);
    window._2faScanTimeout = setTimeout(scanForFields, 500);
  });

  observer.observe(document.body, {
    childList: true,
    subtree: true
  });
}

// Reposition on scroll/resize
function setupRepositioning() {
  const reposition = () => {
    if (currentField && suggestionBox?.style.display !== 'none') {
      positionSuggestionBox(currentField);
    }
  };

  window.addEventListener('scroll', reposition, { passive: true });
  window.addEventListener('resize', reposition, { passive: true });
}

// Initialize
function init() {
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
    return;
  }

  // Initial scan
  setTimeout(scanForFields, 1000);

  // Set up listeners
  setupFocusListeners();
  setupObserver();
  setupRepositioning();

  // Periodic check for new codes (poll background)
  setInterval(async () => {
    if (currentField) {
      await requestCode();
    }
  }, 2000);
}

init();

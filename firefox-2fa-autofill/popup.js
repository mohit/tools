// Cross-browser compatibility: use browserAPI.* or chrome.*
const browserAPI = typeof browser !== 'undefined' ? browser : chrome;

// Popup script - shows recent codes with sender info

const FIFTEEN_MINUTES = 15 * 60 * 1000;

async function updateStatus() {
  const contentEl = document.getElementById('content');

  try {
    const response = await browserAPI.runtime.sendMessage({ type: 'GET_ALL_CODES' });
    const codes = response?.codes || [];

    // Filter to codes within last 15 minutes
    const recentCodes = codes.filter(c => (Date.now() - c.timestamp) < FIFTEEN_MINUTES);

    if (recentCodes.length === 0) {
      contentEl.innerHTML = `
        <div class="no-code">
          <div class="spinner"></div>
          <div>Waiting for codes from Google Voice...</div>
        </div>
      `;
      return;
    }

    // Primary (most recent) code
    const primary = recentCodes[0];
    const olderCodes = recentCodes.slice(1, 3); // Up to 2 older codes

    let html = `
      <div class="primary-code">
        <div class="code-display">${primary.code}</div>
        <div class="code-meta">
          ${primary.source ? `From: ${escapeHtml(primary.source)} · ` : ''}${getTimeAgo(primary.timestamp)}
        </div>
        <button class="copy-btn" data-code="${primary.code}">Copy Code</button>
      </div>
    `;

    if (olderCodes.length > 0) {
      html += `<div class="older-codes">
        <div class="older-codes-label">Recent codes</div>`;

      for (const code of olderCodes) {
        html += `
          <div class="older-code" data-code="${code.code}">
            <span class="code-value">${code.code}</span>
            <div class="code-info">
              ${code.source ? `<div class="code-source">${escapeHtml(code.source)}</div>` : ''}
              <div class="code-time">${getTimeAgo(code.timestamp)}</div>
            </div>
          </div>
        `;
      }

      html += `</div>`;
    }

    contentEl.innerHTML = html;

    // Add click handlers
    contentEl.querySelectorAll('[data-code]').forEach(el => {
      el.addEventListener('click', async () => {
        const code = el.dataset.code;
        await navigator.clipboard.writeText(code);

        // Visual feedback
        if (el.classList.contains('copy-btn')) {
          const originalText = el.textContent;
          el.textContent = 'Copied!';
          el.style.background = '#2e7d32';
          setTimeout(() => {
            el.textContent = originalText;
            el.style.background = '';
          }, 1500);
        } else {
          // For older code items
          el.style.background = '#c8e6c9';
          setTimeout(() => {
            el.style.background = '';
          }, 500);
        }
      });
    });

  } catch (err) {
    console.error('Error getting status:', err);
    contentEl.innerHTML = `
      <div class="no-code">
        <div>Error loading codes</div>
      </div>
    `;
  }
}

function getTimeAgo(timestamp) {
  if (!timestamp) return '';
  const seconds = Math.floor((Date.now() - timestamp) / 1000);

  if (seconds < 5) return 'Just now';
  if (seconds < 60) return `${seconds}s ago`;
  if (seconds < 120) return '1 min ago';
  if (seconds < 3600) return `${Math.floor(seconds / 60)} min ago`;
  return 'Over an hour ago';
}

function escapeHtml(text) {
  const div = document.createElement('div');
  div.textContent = text;
  return div.innerHTML;
}

// Update on load
updateStatus();

// Refresh every 3 seconds
setInterval(updateStatus, 3000);

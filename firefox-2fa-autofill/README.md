# Google Voice 2FA Autofill

A browser extension that monitors Google Voice for SMS verification codes and helps autofill them on websites.

Works on **Firefox** and **Chrome/Chromium** browsers.

## Features

- **Monitors Google Voice** for incoming SMS messages containing 2FA codes
- **Smart code detection** - requires verification keywords to avoid false positives
- **Suggestion popup** on websites - shows available codes near 2FA input fields
- **Source identification** - extracts sender name from message or contact
- **Multiple code history** - keeps recent codes (last 15 minutes) for reference
- **Click to fill** - non-intrusive, user-controlled autofill

## How It Works

1. Keep Google Voice open in a browser tab
2. When you receive a verification SMS, the extension detects and extracts the code
3. Navigate to a website requiring 2FA
4. A popup appears near the code input field showing available codes
5. Click a code to fill it in

## Installation

### Firefox

1. Open Firefox and go to `about:debugging`
2. Click **"This Firefox"** in the sidebar
3. Click **"Load Temporary Add-on..."**
4. Select `manifest.json` from this folder

### Chrome / Chromium / Edge

1. First, use the Chrome manifest:
   ```bash
   cp manifest_chrome.json manifest.json
   ```
2. Open Chrome and go to `chrome://extensions`
3. Enable **"Developer mode"** (toggle in top right)
4. Click **"Load unpacked"**
5. Select this extension folder

## Usage

1. **Open Google Voice** - Navigate to [voice.google.com](https://voice.google.com) and ensure you're logged in
2. **Keep the tab open** - The extension monitors this tab for new messages
3. **Request a 2FA code** - On any website that sends SMS verification
4. **Look for the popup** - When you reach the code entry page, a suggestion box appears
5. **Click to fill** - Select the code you want to use

### Extension Popup

Click the extension icon in your toolbar to:
- See the most recent code (large, easy to copy)
- View older codes from the last 15 minutes
- Copy codes manually

## Supported Code Formats

The extension detects codes in messages containing keywords like:
- "verification code", "security code", "OTP"
- "your code is", "enter code", "use code"
- Google-style `G-123456` format

## File Structure

```
├── manifest.json           # Active manifest (Firefox V2 by default)
├── manifest_firefox.json   # Firefox Manifest V2
├── manifest_chrome.json    # Chrome Manifest V3
├── background.js           # Coordinates between tabs, stores codes
├── content-google-voice.js # Monitors Google Voice for SMS codes
├── content-2fa-detector.js # Detects 2FA fields, shows suggestion popup
├── popup.html/js           # Extension popup UI
├── icon.svg                # Extension icon
└── test-2fa.html           # Test page for development
```

## Development

### Testing

1. Load the extension (see Installation)
2. Open `test-2fa.html` in your browser
3. Open Google Voice in another tab
4. Send a test SMS to your Google Voice number:
   > Your verification code is 123456
5. The code should appear in the test page's suggestion popup

### Debugging

Open the browser's Developer Tools console to see logs prefixed with `[2FA Autofill]`:
- On Google Voice tab: shows detected codes
- On other sites: shows detected 2FA fields

### Switching Between Browsers

```bash
# For Firefox (default)
cp manifest_firefox.json manifest.json

# For Chrome/Chromium
cp manifest_chrome.json manifest.json
```

## Privacy

- **No external servers** - All processing happens locally in your browser
- **No data collection** - Codes are stored temporarily in browser memory only
- **Auto-expiry** - Codes are automatically cleared after 15 minutes
- **Minimal permissions** - Only requests access to Google Voice and active tab

## Limitations

- Requires Google Voice tab to be open (doesn't work with notifications alone)
- Some websites with non-standard input fields may not be detected
- Code detection requires standard 2FA message formats

## License

MIT License - see [LICENSE](LICENSE)

# Google Voice 2FA Autofill - Browser Extension

Automatically extracts 2FA/verification codes from Google Voice SMS messages and fills them into websites.

Works on both **Firefox** and **Chrome**.

## Installation

### Firefox

**Temporary (Development):**
1. Open Firefox and navigate to `about:debugging`
2. Click "This Firefox" in the left sidebar
3. Click "Load Temporary Add-on..."
4. Select `manifest.json` from this folder

**Permanent:**
```bash
cd firefox-2fa-autofill
zip -r ../gv-2fa-autofill.xpi *
```
Then in Firefox: `about:addons` → gear icon → "Install Add-on From File..."

### Chrome

**Setup:**
```bash
# Use the Chrome manifest (Manifest V3)
cp manifest_chrome.json manifest.json
```

**Load Extension:**
1. Open Chrome and go to `chrome://extensions`
2. Enable "Developer mode" (toggle in top right)
3. Click "Load unpacked"
4. Select this extension folder

**Note:** To switch back to Firefox, restore the original manifest:
```bash
git checkout manifest.json
# Or manually copy content from manifest.json backup
```

## Usage

1. **Keep Google Voice open**: Have Google Voice (voice.google.com) open in a tab
2. **Navigate to a login page**: Go to any website that requires 2FA
3. **Receive your code**: When a 2FA code arrives via SMS to your Google Voice number
4. **Auto-fill**: The extension automatically detects the code and fills it into the appropriate field

## Features

- **Automatic code detection**: Monitors Google Voice for incoming SMS with verification codes
- **Smart field detection**: Identifies 2FA input fields on websites using multiple heuristics
- **Split input support**: Works with both single field and split digit input fields
- **Visual feedback**: Briefly highlights detected 2FA fields
- **Popup status**: Click the extension icon to see the current code and copy it manually

## How It Works

1. **Google Voice Monitor**: A content script runs on voice.google.com watching for new messages
2. **Code Extraction**: Uses pattern matching to identify 2FA codes (4-8 digits with verification keywords)
3. **Background Coordination**: The background script stores codes and coordinates between tabs
4. **Field Detection**: Content scripts on other sites detect OTP/2FA input fields
5. **Auto-fill**: When both a code and field are detected, the code is automatically filled

## Supported Code Formats

- Standard 4-8 digit codes
- Google format: `G-123456`
- Codes with keywords: "Your verification code is 123456"
- Split input fields (one digit per box)

## Privacy

- All processing happens locally in your browser
- No data is sent to external servers
- Codes are stored temporarily (5 minutes max) and cleared after use

## Troubleshooting

**Code not detected:**
- Make sure Google Voice is open and logged in
- Check that the message contains standard verification keywords
- Try refreshing the Google Voice tab

**Field not detected:**
- Some sites use non-standard input fields
- You can manually copy the code from the extension popup
- Click the extension icon to see and copy the current code

**Code not filling:**
- Some sites block programmatic input
- Try clicking in the field first, then check the popup for the code

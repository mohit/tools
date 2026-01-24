# Quick Setup Guide

This guide will help you get the Personal Assistant iOS app up and running quickly.

## Prerequisites Checklist

- [ ] Mac with Xcode 15.0 or later installed
- [ ] iPhone or iPad running iOS 17.0 or later
- [ ] Active Apple Developer account
- [ ] iCloud account configured on your device
- [ ] Google account (for Google services)

## Step-by-Step Setup

### 1. Clone and Open Project (5 minutes)

```bash
# Navigate to the project directory
cd personal-assistant-ios

# Open in Xcode
open PersonalAssistant.xcodeproj
```

### 2. Configure Xcode Project (10 minutes)

1. **Select your team**:
   - Click on the project in the navigator
   - Select the "PersonalAssistant" target
   - Go to "Signing & Capabilities"
   - Select your team from the dropdown

2. **Update Bundle Identifier**:
   - Change `com.yourcompany.PersonalAssistant` to your own identifier
   - Example: `com.yourname.PersonalAssistant`

3. **Verify Capabilities**:
   - Ensure these capabilities are enabled:
     - HealthKit
     - iCloud (with CloudKit)
     - Push Notifications
     - Background Modes (Background fetch, Remote notifications)

### 3. Configure iCloud & CloudKit (15 minutes)

1. **Create CloudKit Container**:
   ```
   Go to: https://developer.apple.com/icloud/dashboard/
   - Click "+" to create a new container
   - Name it: iCloud.com.yourname.PersonalAssistant
   - Note: Use the same identifier as your bundle ID
   ```

2. **Update Entitlements**:
   - Open `PersonalAssistant.entitlements`
   - Find `com.apple.developer.icloud-container-identifiers`
   - Update the container ID to match your CloudKit container

3. **Update Code**:
   - Open `Services/CloudKitManager.swift`
   - Line 11: Update container identifier:
     ```swift
     container = CKContainer(identifier: "iCloud.com.yourname.PersonalAssistant")
     ```

### 4. Configure Google Services (20 minutes)

#### A. Create Google Cloud Project

1. Go to [Google Cloud Console](https://console.cloud.google.com)
2. Create a new project:
   - Click "Select a project" > "New Project"
   - Name: "Personal Assistant iOS"
   - Click "Create"

#### B. Enable APIs

In your Google Cloud project:

1. Go to **APIs & Services > Library**
2. Search and enable these APIs:
   - **Google People API** (for Contacts)
   - **Google Calendar API**
   - **Gmail API**

#### C. Create OAuth Credentials

1. Go to **APIs & Services > Credentials**
2. Click **Create Credentials > OAuth client ID**
3. If prompted, configure OAuth consent screen:
   - User Type: External
   - App name: Personal Assistant
   - Support email: Your email
   - Scopes: Add the following scopes:
     - `.../auth/userinfo.email`
     - `.../auth/userinfo.profile`
     - `.../auth/contacts.readonly`
     - `.../auth/calendar.readonly`
     - `.../auth/gmail.readonly`
   - Test users: Add your Google account email

4. Create iOS OAuth client:
   - Application type: **iOS**
   - Name: "Personal Assistant iOS"
   - Bundle ID: Your app's bundle identifier
   - Click **Create**

5. **Save your credentials**:
   - Copy the **Client ID** (looks like: `123456-abcdef.apps.googleusercontent.com`)
   - Copy the **iOS URL scheme** (looks like: `com.googleusercontent.apps.123456-abcdef`)

#### D. Update iOS Project

1. **Update Info.plist**:
   - Open `PersonalAssistant/Info.plist`
   - Find `CFBundleURLTypes` > `CFBundleURLSchemes`
   - Replace `com.googleusercontent.apps.YOUR-CLIENT-ID` with your iOS URL scheme
   - Find `GIDClientID`
   - Replace `YOUR-GOOGLE-CLIENT-ID.apps.googleusercontent.com` with your Client ID

2. **Update GoogleAuthService.swift**:
   - Open `Services/GoogleAuthService.swift`
   - Line 9: Replace `YOUR-GOOGLE-CLIENT-ID.apps.googleusercontent.com` with your Client ID
   - Line 10: Replace the redirect URI with your iOS URL scheme

   **Note**: For development purposes, you can use the client secret. For production, implement a backend server for OAuth.

### 5. Build and Test (5 minutes)

1. **Connect your iPhone/iPad**:
   - Connect via USB or wirelessly
   - Trust the device if prompted

2. **Select Device**:
   - In Xcode, select your device from the device dropdown

3. **Build and Run**:
   - Press ⌘R or click the Play button
   - Wait for the app to build and install

4. **Grant Permissions**:
   - When prompted, allow HealthKit access
   - Allow push notifications
   - Sign in with your Apple ID for iCloud (if not already)

### 6. Verify Everything Works

#### Test HealthKit:
1. Open the app
2. Go to the **Health** tab
3. You should see health metrics populated
4. Tap **Sync to CloudKit**
5. Verify the sync succeeds

#### Test Device Messaging:
1. Install the app on another device (iPad, iPhone, or Mac)
2. Sign in with the same Apple ID
3. Open the **Devices** tab
4. You should see your other device listed
5. Tap on it and send a test message

#### Test Google Services:
1. Go to the **Google** tab
2. Tap **Sign in with Google**
3. Sign in with your Google account
4. Authorize the permissions
5. Tap **Refresh All Data**
6. Navigate to Contacts, Calendar, and Gmail to verify data loads

## Troubleshooting

### HealthKit Not Working
- **Error**: "HealthKit is not available on this device"
- **Solution**: You must use a physical device (not Simulator)

### CloudKit Sync Fails
- **Error**: "iCloud account not available"
- **Solutions**:
  1. Go to Settings > [Your Name] > iCloud
  2. Enable iCloud Drive
  3. Ensure you have storage available
  4. Sign out and back into iCloud if needed

### Google Sign-In Fails
- **Error**: "Invalid client ID"
- **Solutions**:
  1. Verify your Client ID is correct in both Info.plist and GoogleAuthService.swift
  2. Ensure the bundle identifier matches your OAuth client
  3. Check that the iOS URL scheme is correct
  4. Verify all required APIs are enabled in Google Cloud Console

### Build Errors
- **Error**: "Provisioning profile doesn't include the HealthKit entitlement"
- **Solution**:
  1. Delete derived data: Xcode > Preferences > Locations > Derived Data > Delete
  2. Clean build folder: Product > Clean Build Folder (⇧⌘K)
  3. Rebuild the project

### Devices Not Showing Up
- **Issue**: Other devices don't appear in Devices tab
- **Solutions**:
  1. Ensure all devices are signed into the same Apple ID
  2. Check iCloud sync is enabled on all devices
  3. Open the app on all devices (the app must be running to register)
  4. Pull to refresh in the Devices tab
  5. Wait a few minutes for CloudKit to sync

## Next Steps

Once everything is working:

1. **Customize the UI**: Modify colors, icons, and layouts in the Views
2. **Add More Health Metrics**: Extend HealthKitManager to read additional data
3. **Implement Backend**: Create a backend server for secure Google OAuth
4. **Add Analytics**: Implement health data trends and visualizations
5. **Test on Multiple Devices**: Verify cross-device sync works reliably

## Getting Help

If you encounter issues:

1. Check the main README.md for detailed documentation
2. Review Apple's documentation:
   - [HealthKit Documentation](https://developer.apple.com/documentation/healthkit)
   - [CloudKit Documentation](https://developer.apple.com/documentation/cloudkit)
3. Review Google's documentation:
   - [OAuth 2.0 for Mobile](https://developers.google.com/identity/protocols/oauth2/native-app)
   - [Google People API](https://developers.google.com/people)

## Security Reminders

- **Never commit** your Google Client Secret to version control
- **Use Keychain** for storing OAuth tokens in production
- **Implement a backend** for OAuth token exchange in production
- **Review permissions** regularly to ensure you're only requesting what you need

---

Happy coding! 🚀

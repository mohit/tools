# Personal Assistant iOS App

A comprehensive personal assistant iOS app that integrates Apple Health, CloudKit sync with end-to-end encryption, cross-device messaging, and Google services (Contacts, Calendar, Gmail).

## Features

### 1. Apple Health Integration with CloudKit Sync
- **HealthKit Integration**: Read comprehensive health data including steps, active energy, heart rate, sleep, distance, and more
- **CloudKit Sync**: Automatically sync health data to CloudKit for access across all your devices
- **End-to-End Encryption**: All health data is encrypted using AES-256-GCM before being uploaded to CloudKit
- **Background Sync**: Automatic background updates when new health data is available
- **Secure Key Management**: Encryption keys are stored in iCloud Keychain and synced across devices

### 2. Cross-Device Messaging
- **Device Discovery**: Automatically discover all devices running this app and logged into the same Apple ID
- **Real-time Messaging**: Send encrypted messages between your devices (iPhone, iPad, Mac)
- **Online Status**: See which devices are currently online
- **Message History**: View conversation history with each device
- **Push Notifications**: Get notified when you receive a new message on any device

### 3. Google Services Integration
- **Google Sign-In**: OAuth 2.0 authentication with Google
- **Contacts**: Access and search your Google contacts
- **Calendar**: View upcoming events, today's schedule, and search events
- **Gmail**: Read emails, view unread count, search messages, and mark as read

## Requirements

- iOS 17.0 or later
- Xcode 15.0 or later
- Active iCloud account
- Google account (for Google services integration)

## Setup Instructions

### 1. Xcode Project Setup

1. Open `PersonalAssistant.xcodeproj` in Xcode
2. Select your development team in the project settings
3. Update the bundle identifier to match your team

### 2. iCloud Configuration

1. Log into [Apple Developer Portal](https://developer.apple.com)
2. Create a new App ID with the following capabilities:
   - HealthKit
   - iCloud (with CloudKit)
   - Push Notifications

3. Create a CloudKit container:
   - Go to CloudKit Dashboard
   - Create a new container: `iCloud.com.yourcompany.PersonalAssistant`
   - Note: Replace `com.yourcompany` with your actual bundle identifier

4. Update the entitlements file:
   - Open `PersonalAssistant.entitlements`
   - Update the CloudKit container identifier to match your container

5. Configure CloudKit Schema:
   - The app will automatically create the necessary record types on first run
   - Record types: `HealthData`, `Device`, `Message`

### 3. HealthKit Configuration

1. In Xcode, ensure HealthKit capability is enabled
2. The Info.plist already includes the required usage descriptions
3. On first launch, the app will request HealthKit permissions

### 4. Google Services Setup

#### Create Google Cloud Project

1. Go to [Google Cloud Console](https://console.cloud.google.com)
2. Create a new project or select an existing one
3. Enable the following APIs:
   - Google People API (for Contacts)
   - Google Calendar API
   - Gmail API

#### Configure OAuth 2.0

1. In Google Cloud Console, go to **APIs & Services > Credentials**
2. Create OAuth 2.0 Client ID:
   - Application type: iOS
   - Bundle ID: Your app's bundle identifier
3. Note your Client ID

4. Add URL scheme to Info.plist:
   - Open `PersonalAssistant/Info.plist`
   - Find `CFBundleURLTypes` > `CFBundleURLSchemes`
   - Replace `YOUR-CLIENT-ID` with your actual Google Client ID

5. Update Google Client ID in code:
   - Open `Services/GoogleAuthService.swift`
   - Replace `YOUR-GOOGLE-CLIENT-ID` with your actual Client ID
   - Replace `YOUR-CLIENT-SECRET` with your actual Client Secret

   **Important**: In production, you should use a backend server to handle OAuth token exchange to keep your client secret secure.

### 5. Build and Run

1. Connect your iOS device (Simulator won't work for HealthKit)
2. Select your device in Xcode
3. Build and run the app (⌘R)
4. Grant permissions when prompted:
   - HealthKit access
   - Push notifications (for messaging)

## Architecture

The app follows a clean MVVM (Model-View-ViewModel) architecture:

```
PersonalAssistant/
├── Models/              # Data models
│   ├── HealthData.swift
│   ├── DeviceInfo.swift
│   └── Message.swift
├── Views/               # SwiftUI views
│   ├── HealthView.swift
│   ├── DevicesView.swift
│   └── GoogleServicesView.swift
├── ViewModels/          # View models
│   ├── HealthViewModel.swift
│   ├── DevicesViewModel.swift
│   └── GoogleServicesViewModel.swift
├── Services/            # Business logic and API integration
│   ├── HealthKitManager.swift
│   ├── CloudKitManager.swift
│   ├── DeviceMessagingService.swift
│   ├── EncryptionHelper.swift
│   ├── GoogleAuthService.swift
│   ├── GoogleContactsService.swift
│   ├── GoogleCalendarService.swift
│   └── GmailService.swift
└── Resources/           # Assets and resources
    └── Assets.xcassets
```

## Security Features

### End-to-End Encryption

All sensitive data (health data and messages) is encrypted before being uploaded to CloudKit:

1. **AES-256-GCM Encryption**: Industry-standard authenticated encryption
2. **Key Management**: Encryption keys are stored in iCloud Keychain
3. **Key Sync**: Keys automatically sync across your devices via iCloud Keychain
4. **No Server-Side Access**: Apple cannot decrypt your data

### OAuth Security

- Google authentication uses OAuth 2.0 with PKCE
- Access tokens are stored securely in UserDefaults (for development)
- **Production Recommendation**: Use Keychain for token storage

## Usage

### Health Data Sync

1. Launch the app and grant HealthKit permissions
2. Go to the **Health** tab
3. Tap **Sync to CloudKit** to upload your health data
4. Data is automatically encrypted and uploaded
5. Access your health data on any device logged into the same Apple ID

### Cross-Device Messaging

1. Install the app on multiple devices (iPhone, iPad, Mac)
2. Log into the same Apple ID on all devices
3. Go to the **Devices** tab
4. You'll see all your other devices listed
5. Tap on a device to start a conversation
6. Messages are end-to-end encrypted

### Google Services

1. Go to the **Google** tab
2. Tap **Sign in with Google**
3. Authorize the requested permissions
4. Access your Contacts, Calendar, and Gmail:
   - **Contacts**: Browse and search your Google contacts
   - **Calendar**: View upcoming events and today's schedule
   - **Gmail**: Read emails, see unread count, mark as read

## Privacy & Data Handling

- **Local Storage**: Minimal data stored locally (only cached for performance)
- **CloudKit**: All data stored in your private CloudKit database (not shared)
- **Encryption**: Health data and messages are encrypted end-to-end
- **Google Data**: OAuth tokens stored locally; data fetched on-demand
- **No Analytics**: The app does not collect any analytics or telemetry

## Troubleshooting

### HealthKit Permission Denied
- Go to Settings > Privacy & Security > Health > PersonalAssistant
- Enable all requested permissions

### CloudKit Not Syncing
- Ensure you're signed into iCloud: Settings > [Your Name] > iCloud
- Check iCloud Drive is enabled
- Verify you have iCloud storage available

### Google Sign-In Fails
- Verify your Google Client ID is correct in Info.plist and GoogleAuthService.swift
- Check that all required APIs are enabled in Google Cloud Console
- Ensure the bundle identifier matches your OAuth client configuration

### Devices Not Appearing
- Ensure all devices are signed into the same Apple ID
- Check that iCloud sync is enabled
- Try pulling to refresh in the Devices tab
- Verify the app is running on the other devices

## Development Notes

### Testing on Simulator

- HealthKit is not available in the iOS Simulator
- CloudKit works in Simulator with some limitations
- Always test on a physical device for full functionality

### CloudKit Development vs. Production

- The app uses CloudKit's default container
- Test data is stored in the Development environment
- Deploy to Production via CloudKit Dashboard when ready

### Google API Quotas

- Be aware of Google API quotas
- Contacts API: 10 requests per second
- Calendar API: 10 requests per second
- Gmail API: 250 quota units per second

## Extending the App

### Adding More Health Metrics

Edit `HealthKitManager.swift` and add new `HKQuantityType` identifiers:

```swift
private var healthDataTypesToRead: Set<HKObjectType> {
    // Add your new health types here
    HKQuantityType(.bodyFatPercentage),
    // ...
}
```

### Custom CloudKit Record Types

To add custom data types:

1. Define a new model in `Models/`
2. Update `CloudKitManager.swift` to handle the new record type
3. Create corresponding UI in `Views/`

### Additional Google Services

To integrate more Google services:

1. Enable the API in Google Cloud Console
2. Add the scope to `GoogleAuthService.swift`
3. Create a new service class (e.g., `GoogleDriveService.swift`)
4. Update the UI in `GoogleServicesView.swift`

## Known Limitations

1. **macOS Support**: While CloudKit works on macOS, HealthKit is iOS-only
2. **Google Client Secret**: The client secret is embedded in the app (use a backend in production)
3. **Message Encryption**: Messages use symmetric encryption (consider asymmetric for better security)
4. **Offline Support**: Limited offline functionality; requires internet for CloudKit sync

## Future Enhancements

- [ ] macOS Catalyst support
- [ ] Apple Watch companion app
- [ ] Export health data to CSV/PDF
- [ ] Advanced health analytics and trends
- [ ] Calendar event creation
- [ ] Email composition in Gmail
- [ ] Siri shortcuts integration
- [ ] Widgets for health summary and upcoming events

## License

MIT License - see LICENSE file for details

## Contributing

This is a personal project, but contributions are welcome! Please open an issue first to discuss proposed changes.

## Support

For issues or questions:
1. Check the Troubleshooting section above
2. Review Apple's HealthKit documentation
3. Check Google's OAuth 2.0 documentation
4. Open an issue in this repository

## Credits

Built with:
- SwiftUI for UI
- HealthKit for health data
- CloudKit for cloud sync
- CryptoKit for encryption
- Google APIs for Google services integration

---

**Note**: This app is for personal use and development purposes. Always review and comply with Apple's App Store Review Guidelines and Google's API Terms of Service before publishing.

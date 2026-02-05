import SwiftUI
import CloudKit

@main
struct PersonalAssistantApp: App {
    @StateObject private var healthViewModel = HealthViewModel()
    @StateObject private var devicesViewModel = DevicesViewModel()
    @StateObject private var googleServicesViewModel = GoogleServicesViewModel()

    init() {
        // Initialize CloudKit container
        CloudKitManager.shared.initialize()

        // Register device on launch
        DeviceMessagingService.shared.registerCurrentDevice()
    }

    var body: some Scene {
        WindowGroup {
            ContentView()
                .environmentObject(healthViewModel)
                .environmentObject(devicesViewModel)
                .environmentObject(googleServicesViewModel)
                .onAppear {
                    // Request HealthKit authorization
                    healthViewModel.requestAuthorization()

                    // Start listening for device updates
                    devicesViewModel.startListening()
                }
        }
    }
}

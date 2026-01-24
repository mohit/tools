import Foundation
import Combine

class DeviceMessagingService: ObservableObject {
    static let shared = DeviceMessagingService()

    @Published var devices: [DeviceInfo] = []
    @Published var messages: [Message] = []
    @Published var currentDevice: DeviceInfo = DeviceInfo.current

    private var timer: Timer?

    private init() {
        setupPeriodicUpdates()
    }

    // MARK: - Device Management

    func registerCurrentDevice() {
        currentDevice = DeviceInfo.current

        CloudKitManager.shared.registerDevice(currentDevice) { success in
            if success {
                print("Device registered successfully")
                self.subscribeToMessages()
            } else {
                print("Failed to register device")
            }
        }
    }

    func fetchDevices() {
        CloudKitManager.shared.fetchAllDevices { [weak self] devices in
            DispatchQueue.main.async {
                self?.devices = devices.filter { $0.id != self?.currentDevice.id }
            }
        }
    }

    func updateDeviceStatus(isOnline: Bool) {
        CloudKitManager.shared.updateDeviceStatus(deviceId: currentDevice.id, isOnline: isOnline) { _ in }
    }

    // MARK: - Messaging

    func sendMessage(to deviceId: String, content: String, completion: @escaping (Bool) -> Void) {
        let message = Message(
            fromDeviceId: currentDevice.id,
            toDeviceId: deviceId,
            content: content
        )

        CloudKitManager.shared.sendMessage(message, completion: completion)
    }

    func fetchMessages() {
        CloudKitManager.shared.fetchMessages(forDeviceId: currentDevice.id) { [weak self] messages in
            DispatchQueue.main.async {
                self?.messages = messages
            }
        }
    }

    func markMessageAsRead(_ message: Message) {
        CloudKitManager.shared.markMessageAsRead(message.id) { success in
            if success {
                self.fetchMessages()
            }
        }
    }

    // MARK: - Real-time Updates

    private func subscribeToMessages() {
        CloudKitManager.shared.subscribeToMessages(forDeviceId: currentDevice.id) { success in
            if success {
                print("Subscribed to messages")
            } else {
                print("Failed to subscribe to messages")
            }
        }
    }

    private func setupPeriodicUpdates() {
        // Update device status and fetch messages every 30 seconds
        timer = Timer.scheduledTimer(withTimeInterval: 30.0, repeats: true) { [weak self] _ in
            self?.updateDeviceStatus(isOnline: true)
            self?.fetchMessages()
            self?.fetchDevices()
        }
    }

    func startListening() {
        fetchDevices()
        fetchMessages()
        updateDeviceStatus(isOnline: true)
    }

    func stopListening() {
        timer?.invalidate()
        timer = nil
        updateDeviceStatus(isOnline: false)
    }

    deinit {
        stopListening()
    }
}

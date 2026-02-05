import Foundation
import Combine

class DevicesViewModel: ObservableObject {
    @Published var devices: [DeviceInfo] = []
    @Published var messages: [Message] = []
    @Published var selectedDevice: DeviceInfo?
    @Published var messageText: String = ""
    @Published var isSending: Bool = false

    private let messagingService = DeviceMessagingService.shared
    private var cancellables = Set<AnyCancellable>()

    init() {
        // Subscribe to messaging service updates
        messagingService.$devices
            .assign(to: &$devices)

        messagingService.$messages
            .assign(to: &$messages)
    }

    func startListening() {
        messagingService.startListening()
    }

    func sendMessage(to device: DeviceInfo) {
        guard !messageText.isEmpty else { return }

        isSending = true

        messagingService.sendMessage(to: device.id, content: messageText) { [weak self] success in
            DispatchQueue.main.async {
                self?.isSending = false
                if success {
                    self?.messageText = ""
                }
            }
        }
    }

    func markAsRead(_ message: Message) {
        messagingService.markMessageAsRead(message)
    }

    func getMessages(for device: DeviceInfo) -> [Message] {
        messages.filter { message in
            (message.fromDeviceId == device.id && message.toDeviceId == messagingService.currentDevice.id) ||
            (message.toDeviceId == device.id && message.fromDeviceId == messagingService.currentDevice.id)
        }
        .sorted { $0.timestamp > $1.timestamp }
    }

    func getUnreadCount(for device: DeviceInfo) -> Int {
        messages.filter { message in
            message.fromDeviceId == device.id &&
            message.toDeviceId == messagingService.currentDevice.id &&
            !message.isRead
        }.count
    }

    func getCurrentDevice() -> DeviceInfo {
        messagingService.currentDevice
    }

    func refreshDevices() {
        messagingService.fetchDevices()
        messagingService.fetchMessages()
    }
}

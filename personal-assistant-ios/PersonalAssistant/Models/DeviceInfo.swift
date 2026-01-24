import Foundation
import UIKit

struct DeviceInfo: Identifiable, Codable {
    let id: String
    let name: String
    let model: String
    let systemVersion: String
    let lastSeen: Date
    var isOnline: Bool

    static var current: DeviceInfo {
        let device = UIDevice.current
        return DeviceInfo(
            id: device.identifierForVendor?.uuidString ?? UUID().uuidString,
            name: device.name,
            model: device.model,
            systemVersion: device.systemVersion,
            lastSeen: Date(),
            isOnline: true
        )
    }

    var displayName: String {
        "\(name) (\(model))"
    }

    var lastSeenText: String {
        let formatter = RelativeDateTimeFormatter()
        formatter.unitsStyle = .abbreviated
        return formatter.localizedString(for: lastSeen, relativeTo: Date())
    }
}

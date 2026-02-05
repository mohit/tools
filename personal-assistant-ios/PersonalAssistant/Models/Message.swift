import Foundation

struct Message: Identifiable, Codable {
    let id: String
    let fromDeviceId: String
    let toDeviceId: String
    let content: String
    let timestamp: Date
    var isRead: Bool

    init(id: String = UUID().uuidString, fromDeviceId: String, toDeviceId: String, content: String, timestamp: Date = Date(), isRead: Bool = false) {
        self.id = id
        self.fromDeviceId = fromDeviceId
        self.toDeviceId = toDeviceId
        self.content = content
        self.timestamp = timestamp
        self.isRead = isRead
    }
}

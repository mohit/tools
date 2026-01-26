import Foundation
import CloudKit

class CloudKitManager: ObservableObject {
    static let shared = CloudKitManager()

    private let container: CKContainer
    private let privateDatabase: CKDatabase

    // Record types
    private let healthDataRecordType = "HealthData"
    private let deviceRecordType = "Device"
    private let messageRecordType = "Message"

    private init() {
        container = CKContainer(identifier: "iCloud.com.yourcompany.PersonalAssistant")
        privateDatabase = container.privateCloudDatabase
    }

    func initialize() {
        // Check iCloud account status
        container.accountStatus { status, error in
            if let error = error {
                print("CloudKit account error: \(error.localizedDescription)")
                return
            }

            switch status {
            case .available:
                print("iCloud account available")
            case .noAccount:
                print("No iCloud account")
            case .restricted:
                print("iCloud account restricted")
            case .couldNotDetermine:
                print("Could not determine iCloud account status")
            case .temporarilyUnavailable:
                print("iCloud temporarily unavailable")
            @unknown default:
                print("Unknown iCloud account status")
            }
        }
    }

    // MARK: - Health Data Sync

    func uploadHealthData(_ healthData: [HealthData], completion: @escaping (Bool) -> Void) {
        guard !healthData.isEmpty else {
            completion(true)
            return
        }

        let group = DispatchGroup()
        var hasError = false

        for data in healthData {
            group.enter()

            do {
                // Encrypt the health data
                let encryptedData = try EncryptionHelper.shared.encryptCodable(data)

                let record = CKRecord(recordType: healthDataRecordType)
                record["id"] = data.id.uuidString as CKRecordValue
                record["encryptedData"] = encryptedData as CKRecordValue
                record["type"] = data.type as CKRecordValue
                record["startDate"] = data.startDate as CKRecordValue
                record["endDate"] = data.endDate as CKRecordValue

                privateDatabase.save(record) { _, error in
                    if let error = error {
                        print("Error saving health data: \(error.localizedDescription)")
                        hasError = true
                    }
                    group.leave()
                }
            } catch {
                print("Error encrypting health data: \(error.localizedDescription)")
                hasError = true
                group.leave()
            }
        }

        group.notify(queue: .main) {
            completion(!hasError)
        }
    }

    func fetchHealthData(since date: Date, completion: @escaping ([HealthData]) -> Void) {
        let predicate = NSPredicate(format: "startDate >= %@", date as NSDate)
        let query = CKQuery(recordType: healthDataRecordType, predicate: predicate)
        query.sortDescriptors = [NSSortDescriptor(key: "startDate", ascending: false)]

        privateDatabase.perform(query, inZoneWith: nil) { records, error in
            if let error = error {
                print("Error fetching health data: \(error.localizedDescription)")
                completion([])
                return
            }

            guard let records = records else {
                completion([])
                return
            }

            var healthData: [HealthData] = []

            for record in records {
                guard let encryptedData = record["encryptedData"] as? Data else {
                    continue
                }

                do {
                    let data = try EncryptionHelper.shared.decryptCodable(encryptedData, as: HealthData.self)
                    healthData.append(data)
                } catch {
                    print("Error decrypting health data: \(error.localizedDescription)")
                }
            }

            completion(healthData)
        }
    }

    // MARK: - Device Management

    func registerDevice(_ device: DeviceInfo, completion: @escaping (Bool) -> Void) {
        let record = CKRecord(recordType: deviceRecordType, recordID: CKRecord.ID(recordName: device.id))
        record["name"] = device.name as CKRecordValue
        record["model"] = device.model as CKRecordValue
        record["systemVersion"] = device.systemVersion as CKRecordValue
        record["lastSeen"] = device.lastSeen as CKRecordValue
        record["isOnline"] = 1 as CKRecordValue

        privateDatabase.save(record) { _, error in
            if let error = error {
                print("Error registering device: \(error.localizedDescription)")
                completion(false)
            } else {
                completion(true)
            }
        }
    }

    func updateDeviceStatus(deviceId: String, isOnline: Bool, completion: @escaping (Bool) -> Void) {
        let recordID = CKRecord.ID(recordName: deviceId)

        privateDatabase.fetch(withRecordID: recordID) { record, error in
            if let error = error {
                print("Error fetching device: \(error.localizedDescription)")
                completion(false)
                return
            }

            guard let record = record else {
                completion(false)
                return
            }

            record["isOnline"] = (isOnline ? 1 : 0) as CKRecordValue
            record["lastSeen"] = Date() as CKRecordValue

            self.privateDatabase.save(record) { _, error in
                if let error = error {
                    print("Error updating device status: \(error.localizedDescription)")
                    completion(false)
                } else {
                    completion(true)
                }
            }
        }
    }

    func fetchAllDevices(completion: @escaping ([DeviceInfo]) -> Void) {
        let predicate = NSPredicate(value: true)
        let query = CKQuery(recordType: deviceRecordType, predicate: predicate)
        query.sortDescriptors = [NSSortDescriptor(key: "lastSeen", ascending: false)]

        privateDatabase.perform(query, inZoneWith: nil) { records, error in
            if let error = error {
                print("Error fetching devices: \(error.localizedDescription)")
                completion([])
                return
            }

            guard let records = records else {
                completion([])
                return
            }

            let devices = records.compactMap { record -> DeviceInfo? in
                guard let name = record["name"] as? String,
                      let model = record["model"] as? String,
                      let systemVersion = record["systemVersion"] as? String,
                      let lastSeen = record["lastSeen"] as? Date,
                      let isOnlineValue = record["isOnline"] as? Int else {
                    return nil
                }

                return DeviceInfo(
                    id: record.recordID.recordName,
                    name: name,
                    model: model,
                    systemVersion: systemVersion,
                    lastSeen: lastSeen,
                    isOnline: isOnlineValue == 1
                )
            }

            completion(devices)
        }
    }

    // MARK: - Messaging

    func sendMessage(_ message: Message, completion: @escaping (Bool) -> Void) {
        do {
            let encryptedContent = try EncryptionHelper.shared.encryptString(message.content)

            let record = CKRecord(recordType: messageRecordType, recordID: CKRecord.ID(recordName: message.id))
            record["fromDeviceId"] = message.fromDeviceId as CKRecordValue
            record["toDeviceId"] = message.toDeviceId as CKRecordValue
            record["encryptedContent"] = encryptedContent as CKRecordValue
            record["timestamp"] = message.timestamp as CKRecordValue
            record["isRead"] = 0 as CKRecordValue

            privateDatabase.save(record) { _, error in
                if let error = error {
                    print("Error sending message: \(error.localizedDescription)")
                    completion(false)
                } else {
                    completion(true)
                }
            }
        } catch {
            print("Error encrypting message: \(error.localizedDescription)")
            completion(false)
        }
    }

    func fetchMessages(forDeviceId deviceId: String, completion: @escaping ([Message]) -> Void) {
        let predicate = NSPredicate(format: "toDeviceId == %@", deviceId)
        let query = CKQuery(recordType: messageRecordType, predicate: predicate)
        query.sortDescriptors = [NSSortDescriptor(key: "timestamp", ascending: false)]

        privateDatabase.perform(query, inZoneWith: nil) { records, error in
            if let error = error {
                print("Error fetching messages: \(error.localizedDescription)")
                completion([])
                return
            }

            guard let records = records else {
                completion([])
                return
            }

            var messages: [Message] = []

            for record in records {
                guard let fromDeviceId = record["fromDeviceId"] as? String,
                      let toDeviceId = record["toDeviceId"] as? String,
                      let encryptedContent = record["encryptedContent"] as? Data,
                      let timestamp = record["timestamp"] as? Date,
                      let isReadValue = record["isRead"] as? Int else {
                    continue
                }

                do {
                    let content = try EncryptionHelper.shared.decryptString(encryptedContent)
                    let message = Message(
                        id: record.recordID.recordName,
                        fromDeviceId: fromDeviceId,
                        toDeviceId: toDeviceId,
                        content: content,
                        timestamp: timestamp,
                        isRead: isReadValue == 1
                    )
                    messages.append(message)
                } catch {
                    print("Error decrypting message: \(error.localizedDescription)")
                }
            }

            completion(messages)
        }
    }

    func markMessageAsRead(_ messageId: String, completion: @escaping (Bool) -> Void) {
        let recordID = CKRecord.ID(recordName: messageId)

        privateDatabase.fetch(withRecordID: recordID) { record, error in
            if let error = error {
                print("Error fetching message: \(error.localizedDescription)")
                completion(false)
                return
            }

            guard let record = record else {
                completion(false)
                return
            }

            record["isRead"] = 1 as CKRecordValue

            self.privateDatabase.save(record) { _, error in
                if let error = error {
                    print("Error marking message as read: \(error.localizedDescription)")
                    completion(false)
                } else {
                    completion(true)
                }
            }
        }
    }

    // MARK: - Subscriptions for Real-time Updates

    func subscribeToMessages(forDeviceId deviceId: String, completion: @escaping (Bool) -> Void) {
        let predicate = NSPredicate(format: "toDeviceId == %@", deviceId)
        let subscription = CKQuerySubscription(
            recordType: messageRecordType,
            predicate: predicate,
            subscriptionID: "messages-\(deviceId)",
            options: .firesOnRecordCreation
        )

        let notification = CKSubscription.NotificationInfo()
        notification.alertBody = "New message received"
        notification.shouldSendContentAvailable = true
        subscription.notificationInfo = notification

        privateDatabase.save(subscription) { _, error in
            if let error = error {
                print("Error creating subscription: \(error.localizedDescription)")
                completion(false)
            } else {
                completion(true)
            }
        }
    }
}

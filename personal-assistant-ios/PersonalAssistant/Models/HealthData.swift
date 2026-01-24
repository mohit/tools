import Foundation
import HealthKit

struct HealthData: Identifiable, Codable {
    let id: UUID
    let type: String
    let value: Double
    let unit: String
    let startDate: Date
    let endDate: Date
    let sourceIdentifier: String

    init(id: UUID = UUID(), type: String, value: Double, unit: String, startDate: Date, endDate: Date, sourceIdentifier: String) {
        self.id = id
        self.type = type
        self.value = value
        self.unit = unit
        self.startDate = startDate
        self.endDate = endDate
        self.sourceIdentifier = sourceIdentifier
    }

    init(from sample: HKQuantitySample) {
        self.id = UUID()
        self.type = sample.quantityType.identifier
        self.value = sample.quantity.doubleValue(for: HKUnit.count())
        self.unit = "count"
        self.startDate = sample.startDate
        self.endDate = sample.endDate
        self.sourceIdentifier = sample.sourceRevision.source.bundleIdentifier
    }
}

struct HealthSummary: Codable {
    var stepCount: Double = 0
    var activeEnergy: Double = 0
    var heartRate: Double = 0
    var sleepHours: Double = 0
    var distance: Double = 0
    var lastSyncDate: Date?
}

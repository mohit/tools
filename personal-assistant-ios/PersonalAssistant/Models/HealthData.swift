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

        // Get the appropriate unit for this quantity type
        let appropriateUnit = HealthData.unit(for: sample.quantityType)
        self.value = sample.quantity.doubleValue(for: appropriateUnit)
        self.unit = appropriateUnit.unitString

        self.startDate = sample.startDate
        self.endDate = sample.endDate
        self.sourceIdentifier = sample.sourceRevision.source.bundleIdentifier
    }

    // Map quantity type to appropriate HKUnit
    private static func unit(for quantityType: HKQuantityType) -> HKUnit {
        switch quantityType.identifier {
        // Count-based metrics
        case HKQuantityTypeIdentifier.stepCount.rawValue:
            return .count()

        // Energy metrics
        case HKQuantityTypeIdentifier.activeEnergyBurned.rawValue,
             HKQuantityTypeIdentifier.basalEnergyBurned.rawValue:
            return .kilocalorie()

        // Distance metrics
        case HKQuantityTypeIdentifier.distanceWalkingRunning.rawValue,
             HKQuantityTypeIdentifier.distanceCycling.rawValue,
             HKQuantityTypeIdentifier.distanceSwimming.rawValue:
            return .meter()

        // Heart rate metrics
        case HKQuantityTypeIdentifier.heartRate.rawValue,
             HKQuantityTypeIdentifier.restingHeartRate.rawValue,
             HKQuantityTypeIdentifier.walkingHeartRateAverage.rawValue:
            return HKUnit.count().unitDivided(by: .minute())

        // Heart rate variability
        case HKQuantityTypeIdentifier.heartRateVariabilitySDNN.rawValue:
            return .secondUnit(with: .milli)

        // Body measurements
        case HKQuantityTypeIdentifier.bodyMass.rawValue:
            return .gramUnit(with: .kilo)

        case HKQuantityTypeIdentifier.height.rawValue:
            return .meter()

        case HKQuantityTypeIdentifier.bodyMassIndex.rawValue:
            return .count()

        // Oxygen saturation
        case HKQuantityTypeIdentifier.oxygenSaturation.rawValue:
            return .percent()

        // Respiratory rate
        case HKQuantityTypeIdentifier.respiratoryRate.rawValue:
            return HKUnit.count().unitDivided(by: .minute())

        // Body temperature
        case HKQuantityTypeIdentifier.bodyTemperature.rawValue:
            return .degreeCelsius()

        // Blood pressure
        case HKQuantityTypeIdentifier.bloodPressureSystolic.rawValue,
             HKQuantityTypeIdentifier.bloodPressureDiastolic.rawValue:
            return .millimeterOfMercury()

        // Blood glucose
        case HKQuantityTypeIdentifier.bloodGlucose.rawValue:
            return HKUnit.gramUnit(with: .milli).unitDivided(by: .literUnit(with: .deci))

        // VO2 Max
        case HKQuantityTypeIdentifier.vo2Max.rawValue:
            return HKUnit.literUnit(with: .milli).unitDivided(by: .gramUnit(with: .kilo).unitMultiplied(by: .minute()))

        // Default to count for unknown types
        default:
            return .count()
        }
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

import Foundation
import HealthKit

class HealthKitManager: ObservableObject {
    static let shared = HealthKitManager()

    private let healthStore = HKHealthStore()

    private init() {}

    // All health data types we want to read
    private var healthDataTypesToRead: Set<HKObjectType> {
        let types: [HKQuantityType] = [
            HKQuantityType(.stepCount),
            HKQuantityType(.activeEnergyBurned),
            HKQuantityType(.heartRate),
            HKQuantityType(.distanceWalkingRunning),
            HKQuantityType(.bodyMass),
            HKQuantityType(.height),
            HKQuantityType(.bodyMassIndex),
            HKQuantityType(.vo2Max),
            HKQuantityType(.restingHeartRate),
            HKQuantityType(.walkingHeartRateAverage),
            HKQuantityType(.heartRateVariabilitySDNN),
            HKQuantityType(.oxygenSaturation),
            HKQuantityType(.respiratoryRate),
            HKQuantityType(.bodyTemperature),
            HKQuantityType(.bloodPressureSystolic),
            HKQuantityType(.bloodPressureDiastolic),
            HKQuantityType(.bloodGlucose)
        ]

        var objectTypes = Set<HKObjectType>(types)

        // Add category types
        if let sleepAnalysis = HKObjectType.categoryType(forIdentifier: .sleepAnalysis) {
            objectTypes.insert(sleepAnalysis)
        }

        // Add workout type
        objectTypes.insert(HKObjectType.workoutType())

        return objectTypes
    }

    func requestAuthorization(completion: @escaping (Bool, Error?) -> Void) {
        guard HKHealthStore.isHealthDataAvailable() else {
            completion(false, NSError(domain: "HealthKit", code: 1, userInfo: [NSLocalizedDescriptionKey: "HealthKit is not available on this device"]))
            return
        }

        healthStore.requestAuthorization(toShare: nil, read: healthDataTypesToRead) { success, error in
            completion(success, error)
        }
    }

    // MARK: - Fetch Health Data

    func fetchStepCount(for date: Date, completion: @escaping (Double) -> Void) {
        guard let stepType = HKQuantityType.quantityType(forIdentifier: .stepCount) else {
            completion(0)
            return
        }

        let startOfDay = Calendar.current.startOfDay(for: date)
        let endOfDay = Calendar.current.date(byAdding: .day, value: 1, to: startOfDay)!

        let predicate = HKQuery.predicateForSamples(withStart: startOfDay, end: endOfDay, options: .strictStartDate)

        let query = HKStatisticsQuery(quantityType: stepType, quantitySamplePredicate: predicate, options: .cumulativeSum) { _, result, _ in
            guard let result = result, let sum = result.sumQuantity() else {
                completion(0)
                return
            }
            completion(sum.doubleValue(for: HKUnit.count()))
        }

        healthStore.execute(query)
    }

    func fetchActiveEnergy(for date: Date, completion: @escaping (Double) -> Void) {
        guard let energyType = HKQuantityType.quantityType(forIdentifier: .activeEnergyBurned) else {
            completion(0)
            return
        }

        let startOfDay = Calendar.current.startOfDay(for: date)
        let endOfDay = Calendar.current.date(byAdding: .day, value: 1, to: startOfDay)!

        let predicate = HKQuery.predicateForSamples(withStart: startOfDay, end: endOfDay, options: .strictStartDate)

        let query = HKStatisticsQuery(quantityType: energyType, quantitySamplePredicate: predicate, options: .cumulativeSum) { _, result, _ in
            guard let result = result, let sum = result.sumQuantity() else {
                completion(0)
                return
            }
            completion(sum.doubleValue(for: HKUnit.kilocalorie()))
        }

        healthStore.execute(query)
    }

    func fetchHeartRate(completion: @escaping (Double) -> Void) {
        guard let heartRateType = HKQuantityType.quantityType(forIdentifier: .heartRate) else {
            completion(0)
            return
        }

        let sortDescriptor = NSSortDescriptor(key: HKSampleSortIdentifierEndDate, ascending: false)

        let query = HKSampleQuery(sampleType: heartRateType, predicate: nil, limit: 1, sortDescriptors: [sortDescriptor]) { _, samples, _ in
            guard let sample = samples?.first as? HKQuantitySample else {
                completion(0)
                return
            }
            completion(sample.quantity.doubleValue(for: HKUnit(from: "count/min")))
        }

        healthStore.execute(query)
    }

    func fetchSleepHours(for date: Date, completion: @escaping (Double) -> Void) {
        guard let sleepType = HKObjectType.categoryType(forIdentifier: .sleepAnalysis) else {
            completion(0)
            return
        }

        let startOfDay = Calendar.current.startOfDay(for: date)
        let endOfDay = Calendar.current.date(byAdding: .day, value: 1, to: startOfDay)!

        let predicate = HKQuery.predicateForSamples(withStart: startOfDay, end: endOfDay, options: .strictStartDate)

        let query = HKSampleQuery(sampleType: sleepType, predicate: predicate, limit: HKObjectQueryNoLimit, sortDescriptors: nil) { _, samples, _ in
            guard let samples = samples as? [HKCategorySample] else {
                completion(0)
                return
            }

            let sleepSamples = samples.filter { $0.value == HKCategoryValueSleepAnalysis.asleepUnspecified.rawValue }
            let totalSeconds = sleepSamples.reduce(0.0) { $0 + $1.endDate.timeIntervalSince($1.startDate) }
            completion(totalSeconds / 3600.0) // Convert to hours
        }

        healthStore.execute(query)
    }

    func fetchDistance(for date: Date, completion: @escaping (Double) -> Void) {
        guard let distanceType = HKQuantityType.quantityType(forIdentifier: .distanceWalkingRunning) else {
            completion(0)
            return
        }

        let startOfDay = Calendar.current.startOfDay(for: date)
        let endOfDay = Calendar.current.date(byAdding: .day, value: 1, to: startOfDay)!

        let predicate = HKQuery.predicateForSamples(withStart: startOfDay, end: endOfDay, options: .strictStartDate)

        let query = HKStatisticsQuery(quantityType: distanceType, quantitySamplePredicate: predicate, options: .cumulativeSum) { _, result, _ in
            guard let result = result, let sum = result.sumQuantity() else {
                completion(0)
                return
            }
            completion(sum.doubleValue(for: HKUnit.meter()))
        }

        healthStore.execute(query)
    }

    // MARK: - Fetch All Data for Sync

    func fetchAllHealthData(since date: Date, completion: @escaping ([HealthData]) -> Void) {
        let group = DispatchGroup()
        let syncQueue = DispatchQueue(label: "com.personalassistant.healthdata.sync")
        var allData: [HealthData] = []

        let types: [HKQuantityTypeIdentifier] = [
            .stepCount,
            .activeEnergyBurned,
            .heartRate,
            .distanceWalkingRunning,
            .bodyMass,
            .height,
            .restingHeartRate
        ]

        for typeIdentifier in types {
            guard let quantityType = HKQuantityType.quantityType(forIdentifier: typeIdentifier) else {
                continue
            }

            group.enter()

            let predicate = HKQuery.predicateForSamples(withStart: date, end: Date(), options: .strictStartDate)
            let sortDescriptor = NSSortDescriptor(key: HKSampleSortIdentifierEndDate, ascending: false)

            let query = HKSampleQuery(sampleType: quantityType, predicate: predicate, limit: HKObjectQueryNoLimit, sortDescriptors: [sortDescriptor]) { _, samples, _ in
                defer { group.leave() }

                guard let samples = samples as? [HKQuantitySample] else { return }

                let healthDataBatch = samples.map { HealthData(from: $0) }

                // Safely append to shared array using serial queue
                syncQueue.async {
                    allData.append(contentsOf: healthDataBatch)
                }
            }

            healthStore.execute(query)
        }

        group.notify(queue: .main) {
            // Ensure final read happens after all writes complete
            syncQueue.async {
                DispatchQueue.main.async {
                    completion(allData)
                }
            }
        }
    }

    // MARK: - Background Sync

    func enableBackgroundSync() {
        let types: [HKQuantityTypeIdentifier] = [
            .stepCount,
            .activeEnergyBurned,
            .heartRate,
            .distanceWalkingRunning
        ]

        for typeIdentifier in types {
            guard let quantityType = HKQuantityType.quantityType(forIdentifier: typeIdentifier) else {
                continue
            }

            let query = HKObserverQuery(sampleType: quantityType, predicate: nil) { [weak self] _, completionHandler, error in
                guard error == nil else {
                    completionHandler()
                    return
                }

                // Sync to CloudKit when new data is available
                self?.syncToCloudKit()
                completionHandler()
            }

            healthStore.execute(query)
        }
    }

    private func syncToCloudKit() {
        // Get last sync date
        let lastSyncDate = UserDefaults.standard.object(forKey: "lastHealthSyncDate") as? Date ?? Date(timeIntervalSince1970: 0)

        fetchAllHealthData(since: lastSyncDate) { healthData in
            CloudKitManager.shared.uploadHealthData(healthData) { success in
                if success {
                    UserDefaults.standard.set(Date(), forKey: "lastHealthSyncDate")
                }
            }
        }
    }
}

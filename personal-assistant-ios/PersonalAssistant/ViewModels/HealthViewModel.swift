import Foundation
import Combine

class HealthViewModel: ObservableObject {
    @Published var healthSummary = HealthSummary()
    @Published var isAuthorized = false
    @Published var isSyncing = false
    @Published var lastSyncDate: Date?
    @Published var errorMessage: String?

    private let healthKitManager = HealthKitManager.shared
    private var cancellables = Set<AnyCancellable>()

    init() {
        loadLastSyncDate()
    }

    func requestAuthorization() {
        healthKitManager.requestAuthorization { [weak self] success, error in
            DispatchQueue.main.async {
                self?.isAuthorized = success
                if let error = error {
                    self?.errorMessage = error.localizedDescription
                } else if success {
                    self?.fetchHealthData()
                    self?.enableBackgroundSync()
                }
            }
        }
    }

    func fetchHealthData() {
        let today = Date()

        healthKitManager.fetchStepCount(for: today) { [weak self] steps in
            DispatchQueue.main.async {
                self?.healthSummary.stepCount = steps
            }
        }

        healthKitManager.fetchActiveEnergy(for: today) { [weak self] energy in
            DispatchQueue.main.async {
                self?.healthSummary.activeEnergy = energy
            }
        }

        healthKitManager.fetchHeartRate { [weak self] heartRate in
            DispatchQueue.main.async {
                self?.healthSummary.heartRate = heartRate
            }
        }

        healthKitManager.fetchSleepHours(for: today) { [weak self] sleep in
            DispatchQueue.main.async {
                self?.healthSummary.sleepHours = sleep
            }
        }

        healthKitManager.fetchDistance(for: today) { [weak self] distance in
            DispatchQueue.main.async {
                self?.healthSummary.distance = distance
            }
        }
    }

    func syncToCloudKit() {
        guard isAuthorized else {
            errorMessage = "HealthKit not authorized"
            return
        }

        isSyncing = true
        let lastSync = lastSyncDate ?? Date(timeIntervalSince1970: 0)

        healthKitManager.fetchAllHealthData(since: lastSync) { [weak self] healthData in
            CloudKitManager.shared.uploadHealthData(healthData) { success in
                DispatchQueue.main.async {
                    self?.isSyncing = false
                    if success {
                        self?.lastSyncDate = Date()
                        self?.healthSummary.lastSyncDate = Date()
                        self?.saveLastSyncDate()
                    } else {
                        self?.errorMessage = "Failed to sync health data"
                    }
                }
            }
        }
    }

    func enableBackgroundSync() {
        healthKitManager.enableBackgroundSync()
    }

    private func loadLastSyncDate() {
        if let date = UserDefaults.standard.object(forKey: "lastHealthSyncDate") as? Date {
            lastSyncDate = date
            healthSummary.lastSyncDate = date
        }
    }

    private func saveLastSyncDate() {
        if let date = lastSyncDate {
            UserDefaults.standard.set(date, forKey: "lastHealthSyncDate")
        }
    }

    func formattedStepCount() -> String {
        return "\(Int(healthSummary.stepCount))"
    }

    func formattedActiveEnergy() -> String {
        return String(format: "%.0f kcal", healthSummary.activeEnergy)
    }

    func formattedHeartRate() -> String {
        return healthSummary.heartRate > 0 ? "\(Int(healthSummary.heartRate)) bpm" : "--"
    }

    func formattedSleepHours() -> String {
        return healthSummary.sleepHours > 0 ? String(format: "%.1f hrs", healthSummary.sleepHours) : "--"
    }

    func formattedDistance() -> String {
        let km = healthSummary.distance / 1000.0
        return String(format: "%.2f km", km)
    }
}

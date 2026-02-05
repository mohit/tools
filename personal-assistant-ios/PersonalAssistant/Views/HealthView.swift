import SwiftUI

struct HealthView: View {
    @EnvironmentObject var viewModel: HealthViewModel

    var body: some View {
        NavigationView {
            ScrollView {
                VStack(spacing: 20) {
                    // Sync Status Card
                    syncStatusCard

                    // Health Metrics
                    healthMetricsGrid

                    // Sync Button
                    syncButton

                    if let errorMessage = viewModel.errorMessage {
                        Text(errorMessage)
                            .foregroundColor(.red)
                            .padding()
                    }
                }
                .padding()
            }
            .navigationTitle("Health Data")
            .toolbar {
                ToolbarItem(placement: .navigationBarTrailing) {
                    Button(action: {
                        viewModel.fetchHealthData()
                    }) {
                        Image(systemName: "arrow.clockwise")
                    }
                }
            }
        }
    }

    private var syncStatusCard: some View {
        VStack(alignment: .leading, spacing: 8) {
            HStack {
                Image(systemName: "cloud.fill")
                    .foregroundColor(.blue)
                Text("CloudKit Sync")
                    .font(.headline)
                Spacer()
                if viewModel.isSyncing {
                    ProgressView()
                } else if viewModel.lastSyncDate != nil {
                    Image(systemName: "checkmark.circle.fill")
                        .foregroundColor(.green)
                }
            }

            if let lastSync = viewModel.lastSyncDate {
                Text("Last synced: \(lastSync, formatter: dateFormatter)")
                    .font(.caption)
                    .foregroundColor(.secondary)
            } else {
                Text("Not synced yet")
                    .font(.caption)
                    .foregroundColor(.secondary)
            }
        }
        .padding()
        .background(Color(.systemGray6))
        .cornerRadius(12)
    }

    private var healthMetricsGrid: some View {
        LazyVGrid(columns: [GridItem(.flexible()), GridItem(.flexible())], spacing: 16) {
            HealthMetricCard(
                title: "Steps",
                value: viewModel.formattedStepCount(),
                icon: "figure.walk",
                color: .blue
            )

            HealthMetricCard(
                title: "Active Energy",
                value: viewModel.formattedActiveEnergy(),
                icon: "flame.fill",
                color: .orange
            )

            HealthMetricCard(
                title: "Heart Rate",
                value: viewModel.formattedHeartRate(),
                icon: "heart.fill",
                color: .red
            )

            HealthMetricCard(
                title: "Sleep",
                value: viewModel.formattedSleepHours(),
                icon: "moon.fill",
                color: .purple
            )

            HealthMetricCard(
                title: "Distance",
                value: viewModel.formattedDistance(),
                icon: "location.fill",
                color: .green
            )
        }
    }

    private var syncButton: some View {
        Button(action: {
            viewModel.syncToCloudKit()
        }) {
            HStack {
                Image(systemName: "icloud.and.arrow.up")
                Text(viewModel.isSyncing ? "Syncing..." : "Sync to CloudKit")
            }
            .frame(maxWidth: .infinity)
            .padding()
            .background(Color.blue)
            .foregroundColor(.white)
            .cornerRadius(12)
        }
        .disabled(viewModel.isSyncing || !viewModel.isAuthorized)
    }

    private var dateFormatter: DateFormatter {
        let formatter = DateFormatter()
        formatter.dateStyle = .short
        formatter.timeStyle = .short
        return formatter
    }
}

struct HealthMetricCard: View {
    let title: String
    let value: String
    let icon: String
    let color: Color

    var body: some View {
        VStack(spacing: 12) {
            HStack {
                Image(systemName: icon)
                    .foregroundColor(color)
                    .font(.title2)
                Spacer()
            }

            VStack(alignment: .leading, spacing: 4) {
                Text(title)
                    .font(.caption)
                    .foregroundColor(.secondary)
                Text(value)
                    .font(.title3)
                    .fontWeight(.semibold)
            }
            .frame(maxWidth: .infinity, alignment: .leading)
        }
        .padding()
        .background(Color(.systemGray6))
        .cornerRadius(12)
    }
}

#Preview {
    HealthView()
        .environmentObject(HealthViewModel())
}

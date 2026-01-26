import SwiftUI

struct DevicesView: View {
    @EnvironmentObject var viewModel: DevicesViewModel

    var body: some View {
        NavigationView {
            VStack {
                if viewModel.devices.isEmpty {
                    emptyState
                } else {
                    deviceList
                }
            }
            .navigationTitle("Devices")
            .toolbar {
                ToolbarItem(placement: .navigationBarTrailing) {
                    Button(action: {
                        viewModel.refreshDevices()
                    }) {
                        Image(systemName: "arrow.clockwise")
                    }
                }
            }
        }
    }

    private var emptyState: some View {
        VStack(spacing: 16) {
            Image(systemName: "iphone.and.laptop")
                .font(.system(size: 60))
                .foregroundColor(.secondary)

            Text("No Other Devices")
                .font(.title2)
                .fontWeight(.semibold)

            Text("Install this app on other devices signed in with the same Apple ID to see them here.")
                .multilineTextAlignment(.center)
                .foregroundColor(.secondary)
                .padding(.horizontal)
        }
        .padding()
    }

    private var deviceList: some View {
        List {
            Section {
                currentDeviceRow
            } header: {
                Text("Current Device")
            }

            Section {
                ForEach(viewModel.devices) { device in
                    NavigationLink(destination: DeviceDetailView(device: device)) {
                        DeviceRow(device: device, unreadCount: viewModel.getUnreadCount(for: device))
                    }
                }
            } header: {
                Text("Other Devices")
            }
        }
    }

    private var currentDeviceRow: some View {
        HStack {
            deviceIcon(for: viewModel.getCurrentDevice().model)

            VStack(alignment: .leading, spacing: 4) {
                Text(viewModel.getCurrentDevice().name)
                    .font(.headline)
                Text(viewModel.getCurrentDevice().model)
                    .font(.caption)
                    .foregroundColor(.secondary)
            }

            Spacer()

            HStack(spacing: 4) {
                Circle()
                    .fill(Color.green)
                    .frame(width: 8, height: 8)
                Text("This Device")
                    .font(.caption)
                    .foregroundColor(.secondary)
            }
        }
        .padding(.vertical, 4)
    }

    private func deviceIcon(for model: String) -> some View {
        Image(systemName: model.contains("iPad") ? "ipad" : model.contains("Mac") ? "laptopcomputer" : "iphone")
            .font(.title2)
            .foregroundColor(.blue)
            .frame(width: 40)
    }
}

struct DeviceRow: View {
    let device: DeviceInfo
    let unreadCount: Int

    var body: some View {
        HStack {
            deviceIcon

            VStack(alignment: .leading, spacing: 4) {
                Text(device.name)
                    .font(.headline)

                HStack(spacing: 8) {
                    Text(device.model)
                        .font(.caption)
                        .foregroundColor(.secondary)

                    Circle()
                        .fill(Color.secondary.opacity(0.5))
                        .frame(width: 3, height: 3)

                    Text(device.lastSeenText)
                        .font(.caption)
                        .foregroundColor(.secondary)
                }
            }

            Spacer()

            VStack(alignment: .trailing, spacing: 4) {
                HStack(spacing: 4) {
                    Circle()
                        .fill(device.isOnline ? Color.green : Color.gray)
                        .frame(width: 8, height: 8)
                    Text(device.isOnline ? "Online" : "Offline")
                        .font(.caption)
                        .foregroundColor(.secondary)
                }

                if unreadCount > 0 {
                    Text("\(unreadCount)")
                        .font(.caption)
                        .fontWeight(.semibold)
                        .foregroundColor(.white)
                        .padding(.horizontal, 8)
                        .padding(.vertical, 2)
                        .background(Color.blue)
                        .cornerRadius(10)
                }
            }
        }
        .padding(.vertical, 4)
    }

    private var deviceIcon: some View {
        Image(systemName: device.model.contains("iPad") ? "ipad" : device.model.contains("Mac") ? "laptopcomputer" : "iphone")
            .font(.title2)
            .foregroundColor(.blue)
            .frame(width: 40)
    }
}

struct DeviceDetailView: View {
    let device: DeviceInfo
    @EnvironmentObject var viewModel: DevicesViewModel
    @State private var messageText = ""

    var body: some View {
        VStack(spacing: 0) {
            // Messages List
            ScrollView {
                LazyVStack(spacing: 12) {
                    ForEach(viewModel.getMessages(for: device)) { message in
                        MessageBubble(message: message, isFromCurrentDevice: message.fromDeviceId == viewModel.getCurrentDevice().id)
                            .onAppear {
                                if !message.isRead && message.toDeviceId == viewModel.getCurrentDevice().id {
                                    viewModel.markAsRead(message)
                                }
                            }
                    }
                }
                .padding()
            }

            // Message Input
            HStack {
                TextField("Type a message...", text: $messageText)
                    .textFieldStyle(RoundedBorderTextFieldStyle())
                    .frame(minHeight: 40)

                Button(action: {
                    sendMessage()
                }) {
                    Image(systemName: "arrow.up.circle.fill")
                        .font(.title2)
                        .foregroundColor(messageText.isEmpty ? .gray : .blue)
                }
                .disabled(messageText.isEmpty || viewModel.isSending)
            }
            .padding()
            .background(Color(.systemGray6))
        }
        .navigationTitle(device.name)
        .navigationBarTitleDisplayMode(.inline)
    }

    private func sendMessage() {
        viewModel.messageText = messageText
        viewModel.sendMessage(to: device)
        messageText = ""
    }
}

struct MessageBubble: View {
    let message: Message
    let isFromCurrentDevice: Bool

    var body: some View {
        HStack {
            if isFromCurrentDevice {
                Spacer()
            }

            VStack(alignment: isFromCurrentDevice ? .trailing : .leading, spacing: 4) {
                Text(message.content)
                    .padding(12)
                    .background(isFromCurrentDevice ? Color.blue : Color(.systemGray5))
                    .foregroundColor(isFromCurrentDevice ? .white : .primary)
                    .cornerRadius(16)

                Text(message.timestamp, formatter: timeFormatter)
                    .font(.caption2)
                    .foregroundColor(.secondary)
            }

            if !isFromCurrentDevice {
                Spacer()
            }
        }
    }

    private var timeFormatter: DateFormatter {
        let formatter = DateFormatter()
        formatter.timeStyle = .short
        return formatter
    }
}

#Preview {
    DevicesView()
        .environmentObject(DevicesViewModel())
}

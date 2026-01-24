import SwiftUI

struct GoogleServicesView: View {
    @EnvironmentObject var viewModel: GoogleServicesViewModel

    var body: some View {
        NavigationView {
            Group {
                if viewModel.isAuthenticated {
                    authenticatedView
                } else {
                    signInView
                }
            }
            .navigationTitle("Google Services")
        }
    }

    private var signInView: some View {
        VStack(spacing: 24) {
            Image(systemName: "g.circle.fill")
                .font(.system(size: 80))
                .foregroundColor(.blue)

            VStack(spacing: 8) {
                Text("Sign in with Google")
                    .font(.title2)
                    .fontWeight(.semibold)

                Text("Access your contacts, calendar, and Gmail")
                    .multilineTextAlignment(.center)
                    .foregroundColor(.secondary)
                    .padding(.horizontal)
            }

            Button(action: {
                viewModel.signIn()
            }) {
                HStack {
                    Image(systemName: "g.circle.fill")
                    Text("Sign in with Google")
                }
                .frame(maxWidth: .infinity)
                .padding()
                .background(Color.blue)
                .foregroundColor(.white)
                .cornerRadius(12)
            }
            .padding(.horizontal, 40)
        }
        .padding()
    }

    private var authenticatedView: some View {
        List {
            Section {
                HStack {
                    Image(systemName: "person.circle.fill")
                        .font(.title)
                        .foregroundColor(.blue)

                    VStack(alignment: .leading) {
                        Text(viewModel.userEmail ?? "Unknown")
                            .font(.headline)
                        Text("Google Account")
                            .font(.caption)
                            .foregroundColor(.secondary)
                    }

                    Spacer()

                    Button("Sign Out") {
                        viewModel.signOut()
                    }
                    .font(.caption)
                    .foregroundColor(.red)
                }
                .padding(.vertical, 4)
            }

            Section {
                NavigationLink(destination: ContactsListView()) {
                    ServiceRow(
                        icon: "person.2.fill",
                        title: "Contacts",
                        subtitle: "\(viewModel.contacts.count) contacts",
                        color: .blue
                    )
                }

                NavigationLink(destination: CalendarListView()) {
                    ServiceRow(
                        icon: "calendar",
                        title: "Calendar",
                        subtitle: "\(viewModel.events.count) events",
                        color: .red
                    )
                }

                NavigationLink(destination: GmailListView()) {
                    ServiceRow(
                        icon: "envelope.fill",
                        title: "Gmail",
                        subtitle: "\(viewModel.unreadCount) unread",
                        color: .orange,
                        badge: viewModel.unreadCount > 0 ? "\(viewModel.unreadCount)" : nil
                    )
                }
            } header: {
                Text("Services")
            }

            Section {
                Button(action: {
                    viewModel.fetchAllGoogleData()
                }) {
                    HStack {
                        Image(systemName: "arrow.clockwise")
                        Text("Refresh All Data")
                    }
                }
            }
        }
    }
}

struct ServiceRow: View {
    let icon: String
    let title: String
    let subtitle: String
    let color: Color
    var badge: String? = nil

    var body: some View {
        HStack {
            Image(systemName: icon)
                .font(.title2)
                .foregroundColor(color)
                .frame(width: 40)

            VStack(alignment: .leading, spacing: 4) {
                Text(title)
                    .font(.headline)
                Text(subtitle)
                    .font(.caption)
                    .foregroundColor(.secondary)
            }

            Spacer()

            if let badge = badge {
                Text(badge)
                    .font(.caption)
                    .fontWeight(.semibold)
                    .foregroundColor(.white)
                    .padding(.horizontal, 8)
                    .padding(.vertical, 2)
                    .background(color)
                    .cornerRadius(10)
            }
        }
        .padding(.vertical, 4)
    }
}

// MARK: - Contacts List View

struct ContactsListView: View {
    @EnvironmentObject var viewModel: GoogleServicesViewModel
    @State private var searchText = ""

    var body: some View {
        List {
            ForEach(filteredContacts) { contact in
                ContactRow(contact: contact)
            }
        }
        .navigationTitle("Contacts")
        .searchable(text: $searchText, prompt: "Search contacts")
        .onAppear {
            if viewModel.contacts.isEmpty {
                viewModel.fetchContacts()
            }
        }
    }

    private var filteredContacts: [GoogleContact] {
        if searchText.isEmpty {
            return viewModel.contacts
        }
        return viewModel.searchContacts(query: searchText)
    }
}

struct ContactRow: View {
    let contact: GoogleContact

    var body: some View {
        VStack(alignment: .leading, spacing: 8) {
            Text(contact.name)
                .font(.headline)

            if !contact.emailAddresses.isEmpty {
                ForEach(contact.emailAddresses, id: \.self) { email in
                    HStack {
                        Image(systemName: "envelope.fill")
                            .font(.caption)
                            .foregroundColor(.secondary)
                        Text(email)
                            .font(.caption)
                            .foregroundColor(.secondary)
                    }
                }
            }

            if !contact.phoneNumbers.isEmpty {
                ForEach(contact.phoneNumbers, id: \.self) { phone in
                    HStack {
                        Image(systemName: "phone.fill")
                            .font(.caption)
                            .foregroundColor(.secondary)
                        Text(phone)
                            .font(.caption)
                            .foregroundColor(.secondary)
                    }
                }
            }
        }
        .padding(.vertical, 4)
    }
}

// MARK: - Calendar List View

struct CalendarListView: View {
    @EnvironmentObject var viewModel: GoogleServicesViewModel
    @State private var showUpcomingOnly = true

    var body: some View {
        List {
            Section {
                Toggle("Show Upcoming Only", isOn: $showUpcomingOnly)
            }

            Section {
                ForEach(displayedEvents) { event in
                    CalendarEventRow(event: event)
                }
            } header: {
                Text(showUpcomingOnly ? "Upcoming Events" : "All Events")
            }
        }
        .navigationTitle("Calendar")
        .onAppear {
            if viewModel.events.isEmpty {
                viewModel.fetchCalendarEvents()
            }
        }
    }

    private var displayedEvents: [CalendarEvent] {
        if showUpcomingOnly {
            return viewModel.getUpcomingEvents(count: 20)
        }
        return viewModel.events
    }
}

struct CalendarEventRow: View {
    let event: CalendarEvent

    var body: some View {
        VStack(alignment: .leading, spacing: 8) {
            Text(event.summary)
                .font(.headline)

            HStack {
                Image(systemName: "calendar")
                    .font(.caption)
                    .foregroundColor(.secondary)
                Text(event.startDate, style: .date)
                    .font(.caption)
                    .foregroundColor(.secondary)

                Image(systemName: "clock")
                    .font(.caption)
                    .foregroundColor(.secondary)
                Text(event.startDate, style: .time)
                    .font(.caption)
                    .foregroundColor(.secondary)
            }

            if let location = event.location {
                HStack {
                    Image(systemName: "location.fill")
                        .font(.caption)
                        .foregroundColor(.secondary)
                    Text(location)
                        .font(.caption)
                        .foregroundColor(.secondary)
                }
            }

            if let description = event.description {
                Text(description)
                    .font(.caption)
                    .foregroundColor(.secondary)
                    .lineLimit(2)
            }
        }
        .padding(.vertical, 4)
    }
}

// MARK: - Gmail List View

struct GmailListView: View {
    @EnvironmentObject var viewModel: GoogleServicesViewModel
    @State private var searchText = ""
    @State private var showUnreadOnly = false

    var body: some View {
        List {
            Section {
                Toggle("Show Unread Only", isOn: $showUnreadOnly)
            }

            Section {
                ForEach(displayedMessages) { message in
                    GmailMessageRow(message: message)
                        .onTapGesture {
                            if message.isUnread {
                                viewModel.markAsRead(messageId: message.id)
                            }
                        }
                }
            } header: {
                Text("\(displayedMessages.count) messages")
            }
        }
        .navigationTitle("Gmail")
        .searchable(text: $searchText, prompt: "Search messages")
        .onAppear {
            if viewModel.messages.isEmpty {
                viewModel.fetchGmail()
            }
        }
    }

    private var displayedMessages: [GmailMessage] {
        var messages = viewModel.messages

        if showUnreadOnly {
            messages = viewModel.getUnreadMessages()
        }

        if !searchText.isEmpty {
            messages = viewModel.searchMessages(query: searchText)
        }

        return messages
    }
}

struct GmailMessageRow: View {
    let message: GmailMessage

    var body: some View {
        VStack(alignment: .leading, spacing: 8) {
            HStack {
                Text(message.subject)
                    .font(.headline)
                    .fontWeight(message.isUnread ? .bold : .regular)

                Spacer()

                if message.isUnread {
                    Circle()
                        .fill(Color.blue)
                        .frame(width: 8, height: 8)
                }
            }

            Text(message.from)
                .font(.caption)
                .foregroundColor(.secondary)

            Text(message.snippet)
                .font(.caption)
                .foregroundColor(.secondary)
                .lineLimit(2)

            Text(message.date, style: .relative)
                .font(.caption2)
                .foregroundColor(.secondary)
        }
        .padding(.vertical, 4)
    }
}

#Preview {
    GoogleServicesView()
        .environmentObject(GoogleServicesViewModel())
}

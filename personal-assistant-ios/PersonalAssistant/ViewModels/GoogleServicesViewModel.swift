import Foundation
import Combine

class GoogleServicesViewModel: ObservableObject {
    @Published var isAuthenticated = false
    @Published var userEmail: String?
    @Published var contacts: [GoogleContact] = []
    @Published var events: [CalendarEvent] = []
    @Published var messages: [GmailMessage] = []
    @Published var unreadCount: Int = 0

    private let authService = GoogleAuthService.shared
    private let contactsService = GoogleContactsService.shared
    private let calendarService = GoogleCalendarService.shared
    private let gmailService = GmailService.shared

    private var cancellables = Set<AnyCancellable>()

    init() {
        // Subscribe to auth service
        authService.$isAuthenticated
            .assign(to: &$isAuthenticated)

        authService.$userEmail
            .assign(to: &$userEmail)

        // Subscribe to contacts service
        contactsService.$contacts
            .assign(to: &$contacts)

        // Subscribe to calendar service
        calendarService.$events
            .assign(to: &$events)

        // Subscribe to Gmail service
        gmailService.$messages
            .assign(to: &$messages)

        gmailService.$unreadCount
            .assign(to: &$unreadCount)
    }

    // MARK: - Authentication

    func signIn() {
        authService.signIn()
    }

    func signOut() {
        authService.signOut()
    }

    // MARK: - Contacts

    func fetchContacts() {
        guard isAuthenticated else { return }
        contactsService.fetchContacts()
    }

    func searchContacts(query: String) -> [GoogleContact] {
        contactsService.searchContacts(query: query)
    }

    // MARK: - Calendar

    func fetchCalendarEvents() {
        guard isAuthenticated else { return }
        calendarService.fetchEvents()
    }

    func getUpcomingEvents(count: Int = 5) -> [CalendarEvent] {
        calendarService.getUpcomingEvents(count: count)
    }

    func getTodayEvents() -> [CalendarEvent] {
        calendarService.getTodayEvents()
    }

    // MARK: - Gmail

    func fetchGmail() {
        guard isAuthenticated else { return }
        gmailService.fetchMessages()
    }

    func getUnreadMessages() -> [GmailMessage] {
        gmailService.getUnreadMessages()
    }

    func searchMessages(query: String) -> [GmailMessage] {
        gmailService.searchMessages(query: query)
    }

    func markAsRead(messageId: String) {
        gmailService.markAsRead(messageId: messageId) { _ in }
    }

    // MARK: - Convenience

    func fetchAllGoogleData() {
        guard isAuthenticated else { return }
        fetchContacts()
        fetchCalendarEvents()
        fetchGmail()
    }
}

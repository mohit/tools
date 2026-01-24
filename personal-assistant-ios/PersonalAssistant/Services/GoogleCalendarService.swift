import Foundation

struct CalendarEvent: Identifiable, Codable {
    let id: String
    let summary: String
    let description: String?
    let startDate: Date
    let endDate: Date
    let location: String?
    let attendees: [String]
    let colorId: String?
}

class GoogleCalendarService: ObservableObject {
    static let shared = GoogleCalendarService()

    @Published var events: [CalendarEvent] = []
    @Published var isLoading = false

    private let dateFormatter: ISO8601DateFormatter = {
        let formatter = ISO8601DateFormatter()
        formatter.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
        return formatter
    }()

    private init() {}

    func fetchEvents(from startDate: Date = Date(), to endDate: Date? = nil) {
        guard let accessToken = GoogleAuthService.shared.accessToken else {
            print("No access token available")
            return
        }

        isLoading = true

        let end = endDate ?? Calendar.current.date(byAdding: .month, value: 1, to: startDate)!
        let startISO = dateFormatter.string(from: startDate)
        let endISO = dateFormatter.string(from: end)

        let urlString = "https://www.googleapis.com/calendar/v3/calendars/primary/events?timeMin=\(startISO)&timeMax=\(endISO)&singleEvents=true&orderBy=startTime"

        guard let url = URL(string: urlString) else {
            isLoading = false
            return
        }

        var request = URLRequest(url: url)
        request.setValue("Bearer \(accessToken)", forHTTPHeaderField: "Authorization")

        URLSession.shared.dataTask(with: request) { [weak self] data, response, error in
            DispatchQueue.main.async {
                self?.isLoading = false
            }

            guard let data = data, error == nil else {
                print("Error fetching events: \(error?.localizedDescription ?? "Unknown error")")
                return
            }

            do {
                if let json = try JSONSerialization.jsonObject(with: data) as? [String: Any],
                   let items = json["items"] as? [[String: Any]] {
                    let events = items.compactMap { self?.parseEvent($0) }

                    DispatchQueue.main.async {
                        self?.events = events
                    }
                }
            } catch {
                print("Error parsing events: \(error.localizedDescription)")
            }
        }.resume()
    }

    private func parseEvent(_ json: [String: Any]) -> CalendarEvent? {
        guard let id = json["id"] as? String,
              let summary = json["summary"] as? String else {
            return nil
        }

        let description = json["description"] as? String
        let location = json["location"] as? String
        let colorId = json["colorId"] as? String

        // Parse dates
        let start = json["start"] as? [String: Any]
        let end = json["end"] as? [String: Any]

        guard let startDateString = start?["dateTime"] as? String ?? start?["date"] as? String,
              let endDateString = end?["dateTime"] as? String ?? end?["date"] as? String else {
            return nil
        }

        let startDate = parseDate(startDateString) ?? Date()
        let endDate = parseDate(endDateString) ?? Date()

        // Parse attendees
        let attendeesArray = json["attendees"] as? [[String: Any]]
        let attendees = attendeesArray?.compactMap { $0["email"] as? String } ?? []

        return CalendarEvent(
            id: id,
            summary: summary,
            description: description,
            startDate: startDate,
            endDate: endDate,
            location: location,
            attendees: attendees,
            colorId: colorId
        )
    }

    private func parseDate(_ dateString: String) -> Date? {
        // Try ISO8601 format first
        if let date = dateFormatter.date(from: dateString) {
            return date
        }

        // Try date-only format (all-day events)
        let simpleDateFormatter = DateFormatter()
        simpleDateFormatter.dateFormat = "yyyy-MM-dd"
        return simpleDateFormatter.date(from: dateString)
    }

    func getUpcomingEvents(count: Int = 5) -> [CalendarEvent] {
        let now = Date()
        return events
            .filter { $0.startDate >= now }
            .sorted { $0.startDate < $1.startDate }
            .prefix(count)
            .map { $0 }
    }

    func getTodayEvents() -> [CalendarEvent] {
        let calendar = Calendar.current
        let today = calendar.startOfDay(for: Date())
        let tomorrow = calendar.date(byAdding: .day, value: 1, to: today)!

        return events.filter { event in
            event.startDate >= today && event.startDate < tomorrow
        }
    }
}

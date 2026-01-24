import Foundation

struct GmailMessage: Identifiable, Codable {
    let id: String
    let threadId: String
    let subject: String
    let from: String
    let to: String
    let date: Date
    let snippet: String
    let isUnread: Bool
    let labels: [String]
}

class GmailService: ObservableObject {
    static let shared = GmailService()

    @Published var messages: [GmailMessage] = []
    @Published var unreadCount: Int = 0
    @Published var isLoading = false

    private init() {}

    func fetchMessages(maxResults: Int = 50) {
        guard let accessToken = GoogleAuthService.shared.accessToken else {
            print("No access token available")
            return
        }

        isLoading = true

        let urlString = "https://gmail.googleapis.com/gmail/v1/users/me/messages?maxResults=\(maxResults)"

        guard let url = URL(string: urlString) else {
            isLoading = false
            return
        }

        var request = URLRequest(url: url)
        request.setValue("Bearer \(accessToken)", forHTTPHeaderField: "Authorization")

        URLSession.shared.dataTask(with: request) { [weak self] data, response, error in
            guard let data = data, error == nil else {
                print("Error fetching messages: \(error?.localizedDescription ?? "Unknown error")")
                DispatchQueue.main.async {
                    self?.isLoading = false
                }
                return
            }

            do {
                if let json = try JSONSerialization.jsonObject(with: data) as? [String: Any],
                   let messages = json["messages"] as? [[String: Any]] {
                    let messageIds = messages.compactMap { $0["id"] as? String }
                    self?.fetchMessageDetails(messageIds)
                }
            } catch {
                print("Error parsing message list: \(error.localizedDescription)")
                DispatchQueue.main.async {
                    self?.isLoading = false
                }
            }
        }.resume()
    }

    private func fetchMessageDetails(_ messageIds: [String]) {
        guard let accessToken = GoogleAuthService.shared.accessToken else {
            isLoading = false
            return
        }

        let group = DispatchGroup()
        let syncQueue = DispatchQueue(label: "com.personalassistant.gmail.sync")
        var fetchedMessages: [GmailMessage] = []

        for messageId in messageIds {
            group.enter()

            let urlString = "https://gmail.googleapis.com/gmail/v1/users/me/messages/\(messageId)?format=metadata&metadataHeaders=From&metadataHeaders=To&metadataHeaders=Subject&metadataHeaders=Date"

            guard let url = URL(string: urlString) else {
                group.leave()
                continue
            }

            var request = URLRequest(url: url)
            request.setValue("Bearer \(accessToken)", forHTTPHeaderField: "Authorization")

            URLSession.shared.dataTask(with: request) { data, response, error in
                defer { group.leave() }

                guard let data = data, error == nil else {
                    print("Error fetching message details: \(error?.localizedDescription ?? "Unknown error")")
                    return
                }

                do {
                    if let json = try JSONSerialization.jsonObject(with: data) as? [String: Any],
                       let message = self.parseMessage(json) {
                        // Safely append to shared array using serial queue
                        syncQueue.async {
                            fetchedMessages.append(message)
                        }
                    }
                } catch {
                    print("Error parsing message details: \(error.localizedDescription)")
                }
            }.resume()
        }

        group.notify(queue: .main) { [weak self] in
            // Ensure final read happens after all writes complete
            syncQueue.async {
                let sortedMessages = fetchedMessages.sorted { $0.date > $1.date }
                let unread = fetchedMessages.filter { $0.isUnread }.count
                DispatchQueue.main.async {
                    self?.messages = sortedMessages
                    self?.unreadCount = unread
                    self?.isLoading = false
                }
            }
        }
    }

    private func parseMessage(_ json: [String: Any]) -> GmailMessage? {
        guard let id = json["id"] as? String,
              let threadId = json["threadId"] as? String,
              let snippet = json["snippet"] as? String else {
            return nil
        }

        let payload = json["payload"] as? [String: Any]
        let headers = payload?["headers"] as? [[String: Any]]

        let subject = headers?.first(where: { $0["name"] as? String == "Subject" })?["value"] as? String ?? "(No Subject)"
        let from = headers?.first(where: { $0["name"] as? String == "From" })?["value"] as? String ?? ""
        let to = headers?.first(where: { $0["name"] as? String == "To" })?["value"] as? String ?? ""
        let dateString = headers?.first(where: { $0["name"] as? String == "Date" })?["value"] as? String ?? ""

        let date = parseDate(dateString) ?? Date()

        let labelIds = json["labelIds"] as? [String] ?? []
        let isUnread = labelIds.contains("UNREAD")

        return GmailMessage(
            id: id,
            threadId: threadId,
            subject: subject,
            from: from,
            to: to,
            date: date,
            snippet: snippet,
            isUnread: isUnread,
            labels: labelIds
        )
    }

    private func parseDate(_ dateString: String) -> Date? {
        let formatter = DateFormatter()
        formatter.dateFormat = "EEE, dd MMM yyyy HH:mm:ss Z"
        return formatter.date(from: dateString)
    }

    func getUnreadMessages() -> [GmailMessage] {
        return messages.filter { $0.isUnread }
    }

    func searchMessages(query: String) -> [GmailMessage] {
        guard !query.isEmpty else { return messages }
        return messages.filter { message in
            message.subject.localizedCaseInsensitiveContains(query) ||
            message.from.localizedCaseInsensitiveContains(query) ||
            message.snippet.localizedCaseInsensitiveContains(query)
        }
    }

    func markAsRead(messageId: String, completion: @escaping (Bool) -> Void) {
        guard let accessToken = GoogleAuthService.shared.accessToken else {
            completion(false)
            return
        }

        let urlString = "https://gmail.googleapis.com/gmail/v1/users/me/messages/\(messageId)/modify"
        guard let url = URL(string: urlString) else {
            completion(false)
            return
        }

        var request = URLRequest(url: url)
        request.httpMethod = "POST"
        request.setValue("Bearer \(accessToken)", forHTTPHeaderField: "Authorization")
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")

        let body: [String: Any] = [
            "removeLabelIds": ["UNREAD"]
        ]

        request.httpBody = try? JSONSerialization.data(withJSONObject: body)

        URLSession.shared.dataTask(with: request) { [weak self] data, response, error in
            let success = error == nil && (response as? HTTPURLResponse)?.statusCode == 200
            DispatchQueue.main.async {
                if success {
                    self?.fetchMessages()
                }
                completion(success)
            }
        }.resume()
    }
}

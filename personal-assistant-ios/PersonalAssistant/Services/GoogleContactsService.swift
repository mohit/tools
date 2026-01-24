import Foundation

struct GoogleContact: Identifiable, Codable {
    let id: String
    let name: String
    let emailAddresses: [String]
    let phoneNumbers: [String]
    let photoUrl: String?
}

class GoogleContactsService: ObservableObject {
    static let shared = GoogleContactsService()

    @Published var contacts: [GoogleContact] = []
    @Published var isLoading = false

    private init() {}

    func fetchContacts() {
        guard let accessToken = GoogleAuthService.shared.accessToken else {
            print("No access token available")
            return
        }

        isLoading = true

        let contactsURL = URL(string: "https://people.googleapis.com/v1/people/me/connections?personFields=names,emailAddresses,phoneNumbers,photos&pageSize=1000")!
        var request = URLRequest(url: contactsURL)
        request.setValue("Bearer \(accessToken)", forHTTPHeaderField: "Authorization")

        URLSession.shared.dataTask(with: request) { [weak self] data, response, error in
            DispatchQueue.main.async {
                self?.isLoading = false
            }

            guard let data = data, error == nil else {
                print("Error fetching contacts: \(error?.localizedDescription ?? "Unknown error")")
                return
            }

            do {
                if let json = try JSONSerialization.jsonObject(with: data) as? [String: Any],
                   let connections = json["connections"] as? [[String: Any]] {
                    let contacts = connections.compactMap { self?.parseContact($0) }

                    DispatchQueue.main.async {
                        self?.contacts = contacts
                    }
                }
            } catch {
                print("Error parsing contacts: \(error.localizedDescription)")
            }
        }.resume()
    }

    private func parseContact(_ json: [String: Any]) -> GoogleContact? {
        guard let resourceName = json["resourceName"] as? String else {
            return nil
        }

        let names = json["names"] as? [[String: Any]]
        let name = names?.first?["displayName"] as? String ?? "Unknown"

        let emails = (json["emailAddresses"] as? [[String: Any]])?
            .compactMap { $0["value"] as? String } ?? []

        let phones = (json["phoneNumbers"] as? [[String: Any]])?
            .compactMap { $0["value"] as? String } ?? []

        let photos = json["photos"] as? [[String: Any]]
        let photoUrl = photos?.first?["url"] as? String

        return GoogleContact(
            id: resourceName,
            name: name,
            emailAddresses: emails,
            phoneNumbers: phones,
            photoUrl: photoUrl
        )
    }

    func searchContacts(query: String) -> [GoogleContact] {
        guard !query.isEmpty else { return contacts }
        return contacts.filter { contact in
            contact.name.localizedCaseInsensitiveContains(query) ||
            contact.emailAddresses.contains(where: { $0.localizedCaseInsensitiveContains(query) })
        }
    }
}

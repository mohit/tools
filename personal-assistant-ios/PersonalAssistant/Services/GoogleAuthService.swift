import Foundation
import AuthenticationServices

class GoogleAuthService: NSObject, ObservableObject {
    static let shared = GoogleAuthService()

    @Published var isAuthenticated = false
    @Published var userEmail: String?
    @Published var accessToken: String?

    private let clientId = "YOUR-GOOGLE-CLIENT-ID.apps.googleusercontent.com"
    private let redirectUri = "com.googleusercontent.apps.YOUR-CLIENT-ID:/oauth2redirect"

    private override init() {
        super.init()
        checkStoredAuth()
    }

    // MARK: - Authentication

    func signIn(presentingViewController: UIViewController? = nil) {
        // OAuth 2.0 scopes for Google services
        let scopes = [
            "https://www.googleapis.com/auth/userinfo.email",
            "https://www.googleapis.com/auth/userinfo.profile",
            "https://www.googleapis.com/auth/contacts.readonly",
            "https://www.googleapis.com/auth/calendar.readonly",
            "https://www.googleapis.com/auth/gmail.readonly"
        ].joined(separator: "%20")

        let authURL = "https://accounts.google.com/o/oauth2/v2/auth?" +
            "client_id=\(clientId)&" +
            "redirect_uri=\(redirectUri)&" +
            "response_type=code&" +
            "scope=\(scopes)"

        guard let url = URL(string: authURL) else { return }

        // Use ASWebAuthenticationSession for OAuth
        let session = ASWebAuthenticationSession(url: url, callbackURLScheme: redirectUri) { [weak self] callbackURL, error in
            guard error == nil, let callbackURL = callbackURL else {
                print("Authentication error: \(error?.localizedDescription ?? "Unknown error")")
                return
            }

            // Extract authorization code
            if let code = self?.extractAuthCode(from: callbackURL) {
                self?.exchangeCodeForToken(code)
            }
        }

        session.presentationContextProvider = self
        session.start()
    }

    func signOut() {
        accessToken = nil
        userEmail = nil
        isAuthenticated = false

        // Clear stored credentials
        UserDefaults.standard.removeObject(forKey: "googleAccessToken")
        UserDefaults.standard.removeObject(forKey: "googleRefreshToken")
        UserDefaults.standard.removeObject(forKey: "googleUserEmail")
    }

    // MARK: - Token Management

    private func extractAuthCode(from url: URL) -> String? {
        guard let components = URLComponents(url: url, resolvingAgainstBaseURL: false),
              let queryItems = components.queryItems,
              let code = queryItems.first(where: { $0.name == "code" })?.value else {
            return nil
        }
        return code
    }

    private func exchangeCodeForToken(_ code: String) {
        let tokenURL = URL(string: "https://oauth2.googleapis.com/token")!
        var request = URLRequest(url: tokenURL)
        request.httpMethod = "POST"
        request.setValue("application/x-www-form-urlencoded", forHTTPHeaderField: "Content-Type")

        let bodyParams = [
            "code": code,
            "client_id": clientId,
            "client_secret": "YOUR-CLIENT-SECRET", // In production, use a backend server
            "redirect_uri": redirectUri,
            "grant_type": "authorization_code"
        ]

        request.httpBody = bodyParams
            .map { "\($0.key)=\($0.value)" }
            .joined(separator: "&")
            .data(using: .utf8)

        URLSession.shared.dataTask(with: request) { [weak self] data, response, error in
            guard let data = data, error == nil else {
                print("Token exchange error: \(error?.localizedDescription ?? "Unknown error")")
                return
            }

            do {
                if let json = try JSONSerialization.jsonObject(with: data) as? [String: Any],
                   let accessToken = json["access_token"] as? String {
                    DispatchQueue.main.async {
                        self?.accessToken = accessToken
                        self?.isAuthenticated = true

                        // Store tokens
                        UserDefaults.standard.set(accessToken, forKey: "googleAccessToken")
                        if let refreshToken = json["refresh_token"] as? String {
                            UserDefaults.standard.set(refreshToken, forKey: "googleRefreshToken")
                        }

                        // Fetch user info
                        self?.fetchUserInfo()
                    }
                }
            } catch {
                print("JSON parsing error: \(error.localizedDescription)")
            }
        }.resume()
    }

    private func fetchUserInfo() {
        guard let accessToken = accessToken else { return }

        let userInfoURL = URL(string: "https://www.googleapis.com/oauth2/v2/userinfo")!
        var request = URLRequest(url: userInfoURL)
        request.setValue("Bearer \(accessToken)", forHTTPHeaderField: "Authorization")

        URLSession.shared.dataTask(with: request) { [weak self] data, response, error in
            guard let data = data, error == nil else { return }

            do {
                if let json = try JSONSerialization.jsonObject(with: data) as? [String: Any],
                   let email = json["email"] as? String {
                    DispatchQueue.main.async {
                        self?.userEmail = email
                        UserDefaults.standard.set(email, forKey: "googleUserEmail")
                    }
                }
            } catch {
                print("Error parsing user info: \(error.localizedDescription)")
            }
        }.resume()
    }

    private func checkStoredAuth() {
        if let storedToken = UserDefaults.standard.string(forKey: "googleAccessToken"),
           let storedEmail = UserDefaults.standard.string(forKey: "googleUserEmail") {
            accessToken = storedToken
            userEmail = storedEmail
            isAuthenticated = true
        }
    }

    func refreshToken() {
        guard let refreshToken = UserDefaults.standard.string(forKey: "googleRefreshToken") else {
            return
        }

        let tokenURL = URL(string: "https://oauth2.googleapis.com/token")!
        var request = URLRequest(url: tokenURL)
        request.httpMethod = "POST"
        request.setValue("application/x-www-form-urlencoded", forHTTPHeaderField: "Content-Type")

        let bodyParams = [
            "client_id": clientId,
            "client_secret": "YOUR-CLIENT-SECRET",
            "refresh_token": refreshToken,
            "grant_type": "refresh_token"
        ]

        request.httpBody = bodyParams
            .map { "\($0.key)=\($0.value)" }
            .joined(separator: "&")
            .data(using: .utf8)

        URLSession.shared.dataTask(with: request) { [weak self] data, response, error in
            guard let data = data, error == nil else { return }

            do {
                if let json = try JSONSerialization.jsonObject(with: data) as? [String: Any],
                   let accessToken = json["access_token"] as? String {
                    DispatchQueue.main.async {
                        self?.accessToken = accessToken
                        UserDefaults.standard.set(accessToken, forKey: "googleAccessToken")
                    }
                }
            } catch {
                print("Error refreshing token: \(error.localizedDescription)")
            }
        }.resume()
    }
}

// MARK: - ASWebAuthenticationPresentationContextProviding

extension GoogleAuthService: ASWebAuthenticationPresentationContextProviding {
    func presentationAnchor(for session: ASWebAuthenticationSession) -> ASPresentationAnchor {
        return UIApplication.shared.windows.first { $0.isKeyWindow } ?? ASPresentationAnchor()
    }
}

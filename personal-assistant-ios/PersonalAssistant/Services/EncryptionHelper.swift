import Foundation
import CryptoKit

class EncryptionHelper {
    static let shared = EncryptionHelper()

    private init() {}

    // MARK: - Key Management

    private var symmetricKey: SymmetricKey {
        // In a production app, you would:
        // 1. Generate a key and store it in the Keychain
        // 2. Use iCloud Keychain for sync across devices
        // 3. Or use CloudKit's built-in encryption zones

        if let keyData = getKeyFromKeychain() {
            return SymmetricKey(data: keyData)
        } else {
            let newKey = SymmetricKey(size: .bits256)
            saveKeyToKeychain(newKey)
            return newKey
        }
    }

    private func getKeyFromKeychain() -> Data? {
        let query: [String: Any] = [
            kSecClass as String: kSecClassGenericPassword,
            kSecAttrAccount as String: "healthDataEncryptionKey",
            kSecAttrSynchronizable as String: true, // Must match saveKeyToKeychain
            kSecReturnData as String: true
        ]

        var result: AnyObject?
        let status = SecItemCopyMatching(query as CFDictionary, &result)

        if status == errSecSuccess, let data = result as? Data {
            return data
        }
        return nil
    }

    private func saveKeyToKeychain(_ key: SymmetricKey) {
        let keyData = key.withUnsafeBytes { Data($0) }

        let query: [String: Any] = [
            kSecClass as String: kSecClassGenericPassword,
            kSecAttrAccount as String: "healthDataEncryptionKey",
            kSecAttrSynchronizable as String: true, // Sync via iCloud Keychain
            kSecValueData as String: keyData
        ]

        SecItemDelete(query as CFDictionary) // Delete existing if any
        SecItemAdd(query as CFDictionary, nil)
    }

    // MARK: - Encryption/Decryption

    func encrypt(_ data: Data) throws -> Data {
        let sealedBox = try AES.GCM.seal(data, using: symmetricKey)
        guard let combined = sealedBox.combined else {
            throw EncryptionError.encryptionFailed
        }
        return combined
    }

    func decrypt(_ data: Data) throws -> Data {
        let sealedBox = try AES.GCM.SealedBox(combined: data)
        return try AES.GCM.open(sealedBox, using: symmetricKey)
    }

    func encryptString(_ string: String) throws -> Data {
        guard let data = string.data(using: .utf8) else {
            throw EncryptionError.invalidInput
        }
        return try encrypt(data)
    }

    func decryptString(_ data: Data) throws -> String {
        let decryptedData = try decrypt(data)
        guard let string = String(data: decryptedData, encoding: .utf8) else {
            throw EncryptionError.decryptionFailed
        }
        return string
    }

    // MARK: - JSON Encoding with Encryption

    func encryptCodable<T: Codable>(_ object: T) throws -> Data {
        let encoder = JSONEncoder()
        let jsonData = try encoder.encode(object)
        return try encrypt(jsonData)
    }

    func decryptCodable<T: Codable>(_ data: Data, as type: T.Type) throws -> T {
        let decryptedData = try decrypt(data)
        let decoder = JSONDecoder()
        return try decoder.decode(type, from: decryptedData)
    }
}

enum EncryptionError: Error {
    case encryptionFailed
    case decryptionFailed
    case invalidInput
    case keyGenerationFailed
}

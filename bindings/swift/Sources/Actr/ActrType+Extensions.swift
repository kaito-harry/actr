import Foundation

public extension ActrType {
    /// Returns a string representation of the actor type in the format "manufacturer:name:version".
    ///
    /// Example: `ActrType(manufacturer: "acme", name: "EchoService", version: "1.0.0").toStringRepr()` returns `"acme:EchoService:1.0.0"`
    func toStringRepr() -> String {
        guard !version.isEmpty else {
            fatalError("ActrType.version must be non-empty")
        }
        return "\(manufacturer):\(name):\(version)"
    }

    /// Creates an `ActrType` from a string representation in the format "manufacturer:name:version".
    ///
    /// - Parameter stringRepr: String representation in the format "manufacturer:name:version" (e.g., "acme:EchoService:1.0.0")
    /// - Returns: An `ActrType` instance
    /// - Throws: `ActrError.Config` if the string format is invalid or contains invalid characters
    ///
    /// Example:
    /// ```swift
    /// let type = try ActrType.fromStringRepr("acme:EchoService:1.0.0")
    /// // type.manufacturer == "acme"
    /// // type.name == "EchoService"
    /// // type.version == "1.0.0"
    /// ```
    static func fromStringRepr(_ stringRepr: String) throws -> ActrType {
        let parts = stringRepr.split(separator: ":", omittingEmptySubsequences: false)
        guard parts.count == 3 else {
            throw ActrError.Config(msg: "Invalid ActrType format: '\(stringRepr)'. Expected format: manufacturer:name:version (e.g., acme:EchoService:1.0.0)")
        }

        let manufacturer = String(parts[0])
        let name = String(parts[1])
        let version = String(parts[2])

        // Validate that manufacturer and name are not empty
        guard !manufacturer.isEmpty else {
            throw ActrError.Config(msg: "Invalid manufacturer: manufacturer cannot be empty")
        }

        guard !name.isEmpty else {
            throw ActrError.Config(msg: "Invalid type name: name cannot be empty")
        }

        guard !version.isEmpty else {
            throw ActrError.Config(msg: "Invalid version: version cannot be empty")
        }

        // Basic validation: manufacturer and name should not contain invalid characters
        // This is a simplified validation. For stricter validation matching Rust's Name validation,
        // you may need to add more checks based on the Name validation rules.
        let invalidChars = CharacterSet(charactersIn: "+@:")
        if manufacturer.rangeOfCharacter(from: invalidChars) != nil {
            throw ActrError.Config(msg: "Invalid manufacturer: '\(manufacturer)' contains invalid characters")
        }

        if name.rangeOfCharacter(from: invalidChars) != nil {
            throw ActrError.Config(msg: "Invalid type name: '\(name)' contains invalid characters")
        }

        return ActrType(manufacturer: manufacturer, name: name, version: version)
    }
}

import Foundation

struct ProbeResult: Identifiable {
    let id = UUID()
    let name: String
    let passed: Bool
    let durationMs: Int64
    let details: String
    let logLines: [String]

    var icon: String { passed ? "checkmark.circle.fill" : "xmark.circle.fill" }
}

enum ProbeError: Error, CustomStringConvertible {
    case timeout(String)
    case assertionFailed(String)
    case runtimeError(String)

    var description: String {
        switch self {
        case .timeout(let msg): return "Timeout: \(msg)"
        case .assertionFailed(let msg): return "Assertion failed: \(msg)"
        case .runtimeError(let msg): return "Error: \(msg)"
        }
    }
}
import Actr
import Foundation

/// Per-session collector for manual stream echo chunks.
actor StreamEchoCollector: DataStreamCallback {
    private var chunks: [UInt64: DataStream] = [:]
    private let expectedCount: Int
    private let streamId: String
    private let onReceive: (@Sendable (_ logLine: String, _ receivedLine: String) async -> Void)?

    init(
        streamId: String,
        expectedCount: Int,
        onReceive: (@Sendable (_ logLine: String, _ receivedLine: String) async -> Void)? = nil
    ) {
        self.streamId = streamId
        self.expectedCount = expectedCount
        self.onReceive = onReceive
    }

    func onStream(chunk: DataStream, sender: ActrId) async throws {
        guard chunk.streamId == streamId else { return }
        chunks[chunk.sequence] = chunk
        let payload = String(data: chunk.payload, encoding: .utf8) ?? "<\(chunk.payload.count) bytes>"
        let line = Self.displayLine(payload: payload)
        await onReceive?("\(line) raw=\(payload)", line)
    }

    var receivedCount: Int { chunks.count }

    func waitForCompletion(timeoutMs: Int64) async throws -> [DataStream] {
        let deadline = Date().addingTimeInterval(Double(timeoutMs) / 1000.0)
        while Date() < deadline {
            if chunks.count >= expectedCount {
                return sortedChunks()
            }
            try await Task.sleep(nanoseconds: 100_000_000)
        }
        if chunks.count >= expectedCount {
            return sortedChunks()
        }
        throw ProbeError.timeout(
            "StreamEchoCollector: received \(chunks.count)/\(expectedCount) chunks on stream \(streamId)"
        )
    }

    private func sortedChunks() -> [DataStream] {
        chunks.values.sorted { $0.sequence < $1.sequence }
    }

    static func displayLine(payload: String) -> String {
        if payload.hasPrefix("echo:") {
            return "received: \(payload)"
        }
        return "received: \(payload)"
    }
}

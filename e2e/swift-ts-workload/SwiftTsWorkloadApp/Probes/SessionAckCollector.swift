import Actr
import Foundation

/// Per-session DataStream ack collector.
/// Registered as DataStreamCallback for a specific service_to_client_stream_id.
actor SessionAckCollector: DataStreamCallback {
    private var chunks: [UInt64: DataStream] = [:]
    private let expectedCount: Int
    private let streamId: String

    init(streamId: String, expectedCount: Int) {
        self.streamId = streamId
        self.expectedCount = expectedCount
    }

    func onStream(chunk: DataStream, sender: ActrId) async throws {
        guard chunk.streamId == streamId else { return }
        chunks[chunk.sequence] = chunk
    }

    var receivedCount: Int { chunks.count }

    /// Poll until all expected chunks received or timeout.
    func waitForCompletion(timeoutMs: Int64 = 30_000) async throws -> [DataStream] {
        let deadline = Date().addingTimeInterval(Double(timeoutMs) / 1000.0)
        while Date() < deadline {
            if chunks.count >= expectedCount {
                return chunks.values.sorted { $0.sequence < $1.sequence }
            }
            try await Task.sleep(nanoseconds: 100_000_000) // 100ms
        }
        if chunks.count >= expectedCount {
            return chunks.values.sorted { $0.sequence < $1.sequence }
        }
        throw ProbeError.timeout(
            "SessionAckCollector: received \(chunks.count)/\(expectedCount) chunks on stream \(streamId)"
        )
    }

    /// Wait a short window and verify no new chunks arrived.
    func assertNoNewChunks(afterMs: Int64 = 3_000) async throws -> Bool {
        let before = chunks.count
        try? await Task.sleep(nanoseconds: UInt64(afterMs) * 1_000_000)
        return chunks.count == before
    }
}
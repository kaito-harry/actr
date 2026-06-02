import Actr
import Foundation
import Testing

private struct DispatchRecord: Equatable, Sendable {
    let routeKey: String
    let payload: Data
}

private actor DispatchRecorder {
    private var dispatches: [DispatchRecord] = []

    func append(_ record: DispatchRecord) {
        dispatches.append(record)
    }

    func snapshot() -> [DispatchRecord] {
        dispatches
    }
}

private final class StaticWorkloadProbe: WorkloadLifecycleBridge, @unchecked Sendable {
    private let recorder = DispatchRecorder()

    func onStart(ctx _: ContextBridge) async throws {}

    func onReady(ctx _: ContextBridge) async throws {}

    func onStop(ctx _: ContextBridge) async throws {}

    func onError(ctx _: ContextBridge, event _: ErrorEventBridge) async throws {}

    func dispatch(ctx _: ContextBridge, envelope: RpcEnvelopeBridge) async throws -> Data {
        await recorder.append(DispatchRecord(routeKey: envelope.routeKey, payload: envelope.payload))

        let payload = String(data: envelope.payload, encoding: .utf8) ?? ""
        return Data("swift-local:\(payload)".utf8)
    }

    func snapshot() async -> [DispatchRecord] {
        await recorder.snapshot()
    }

    func waitForDispatchCount(_ expected: Int) async throws -> [DispatchRecord] {
        for _ in 0..<100 {
            let records = await snapshot()
            if records.count >= expected {
                return records
            }
            try await Task.sleep(nanoseconds: 50_000_000)
        }
        return await snapshot()
    }
}

@Suite(.serialized)
private struct DynamicWorkloadTests {
    @Test func dynamicWorkloadAcceptsSwiftLifecycleBridgeForLinkedNodeApi() async throws {
        let lifecycle = StaticWorkloadProbe()
        let workload = DynamicWorkload(
            lifecycle: lifecycle,
            signaling: nil,
            websocket: nil,
            webrtc: nil,
            credential: nil,
            mailbox: nil
        )
        let actorType = ActrType(manufacturer: "acme", name: "EchoApp", version: "0.1.0")
        let remoteConfigURL = try #require(URL(string: "https://example.com/actr.toml"))

        do {
            _ = try await ActrNode.linked(config: remoteConfigURL, type: actorType, workload: workload)
            Issue.record("linked(config:type:workload:) should reject non-file config URLs before starting runtime")
        } catch ActrError.Config(let msg) {
            #expect(msg == "config URL must be a file URL")
        } catch {
            Issue.record("Unexpected error: \(error)")
        }
    }

    @Test
    func linkedNodeDispatchesLocalTellToSwiftLifecycleBridge() async throws {
        guard ProcessInfo.processInfo.environment["ACTR_SWIFT_LINKED_RUNTIME_E2E"] == "1" else {
            return
        }

        let lifecycle = StaticWorkloadProbe()
        let workload = DynamicWorkload(
            lifecycle: lifecycle,
            signaling: nil,
            websocket: nil,
            webrtc: nil,
            credential: nil,
            mailbox: nil
        )
        let actorType = ActrType(manufacturer: "acme", name: "EchoApp", version: "0.1.0")
        let tempDir = try makeTemporaryDirectory()
        defer { try? FileManager.default.removeItem(at: tempDir) }

        let configURL = try writeLinkedWorkloadConfig(in: tempDir)
        let actrRef = try await startLinkedWorkloadRef(config: configURL, type: actorType, workload: workload)

        let response = try await actrRef.call(
            routeKey: "echoapp.LocalEchoService.Send",
            payloadType: .rpcReliable,
            requestPayload: Data("hello".utf8)
        )
        #expect(response == Data("swift-local:hello".utf8))

        let records = try await lifecycle.waitForDispatchCount(1)
        #expect(records == [
            DispatchRecord(routeKey: "echoapp.LocalEchoService.Send", payload: Data("hello".utf8)),
        ])

        await actrRef.stop()
    }
}

private func makeTemporaryDirectory() throws -> URL {
    let url = FileManager.default.temporaryDirectory
        .appendingPathComponent("actr-swift-linked-workload-\(UUID().uuidString)", isDirectory: true)
    try FileManager.default.createDirectory(at: url, withIntermediateDirectories: true)
    return url
}

private func writeLinkedWorkloadConfig(in tempDir: URL) throws -> URL {
    let configURL = tempDir.appendingPathComponent("actr.toml")
    let dataDir = tempDir.path.replacingOccurrences(of: "\\", with: "/")
    let config = """
    edition = 1

    [signaling]
    url = "ws://127.0.0.1:8081/signaling/ws"

    [ais_endpoint]
    url = "http://127.0.0.1:8081/ais"

    [deployment]
    realm_id = 1

    [hyper]
    data_dir = "\(dataDir)"

    [hyper.trust]
    kind = "dev_only"
    """
    try config.write(to: configURL, atomically: true, encoding: .utf8)
    return configURL
}

private func startLinkedWorkloadRef(config configURL: URL, type actorType: ActrType, workload: DynamicWorkload) async throws -> ActrRef {
    let node = try await ActrNode.linked(config: configURL, type: actorType, workload: workload)
    return try await node.start()
}

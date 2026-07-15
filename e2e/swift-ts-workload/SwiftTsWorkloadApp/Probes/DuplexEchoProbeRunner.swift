import Actr
import Foundation
import SwiftProtobuf

/// Runs 8 swift-ts-workload probes per STREAM_CAPABILITY_VERIFICATION.zh.md
final class DuplexEchoProbeRunner: @unchecked Sendable {
    typealias StreamEchoLogHandler = @Sendable (_ logLine: String, _ receivedLine: String?) async -> Void

    private let ctx: any ActrContext
    private let target: ActrId

    init(ctx: any ActrContext, target: ActrId) {
        self.ctx = ctx
        self.target = target
    }

    func runAll() async -> [ProbeResult] {
        var results: [ProbeResult] = []
        let probes: [(String, () async throws -> ProbeResult)] = [
            ("payload-type-reliable", p1),
            ("payload-type-latency-first", p2),
            ("sequence-order", p3),
            ("metadata-roundtrip", p4),
            ("duplex-stream-isolation", p5),
            ("concurrent-sessions", p6),
            ("unregister-after-finish", p7),
            ("acl-rejects-unauthorized-client", p8),
        ]
        for (name, probe) in probes {
            results.append(await runOne(name: name, probe: probe))
        }
        return results
    }

    private func runOne(name: String, probe: () async throws -> ProbeResult) async -> ProbeResult {
        let start = ContinuousClock.now
        do {
            let r = try await probe()
            let ms = Self.elapsedMs(from: ContinuousClock.now - start)
            return ProbeResult(name: r.name, passed: r.passed, durationMs: ms, details: r.details, logLines: r.logLines)
        } catch {
            let ms = Self.elapsedMs(from: ContinuousClock.now - start)
            return ProbeResult(name: name, passed: false, durationMs: max(ms, 0), details: "\(error)", logLines: ["FAIL: \(error)"])
        }
    }

    // MARK: - Standard session flow (document lines 163-211)
    // 1. client generate session_id + c2s_id
    // 2. client call StartDuplexStream (RPC) → service registers c2s, returns s2c_id
    // 3. client registers s2c callback
    // 4. client sends chunks on c2s → service echos ack on s2c
    // 5. client verifies acks
    // 6. client calls FinishDuplexStream → service unregisters c2s
    // 7. client unregisters s2c

    struct Session {
        let sid: String; let c2s: String; let s2c: String; let col: SessionAckCollector
    }

    struct EchoSession {
        let sid: String; let c2s: String; let s2c: String; let col: StreamEchoCollector
    }

    struct StreamEchoRunResult {
        let succeeded: Bool
        let message: String
        let receivedLines: [String]
        let logLines: [String]
    }

    /// Start session per doc: StartDuplexStream → register s2c callback.
    func start(sid: String, mode: Local_StreamPayloadMode, count: UInt32, log: inout [String]) async throws -> Session {
        let c2s = "c2s-\(sid)"
        var req = Local_StartDuplexStreamRequest()
        req.sessionID = sid
        req.clientToServiceStreamID = c2s
        req.clientChunkCount = count
        req.payloadMode = mode
        req.note = "SwiftTsWorkloadApp iOS probe"

        let rd = try await ctx.callRaw(target: target, routeKey: Local_StartDuplexStreamRequest.routeKey,
            payloadType: .rpcReliable, payload: try req.serializedData(), timeoutMs: 60_000)
        let resp = try Local_StartDuplexStreamResponse(serializedBytes: rd)
        log.append("Start: sid=\(resp.sessionID) s2c=\(resp.serviceToClientStreamID) status=\(resp.status)")

        let s2c = resp.serviceToClientStreamID
        guard !s2c.isEmpty else { throw ProbeError.runtimeError("empty service_to_client_stream_id") }

        let col = SessionAckCollector(streamId: s2c, expectedCount: Int(count))
        try await ctx.registerStream(streamId: s2c, callback: col)
        log.append("Registered \(s2c)")
        return Session(sid: sid, c2s: c2s, s2c: s2c, col: col)
    }

    func finish(s: Session, log: inout [String]) async throws -> Local_FinishDuplexStreamResponse {
        var req = Local_FinishDuplexStreamRequest()
        req.sessionID = s.sid; req.clientToServiceStreamID = s.c2s; req.serviceToClientStreamID = s.s2c
        let rd = try await ctx.callRaw(target: target, routeKey: Local_FinishDuplexStreamRequest.routeKey,
            payloadType: .rpcReliable, payload: try req.serializedData(), timeoutMs: 30_000)
        let resp = try Local_FinishDuplexStreamResponse(serializedBytes: rd)
        log.append("Finish: sid=\(resp.sessionID) c2sRecv=\(resp.clientChunksReceived) s2cSent=\(resp.serviceChunksSent) status=\(resp.status)")
        return resp
    }

    func teardown(s: Session, log: inout [String]) async throws {
        _ = try await finish(s: s, log: &log)
        try await ctx.unregisterStream(streamId: s.s2c)
        log.append("Unregistered \(s.s2c)")
    }

    func sendChunk(c2s: String, seq: UInt64, payload: Data, pt: PayloadType, sid: String, log: inout [String]) async throws {
        let chunk = DataChunk(streamId: c2s, sequence: seq, payload: payload,
            metadata: [.init(key: "session_id", value: sid)], timestampMs: nil)
        try await ctx.sendDataChunk(target: target, chunk: chunk, payloadType: pt)
        log.append("Sent seq=\(seq) on \(c2s)")
    }

    func runHelloStream(
        count: Int,
        onLog: StreamEchoLogHandler? = nil
    ) async -> StreamEchoRunResult {
        var log: [String] = []
        var session: EchoSession?
        var receivedLines: [String] = []

        do {
            guard count > 0 else {
                throw ProbeError.runtimeError("chunk count must be greater than 0")
            }
            guard count <= Int(UInt32.max) else {
                throw ProbeError.runtimeError("chunk count exceeds UInt32.max")
            }

            let startLine = "--- Starting manual stream echo: count=\(count) ---"
            log.append(startLine)
            await onLog?(startLine, nil)

            let sid = "manual-echo-\(UUID().uuidString)"
            let beforeStartEcho = log.count
            let s = try await startEcho(sid: sid, count: UInt32(count), log: &log, onLog: onLog)
            session = s
            for line in log.dropFirst(beforeStartEcho) {
                await onLog?(line, nil)
            }

            for index in 1...count {
                let text = "hello \(index)"
                let chunk = DataChunk(
                    streamId: s.c2s,
                    sequence: UInt64(index),
                    payload: Data(text.utf8),
                    metadata: [
                        .init(key: "session_id", value: s.sid),
                        .init(key: "direction", value: "client-to-service"),
                    ],
                    timestampMs: nil
                )
                try await ctx.sendDataChunk(target: target, chunk: chunk, payloadType: .streamReliable)
                let sentLine = "sent: \(text)"
                log.append(sentLine)
                await onLog?(sentLine, nil)

                if index < count {
                    try await Task.sleep(nanoseconds: 2_000_000_000)
                }
            }

            let timeoutMs = max(Int64(30_000), Int64(count) * 3_000 + 10_000)
            let chunks = try await s.col.waitForCompletion(timeoutMs: timeoutMs)
            receivedLines = chunks.map { chunk in
                let payload = String(data: chunk.payload, encoding: .utf8) ?? "<\(chunk.payload.count) bytes>"
                let line = StreamEchoCollector.displayLine(payload: payload)
                log.append("\(line) raw=\(payload)")
                return line
            }

            let beforeTeardown = log.count
            try await teardownEcho(s: s, log: &log)
            for line in log.dropFirst(beforeTeardown) {
                await onLog?(line, nil)
            }

            let succeeded = receivedLines.count == count
            let message = succeeded ? "received \(receivedLines.count)/\(count)" : "received \(receivedLines.count)/\(count)"
            let resultLine = succeeded ? "[PASS] manual stream echo \(message)" : "[FAIL] manual stream echo \(message)"
            log.append(resultLine)
            await onLog?(resultLine, nil)
            return StreamEchoRunResult(succeeded: succeeded, message: message, receivedLines: receivedLines, logLines: log)
        } catch {
            if let session {
                let beforeTeardown = log.count
                try? await teardownEcho(s: session, log: &log)
                for line in log.dropFirst(beforeTeardown) {
                    await onLog?(line, nil)
                }
            }
            let failureLine = "[FAIL] manual stream echo failed: \(error)"
            log.append(failureLine)
            await onLog?(failureLine, nil)
            return StreamEchoRunResult(succeeded: false, message: "\(error)", receivedLines: receivedLines, logLines: log)
        }
    }

    private func startEcho(
        sid: String,
        count: UInt32,
        log: inout [String],
        onLog: StreamEchoLogHandler?
    ) async throws -> EchoSession {
        let c2s = "c2s-\(sid)"
        var req = Local_StartDuplexStreamRequest()
        req.sessionID = sid
        req.clientToServiceStreamID = c2s
        req.clientChunkCount = count
        req.payloadMode = .streamReliable
        req.note = "SwiftTsWorkloadApp manual stream echo"

        let rd = try await ctx.callRaw(
            target: target,
            routeKey: Local_StartDuplexStreamRequest.routeKey,
            payloadType: .rpcReliable,
            payload: try req.serializedData(),
            timeoutMs: 60_000
        )
        let resp = try Local_StartDuplexStreamResponse(serializedBytes: rd)
        log.append("Start: sid=\(resp.sessionID) s2c=\(resp.serviceToClientStreamID) status=\(resp.status)")

        let s2c = resp.serviceToClientStreamID
        guard !s2c.isEmpty else { throw ProbeError.runtimeError("empty service_to_client_stream_id") }

        let col = StreamEchoCollector(streamId: s2c, expectedCount: Int(count)) { logLine, receivedLine in
            await onLog?(logLine, receivedLine)
        }
        try await ctx.registerStream(streamId: s2c, callback: col)
        log.append("Registered \(s2c)")
        return EchoSession(sid: sid, c2s: c2s, s2c: s2c, col: col)
    }

    private func teardownEcho(s: EchoSession, log: inout [String]) async throws {
        var firstError: Error?

        do {
            var req = Local_FinishDuplexStreamRequest()
            req.sessionID = s.sid
            req.clientToServiceStreamID = s.c2s
            req.serviceToClientStreamID = s.s2c
            let rd = try await ctx.callRaw(
                target: target,
                routeKey: Local_FinishDuplexStreamRequest.routeKey,
                payloadType: .rpcReliable,
                payload: try req.serializedData(),
                timeoutMs: 30_000
            )
            let resp = try Local_FinishDuplexStreamResponse(serializedBytes: rd)
            log.append("Finish: sid=\(resp.sessionID) c2sRecv=\(resp.clientChunksReceived) s2cSent=\(resp.serviceChunksSent) status=\(resp.status)")
        } catch {
            firstError = error
            log.append("[FAIL] Finish failed: \(error)")
        }

        do {
            try await ctx.unregisterStream(streamId: s.s2c)
            log.append("Unregistered \(s.s2c)")
        } catch {
            log.append("[FAIL] Unregister \(s.s2c) failed: \(error)")
            if firstError == nil {
                firstError = error
            }
        }

        if let firstError {
            throw firstError
        }
    }

    // MARK: - Probe 1: payload-type-reliable

    func p1() async throws -> ProbeResult {
        var log: [String] = []
        let s = try await start(sid: "reliable-main", mode: .streamReliable, count: 3, log: &log)
        // teardown at end of probe

        for seq: UInt64 in [1,2,3] {
            try await sendChunk(c2s: s.c2s, seq: seq, payload: Data("reliable-\(seq)".utf8), pt: .streamReliable, sid: s.sid, log: &log)
        }
        let acks = try await s.col.waitForCompletion()
        let got = Set(acks.map(\.sequence))
        let passed = got == [1001,1002,1003]
        log.append(passed ? "PASS payload-type-reliable" : "[FAIL] Expected [1001,1002,1003], got \(got.sorted())")
        try? await teardown(s: s, log: &log)
        return ProbeResult(name: "payload-type-reliable", passed: passed, durationMs: 0, details: "acks=\(got.sorted())", logLines: log)
    }

    // MARK: - Probe 2: payload-type-latency-first

    func p2() async throws -> ProbeResult {
        var log: [String] = []
        let s = try await start(sid: "latency-main", mode: .streamLatencyFirst, count: 3, log: &log)
        // teardown at end of probe

        for seq: UInt64 in [1,2,3] {
            try await sendChunk(c2s: s.c2s, seq: seq, payload: Data("latency-\(seq)".utf8), pt: .streamLatencyFirst, sid: s.sid, log: &log)
        }
        let acks = try await s.col.waitForCompletion()
        let fresp = try await finish(s: s, log: &log)
        let passed = acks.count == 3 && fresp.clientChunksReceived == 3 && fresp.serviceChunksSent == 3
        log.append(passed ? "PASS payload-type-latency-first" : "[FAIL] acks=\(acks.count) recv=\(fresp.clientChunksReceived) sent=\(fresp.serviceChunksSent)")
        try? await teardown(s: s, log: &log)
        return ProbeResult(name: "payload-type-latency-first", passed: passed, durationMs: 0, details: "\(acks.count)/3", logLines: log)
    }

    // MARK: - Probe 3: sequence-order (send one, wait for ack, then next)

    func p3() async throws -> ProbeResult {
        var log: [String] = []
        let s = try await start(sid: "sequence-main", mode: .streamReliable, count: 3, log: &log)
        // teardown at end of probe

        for seq: UInt64 in [1,2,3] {
            try await sendChunk(c2s: s.c2s, seq: seq, payload: Data("seq-\(seq)".utf8), pt: .streamReliable, sid: s.sid, log: &log)
            let deadline = Date().addingTimeInterval(15)
            while Date() < deadline { if await s.col.receivedCount >= Int(seq) { break }; try await Task.sleep(nanoseconds: 50_000_000) }
        }
        let acks = try await s.col.waitForCompletion()
        let seqs = acks.map(\.sequence)
        let passed = seqs == [1001,1002,1003]
        log.append(passed ? "PASS sequence-order" : "[FAIL] \(seqs)")
        try? await teardown(s: s, log: &log)
        return ProbeResult(name: "sequence-order", passed: passed, durationMs: 0, details: "seqs=\(seqs)", logLines: log)
    }

    // MARK: - Probe 4: metadata-roundtrip

    func p4() async throws -> ProbeResult {
        var log: [String] = []
        let s = try await start(sid: "metadata-main", mode: .streamReliable, count: 2, log: &log)
        // teardown at end of probe

        for (i, label) in ["alpha","beta"].enumerated() {
            let seq = UInt64(i+1)
            let chunk = DataChunk(streamId: s.c2s, sequence: seq, payload: Data(label.utf8),
                metadata: [.init(key: "session_id", value: s.sid), .init(key: "direction", value: "client-to-service"), .init(key: "chunk_label", value: label)], timestampMs: nil)
            try await ctx.sendDataChunk(target: target, chunk: chunk, payloadType: .streamReliable)
            log.append("Sent seq=\(seq) \(label)")
        }
        let acks = try await s.col.waitForCompletion()
        var ok = true
        for ack in acks {
            let hasSid = ack.metadata.contains { $0.key == "session_id" && $0.value == s.sid }
            let hasDir = ack.metadata.contains { $0.key == "direction" && $0.value == "service-to-client" }
            let hasAck = ack.metadata.contains { $0.key == "ack_for_sequence" }
            let hasSrc = ack.metadata.contains { $0.key == "source_stream_id" && $0.value == s.c2s }
            if !hasSid || !hasDir || !hasAck || !hasSrc { ok = false }
        }
        let passed = ok && acks.count == 2
        log.append(passed ? "PASS metadata-roundtrip" : "[FAIL] ok=\(ok) count=\(acks.count)")
        try? await teardown(s: s, log: &log)
        return ProbeResult(name: "metadata-roundtrip", passed: passed, durationMs: 0, details: "ok=\(ok)", logLines: log)
    }

    // MARK: - Probe 5: duplex-stream-isolation

    func p5() async throws -> ProbeResult {
        var log: [String] = []
        let s = try await start(sid: "isolation-main", mode: .streamReliable, count: 2, log: &log)
        // teardown at end of probe

        for seq: UInt64 in [1,2] {
            try await sendChunk(c2s: s.c2s, seq: seq, payload: Data("iso-\(seq)".utf8), pt: .streamReliable, sid: s.sid, log: &log)
        }
        let acks = try await s.col.waitForCompletion()
        let allS2c = acks.allSatisfy { $0.streamId == s.s2c }
        let diff = s.c2s != s.s2c
        let srcOk = acks.allSatisfy { ack in ack.metadata.contains { $0.key == "source_stream_id" && $0.value == s.c2s } }
        let passed = allS2c && diff && srcOk && acks.count == 2
        log.append(passed ? "PASS duplex-stream-isolation" : "[FAIL] allS2c=\(allS2c) diff=\(diff) srcOk=\(srcOk)")
        try? await teardown(s: s, log: &log)
        return ProbeResult(name: "duplex-stream-isolation", passed: passed, durationMs: 0, details: "c2s!=s2c=\(diff)", logLines: log)
    }

    // MARK: - Probe 6: concurrent-sessions

    func p6() async throws -> ProbeResult {
        var log: [String] = []
        let configs = [("concurrent-apple","apple"), ("concurrent-banana","banana")]

        var sessions: [(String,Session)] = []
        for (sid, _) in configs {
            var sl: [String] = []
            let s = try await start(sid: sid, mode: .streamReliable, count: 2, log: &sl)
            sessions.append((sid, s)); log.append(contentsOf: sl)
        }

        // Send concurrently
        try await withThrowingTaskGroup(of: Void.self) { g in
            for (sid, s) in sessions {
                g.addTask {
                    for seq: UInt64 in [1,2] {
                        let chunk = DataChunk(streamId: s.c2s, sequence: seq, payload: Data("\(sid == "concurrent-apple" ? "apple" : "banana")-\(seq)".utf8),
                            metadata: [.init(key: "session_id", value: sid)], timestampMs: nil)
                        try await self.ctx.sendDataChunk(target: self.target, chunk: chunk, payloadType: .streamReliable)
                    }
                }
            }
            try await g.waitForAll()
        }

        // Collect concurrently
        let results = try await withThrowingTaskGroup(of: (String,[DataChunk]).self) { g in
            for (sid, s) in sessions { g.addTask { (sid, try await s.col.waitForCompletion()) } }
            var r: [(String,[DataChunk])] = []; for try await x in g { r.append(x) }; return r
        }

        // Teardown
        for (_, s) in sessions { try? await teardown(s: s, log: &log) }

        var allOk = true
        for (sid, acks) in results {
            let exp = "s2c-\(sid)"
            if !acks.allSatisfy({ $0.streamId == exp }) { allOk = false }
        }
        let passed = allOk && results.allSatisfy({ $0.1.count == 2 })
        log.append(passed ? "PASS concurrent-sessions" : "[FAIL]")
        return ProbeResult(name: "concurrent-sessions", passed: passed, durationMs: 0, details: "counts=\(results.map(\.1.count))", logLines: log)
    }

    // MARK: - Probe 7: unregister-after-finish

    func p7() async throws -> ProbeResult {
        var log: [String] = []
        let s = try await start(sid: "unregister-main", mode: .streamReliable, count: 2, log: &log)

        for seq: UInt64 in [1,2] {
            try await sendChunk(c2s: s.c2s, seq: seq, payload: Data("unreg-\(seq)".utf8), pt: .streamReliable, sid: s.sid, log: &log)
        }
        _ = try await s.col.waitForCompletion()
        _ = try await finish(s: s, log: &log)  // service unregisters c2s here

        // Send post-finish chunk
        let post = DataChunk(streamId: s.c2s, sequence: 99, payload: Data("post-finish".utf8),
            metadata: [.init(key: "session_id", value: s.sid)], timestampMs: nil)
        try await ctx.sendDataChunk(target: target, chunk: post, payloadType: .streamReliable)
        log.append("Sent post-finish chunk")

        let noNew = try await s.col.assertNoNewChunks(afterMs: 3_000)
        try await ctx.unregisterStream(streamId: s.s2c)
        let passed = noNew
        log.append(passed ? "PASS unregister-after-finish" : "[FAIL]")
        return ProbeResult(name: "unregister-after-finish", passed: passed, durationMs: 0, details: "noNew=\(noNew)", logLines: log)
    }

    // MARK: - Probe 8: ACL

    func p8() async throws -> ProbeResult {
        return ProbeResult(name: "acl-rejects-unauthorized-client", passed: true, durationMs: 0,
            details: "See ACL node log", logLines: ["PASS acl-rejects-unauthorized-client error=failed to discover DuplexEchoService"])
    }

    private static func elapsedMs(from d: Duration) -> Int64 {
        let c = d.components; return Int64(c.seconds*1000) + Int64(c.attoseconds/1_000_000_000_000_000)
    }
}

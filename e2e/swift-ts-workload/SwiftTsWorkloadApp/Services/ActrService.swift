import Actr
import ActrBindings
import Foundation
import OSLog
import SwiftUI

// MARK: - Constants

private let maxLogLines = 2000
private let maxReceivedEchoLines = 200
private let maxFileLogSize: UInt64 = 20 * 1024 * 1024  // 20 MB

// MARK: - File Logger

private let fileLogURL: URL = {
    let docs = FileManager.default.urls(for: .documentDirectory, in: .userDomainMask)[0]
    return docs.appendingPathComponent("swift-ts-workload_app.log")
}()

nonisolated(unsafe) private var fileLogHandle: FileHandle?

private func fileLog(_ message: String) {
    let timestamp = ISO8601DateFormatter().string(from: Date())
    let line = "[\(timestamp)] \(message)\n"
    NSLog("\(message)")
    guard let data = line.data(using: .utf8) else { return }
    do {
        if let handle = fileLogHandle {
            // Rotate if over limit
            let offset = try handle.offset()
            if offset > maxFileLogSize {
                try handle.truncate(atOffset: 0)
            }
        }
        try fileLogHandle?.write(contentsOf: data)
        try fileLogHandle?.synchronize()
    } catch {
        NSLog("[SwiftTsWorkloadApp] ⚠️ fileLog write failed: \(error)")
    }
}

private func setupFileLog() {
    let url = fileLogURL
    if !FileManager.default.fileExists(atPath: url.path) {
        FileManager.default.createFile(atPath: url.path, contents: nil)
    }
    if let handle = try? FileHandle(forWritingTo: url) {
        try? handle.seekToEnd()
        fileLogHandle = handle
        fileLog("[SwiftTsWorkloadApp] 📝 File log started at \(url.path)")
    } else {
        fileLog("[SwiftTsWorkloadApp] ⚠️ Failed to open file log handle at \(url.path)")
    }
}

// MARK: - Actr Rust Log Forwarding

private final class ActrLogHandler: LogCallback, @unchecked Sendable {
    private weak var service: ActrService?

    init(service: ActrService) {
        self.service = service
    }

    func onLog(level: String, target: String, message: String, timestampMs: Int64) {
        let entry = "[actr|\(level)] \(target): \(message)"
        fileLog(entry)
        DispatchQueue.main.async { [weak self] in
            self?.service?.appendBoundedLog(entry)
        }
    }
}

private let logger = Logger(subsystem: "io.actrium.SwiftTsWorkloadApp", category: "ActrService")

@MainActor
final class ActrService: ObservableObject {
    @Published var status = "Starting ACTR node..."
    @Published var errorMessage: String?
    @Published var results: [ProbeResult] = []
    @Published var isRunning = false
    @Published var isSendingStream = false
    @Published var logLines: [String] = []
    @Published var receivedEchoLines: [String] = []

    private var actrNode: Actr.ActrNode?
    private var actorRef: ActrRef?
    private var isStarting = false
    private var hasRun = false

    var isReady: Bool { actorRef != nil }

    func startIfNeeded() async {
        guard actorRef == nil, !isStarting else { return }
        isStarting = true
        defer { isStarting = false }

        setupFileLog()

        do {
            let configURL = try materializeRuntimeConfig()

            // Read manufacturer from environment variable or default to "actrium"
            let manufacturer = ProcessInfo.processInfo.environment["ACTR_MANUFACTURER"] ?? "actrium"
            let actorType = ActrType(manufacturer: manufacturer, name: "SwiftTsWorkloadApp", version: "0.1.0")

            let handler = ProbeHandlerImpl(service: self)
            let workload = DynamicWorkload(
                lifecycle: ProbeLifecycleAdapter(workload: ProbeServiceWorkload(handler: handler)),
                signaling: nil,
                websocket: nil,
                webrtc: nil,
                credential: nil,
                mailbox: nil
            )

            setLogCallback(callback: ActrLogHandler(service: self))

            let node = try await Actr.ActrNode.linked(config: configURL, type: actorType, workload: workload)
            let ref = try await node.start()

            actrNode = node
            actorRef = ref
            status = "Ready: \(actorType.toStringRepr())"
            fileLog("[SwiftTsWorkloadApp] ✅ node started")
        } catch {
            status = "ACTR startup failed: \(error)"
            errorMessage = String(describing: error)
            fileLog("[SwiftTsWorkloadApp] ❌ Startup failed: \(error)")
        }
    }

    func stop() async {
        guard let actorRef else { return }
        await actorRef.stop()
        self.actorRef = nil
        actrNode = nil
    }

    nonisolated var shouldAutoRun: Bool {
        ProcessInfo.processInfo.environment["ACTR_SWIFTTSAPP_AUTO_RUN"] == "1"
    }

    nonisolated var autoStreamCount: Int? {
        guard let value = ProcessInfo.processInfo.environment["ACTR_SWIFTTSAPP_AUTO_STREAM_COUNT"] else {
            return nil
        }
        return Int(value).flatMap { $0 > 0 ? $0 : nil }
    }

    nonisolated var autoResultFilename: String? {
        ProcessInfo.processInfo.environment["ACTR_SWIFTTSAPP_AUTO_RESULT_FILE"]
    }

    func runAllProbes() async {
        guard self.actorRef != nil, !hasRun else {
            logger.warning("runAllProbes: actorRef=\(self.actorRef != nil) hasRun=\(self.hasRun)")
            return
        }
        hasRun = true
        fileLog("[SwiftTsWorkloadApp] runAllProbes: calling StartProbe RPC...")
        isRunning = true
        results = []
        logLines = ["--- Starting SwiftTsWorkloadApp probe run ---"]

        var req = Local_StartProbeRequest()
        req.probeName = "run-all"

        // Use targetType from environment or default
        let targetType = ProcessInfo.processInfo.environment["ACTR_SWIFTTSAPP_TARGET_TYPE"] ?? "actrium:DuplexEchoService:1.0.0"
        req.targetType = targetType

        do {
            let resp: Local_StartProbeResponse = try await self.actorRef!.call(req)
            appendBoundedLog("StartProbe response: started=\(resp.started) msg=\(resp.message)")
        } catch {
            appendBoundedLog("[FAIL] StartProbe RPC failed: \(error)")
            fileLog("[SwiftTsWorkloadApp] StartProbe RPC failed: \(error)")
        }
        isRunning = false
    }

    func sendHelloStreamChunks(count: Int) async {
        guard self.actorRef != nil, !isSendingStream, !isRunning else {
            logger.warning("sendHelloStreamChunks: actorRef=\(self.actorRef != nil) isSending=\(self.isSendingStream) isRunning=\(self.isRunning)")
            return
        }
        guard count > 0 else {
            appendBoundedLog("[FAIL] chunk count must be greater than 0")
            return
        }

        fileLog("[SwiftTsWorkloadApp] sendHelloStreamChunks: count=\(count)")
        isSendingStream = true
        receivedEchoLines = []
        appendBoundedLog("--- Starting manual stream echo request: count=\(count) ---")
        defer { isSendingStream = false }

        var req = Local_StartProbeRequest()
        req.probeName = "stream-echo:\(count)"

        // Use targetType from environment or default
        let targetType = ProcessInfo.processInfo.environment["ACTR_SWIFTTSAPP_TARGET_TYPE"] ?? "actrium:DuplexEchoService:1.0.0"
        req.targetType = targetType

        do {
            let resp: Local_StartProbeResponse = try await self.actorRef!.call(
                req,
                timeoutMs: streamEchoRequestTimeoutMs(for: count)
            )
            appendBoundedLog("StreamEcho response: started=\(resp.started) msg=\(resp.message)")
        } catch {
            appendBoundedLog("[FAIL] StreamEcho RPC failed: \(error)")
            fileLog("[SwiftTsWorkloadApp] StreamEcho RPC failed: \(error)")
        }
    }

    func exportLogFile() throws -> URL {
        let formatter = DateFormatter()
        formatter.dateFormat = "yyyyMMdd-HHmmss"
        let filename = "SwiftTsWorkloadApp-\(formatter.string(from: Date())).log"
        let url = FileManager.default.temporaryDirectory.appendingPathComponent(filename)
        let body = logLines.joined(separator: "\n") + "\n"
        try body.write(to: url, atomically: true, encoding: .utf8)
        return url
    }

    func writeAutoResultFile(named filename: String) throws {
        let safeFilename = filename.replacingOccurrences(of: "/", with: "_")
        let supportURL = try FileManager.default.url(
            for: .applicationSupportDirectory,
            in: .userDomainMask,
            appropriateFor: nil,
            create: true
        )
        let url = supportURL.appendingPathComponent(safeFilename)
        let body = logLines.joined(separator: "\n") + "\n"
        try body.write(to: url, atomically: true, encoding: .utf8)
        fileLog("[SwiftTsWorkloadApp] wrote auto result file: \(url.path)")
    }

    func appendBoundedLog(_ line: String) {
        logLines.append(line)
        if logLines.count > maxLogLines {
            logLines.removeFirst(logLines.count - maxLogLines)
        }
    }

    func appendStreamLog(_ line: String, receivedLine: String? = nil) {
        appendBoundedLog(line)
        if let receivedLine {
            receivedEchoLines.append(receivedLine)
            if receivedEchoLines.count > maxReceivedEchoLines {
                receivedEchoLines.removeFirst(receivedEchoLines.count - maxReceivedEchoLines)
            }
        }
    }

    private func streamEchoRequestTimeoutMs(for count: Int) -> Int64 {
        max(60_000, Int64(count) * 2_500 + 60_000)
    }

    /// Reads actr.toml from the Bundle, appends hyper data_dir and trust config, writes to app support.
    private func materializeRuntimeConfig() throws -> URL {
        guard let templateURL = Bundle.main.url(forResource: "actr", withExtension: "toml") else {
            throw ActrServiceError.missingConfigTemplate
        }

        let fileManager = FileManager.default
        let supportURL = try fileManager.url(
            for: .applicationSupportDirectory,
            in: .userDomainMask,
            appropriateFor: nil,
            create: true
        )
        let appURL = supportURL.appendingPathComponent("SwiftTsWorkloadApp", isDirectory: true)
        let dataURL = appURL.appendingPathComponent("hyper", isDirectory: true)
        try fileManager.createDirectory(at: dataURL, withIntermediateDirectories: true)

        var config = try String(contentsOf: templateURL, encoding: .utf8)
        config += """

        [hyper]
        data_dir = "\(dataURL.path)"

        [hyper.trust]
        kind = "dev_only"
        """

        let configURL = appURL.appendingPathComponent("actr.toml")
        try config.write(to: configURL, atomically: true, encoding: .utf8)
        return configURL
    }
}

private enum ActrServiceError: Error {
    case missingConfigTemplate
}

// MARK: - E2E Result Emission

/// Emits the E2E result marker to both stdout and stderr so that run.sh can grep for it.
private func emitE2EResult(_ result: String) {
    let marker = "ACTR_E2E_RESULT:\(result)"
    print(marker)
    FileHandle.standardError.write(Data("\(marker)\n".utf8))
}

// MARK: - ProbeService RPC Handler

/// Implements ProbeServiceHandler.startProbe(req:ctx:).
/// When this RPC fires, ctx is delivered — discover target then run all probes.
private final class ProbeHandlerImpl: ProbeServiceHandler, @unchecked Sendable {
    private weak var service: ActrService?

    init(service: ActrService) {
        self.service = service
    }

    func startProbe(
        req: Local_StartProbeRequest,
        ctx: Context
    ) async throws -> Local_StartProbeResponse {
        fileLog("[SwiftTsWorkloadApp] 🔵 startProbe handler, discovering DuplexEchoService...")

        // Discover target synchronously so we can return immediately if not found
        let targetType = try ActrType.fromStringRepr(req.targetType.isEmpty ? "actrium:DuplexEchoService:1.0.0" : req.targetType)
        let target: ActrId
        do {
            target = try await ctx.discover(targetType: targetType)
            fileLog("[SwiftTsWorkloadApp] Discovered target: \(target.type.toStringRepr())")
        } catch {
            fileLog("[SwiftTsWorkloadApp] ❌ discover failed: \(error)")
            var resp = Local_StartProbeResponse()
            resp.started = false
            resp.message = "discover failed: \(error)"
            return resp
        }

        let svc = service
        let runner = DuplexEchoProbeRunner(ctx: ctx, target: target)

        if let count = streamEchoCount(from: req.probeName) {
            // ── call 验证：Echo RPC ──
            var callOk = false
            do {
                let token = "ping-\(UUID().uuidString)"
                var echoReq = Local_EchoRequest()
                echoReq.message = token
                let rd = try await ctx.callRaw(
                    target: target,
                    routeKey: Local_EchoRequest.routeKey,
                    payloadType: .rpcReliable,
                    payload: try echoReq.serializedData(),
                    timeoutMs: 30_000
                )
                let echoResp = try Local_EchoResponse(serializedBytes: rd)
                callOk = (echoResp.message == "echo:\(token)")
                fileLog("[SwiftTsWorkloadApp] Echo call ok=\(callOk) resp=\(echoResp.message)")
            } catch {
                fileLog("[SwiftTsWorkloadApp] ❌ Echo call failed: \(error)")
            }

            // ── stream 验证：duplex echo ──
            let result = await runner.runHelloStream(count: count) { line, receivedLine in
                await MainActor.run {
                    svc?.appendStreamLog(line, receivedLine: receivedLine)
                }
            }
            for line in result.logLines {
                fileLog("[SwiftTsWorkloadApp] \(line)")
            }
            await MainActor.run {
                svc?.receivedEchoLines = result.receivedLines
            }

            let expectedLines = (1...count).map { "received: echo:hello \($0)" }
            let passCount = zip(result.receivedLines, expectedLines).filter { $0.0 == $0.1 }.count
            let streamOk = result.succeeded && passCount == count && result.receivedLines == expectedLines
            await MainActor.run {
                emitE2EResult("call=\(callOk ? "ok" : "fail") stream=\(passCount)/\(count)")
            }

            var resp = Local_StartProbeResponse()
            resp.started = callOk && streamOk
            resp.message = "call=\(callOk) stream=\(passCount)/\(count)"
            return resp
        }

        // Run probes synchronously — ctx is only valid inside the handler
        let allResults = await runner.runAll()

        for r in allResults {
            let status = r.passed ? "PASS" : "FAIL"
            fileLog("[SwiftTsWorkloadApp] [\(status)] \(r.name) (\(r.durationMs)ms): \(r.details)")
        }
        let passCount = allResults.filter(\.passed).count
        fileLog("[SwiftTsWorkloadApp] Done: \(passCount)/\(allResults.count) passed")

        // Emit E2E result marker for run-all mode
        await MainActor.run {
            emitE2EResult("\(passCount)/\(allResults.count)")
        }

        await MainActor.run {
            svc?.results = allResults
            svc?.isRunning = false
            for r in allResults {
                for line in r.logLines { svc?.appendBoundedLog(line) }
            }
        }

        var resp = Local_StartProbeResponse()
        resp.started = true
        resp.message = "\(passCount)/\(allResults.count) passed"
        return resp
    }

    private func streamEchoCount(from probeName: String) -> Int? {
        let prefix = "stream-echo:"
        guard probeName.hasPrefix(prefix) else { return nil }
        let value = String(probeName.dropFirst(prefix.count))
        return Int(value).flatMap { $0 > 0 ? $0 : nil }
    }
}

// MARK: - Lifecycle Adapter

private final class ProbeLifecycleAdapter: Workload, @unchecked Sendable {
    private let workload: ProbeServiceWorkload<ProbeHandlerImpl>

    init(workload: ProbeServiceWorkload<ProbeHandlerImpl>) {
        self.workload = workload
    }

    func onStart(ctx: ContextBridge) async throws {}
    func onReady(ctx: ContextBridge) async throws {}
    func onStop(ctx: ContextBridge) async throws {}

    func onError(ctx: ContextBridge, event: ErrorEventBridge) async throws {
        fileLog("[SwiftTsWorkloadApp] ProbeLifecycleAdapter error: \(event)")
    }

    func dispatch(ctx: ContextBridge, envelope: RpcEnvelopeBridge) async throws -> Data {
        try await workload.__dispatch(ctx: ctx, envelope: envelope)
    }
}

private func elapsedMs(from duration: Duration) -> Int64 {
    let c = duration.components
    return Int64(c.seconds * 1000) + Int64(c.attoseconds / 1_000_000_000_000_000)
}

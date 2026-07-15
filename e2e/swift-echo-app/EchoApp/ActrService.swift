import Actr
import Foundation
import SwiftProtobuf
import SwiftUI

// ACTR: mutable scaffold
// User-owned business scaffold. Keep EchoApp/Generated immutable and customize this file.

@MainActor
final class ActrService: ObservableObject {
    @Published var status = "Starting ACTR node..."
    @Published var errorMessage: String?

    private var actrNode: ActrNode?
    private var actorRef: ActrRef?
    private var linkedWorkload: DynamicWorkload?
    private var hasAutoSent = false
    private var isStarting = false

    var isReady: Bool {
        actorRef != nil
    }

    func startIfNeeded() async {
        guard actorRef == nil, !isStarting else { return }
        isStarting = true
        defer { isStarting = false }

        do {
            let configURL = try materializeRuntimeConfig()
            let echoServiceTarget = "actrium:EchoService:1.0.0"
            let actorType = ActrType(manufacturer: "actrium", name: "EchoApp", version: "0.1.0")

            let workload = Actr.dynamicWorkload(
                lifecycle: LocalEchoServiceLifecycleAdapter(targetType: echoServiceTarget)
            )
            linkedWorkload = workload
            let node = try await ActrNode.linked(config: configURL, type: actorType, workload: workload)

            actorRef = try await node.start()
            actrNode = node
            status = "Ready: \(actorType.toStringRepr())"
        } catch {
            status = "ACTR startup failed: \(error)"
            errorMessage = String(describing: error)
        }
    }

    func stop() async {
        guard let actorRef else { return }
        await actorRef.stop()
        self.actorRef = nil
        actrNode = nil
    }

    func sendEcho(_ input: String) async throws -> String {
        guard let actorRef else {
            throw EchoAppError.actorUnavailable
        }

        var localRequest = Echoapp_LocalEchoRequest()
        localRequest.message = input
        let response: Echoapp_LocalEchoResponse = try await actorRef.call(localRequest)
        autoMarkSent()
        return response.reply
    }
    func shouldAutoSend() -> Bool {
        ProcessInfo.processInfo.environment["ACTR_ECHOAPP_AUTO_SEND"] == "1" && !hasAutoSent
    }

    private func autoMarkSent() {
        hasAutoSent = true
    }

    private func materializeRuntimeConfig() throws -> URL {
        guard let templateURL = Bundle.main.url(forResource: "actr", withExtension: "toml") else {
            throw EchoAppError.missingConfigTemplate
        }

        let fileManager = FileManager.default
        let supportURL = try fileManager.url(
            for: .applicationSupportDirectory,
            in: .userDomainMask,
            appropriateFor: nil,
            create: true
        )
        let appURL = supportURL.appendingPathComponent("EchoApp", isDirectory: true)
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

private enum EchoAppError: Error {
    case missingConfigTemplate
    case actorUnavailable
    case remoteActorUnavailable
}

/// Safe: no mutable state. All fields are value types or immutable references.
private final class LocalEchoServiceHandlerImpl: LocalEchoServiceHandler, @unchecked Sendable {
    private let remoteEchoRoute = "echo.EchoService.Echo"
    private let targetType: String

    init(targetType: String) {
        self.targetType = targetType
    }

    func send(
        req localRequest: Echoapp_LocalEchoRequest,
        ctx: any ActrContext
    ) async throws(ActrError) -> Echoapp_LocalEchoResponse {
        let forwardedMessage = "swift-local:\(localRequest.message)"

        var remoteRequest = Echo_EchoRequest()
        remoteRequest.message = forwardedMessage
        let targetType: ActrType
        do {
            targetType = try ActrType.fromStringRepr(self.targetType)
        } catch {
            throw ActrError.Config(msg: "Invalid target type: \(error)")
        }
        let targetId = try await ctx.discover(targetType: targetType)
        let requestData: Data
        do {
            requestData = try remoteRequest.serializedData()
        } catch {
            throw ActrError.DecodeFailure(msg: "Failed to encode echo request: \(error)")
        }
        let remotePayload = try await ctx.call(
            target: targetId,
            routeKey: remoteEchoRoute,
            payload: requestData
        )

        let remoteResponse: Echo_EchoResponse
        do {
            remoteResponse = try Echo_EchoResponse(serializedBytes: remotePayload)
        } catch {
            throw ActrError.DecodeFailure(msg: "Failed to decode echo response: \(error)")
        }
        var localResponse = Echoapp_LocalEchoResponse()
        localResponse.reply = remoteResponse.reply
        localResponse.forwardedMessage = forwardedMessage
        return localResponse
    }
}

/// Safe: no mutable state. `workload` is a let-bound actor.
private final class LocalEchoServiceLifecycleAdapter: Workload, @unchecked Sendable {
    private let workload: LocalEchoServiceWorkload<LocalEchoServiceHandlerImpl>

    init(targetType: String) {
        self.workload = LocalEchoServiceWorkload(handler: LocalEchoServiceHandlerImpl(targetType: targetType))
    }

    func onStart(ctx: Context) async throws {}

    func onReady(ctx: Context) async throws {}

    func onStop(ctx: Context) async throws {}

    func onError(ctx: Context, event: ErrorEvent) async throws {
        print("LocalEchoServiceLifecycleAdapter error: \(event)")
    }

    func dispatch(ctx: Context, envelope: RpcEnvelope) async throws -> Data {
        return try await workload.__dispatch(ctx: ctx, envelope: envelope)
    }
}

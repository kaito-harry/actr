import ActrBindings
import Foundation

public protocol Workload: AnyObject, Sendable {
    func onStart(ctx: Context) async throws
    func onReady(ctx: Context) async throws
    func onStop(ctx: Context) async throws
    func onError(ctx: Context, event: ErrorEvent) async throws
    func dispatch(ctx: Context, envelope: RpcEnvelope) async throws -> Data
}

public protocol SignalingObserver: AnyObject, Sendable {
    func onConnecting(ctx: Context?) async
    func onConnected(ctx: Context?) async
    func onDisconnected(ctx: Context) async
}

public protocol WebSocketObserver: AnyObject, Sendable {
    func onConnecting(ctx: Context, event: PeerEvent) async
    func onConnected(ctx: Context, event: PeerEvent) async
    func onDisconnected(ctx: Context, event: PeerEvent) async
}

public protocol WebRTCObserver: AnyObject, Sendable {
    func onConnecting(ctx: Context, event: PeerEvent) async
    func onConnected(ctx: Context, event: PeerEvent) async
    func onDisconnected(ctx: Context, event: PeerEvent) async
}

public protocol CredentialObserver: AnyObject, Sendable {
    func onRenewed(ctx: Context, event: CredentialEvent) async
    func onExpiring(ctx: Context, event: CredentialEvent) async
}

public protocol MailboxObserver: AnyObject, Sendable {
    func onBackpressure(ctx: Context, event: BackpressureEvent) async
}

public protocol DataStreamCallback: AnyObject, Sendable {
    func onStream(chunk: DataStream, sender: ActrId) async throws
}

public protocol MediaTrackCallback: AnyObject, Sendable {
    func onSample(sample: MediaSample, sender: ActrId) async throws
}

public protocol LogCallback: AnyObject, Sendable {
    func onLog(level: String, target: String, message: String, timestampMs: Int64)
}

public final class DynamicWorkload: @unchecked Sendable {
    let bridge: ActrBindings.DynamicWorkload
    private let lifecycleAdapter: WorkloadAdapter
    private let signalingAdapter: SignalingObserverAdapter?
    private let websocketAdapter: WebSocketObserverAdapter?
    private let webrtcAdapter: WebRTCObserverAdapter?
    private let credentialAdapter: CredentialObserverAdapter?
    private let mailboxAdapter: MailboxObserverAdapter?

    public init(
        lifecycle: any Workload,
        signaling: (any SignalingObserver)? = nil,
        websocket: (any WebSocketObserver)? = nil,
        webrtc: (any WebRTCObserver)? = nil,
        credential: (any CredentialObserver)? = nil,
        mailbox: (any MailboxObserver)? = nil
    ) {
        let lifecycleAdapter = WorkloadAdapter(lifecycle)
        let signalingAdapter = signaling.map { SignalingObserverAdapter($0) }
        let websocketAdapter = websocket.map { WebSocketObserverAdapter($0) }
        let webrtcAdapter = webrtc.map { WebRTCObserverAdapter($0) }
        let credentialAdapter = credential.map { CredentialObserverAdapter($0) }
        let mailboxAdapter = mailbox.map { MailboxObserverAdapter($0) }

        self.lifecycleAdapter = lifecycleAdapter
        self.signalingAdapter = signalingAdapter
        self.websocketAdapter = websocketAdapter
        self.webrtcAdapter = webrtcAdapter
        self.credentialAdapter = credentialAdapter
        self.mailboxAdapter = mailboxAdapter
        self.bridge = ActrBindings.DynamicWorkload(
            lifecycle: lifecycleAdapter,
            signaling: signalingAdapter,
            websocket: websocketAdapter,
            webrtc: webrtcAdapter,
            credential: credentialAdapter,
            mailbox: mailboxAdapter
        )
    }
}

public final class RuntimeObservers: @unchecked Sendable {
    let bridge: ActrBindings.RuntimeObservers
    private let signalingAdapter: SignalingObserverAdapter?
    private let websocketAdapter: WebSocketObserverAdapter?
    private let webrtcAdapter: WebRTCObserverAdapter?
    private let credentialAdapter: CredentialObserverAdapter?
    private let mailboxAdapter: MailboxObserverAdapter?

    public init(
        signaling: (any SignalingObserver)? = nil,
        websocket: (any WebSocketObserver)? = nil,
        webrtc: (any WebRTCObserver)? = nil,
        credential: (any CredentialObserver)? = nil,
        mailbox: (any MailboxObserver)? = nil
    ) {
        let signalingAdapter = signaling.map { SignalingObserverAdapter($0) }
        let websocketAdapter = websocket.map { WebSocketObserverAdapter($0) }
        let webrtcAdapter = webrtc.map { WebRTCObserverAdapter($0) }
        let credentialAdapter = credential.map { CredentialObserverAdapter($0) }
        let mailboxAdapter = mailbox.map { MailboxObserverAdapter($0) }

        self.signalingAdapter = signalingAdapter
        self.websocketAdapter = websocketAdapter
        self.webrtcAdapter = webrtcAdapter
        self.credentialAdapter = credentialAdapter
        self.mailboxAdapter = mailboxAdapter
        self.bridge = ActrBindings.RuntimeObservers(
            signaling: signalingAdapter,
            websocket: websocketAdapter,
            webrtc: webrtcAdapter,
            credential: credentialAdapter,
            mailbox: mailboxAdapter
        )
    }
}

public final class OpusEncoder: @unchecked Sendable {
    private let bridge: ActrBindings.OpusEncoder

    public init(sampleRate: UInt32, channels: UInt8, frameSize: UInt16) throws {
        do {
            bridge = try ActrBindings.OpusEncoder(
                sampleRate: sampleRate,
                channels: channels,
                frameSize: frameSize
            )
        } catch {
            throw ActrError(error: error)
        }
    }

    public func encode(pcm: [Float]) throws -> Data {
        do {
            return try bridge.encode(pcm: pcm)
        } catch {
            throw ActrError(error: error)
        }
    }

    public func frameSize() -> UInt16 {
        bridge.frameSize()
    }
}

final class WorkloadAdapter: ActrBindings.WorkloadLifecycleBridge, @unchecked Sendable {
    private let workload: any Workload

    init(_ workload: any Workload) {
        self.workload = workload
    }

    func onStart(ctx: ActrBindings.ContextBridge) async throws {
        do {
            try await workload.onStart(ctx: Context(bridge: ctx))
        } catch {
            throw ActrError.bridge(from: error)
        }
    }

    func onReady(ctx: ActrBindings.ContextBridge) async throws {
        do {
            try await workload.onReady(ctx: Context(bridge: ctx))
        } catch {
            throw ActrError.bridge(from: error)
        }
    }

    func onStop(ctx: ActrBindings.ContextBridge) async throws {
        do {
            try await workload.onStop(ctx: Context(bridge: ctx))
        } catch {
            throw ActrError.bridge(from: error)
        }
    }

    func onError(ctx: ActrBindings.ContextBridge, event: ActrBindings.ErrorEventBridge) async throws {
        do {
            try await workload.onError(
                ctx: Context(bridge: ctx),
                event: ErrorEvent(bridge: event)
            )
        } catch {
            throw ActrError.bridge(from: error)
        }
    }

    func dispatch(ctx: ActrBindings.ContextBridge, envelope: ActrBindings.RpcEnvelopeBridge) async throws -> Data {
        do {
            return try await workload.dispatch(
                ctx: Context(bridge: ctx),
                envelope: RpcEnvelope(bridge: envelope)
            )
        } catch {
            throw ActrError.bridge(from: error)
        }
    }
}

final class SignalingObserverAdapter: ActrBindings.SignalingObserverBridge, @unchecked Sendable {
    private let observer: any SignalingObserver

    init(_ observer: any SignalingObserver) {
        self.observer = observer
    }

    func onConnecting(ctx: ActrBindings.ContextBridge?) async {
        await observer.onConnecting(ctx: ctx.map(Context.init(bridge:)))
    }

    func onConnected(ctx: ActrBindings.ContextBridge?) async {
        await observer.onConnected(ctx: ctx.map(Context.init(bridge:)))
    }

    func onDisconnected(ctx: ActrBindings.ContextBridge) async {
        await observer.onDisconnected(ctx: Context(bridge: ctx))
    }
}

final class WebSocketObserverAdapter: ActrBindings.WebSocketObserverBridge, @unchecked Sendable {
    private let observer: any WebSocketObserver

    init(_ observer: any WebSocketObserver) {
        self.observer = observer
    }

    func onConnecting(ctx: ActrBindings.ContextBridge, event: ActrBindings.PeerEventBridge) async {
        await observer.onConnecting(ctx: Context(bridge: ctx), event: PeerEvent(bridge: event))
    }

    func onConnected(ctx: ActrBindings.ContextBridge, event: ActrBindings.PeerEventBridge) async {
        await observer.onConnected(ctx: Context(bridge: ctx), event: PeerEvent(bridge: event))
    }

    func onDisconnected(ctx: ActrBindings.ContextBridge, event: ActrBindings.PeerEventBridge) async {
        await observer.onDisconnected(ctx: Context(bridge: ctx), event: PeerEvent(bridge: event))
    }
}

final class WebRTCObserverAdapter: ActrBindings.WebRtcObserverBridge, @unchecked Sendable {
    private let observer: any WebRTCObserver

    init(_ observer: any WebRTCObserver) {
        self.observer = observer
    }

    func onConnecting(ctx: ActrBindings.ContextBridge, event: ActrBindings.PeerEventBridge) async {
        await observer.onConnecting(ctx: Context(bridge: ctx), event: PeerEvent(bridge: event))
    }

    func onConnected(ctx: ActrBindings.ContextBridge, event: ActrBindings.PeerEventBridge) async {
        await observer.onConnected(ctx: Context(bridge: ctx), event: PeerEvent(bridge: event))
    }

    func onDisconnected(ctx: ActrBindings.ContextBridge, event: ActrBindings.PeerEventBridge) async {
        await observer.onDisconnected(ctx: Context(bridge: ctx), event: PeerEvent(bridge: event))
    }
}

final class CredentialObserverAdapter: ActrBindings.CredentialObserverBridge, @unchecked Sendable {
    private let observer: any CredentialObserver

    init(_ observer: any CredentialObserver) {
        self.observer = observer
    }

    func onRenewed(ctx: ActrBindings.ContextBridge, event: ActrBindings.CredentialEventBridge) async {
        await observer.onRenewed(ctx: Context(bridge: ctx), event: CredentialEvent(bridge: event))
    }

    func onExpiring(ctx: ActrBindings.ContextBridge, event: ActrBindings.CredentialEventBridge) async {
        await observer.onExpiring(ctx: Context(bridge: ctx), event: CredentialEvent(bridge: event))
    }
}

final class MailboxObserverAdapter: ActrBindings.MailboxObserverBridge, @unchecked Sendable {
    private let observer: any MailboxObserver

    init(_ observer: any MailboxObserver) {
        self.observer = observer
    }

    func onBackpressure(ctx: ActrBindings.ContextBridge, event: ActrBindings.BackpressureEventBridge) async {
        await observer.onBackpressure(ctx: Context(bridge: ctx), event: BackpressureEvent(bridge: event))
    }
}

final class DataStreamCallbackAdapter: ActrBindings.DataStreamCallback, @unchecked Sendable {
    private let callback: any DataStreamCallback

    init(_ callback: any DataStreamCallback) {
        self.callback = callback
    }

    func onStream(chunk: ActrBindings.DataStream, sender: ActrBindings.ActrId) async throws {
        do {
            try await callback.onStream(
                chunk: DataStream(bridge: chunk),
                sender: ActrId(bridge: sender)
            )
        } catch {
            throw ActrError.bridge(from: error)
        }
    }
}

final class MediaTrackCallbackAdapter: ActrBindings.MediaTrackCallback, @unchecked Sendable {
    private let callback: any MediaTrackCallback

    init(_ callback: any MediaTrackCallback) {
        self.callback = callback
    }

    func onSample(sample: ActrBindings.MediaSample, sender: ActrBindings.ActrId) async throws {
        do {
            try await callback.onSample(
                sample: MediaSample(bridge: sample),
                sender: ActrId(bridge: sender)
            )
        } catch {
            throw ActrError.bridge(from: error)
        }
    }
}

final class LogCallbackAdapter: ActrBindings.LogCallback, @unchecked Sendable {
    private let callback: any LogCallback

    init(_ callback: any LogCallback) {
        self.callback = callback
    }

    func onLog(level: String, target: String, message: String, timestampMs: Int64) {
        callback.onLog(level: level, target: target, message: message, timestampMs: timestampMs)
    }
}

private final class LogCallbackStore: @unchecked Sendable {
    private let lock = NSLock()
    private var adapter: LogCallbackAdapter?

    func set(_ adapter: LogCallbackAdapter?) {
        lock.lock()
        self.adapter = adapter
        lock.unlock()
    }
}

private let logCallbackStore = LogCallbackStore()

public func setLogCallback(callback: (any LogCallback)?) {
    let adapter = callback.map { LogCallbackAdapter($0) }
    ActrBindings.setLogCallback(callback: adapter)
    logCallbackStore.set(adapter)
}

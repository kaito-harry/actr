import ActrBindings
import Foundation

public struct Realm: Equatable, Hashable, Sendable {
    public var realmId: UInt32

    public init(realmId: UInt32) {
        self.realmId = realmId
    }

    init(bridge: ActrBindings.Realm) {
        self.init(realmId: bridge.realmId)
    }

    var bridge: ActrBindings.Realm {
        ActrBindings.Realm(realmId: realmId)
    }
}

public struct ActrType: Equatable, Hashable, Sendable {
    public var manufacturer: String
    public var name: String
    public var version: String

    public init(manufacturer: String, name: String, version: String) {
        self.manufacturer = manufacturer
        self.name = name
        self.version = version
    }

    init(bridge: ActrBindings.ActrType) {
        self.init(
            manufacturer: bridge.manufacturer,
            name: bridge.name,
            version: bridge.version
        )
    }

    var bridge: ActrBindings.ActrType {
        ActrBindings.ActrType(
            manufacturer: manufacturer,
            name: name,
            version: version
        )
    }
}

public struct ActrId: Equatable, Hashable, Sendable {
    public var realm: Realm
    public var serialNumber: UInt64
    public var type: ActrType

    public init(realm: Realm, serialNumber: UInt64, type: ActrType) {
        self.realm = realm
        self.serialNumber = serialNumber
        self.type = type
    }

    init(bridge: ActrBindings.ActrId) {
        self.init(
            realm: Realm(bridge: bridge.realm),
            serialNumber: bridge.serialNumber,
            type: ActrType(bridge: bridge.type)
        )
    }

    var bridge: ActrBindings.ActrId {
        ActrBindings.ActrId(
            realm: realm.bridge,
            serialNumber: serialNumber,
            type: type.bridge
        )
    }
}

public enum PayloadType: Equatable, Hashable, Sendable {
    case rpcReliable
    case rpcSignal
    case streamReliable
    case streamLatencyFirst
    case mediaRtp

    init(bridge: ActrBindings.PayloadType) {
        switch bridge {
        case .rpcReliable:
            self = .rpcReliable
        case .rpcSignal:
            self = .rpcSignal
        case .streamReliable:
            self = .streamReliable
        case .streamLatencyFirst:
            self = .streamLatencyFirst
        case .mediaRtp:
            self = .mediaRtp
        }
    }

    var bridge: ActrBindings.PayloadType {
        switch self {
        case .rpcReliable:
            return .rpcReliable
        case .rpcSignal:
            return .rpcSignal
        case .streamReliable:
            return .streamReliable
        case .streamLatencyFirst:
            return .streamLatencyFirst
        case .mediaRtp:
            return .mediaRtp
        }
    }
}

public struct ConnectionNotReadyInfo: Equatable, Hashable, Sendable {
    public var retryAfterMs: UInt64?

    public init(retryAfterMs: UInt64?) {
        self.retryAfterMs = retryAfterMs
    }

    init(bridge: ActrBindings.ConnectionNotReadyInfo) {
        self.init(retryAfterMs: bridge.retryAfterMs)
    }

    var bridge: ActrBindings.ConnectionNotReadyInfo {
        ActrBindings.ConnectionNotReadyInfo(retryAfterMs: retryAfterMs)
    }
}

public enum ActrError: Error, Equatable, Hashable, LocalizedError, Sendable {
    case Unavailable(msg: String)
    case ConnectionNotReady(info: ConnectionNotReadyInfo)
    case TimedOut
    case NotFound(msg: String)
    case PermissionDenied(msg: String)
    case InvalidArgument(msg: String)
    case UnknownRoute(msg: String)
    case DependencyNotFound(serviceName: String, detail: String)
    case DecodeFailure(msg: String)
    case NotImplemented(msg: String)
    case Internal(msg: String)
    case Config(msg: String)

    public var errorDescription: String? {
        String(reflecting: self)
    }

    init(bridge: ActrBindings.ActrError) {
        switch bridge {
        case let .Unavailable(msg):
            self = .Unavailable(msg: msg)
        case let .ConnectionNotReady(info):
            self = .ConnectionNotReady(info: ConnectionNotReadyInfo(bridge: info))
        case .TimedOut:
            self = .TimedOut
        case let .NotFound(msg):
            self = .NotFound(msg: msg)
        case let .PermissionDenied(msg):
            self = .PermissionDenied(msg: msg)
        case let .InvalidArgument(msg):
            self = .InvalidArgument(msg: msg)
        case let .UnknownRoute(msg):
            self = .UnknownRoute(msg: msg)
        case let .DependencyNotFound(serviceName, detail):
            self = .DependencyNotFound(serviceName: serviceName, detail: detail)
        case let .DecodeFailure(msg):
            self = .DecodeFailure(msg: msg)
        case let .NotImplemented(msg):
            self = .NotImplemented(msg: msg)
        case let .Internal(msg):
            self = .Internal(msg: msg)
        case let .Config(msg):
            self = .Config(msg: msg)
        }
    }

    init(error: Error) {
        if let error = error as? ActrError {
            self = error
        } else if let error = error as? ActrBindings.ActrError {
            self = ActrError(bridge: error)
        } else {
            self = .Internal(msg: String(describing: error))
        }
    }

    var bridge: ActrBindings.ActrError {
        switch self {
        case let .Unavailable(msg):
            return .Unavailable(msg: msg)
        case let .ConnectionNotReady(info):
            return .ConnectionNotReady(info: info.bridge)
        case .TimedOut:
            return .TimedOut
        case let .NotFound(msg):
            return .NotFound(msg: msg)
        case let .PermissionDenied(msg):
            return .PermissionDenied(msg: msg)
        case let .InvalidArgument(msg):
            return .InvalidArgument(msg: msg)
        case let .UnknownRoute(msg):
            return .UnknownRoute(msg: msg)
        case let .DependencyNotFound(serviceName, detail):
            return .DependencyNotFound(serviceName: serviceName, detail: detail)
        case let .DecodeFailure(msg):
            return .DecodeFailure(msg: msg)
        case let .NotImplemented(msg):
            return .NotImplemented(msg: msg)
        case let .Internal(msg):
            return .Internal(msg: msg)
        case let .Config(msg):
            return .Config(msg: msg)
        }
    }

    static func bridge(from error: Error) -> ActrBindings.ActrError {
        if let error = error as? ActrBindings.ActrError {
            return error
        }
        return ActrError(error: error).bridge
    }
}

public enum ErrorKind: Equatable, Hashable, Sendable {
    case transient
    case client
    case `internal`
    case corrupt

    init(bridge: ActrBindings.ErrorKind) {
        switch bridge {
        case .transient:
            self = .transient
        case .client:
            self = .client
        case .internal:
            self = .internal
        case .corrupt:
            self = .corrupt
        }
    }

    var bridge: ActrBindings.ErrorKind {
        switch self {
        case .transient:
            return .transient
        case .client:
            return .client
        case .internal:
            return .internal
        case .corrupt:
            return .corrupt
        }
    }
}

public enum ErrorCategory: Equatable, Hashable, Sendable {
    case handlerPanic
    case handlerError
    case signalingFailure
    case transportFailure
    case dataStreamDeliveryUncertain

    init(bridge: ActrBindings.ErrorCategoryBridge) {
        switch bridge {
        case .handlerPanic:
            self = .handlerPanic
        case .handlerError:
            self = .handlerError
        case .signalingFailure:
            self = .signalingFailure
        case .transportFailure:
            self = .transportFailure
        case .dataStreamDeliveryUncertain:
            self = .dataStreamDeliveryUncertain
        }
    }

    var bridge: ActrBindings.ErrorCategoryBridge {
        switch self {
        case .handlerPanic:
            return .handlerPanic
        case .handlerError:
            return .handlerError
        case .signalingFailure:
            return .signalingFailure
        case .transportFailure:
            return .transportFailure
        case .dataStreamDeliveryUncertain:
            return .dataStreamDeliveryUncertain
        }
    }
}

public struct ErrorEvent: Equatable, Hashable, Sendable {
    public var source: String
    public var category: ErrorCategory
    public var context: String
    public var timestampMs: Int64

    public init(source: String, category: ErrorCategory, context: String, timestampMs: Int64) {
        self.source = source
        self.category = category
        self.context = context
        self.timestampMs = timestampMs
    }

    init(bridge: ActrBindings.ErrorEventBridge) {
        self.init(
            source: bridge.source,
            category: ErrorCategory(bridge: bridge.category),
            context: bridge.context,
            timestampMs: bridge.timestampMs
        )
    }

    var bridge: ActrBindings.ErrorEventBridge {
        ActrBindings.ErrorEventBridge(
            source: source,
            category: category.bridge,
            context: context,
            timestampMs: timestampMs
        )
    }
}

public enum WebRTCPeerStatus: Equatable, Hashable, Sendable {
    case idle
    case connecting
    case connected
    case recovering

    init(bridge: ActrBindings.WebRtcPeerStatusBridge) {
        switch bridge {
        case .idle:
            self = .idle
        case .connecting:
            self = .connecting
        case .connected:
            self = .connected
        case .recovering:
            self = .recovering
        }
    }

    var bridge: ActrBindings.WebRtcPeerStatusBridge {
        switch self {
        case .idle:
            return .idle
        case .connecting:
            return .connecting
        case .connected:
            return .connected
        case .recovering:
            return .recovering
        }
    }
}

public struct PeerEvent: Equatable, Hashable, Sendable {
    public var peer: ActrId
    public var relayed: Bool?
    public var status: WebRTCPeerStatus?

    public init(peer: ActrId, relayed: Bool?, status: WebRTCPeerStatus?) {
        self.peer = peer
        self.relayed = relayed
        self.status = status
    }

    init(bridge: ActrBindings.PeerEventBridge) {
        self.init(
            peer: ActrId(bridge: bridge.peer),
            relayed: bridge.relayed,
            status: bridge.status.map(WebRTCPeerStatus.init(bridge:))
        )
    }

    var bridge: ActrBindings.PeerEventBridge {
        ActrBindings.PeerEventBridge(
            peer: peer.bridge,
            relayed: relayed,
            status: status?.bridge
        )
    }
}

public struct CredentialEvent: Equatable, Hashable, Sendable {
    public var newExpiryMs: Int64

    public init(newExpiryMs: Int64) {
        self.newExpiryMs = newExpiryMs
    }

    init(bridge: ActrBindings.CredentialEventBridge) {
        self.init(newExpiryMs: bridge.newExpiryMs)
    }

    var bridge: ActrBindings.CredentialEventBridge {
        ActrBindings.CredentialEventBridge(newExpiryMs: newExpiryMs)
    }
}

public struct BackpressureEvent: Equatable, Hashable, Sendable {
    public var queueLen: UInt64
    public var threshold: UInt64

    public init(queueLen: UInt64, threshold: UInt64) {
        self.queueLen = queueLen
        self.threshold = threshold
    }

    init(bridge: ActrBindings.BackpressureEventBridge) {
        self.init(queueLen: bridge.queueLen, threshold: bridge.threshold)
    }

    var bridge: ActrBindings.BackpressureEventBridge {
        ActrBindings.BackpressureEventBridge(queueLen: queueLen, threshold: threshold)
    }
}

public struct RpcEnvelope: Equatable, Hashable, Sendable {
    public var routeKey: String
    public var payload: Data
    public var requestId: String

    public init(routeKey: String, payload: Data, requestId: String) {
        self.routeKey = routeKey
        self.payload = payload
        self.requestId = requestId
    }

    init(bridge: ActrBindings.RpcEnvelopeBridge) {
        self.init(
            routeKey: bridge.routeKey,
            payload: bridge.payload,
            requestId: bridge.requestId
        )
    }

    var bridge: ActrBindings.RpcEnvelopeBridge {
        ActrBindings.RpcEnvelopeBridge(
            routeKey: routeKey,
            payload: payload,
            requestId: requestId
        )
    }
}

public struct MetadataEntry: Equatable, Hashable, Sendable {
    public var key: String
    public var value: String

    public init(key: String, value: String) {
        self.key = key
        self.value = value
    }

    init(bridge: ActrBindings.MetadataEntry) {
        self.init(key: bridge.key, value: bridge.value)
    }

    var bridge: ActrBindings.MetadataEntry {
        ActrBindings.MetadataEntry(key: key, value: value)
    }
}

public struct DataStream: Equatable, Hashable, Sendable {
    public var streamId: String
    public var sequence: UInt64
    public var payload: Data
    public var metadata: [MetadataEntry]
    public var timestampMs: Int64?

    public init(
        streamId: String,
        sequence: UInt64,
        payload: Data,
        metadata: [MetadataEntry],
        timestampMs: Int64?
    ) {
        self.streamId = streamId
        self.sequence = sequence
        self.payload = payload
        self.metadata = metadata
        self.timestampMs = timestampMs
    }

    init(bridge: ActrBindings.DataStream) {
        self.init(
            streamId: bridge.streamId,
            sequence: bridge.sequence,
            payload: bridge.payload,
            metadata: bridge.metadata.map(MetadataEntry.init(bridge:)),
            timestampMs: bridge.timestampMs
        )
    }

    var bridge: ActrBindings.DataStream {
        ActrBindings.DataStream(
            streamId: streamId,
            sequence: sequence,
            payload: payload,
            metadata: metadata.map(\.bridge),
            timestampMs: timestampMs
        )
    }
}

public enum MediaType: Equatable, Hashable, Sendable {
    case audio
    case video

    init(bridge: ActrBindings.MediaType) {
        switch bridge {
        case .audio:
            self = .audio
        case .video:
            self = .video
        }
    }

    var bridge: ActrBindings.MediaType {
        switch self {
        case .audio:
            return .audio
        case .video:
            return .video
        }
    }
}

public struct MediaSample: Equatable, Hashable, Sendable {
    public var data: Data
    public var timestamp: UInt32
    public var codec: String
    public var mediaType: MediaType

    public init(data: Data, timestamp: UInt32, codec: String, mediaType: MediaType) {
        self.data = data
        self.timestamp = timestamp
        self.codec = codec
        self.mediaType = mediaType
    }

    init(bridge: ActrBindings.MediaSample) {
        self.init(
            data: bridge.data,
            timestamp: bridge.timestamp,
            codec: bridge.codec,
            mediaType: MediaType(bridge: bridge.mediaType)
        )
    }

    var bridge: ActrBindings.MediaSample {
        ActrBindings.MediaSample(
            data: data,
            timestamp: timestamp,
            codec: codec,
            mediaType: mediaType.bridge
        )
    }
}

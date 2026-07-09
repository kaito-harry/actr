import ActrBindings
import Foundation
import SwiftProtobuf

public protocol ActrContext: Sendable {
    var selfId: ActrId { get }
    var callerId: ActrId? { get }
    var requestId: String { get }

    func callRaw(
        target: ActrId,
        routeKey: String,
        payloadType: PayloadType,
        payload: Data,
        timeoutMs: Int64
    ) async throws(ActrError) -> Data

    func discover(targetType: ActrType) async throws(ActrError) -> ActrId

    func tellRaw(
        target: ActrId,
        routeKey: String,
        payloadType: PayloadType,
        payload: Data
    ) async throws(ActrError)

    func registerStream(
        streamId: String,
        callback: any DataChunkCallback
    ) async throws(ActrError)

    func unregisterStream(streamId: String) async throws(ActrError)

    func sendDataChunk(
        target: ActrId,
        chunk: DataChunk,
        payloadType: PayloadType
    ) async throws(ActrError)

    func addMediaTrack(
        target: ActrId,
        trackId: String,
        codec: String,
        mediaType: String
    ) async throws(ActrError)

    func removeMediaTrack(target: ActrId, trackId: String) async throws(ActrError)

    func sendMediaSample(
        target: ActrId,
        trackId: String,
        sample: MediaSample
    ) async throws(ActrError)

    func registerMediaTrack(
        trackId: String,
        callback: any MediaTrackCallback
    ) async throws(ActrError)

    func unregisterMediaTrack(trackId: String) async throws(ActrError)

    func log(level: LogLevel, msg: String)
}

func encodeProtobufMessage<M: Message>(_ message: M, context: String) throws(ActrError) -> Data {
    do {
        return try message.serializedData()
    } catch {
        throw ActrError.DecodeFailure(msg: "Failed to encode \(context): \(error)")
    }
}

func decodeProtobufMessage<M: Message>(_ type: M.Type, from data: Data, context: String) throws(ActrError) -> M {
    do {
        return try M(serializedBytes: data)
    } catch {
        throw ActrError.DecodeFailure(msg: "Failed to decode \(context): \(error)")
    }
}

public extension ActrContext {
    func registerStream(streamId _: String, callback _: any DataChunkCallback) async throws(ActrError) {
        throw ActrError.NotImplemented(msg: "registerStream is not implemented by this ActrContext")
    }

    func unregisterStream(streamId _: String) async throws(ActrError) {
        throw ActrError.NotImplemented(msg: "unregisterStream is not implemented by this ActrContext")
    }

    func sendDataChunk(
        target _: ActrId,
        chunk _: DataChunk,
        payloadType _: PayloadType
    ) async throws(ActrError) {
        throw ActrError.NotImplemented(msg: "sendDataChunk is not implemented by this ActrContext")
    }

    func addMediaTrack(
        target _: ActrId,
        trackId _: String,
        codec _: String,
        mediaType _: String
    ) async throws(ActrError) {
        throw ActrError.NotImplemented(msg: "addMediaTrack is not implemented by this ActrContext")
    }

    func removeMediaTrack(target _: ActrId, trackId _: String) async throws(ActrError) {
        throw ActrError.NotImplemented(msg: "removeMediaTrack is not implemented by this ActrContext")
    }

    func sendMediaSample(
        target _: ActrId,
        trackId _: String,
        sample _: MediaSample
    ) async throws(ActrError) {
        throw ActrError.NotImplemented(msg: "sendMediaSample is not implemented by this ActrContext")
    }

    func registerMediaTrack(
        trackId _: String,
        callback _: any MediaTrackCallback
    ) async throws(ActrError) {
        throw ActrError.NotImplemented(msg: "registerMediaTrack is not implemented by this ActrContext")
    }

    func unregisterMediaTrack(trackId _: String) async throws(ActrError) {
        throw ActrError.NotImplemented(msg: "unregisterMediaTrack is not implemented by this ActrContext")
    }

    func call(
        target: ActrId,
        routeKey: String,
        payload: Data,
        payloadType: PayloadType = .rpcReliable,
        timeoutMs: Int64 = 30000
    ) async throws(ActrError) -> Data {
        try await callRaw(
            target: target,
            routeKey: routeKey,
            payloadType: payloadType,
            payload: payload,
            timeoutMs: timeoutMs
        )
    }

    func call<Req: RpcRequest>(
        target: ActrId,
        request: Req,
        timeoutMs: Int64 = 30000
    ) async throws(ActrError) -> Req.Response {
        let requestData = try encodeProtobufMessage(request, context: "\(Req.self) request")
        let responseData = try await callRaw(
            target: target,
            routeKey: Req.routeKey,
            payloadType: Req.payloadType,
            payload: requestData,
            timeoutMs: timeoutMs
        )
        return try decodeProtobufMessage(
            Req.Response.self,
            from: responseData,
            context: "\(Req.Response.self) response"
        )
    }
}

public final class Context: ActrContext, @unchecked Sendable {
    let bridge: ActrBindings.ContextBridge

    init(bridge: ActrBindings.ContextBridge) {
        self.bridge = bridge
    }

    public var selfId: ActrId {
        ActrId(bridge: bridge.selfId())
    }

    public var callerId: ActrId? {
        bridge.callerId().map(ActrId.init(bridge:))
    }

    public var requestId: String {
        bridge.requestId()
    }

    public func log(level: LogLevel, msg: String) {
        bridge.log(level: level.bridge, msg: msg)
    }

    public func addMediaTrack(
        target: ActrId,
        trackId: String,
        codec: String,
        mediaType: String
    ) async throws(ActrError) {
        do {
            try await bridge.addMediaTrack(
                target: target.bridge,
                trackId: trackId,
                codec: codec,
                mediaType: mediaType
            )
        } catch {
            throw ActrError(error: error)
        }
    }

    public func callRaw(
        target: ActrId,
        routeKey: String,
        payloadType: PayloadType,
        payload: Data,
        timeoutMs: Int64
    ) async throws(ActrError) -> Data {
        do {
            return try await bridge.callRaw(
                target: target.bridge,
                routeKey: routeKey,
                payloadType: payloadType.bridge,
                payload: payload,
                timeoutMs: timeoutMs
            )
        } catch {
            throw ActrError(error: error)
        }
    }

    public func discover(targetType: ActrType) async throws(ActrError) -> ActrId {
        do {
            return try await ActrId(bridge: bridge.discover(targetType: targetType.bridge))
        } catch {
            throw ActrError(error: error)
        }
    }

    public func registerMediaTrack(
        trackId: String,
        callback: any MediaTrackCallback
    ) async throws(ActrError) {
        do {
            try await bridge.registerMediaTrack(
                trackId: trackId,
                callback: MediaTrackCallbackAdapter(callback)
            )
        } catch {
            throw ActrError(error: error)
        }
    }

    public func registerStream(
        streamId: String,
        callback: any DataChunkCallback
    ) async throws(ActrError) {
        do {
            try await bridge.registerStream(
                streamId: streamId,
                callback: DataChunkCallbackAdapter(callback)
            )
        } catch {
            throw ActrError(error: error)
        }
    }

    public func removeMediaTrack(target: ActrId, trackId: String) async throws(ActrError) {
        do {
            try await bridge.removeMediaTrack(target: target.bridge, trackId: trackId)
        } catch {
            throw ActrError(error: error)
        }
    }

    public func sendDataChunk(
        target: ActrId,
        chunk: DataChunk,
        payloadType: PayloadType
    ) async throws(ActrError) {
        do {
            try await bridge.sendDataChunk(
                target: target.bridge,
                chunk: chunk.bridge,
                payloadType: payloadType.bridge
            )
        } catch {
            throw ActrError(error: error)
        }
    }

    public func sendMediaSample(
        target: ActrId,
        trackId: String,
        sample: MediaSample
    ) async throws(ActrError) {
        do {
            try await bridge.sendMediaSample(
                target: target.bridge,
                trackId: trackId,
                sample: sample.bridge
            )
        } catch {
            throw ActrError(error: error)
        }
    }

    public func tellRaw(
        target: ActrId,
        routeKey: String,
        payloadType: PayloadType,
        payload: Data
    ) async throws(ActrError) {
        do {
            try await bridge.tellRaw(
                target: target.bridge,
                routeKey: routeKey,
                payloadType: payloadType.bridge,
                payload: payload
            )
        } catch {
            throw ActrError(error: error)
        }
    }

    public func unregisterMediaTrack(trackId: String) async throws(ActrError) {
        do {
            try await bridge.unregisterMediaTrack(trackId: trackId)
        } catch {
            throw ActrError(error: error)
        }
    }

    public func unregisterStream(streamId: String) async throws(ActrError) {
        do {
            try await bridge.unregisterStream(streamId: streamId)
        } catch {
            throw ActrError(error: error)
        }
    }
}

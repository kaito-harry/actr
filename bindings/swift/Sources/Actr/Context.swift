import ActrBindings
import Foundation

public final class Context: @unchecked Sendable {
    let bridge: ActrBindings.ContextBridge

    init(bridge: ActrBindings.ContextBridge) {
        self.bridge = bridge
    }

    public func addMediaTrack(target: ActrId, trackId: String, codec: String, mediaType: String) async throws {
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
    ) async throws -> Data {
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

    public func discover(targetType: ActrType) async throws -> ActrId {
        do {
            return try await ActrId(bridge: bridge.discover(targetType: targetType.bridge))
        } catch {
            throw ActrError(error: error)
        }
    }

    public func registerMediaTrack(trackId: String, callback: any MediaTrackCallback) async throws {
        do {
            try await bridge.registerMediaTrack(
                trackId: trackId,
                callback: MediaTrackCallbackAdapter(callback)
            )
        } catch {
            throw ActrError(error: error)
        }
    }

    public func registerStream(streamId: String, callback: any DataStreamCallback) async throws {
        do {
            try await bridge.registerStream(
                streamId: streamId,
                callback: DataStreamCallbackAdapter(callback)
            )
        } catch {
            throw ActrError(error: error)
        }
    }

    public func removeMediaTrack(target: ActrId, trackId: String) async throws {
        do {
            try await bridge.removeMediaTrack(target: target.bridge, trackId: trackId)
        } catch {
            throw ActrError(error: error)
        }
    }

    public func sendDataStream(target: ActrId, chunk: DataStream, payloadType: PayloadType) async throws {
        do {
            try await bridge.sendDataStream(
                target: target.bridge,
                chunk: chunk.bridge,
                payloadType: payloadType.bridge
            )
        } catch {
            throw ActrError(error: error)
        }
    }

    public func sendMediaSample(target: ActrId, trackId: String, sample: MediaSample) async throws {
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
    ) async throws {
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

    public func unregisterMediaTrack(trackId: String) async throws {
        do {
            try await bridge.unregisterMediaTrack(trackId: trackId)
        } catch {
            throw ActrError(error: error)
        }
    }

    public func unregisterStream(streamId: String) async throws {
        do {
            try await bridge.unregisterStream(streamId: streamId)
        } catch {
            throw ActrError(error: error)
        }
    }
}

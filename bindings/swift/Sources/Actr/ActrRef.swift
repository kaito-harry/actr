/// High-level actor reference and error helpers.
///
/// Public type aliases for the auto-generated UniFFI bindings are centralized
/// in `Aliases.swift`.

import ActrBindings
import Foundation
import SwiftProtobuf

// Observability is automatically initialized when creating ActrNode from a config file.

/// Returns the fault-domain classification for an ACTR error.
public func actrErrorKind(_ error: ActrError) -> ErrorKind {
    ErrorKind(bridge: ActrBindings.actrErrorKind(err: error.bridge))
}

/// Returns true when an ACTR error is transient and can be retried.
public func actrErrorIsRetryable(_ error: ActrError) -> Bool {
    ActrBindings.actrErrorIsRetryable(err: error.bridge)
}

/// Returns true when an ACTR error indicates corrupt payload handling.
public func actrErrorRequiresDlq(_ error: ActrError) -> Bool {
    ActrBindings.actrErrorRequiresDlq(err: error.bridge)
}

/// A high-level reference to a running actor.
///
/// This class wraps the low-level `ActrRefWrapper` and provides a cleaner API
/// for interacting with actors in the ACTR system.
public final class ActrRef: Sendable {
    private let inner: ActrBindings.ActrRefWrapper
    private let retainedWorkload: DynamicWorkload?
    private let retainedObservers: RuntimeObservers?

    /// Get the actor's ID
    public func actorId() -> ActrId {
        ActrId(bridge: inner.actorId())
    }

    /// Performs a type-safe RPC call using an RpcRequest message.
    ///
    /// - Parameters:
    ///   - message: The request message conforming to `RpcRequest`
    ///   - timeoutMs: Timeout in milliseconds (default: 30000)
    /// - Returns: The response message
    public func call<Req: RpcRequest>(
        _ message: Req,
        timeoutMs: Int64 = 30000
    ) async throws(ActrError) -> Req.Response {
        do {
            let requestData = try encodeProtobufMessage(message, context: "\(Req.self) request")
            let responseData = try await inner.call(
                routeKey: Req.routeKey,
                payloadType: Req.payloadType.bridge,
                requestPayload: requestData,
                timeoutMs: timeoutMs
            )
            return try decodeProtobufMessage(Req.Response.self, from: responseData, context: "\(Req.Response.self) response")
        } catch {
            throw ActrError(error: error)
        }
    }

    /// Performs a raw local RPC call.
    public func call(
        routeKey: String,
        payloadType: PayloadType,
        requestPayload: Data,
        timeoutMs: Int64 = 30000
    ) async throws -> Data {
        do {
            return try await inner.call(
                routeKey: routeKey,
                payloadType: payloadType.bridge,
                requestPayload: requestPayload,
                timeoutMs: timeoutMs
            )
        } catch {
            throw ActrError(error: error)
        }
    }

    /// Discover actors of the specified type
    ///
    /// - Parameters:
    ///   - targetType: The type of actors to discover
    ///   - count: Maximum number of actors to discover
    /// - Returns: Array of discovered actor IDs
    public func discover(targetType: ActrType, count: UInt32) async throws -> [ActrId] {
        do {
            return try await inner.discover(targetType: targetType.bridge, count: count).map(ActrId.init(bridge:))
        } catch {
            throw ActrError(error: error)
        }
    }

    /// Shuts down the actor and waits for it to terminate.
    public func stop() async {
        inner.shutdown()
        await inner.waitForShutdown()
    }

    /// Check if the actor is shutting down
    public func isShuttingDown() -> Bool {
        inner.isShuttingDown()
    }

    /// Trigger shutdown
    public func shutdown() {
        inner.shutdown()
    }

    /// Send a one-way message to an actor (fire-and-forget)
    ///
    /// - Parameters:
    ///   - routeKey: RPC route key (e.g., "echo.EchoService/Echo")
    ///   - payloadType: Payload transmission type (RpcReliable, RpcSignal, etc.)
    ///   - messagePayload: Message payload bytes (protobuf encoded)
    public func tell(
        routeKey: String,
        payloadType: PayloadType,
        messagePayload: Data
    ) async throws {
        do {
            try await inner.tell(
                routeKey: routeKey,
                payloadType: payloadType.bridge,
                messagePayload: messagePayload
            )
        } catch {
            throw ActrError(error: error)
        }
    }

    /// Wait for shutdown to complete
    public func waitForShutdown() async {
        await inner.waitForShutdown()
    }

    init(
        inner: ActrBindings.ActrRefWrapper,
        retainedWorkload: DynamicWorkload? = nil,
        retainedObservers: RuntimeObservers? = nil
    ) {
        self.inner = inner
        self.retainedWorkload = retainedWorkload
        self.retainedObservers = retainedObservers
    }
}

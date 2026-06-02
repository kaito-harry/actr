/// Re-exported UniFFI bindings and high-level API wrappers.
///
/// This file provides a clean interface to the auto-generated UniFFI bindings
/// from `ActrBindings/Actr.swift`. Only the types and functions actually used
/// by the high-level Swift API are re-exported here.

import ActrBindings
import Foundation
import SwiftProtobuf

// Re-export core wrapper types used by the high-level API
public typealias ActrRefWrapper = ActrBindings.ActrRefWrapper
public typealias NetworkEventHandleWrapper = ActrBindings.NetworkEventHandleWrapper

// Observability is automatically initialized when creating ActrNode from a config file.

// Re-export error types
public typealias ActrError = ActrBindings.ActrError

// Re-export protocol types used by the bridge
public typealias ContextBridge = ActrBindings.ContextBridge
public typealias WorkloadLifecycleBridge = ActrBindings.WorkloadLifecycleBridge
public typealias DynamicWorkload = ActrBindings.DynamicWorkload
public typealias RpcEnvelopeBridge = ActrBindings.RpcEnvelopeBridge
public typealias ErrorEventBridge = ActrBindings.ErrorEventBridge

// Re-export data types
public typealias ActrId = ActrBindings.ActrId
public typealias ActrType = ActrBindings.ActrType
public typealias PayloadType = ActrBindings.PayloadType

/// A high-level reference to a running actor.
///
/// This class wraps the low-level `ActrRefWrapper` and provides a cleaner API
/// for interacting with actors in the ACTR system.
public final class ActrRef: Sendable {
    private let inner: ActrRefWrapper
    private let retainedWorkload: DynamicWorkload?

    /// Get the actor's ID
    public func actorId() -> ActrId {
        inner.actorId()
    }

    /// Performs a type-safe RPC call using an RpcRequest message.
    ///
    /// - Parameters:
    ///   - message: The request message conforming to `RpcRequest`
    ///   - payloadType: Payload transmission type (default: .rpcReliable)
    ///   - timeoutMs: Timeout in milliseconds (default: 30000)
    /// - Returns: The response message
    public func call<Req: RpcRequest>(
        _ message: Req,
        payloadType: PayloadType = .rpcReliable,
        timeoutMs: Int64 = 30000
    ) async throws -> Req.Response {
        let requestData = try message.serializedData()
        let responseData = try await inner.call(
            routeKey: Req.routeKey,
            payloadType: payloadType,
            requestPayload: requestData,
            timeoutMs: timeoutMs
        )
        return try Req.Response(serializedBytes: responseData)
    }

    /// Performs a raw local RPC call.
    public func call(
        routeKey: String,
        payloadType: PayloadType,
        requestPayload: Data,
        timeoutMs: Int64 = 30000
    ) async throws -> Data {
        try await inner.call(
            routeKey: routeKey,
            payloadType: payloadType,
            requestPayload: requestPayload,
            timeoutMs: timeoutMs
        )
    }

    /// Discover actors of the specified type
    ///
    /// - Parameters:
    ///   - targetType: The type of actors to discover
    ///   - count: Maximum number of actors to discover
    /// - Returns: Array of discovered actor IDs
    public func discover(targetType: ActrType, count: UInt32) async throws -> [ActrId] {
        try await inner.discover(targetType: targetType, count: count)
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
        try await inner.tell(
            routeKey: routeKey,
            payloadType: payloadType,
            messagePayload: messagePayload
        )
    }

    /// Wait for shutdown to complete
    public func waitForShutdown() async {
        await inner.waitForShutdown()
    }

    init(inner: ActrRefWrapper, retainedWorkload: DynamicWorkload? = nil) {
        self.inner = inner
        self.retainedWorkload = retainedWorkload
    }
}

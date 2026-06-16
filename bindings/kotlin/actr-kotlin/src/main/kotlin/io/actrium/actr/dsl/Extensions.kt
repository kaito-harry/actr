/** Utility functions and extensions for Actrium SDK. */
package io.actrium.actr.dsl

import io.actrium.actr.ActrException
import io.actrium.actr.ActrId
import io.actrium.actr.ContextBridge
import io.actrium.actr.ErrorKind
import io.actrium.actr.AppLifecycleState
import io.actrium.actr.CleanupReason
import io.actrium.actr.NetworkEventResult
import io.actrium.actr.NetworkSnapshot
import io.actrium.actr.PayloadType
import io.actrium.actr.ReconnectReason
import io.actrium.actr.actrErrorIsRetryable
import io.actrium.actr.actrErrorKind
import io.actrium.actr.actrErrorRequiresDlq

// ============================================================================
// ActrRef Call Extensions - Convenience wrappers with default parameters
// ============================================================================

/**
 * Call via RPC proxy with default PayloadType.RPC_RELIABLE and 30s timeout.
 *
 * This sends a request through the local workload's RPC proxy mechanism.
 * The workload's dispatch() method handles routing to the remote actor.
 *
 * Example:
 * ```kotlin
 * val response = ref.call("echo.EchoService.Echo", requestPayload)
 * ```
 */
suspend fun ActrRef.call(
    routeKey: String,
    requestPayload: ByteArray,
    payloadType: PayloadType = PayloadType.RPC_RELIABLE,
    timeoutMs: Long = 30000L,
): ByteArray = call(routeKey, payloadType, requestPayload, timeoutMs)

/**
 * Send a one-way message via RPC proxy with default PayloadType.RPC_RELIABLE.
 *
 * This sends a message through the local workload's RPC proxy mechanism.
 * The workload's dispatch() method handles routing to the remote actor.
 *
 * Example:
 * ```kotlin
 * ref.tell("echo.EchoService.Notify", messagePayload)
 * ```
 */
suspend fun ActrRef.tell(
    routeKey: String,
    messagePayload: ByteArray,
    payloadType: PayloadType = PayloadType.RPC_RELIABLE,
) {
    tell(routeKey, payloadType, messagePayload)
}

// ============================================================================
// Result Extensions - For functional error handling
// ============================================================================

/**
 * Execute an RPC call and wrap the result.
 *
 * Example:
 * ```kotlin
 * val result = ref.callCatching("echo.EchoService.Echo", payload)
 * result.onSuccess { response ->
 *     println("Got response: $response")
 * }.onFailure { error ->
 *     println("Call failed: $error")
 * }
 * ```
 */
suspend fun ActrRef.callCatching(
    routeKey: String,
    requestPayload: ByteArray,
    payloadType: PayloadType = PayloadType.RPC_RELIABLE,
    timeoutMs: Long = 30000L,
): Result<ByteArray> = runCatching { call(routeKey, requestPayload, payloadType, timeoutMs) }

/** Discover actors and wrap the result. */
suspend fun ActrRef.discoverCatching(
    typeString: String,
    count: UInt = 1u,
): Result<List<ActrId>> = runCatching { discover(typeString, count) }

// ============================================================================
// ContextBridge Extensions — convenience wrappers with default parameters
// ============================================================================

/**
 * Convenience wrapper around [ContextBridge.callRaw] with default parameters.
 *
 * Equivalent to:
 * ```kotlin
 * ctx.callRaw(target, routeKey, PayloadType.RPC_RELIABLE, payload, 30000L)
 * ```
 *
 * @param target Target actor ID (obtained via [ContextBridge.discover])
 * @param routeKey RPC route key (e.g., "echo.EchoService.Echo")
 * @param payload Serialized request payload
 * @param payloadType Transmission type (default: RPC_RELIABLE)
 * @param timeoutMs Timeout in milliseconds (default: 30000)
 * @return Response bytes
 */
suspend fun ContextBridge.call(
    target: ActrId,
    routeKey: String,
    payload: ByteArray,
    payloadType: PayloadType = PayloadType.RPC_RELIABLE,
    timeoutMs: Long = 30000L,
): ByteArray = callRaw(target, routeKey, payloadType, payload, timeoutMs)

// ============================================================================
// NetworkEventHandle Extensions - For functional error handling
// ============================================================================

/**
 * Handle network path changed event and wrap the result.
 *
 * Example:
 * ```kotlin
 * val snapshot = NetworkSnapshot(
 *     sequence = 1uL,
 *     availability = NetworkAvailability.AVAILABLE,
 *     transport = NetworkTransportFlags(wifi = true, cellular = false, ethernet = false, vpn = false, other = false),
 *     isExpensive = false,
 *     isConstrained = false,
 * )
 * val result = networkHandle.handleNetworkPathChangedCatching(snapshot)
 * result.onSuccess { eventResult ->
 *     println("Network path changed handled: $eventResult")
 * }.onFailure { error ->
 *     println("Failed to handle network path changed: $error")
 * }
 * ```
 */
suspend fun NetworkEventHandle.handleNetworkPathChangedCatching(
    snapshot: NetworkSnapshot,
): Result<NetworkEventResult> = runCatching { handleNetworkPathChanged(snapshot) }

/**
 * Handle app lifecycle changed event and wrap the result.
 *
 * Example:
 * ```kotlin
 * val result = networkHandle.handleAppLifecycleChangedCatching(AppLifecycleState.Background)
 * result.onSuccess { eventResult ->
 *     println("App lifecycle changed handled: $eventResult")
 * }.onFailure { error ->
 *     println("Failed to handle app lifecycle changed: $error")
 * }
 * ```
 */
suspend fun NetworkEventHandle.handleAppLifecycleChangedCatching(
    state: AppLifecycleState,
): Result<NetworkEventResult> = runCatching { handleAppLifecycleChanged(state) }

/**
 * Cleanup connections and wrap the result.
 *
 * Example:
 * ```kotlin
 * val result = networkHandle.cleanupConnectionsCatching(CleanupReason.MANUAL_RESET)
 * result.onSuccess { eventResult ->
 *     println("Cleanup connections handled: $eventResult")
 * }.onFailure { error ->
 *     println("Failed to cleanup connections: $error")
 * }
 * ```
 */
suspend fun NetworkEventHandle.cleanupConnectionsCatching(
    reason: CleanupReason,
): Result<NetworkEventResult> = runCatching { cleanupConnections(reason) }

/**
 * Force reconnect and wrap the result.
 *
 * Example:
 * ```kotlin
 * val result = networkHandle.forceReconnectCatching(ReconnectReason.MANUAL_RECONNECT)
 * result.onSuccess { eventResult ->
 *     println("Force reconnect handled: $eventResult")
 * }.onFailure { error ->
 *     println("Failed to force reconnect: $error")
 * }
 * ```
 */
suspend fun NetworkEventHandle.forceReconnectCatching(
    reason: ReconnectReason,
): Result<NetworkEventResult> = runCatching { forceReconnect(reason) }

// ============================================================================
// Exception Extensions
// ============================================================================
//
// The underlying sealed `ActrException` mirrors `actr_protocol::ActrError`
// 1:1 (10 variants) plus a small number of binding-local variants. Rather
// than reasoning about each concrete subclass, consumers typically branch
// on fault domain via `ErrorKind` — see `actrErrorKind(ex)` below.

/** Get a user-friendly error message for logs or UI. */
val ActrException.userMessage: String
    get() =
        when (this) {
            is ActrException.Unavailable -> "Peer unavailable: $msg"
            is ActrException.ConnectionNotReady -> {
                val retryMsg = info.retryAfterMs?.let { " Retry after ${it}ms." } ?: ""
                "Connection not ready.$retryMsg"
            }
            is ActrException.TimedOut -> "Request timed out"
            is ActrException.NotFound -> "Not found: $msg"
            is ActrException.PermissionDenied -> "Permission denied: $msg"
            is ActrException.InvalidArgument -> "Invalid argument: $msg"
            is ActrException.UnknownRoute -> "Unknown route: $msg"
            is ActrException.DependencyNotFound ->
                "Dependency '$serviceName' not found: $detail"
            is ActrException.DecodeFailure -> "Decode failure: $msg"
            is ActrException.NotImplemented -> "Not implemented: $msg"
            is ActrException.Internal -> "Internal error: $msg"
            is ActrException.Config -> "Configuration error: $msg"
        }

/** Check if the exception is a timeout. */
val ActrException.isTimeout: Boolean
    get() = this is ActrException.TimedOut

/**
 * Check if the exception is a transient connectivity error — use this as a
 * hint for retrying with backoff.
 *
 * Prefer [isRecoverable] (which consults the fault-domain classification)
 * for new code.
 */
val ActrException.isConnectionError: Boolean
    get() = this is ActrException.Unavailable

/**
 * Check if the exception is recoverable (worth retrying).
 *
 * Delegates to the fault-domain classifier exported by the Rust binding:
 * only `ErrorKind.TRANSIENT` errors are retryable, everything else is a
 * terminal failure.
 */
val ActrException.isRecoverable: Boolean
    get() = actrErrorIsRetryable(this)

/**
 * Fault-domain bucket for this exception — one of `Transient` / `Client` /
 * `Internal` / `Corrupt`.
 */
val ActrException.kind: ErrorKind
    get() = actrErrorKind(this)

/**
 * `true` iff the underlying payload should be routed to a Dead Letter
 * Queue (only `ErrorKind.Corrupt` errors).
 */
val ActrException.requiresDlq: Boolean
    get() = actrErrorRequiresDlq(this)

// ============================================================================
// Retry Utilities
// ============================================================================

/** Retry configuration for operations. */
data class RetryConfig(
    val maxAttempts: Int = 3,
    val initialDelayMs: Long = 1000,
    val maxDelayMs: Long = 10000,
    val factor: Double = 2.0,
)

/**
 * Execute a suspending block with exponential backoff retry.
 *
 * Example:
 * ```kotlin
 * val result = withRetry(maxAttempts = 5) {
 *     ref.discover("acme:EchoService")
 * }
 * ```
 */
suspend fun <T> withRetry(
    maxAttempts: Int = 3,
    initialDelayMs: Long = 1000,
    maxDelayMs: Long = 10000,
    factor: Double = 2.0,
    shouldRetry: (Exception) -> Boolean = { it is ActrException && it.isRecoverable },
    block: suspend () -> T,
): T {
    var currentDelay = initialDelayMs
    var lastException: Exception? = null

    repeat(maxAttempts) { attempt ->
        try {
            return block()
        } catch (e: Exception) {
            lastException = e
            if (attempt == maxAttempts - 1 || !shouldRetry(e)) {
                throw e
            }
            kotlinx.coroutines.delay(currentDelay)
            currentDelay = (currentDelay * factor).toLong().coerceAtMost(maxDelayMs)
        }
    }

    throw lastException ?: IllegalStateException("Retry failed without exception")
}

/** Execute a suspending block with retry using RetryConfig. */
suspend fun <T> withRetry(
    config: RetryConfig,
    shouldRetry: (Exception) -> Boolean = { it is ActrException && it.isRecoverable },
    block: suspend () -> T,
): T =
    withRetry(
        maxAttempts = config.maxAttempts,
        initialDelayMs = config.initialDelayMs,
        maxDelayMs = config.maxDelayMs,
        factor = config.factor,
        shouldRetry = shouldRetry,
        block = block,
    )

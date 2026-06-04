/**
 * Actor-RTC Kotlin SDK
 *
 * A Kotlin-idiomatic wrapper for the Actor-RTC framework.
 *
 * Example usage:
 * ```kotlin
 * // Create and start a package-backed actor
 * val node = ActrNode.fromPackageFile("config.toml", "dist/app.actr")
 * val ref = node.start()
 *
 * // Discover and call remote services
 * val echoService = ref.discover("acme:EchoService").firstOrNull()
 * val response = ref.call(echoService, "echo.EchoService.Echo", request)
 *
 * // Send data stream
 * ref.sendStream(target) {
 *     streamId = "stream-001"
 *     sequence = 0uL
 *     payload = data
 *     metadata {
 *         "content-type" to "application/octet-stream"
 *     }
 * }
 *
 * // Clean shutdown
 * ref.shutdown()
 * ref.awaitShutdown()
 * ```
 */
package io.actor_rtc.actr.dsl

import io.actor_rtc.actr.ActrException
import io.actor_rtc.actr.ActrId
import io.actor_rtc.actr.ActrNode as ActrNodeGenerated
import io.actor_rtc.actr.ActrRefWrapper
import io.actor_rtc.actr.ActrType
import io.actor_rtc.actr.NetworkEventHandleWrapper
import io.actor_rtc.actr.WorkloadLifecycleBridge

// ============================================================================
// Type Aliases - Provide DSL-friendly names
// ============================================================================

/**
 * Entry point for creating actors. Use [ActrNode.fromPackageFile] to create an instance.
 *
 * This aliases the UniFFI-generated `io.actor_rtc.actr.ActrNode` class into the DSL
 * package so callers can write `import io.actor_rtc.actr.dsl.ActrNode` alongside the
 * other DSL helpers.
 */
typealias ActrNode = ActrNodeGenerated

/**
 * Reference to a running actor. Provides methods for:
 * - [ActrRef.call]
 * - RPC calls to remote actors
 * - [ActrRef.discover]
 * - Service discovery
 * - [ActrRef.sendDataStream]
 * - Send data streams
 * - [ActrRef.shutdown]
 * - Graceful shutdown
 */
typealias ActrRef = ActrRefWrapper

/** Handle for network event callbacks. Used for platform integration. */
typealias NetworkEventHandle = NetworkEventHandleWrapper

/** Workload callback interface for handling lifecycle events. */
typealias Workload = WorkloadLifecycleBridge

// ============================================================================
// ActrNode Factory Functions
// ============================================================================

/**
 * Create an ActrNode from a config file and package file.
 *
 * Example:
 * ```kotlin
 * val node = ActrNode.fromPackageFile("config.toml", "dist/app.actr")
 * ```
 *
 * @param configPath Path to the TOML configuration file
 * @param packagePath Path to the `.actr` package file
 * @return A new ActrNode instance
 * @throws ActrException.Config if the config file is invalid
 */
suspend fun ActrNodeGenerated.Companion.fromPackageFile(
    configPath: String,
    packagePath: String
): ActrNode {
    return ActrNodeGenerated.newFromPackageFile(configPath, packagePath)
}

/**
 * Create an ActrNode from a config file and package file (top-level function).
 *
 * Example:
 * ```kotlin
 * val node = createActrNode("config.toml", "dist/app.actr")
 * ```
 *
 * @param configPath Path to the TOML configuration file
 * @param packagePath Path to the `.actr` package file
 * @return A new ActrNode instance
 * @throws ActrException.Config if the config file is invalid
 */
suspend fun createActrNode(configPath: String, packagePath: String): ActrNode {
    return ActrNodeGenerated.newFromPackageFile(configPath, packagePath)
}

/**
 * Create an ActrNode backed by a linked dynamic workload.
 *
 * Use this when workload logic lives in Kotlin instead of a packaged `.actr` guest.
 */
suspend fun ActrNodeGenerated.Companion.linked(
    configPath: String,
    actorType: ActrType,
    workload: DynamicWorkload
): ActrNode {
    return ActrNodeGenerated.newFromLinkedWorkload(configPath, actorType, workload)
}

/**
 * Create an ActrNode backed by a linked dynamic workload.
 */
suspend fun linked(
    configPath: String,
    actorType: ActrType,
    workload: DynamicWorkload
): ActrNode {
    return ActrNodeGenerated.newFromLinkedWorkload(configPath, actorType, workload)
}

// ============================================================================
// ActrNode Extensions
// ============================================================================

/**
 * Create a network event handle for platform callbacks.
 *
 * This handle is used to notify the actor runtime about network state changes,
 * which is important for WebRTC connection management on mobile platforms.
 *
 * Example:
 * ```kotlin
 * val node = createActrNode("config.toml", "dist/app.actr")
 * val networkHandle = node.createNetworkEventHandle()
 *
 * // Notify when network becomes available
 * networkHandle.handleNetworkAvailable()
 * ```
 *
 * @return A new NetworkEventHandle instance
 * @throws ActrException if the handle cannot be created
 */
suspend fun ActrNode.createNetworkEventHandle(): NetworkEventHandle {
    return createNetworkEventHandle()
}

// ============================================================================
// ActrRef Extensions
// ============================================================================

/**
 * Discover actors of the specified type using a type string.
 *
 * @param typeString Actor type in "manufacturer:name:version" format (e.g., "acme:EchoService:1.0.0")
 * @param count Maximum number of candidates to return (default: 1)
 * @return List of discovered actor IDs
 */
suspend fun ActrRef.discover(typeString: String, count: UInt = 1u): List<ActrId> {
    return discover(typeString.toActrType(), count)
}

/**
 * Discover a single actor of the specified type.
 *
 * @param typeString Actor type in "manufacturer:name:version" format
 * @return The first discovered actor ID, or null if none found
 */
suspend fun ActrRef.discoverOne(typeString: String): ActrId? {
    return discover(typeString, 1u).firstOrNull()
}

/**
 * Discover a single actor of the specified type.
 *
 * @param type Actor type
 * @return The first discovered actor ID, or null if none found
 */
suspend fun ActrRef.discoverOne(type: ActrType): ActrId? {
    return discover(type, 1u).firstOrNull()
}

/**
 * Send a DataStream built with DSL syntax.
 *
 * Example:
 * ```kotlin
 * workload.sendStream(targetId) {
 *     streamId = "my-stream"
 *     sequence = 0uL
 *     payload = "Hello".toByteArray()
 *     metadata {
 *         "key1" to "value1"
 *         "key2" to "value2"
 *     }
 * }
 * ```
 */
suspend fun SimpleWorkload.sendStream(target: ActrId, builder: DataStreamBuilder.() -> Unit) {
    val dataStream = DataStreamBuilder().apply(builder).build()
    sendDataStream(target, dataStream)
}

/** Await shutdown completion. Alias for [waitForShutdown]. */
suspend fun ActrRef.awaitShutdown() {
    waitForShutdown()
}

/** Check if this actor reference is still valid (not destroyed). */
val ActrRef.isActive: Boolean
    get() = !isShuttingDown()

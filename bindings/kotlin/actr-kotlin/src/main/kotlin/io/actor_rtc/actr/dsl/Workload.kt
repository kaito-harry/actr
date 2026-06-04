/** Simplified Workload interface and base implementations. */
package io.actor_rtc.actr.dsl

import io.actor_rtc.actr.ActrId
import io.actor_rtc.actr.ActrType
import io.actor_rtc.actr.ContextBridge
import io.actor_rtc.actr.DataStream
import io.actor_rtc.actr.DynamicWorkload as DynamicWorkloadGenerated
import io.actor_rtc.actr.ErrorEventBridge
import io.actor_rtc.actr.PayloadType
import io.actor_rtc.actr.Realm
import io.actor_rtc.actr.RpcEnvelopeBridge
import io.actor_rtc.actr.SignalingObserverBridge
import io.actor_rtc.actr.WebSocketObserverBridge
import io.actor_rtc.actr.WebRtcObserverBridge
import io.actor_rtc.actr.CredentialObserverBridge
import io.actor_rtc.actr.MailboxObserverBridge
import io.actor_rtc.actr.WorkloadLifecycleBridge
import java.util.concurrent.atomic.AtomicReference
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.launch

typealias DynamicWorkload = DynamicWorkloadGenerated

fun dynamicWorkload(
    lifecycle: WorkloadLifecycleBridge,
    signaling: SignalingObserverBridge? = null,
    websocket: WebSocketObserverBridge? = null,
    webrtc: WebRtcObserverBridge? = null,
    credential: CredentialObserverBridge? = null,
    mailbox: MailboxObserverBridge? = null
): DynamicWorkload {
    return DynamicWorkloadGenerated(
        lifecycle = lifecycle,
        signaling = signaling,
        websocket = websocket,
        webrtc = webrtc,
        credential = credential,
        mailbox = mailbox
    )
}

/**
 * Simple workload implementation that only needs type information.
 *
 * This is useful for client applications that don't need to handle incoming requests. Before making
 * RPC calls, you must set the target server ID using [setTargetServerId].
 *
 * Example:
 * ```kotlin
 * val workload = SimpleWorkload(
 *     realm = 2281844430u,
 *     type = "acme:my-client"
 * )
 * val node = createActrNode("actr.toml", "dist/app.actr")
 * val actrRef = node.start()
 *
 * // Discover and set target server before calling
 * val serverId = actrRef.discoverOne("acme:EchoService")
 * workload.setTargetServerId(serverId)
 *
 * // Now RPC calls will be routed to the correct server
 * val response = actrRef.call(serverId, "echo.EchoService.Echo", payload)
 * ```
 */
open class SimpleWorkload(
        private val realmId: UInt,
        private val type: ActrType,
        private val onStartHandler: suspend (ContextBridge) -> Unit = {},
        private val onStopHandler: suspend (ContextBridge) -> Unit = {}
) : WorkloadLifecycleBridge {

    /** Channel for sending DataStream requests from UI to workload. */
    private val dataStreamChannel = Channel<DataStreamRequest>(Channel.UNLIMITED)

    /** Data class for DataStream requests. */
    data class DataStreamRequest(val target: ActrId, val dataStream: DataStream)

    /**
     * The target server ID for RPC calls. Must be set before making RPC calls via
     * [setTargetServerId].
     */
    private val targetServerId = AtomicReference<ActrId?>(null)

    /**
     * Create a SimpleWorkload from a type string.
     *
     * @param realmId The realm ID
     * @param typeString Actor type in "manufacturer:name:version" format
     */
    constructor(
            realmId: UInt,
            typeString: String,
            onStartHandler: suspend (ContextBridge) -> Unit = {},
            onStopHandler: suspend (ContextBridge) -> Unit = {}
    ) : this(realmId, typeString.toActrType(), onStartHandler, onStopHandler)

    /** Create a SimpleWorkload with named parameters. */
    constructor(
            realm: UInt,
            manufacturer: String,
            name: String,
            version: String,
            onStartHandler: suspend (ContextBridge) -> Unit = {},
            onStopHandler: suspend (ContextBridge) -> Unit = {}
    ) : this(
            realm,
            ActrType(manufacturer = manufacturer, name = name, version = version),
            onStartHandler,
            onStopHandler
    )

    /**
     * Set the target server ID for RPC calls.
     *
     * This must be called after discovering the server and before making RPC calls. The server ID
     * is obtained from [ActrRefWrapper.discoverOne] or [ActrRefWrapper.discover].
     *
     * @param serverId The target server's ActrId
     */
    fun setTargetServerId(serverId: ActrId) {
        targetServerId.set(serverId)
    }

    /** Get the current target server ID, or null if not set. */
    fun getTargetServerId(): ActrId? {
        return targetServerId.get()
    }

    /**
     * Send a DataStream through the workload's context. This method is thread-safe and can be
     * called from UI threads.
     */
    suspend fun sendDataStream(target: ActrId, dataStream: DataStream) {
        dataStreamChannel.send(DataStreamRequest(target, dataStream))
    }

    override suspend fun onStart(ctx: ContextBridge) {
        // Start a coroutine to handle DataStream requests
        kotlinx.coroutines.CoroutineScope(kotlinx.coroutines.Dispatchers.Default).launch {
            for (request in dataStreamChannel) {
                try {
                    ctx.sendDataStream(
                        request.target,
                        request.dataStream,
                        PayloadType.STREAM_RELIABLE,
                    )
                } catch (e: Exception) {
                    // Log error but continue processing
                    println("Failed to send DataStream: ${e.message}")
                }
            }
        }

        // Call user-provided handler
        onStartHandler(ctx)
    }

    override suspend fun onReady(ctx: ContextBridge) {
        // Default: do nothing
    }

    override suspend fun onStop(ctx: ContextBridge) {
        onStopHandler(ctx)
    }

    override suspend fun onError(ctx: ContextBridge, event: ErrorEventBridge) {
        // Default: do nothing
    }

    /**
     * Dispatch an incoming RPC message.
     *
     * This method **must** be implemented by subclasses to handle incoming RPC requests from the
     * Shell (local application) side. Unlike the Rust version, there is no default forwarding
     * behavior - you must implement the logic.
     *
     * See [shell-actr-echo/client](https://github.com/actor-rtc/actr-examples) for a reference
     * implementation pattern.
     *
     * @param ctx Context for making RPC calls
     * @param envelope The incoming RPC envelope
     * @return Response bytes (protobuf encoded)
     * @throws IllegalStateException if dispatch is not implemented
     */
    override suspend fun dispatch(ctx: ContextBridge, envelope: RpcEnvelopeBridge): ByteArray {
        throw IllegalStateException(
                "dispatch() must be implemented by subclass or use a custom WorkloadLifecycleBridge"
        )
    }
}

/**
 * DSL builder for creating a workload.
 *
 * Example:
 * ```kotlin
 * val workload = workload {
 *     realm = 2281844430u
 *     type = "acme:my-service"
 *
 *     onStart { ctx ->
 *         // Called when the workload starts
 *     }
 *
 *     onStop { ctx ->
 *         // Called when the workload stops
 *     }
 * }
 * ```
 */
inline fun workload(builder: WorkloadBuilder.() -> Unit): SimpleWorkload {
    return WorkloadBuilder().apply(builder).build()
}

/** Builder for creating workloads. */
class WorkloadBuilder {
    var realm: UInt = 0u
    private var _type: ActrType? = null
    private var startHandler: suspend (ContextBridge) -> Unit = {}
    private var stopHandler: suspend (ContextBridge) -> Unit = {}

    /** Set the actor type from a string. */
    var type: String
        get() = _type?.toTypeString() ?: ""
        set(value) {
            _type = value.toActrType()
        }

    /** Set the actor type directly. */
    fun type(actrType: ActrType) {
        _type = actrType
    }

    /** Set the actor type with manufacturer, name, and version. */
    fun type(manufacturer: String, name: String, version: String) {
        _type = ActrType(manufacturer = manufacturer, name = name, version = version)
    }

    /**
     * Set the onStart handler.
     *
     * @param handler Function called when the workload starts, receives the context
     */
    fun onStart(handler: suspend (ctx: ContextBridge) -> Unit) {
        startHandler = handler
    }

    /**
     * Set the onStop handler.
     *
     * @param handler Function called when the workload stops, receives the context
     */
    fun onStop(handler: suspend (ctx: ContextBridge) -> Unit) {
        stopHandler = handler
    }

    /**
     * Build the workload. Returns [SimpleWorkload] to allow setting target server ID before RPC
     * calls.
     */
    fun build(): SimpleWorkload {
        require(realm > 0u) { "realm must be set" }
        requireNotNull(_type) { "type must be set" }
        return SimpleWorkload(realm, _type!!, startHandler, stopHandler)
    }
}

/**
 * Abstract base class for workloads with lifecycle hooks.
 *
 * Subclass this to create a workload with custom lifecycle handling. Before making RPC calls, you
 * must set the target server ID using [setTargetServerId].
 *
 * Example:
 * ```kotlin
 * class MyWorkload : RoutedWorkload(
 *     realm = 2281844430u,
 *     type = "acme:my-service"
 * ) {
 *     override suspend fun onStart(ctx: ContextBridge) {
 *         // Custom start logic
 *     }
 *
 *     override suspend fun onStop(ctx: ContextBridge) {
 *         // Custom stop logic
 *     }
 * }
 * ```
 */
abstract class RoutedWorkload(private val realmId: UInt, private val type: ActrType) :
        WorkloadLifecycleBridge {

    constructor(realmId: UInt, typeString: String) : this(realmId, typeString.toActrType())

    /**
     * The target server ID for RPC calls. Must be set before making RPC calls via
     * [setTargetServerId].
     */
    private val targetServerId = AtomicReference<ActrId?>(null)

    /**
     * Set the target server ID for RPC calls.
     *
     * This must be called after discovering the server and before making RPC calls.
     *
     * @param serverId The target server's ActrId
     */
    fun setTargetServerId(serverId: ActrId) {
        targetServerId.set(serverId)
    }

    /** Get the current target server ID, or null if not set. */
    fun getTargetServerId(): ActrId? {
        return targetServerId.get()
    }

    /** Called when the workload starts. Override to add custom logic. */
    override suspend fun onStart(ctx: ContextBridge) {
        // Default: do nothing
    }

    /** Called when the workload is ready. Override to add custom logic. */
    override suspend fun onReady(ctx: ContextBridge) {
        // Default: do nothing
    }

    /** Called when the workload stops. Override to add custom logic. */
    override suspend fun onStop(ctx: ContextBridge) {
        // Default: do nothing
    }

    /** Called when the runtime reports a workload error. Override to add custom logic. */
    override suspend fun onError(ctx: ContextBridge, event: ErrorEventBridge) {
        // Default: do nothing
    }

    /**
     * Dispatch an incoming RPC message. Override to implement message handling.
     *
     * This method **must** be overridden to handle incoming RPC requests. There is no default
     * forwarding behavior.
     *
     * @param ctx Context for making RPC calls
     * @param envelope The incoming RPC envelope
     * @return Response bytes (protobuf encoded)
     * @throws IllegalStateException if dispatch is not implemented
     */
    override suspend fun dispatch(ctx: ContextBridge, envelope: RpcEnvelopeBridge): ByteArray {
        throw IllegalStateException("dispatch() must be overridden in subclass")
    }
}

package com.example.actrdemo

import android.os.Bundle
import android.util.Log
import android.widget.Button
import android.widget.EditText
import android.widget.ScrollView
import android.widget.TextView
import androidx.appcompat.app.AppCompatActivity
import androidx.lifecycle.lifecycleScope
import com.example.MyUnifiedHandler
import com.example.UnifiedWorkload
import data_stream_peer.StreamClientOuterClass.ClientStartStreamRequest
import data_stream_peer.StreamClientOuterClass.ClientStartStreamResponse
import echo.Echo.EchoRequest
import echo.Echo.EchoResponse
import io.actor_rtc.actr.ActrType
import io.actor_rtc.actr.PayloadType
import io.actor_rtc.actr.dsl.*
import io.actorrtc.demo.R
import java.io.File
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext

class ClientActivity : AppCompatActivity() {

    companion object {
        private const val TAG = "ClientActivity"
    }

    private lateinit var statusText: TextView
    private lateinit var connectButton: Button
    private lateinit var disconnectButton: Button
    private lateinit var messageInput: EditText
    private lateinit var sendButton: Button
    private lateinit var sendFileButton: Button
    private lateinit var logText: TextView
    private lateinit var scrollView: ScrollView

    private var clientRef: ActrRef? = null
    private var clientSystem: ActrNode? = null
    private lateinit var networkMonitor: NetworkMonitor

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        setContentView(R.layout.activity_client)

        initViews()
        setupClickListeners()
        initNetworkMonitoring()

        log("Ready to connect (linked multi-service workload)")
    }

    private fun initNetworkMonitoring() {
        networkMonitor =
                NetworkMonitor.create(
                        context = this,
                        scope = lifecycleScope,
                        getSystem = { clientSystem },
                        onNetworkStatusLog = { message ->
                            lifecycleScope.launch(Dispatchers.Main) { log(message) }
                        }
                )

        networkMonitor.startMonitoring()
    }

    private fun initViews() {
        statusText = findViewById(R.id.statusText)
        connectButton = findViewById(R.id.connectButton)
        disconnectButton = findViewById(R.id.disconnectButton)
        messageInput = findViewById(R.id.messageInput)
        sendButton = findViewById(R.id.sendButton)
        sendFileButton = findViewById(R.id.sendFileButton)
        logText = findViewById(R.id.logText)
        scrollView = findViewById(R.id.scrollView)
    }

    private fun setupClickListeners() {
        connectButton.setOnClickListener { connect() }
        disconnectButton.setOnClickListener { disconnect() }
        sendButton.setOnClickListener { sendMessage() }
        sendFileButton.setOnClickListener {
            val networkStatus = networkMonitor.getCurrentNetworkStatus()
            log("📡 Current network: $networkStatus")
            networkMonitor.triggerNetworkCheck()
            sendFile()
        }
    }

    private fun copyAssetToInternalStorage(assetName: String): String {
        val inputStream = assets.open(assetName)
        val outputFile = File(filesDir, assetName)
        outputFile.parentFile?.mkdirs()
        inputStream.use { input ->
            outputFile.outputStream().use { output -> input.copyTo(output) }
        }
        return outputFile.absolutePath
    }

    private fun connect() {
        updateStatus("Connecting...")
        connectButton.isEnabled = false

        lifecycleScope.launch {
            try {
                val configPath = copyAssetToInternalStorage("actr.toml")
                Log.i(TAG, "Config path: $configPath")

                val actorType =
                        ActrType(manufacturer = "acme", name = "UnifiedActor", version = "1.0.0")
                val workload = UnifiedWorkload(MyUnifiedHandler())
                val system = linked(configPath, actorType, workload.toDynamicWorkload())
                clientSystem = system
                Log.i(TAG, "✅ ActrNode created - NetworkMonitor will auto-handle network events")

                Log.i(TAG, "🚀 Starting linked multi-service actor...")
                clientRef = system.start()
                Log.i(TAG, "✅ Client started: ${clientRef?.actorId()?.serialNumber}")

                delay(2000)

                withContext(Dispatchers.Main) {
                    updateStatus("Connected")
                    disconnectButton.isEnabled = true
                    messageInput.isEnabled = true
                    sendButton.isEnabled = true
                    sendFileButton.isEnabled = true
                    log("Connected (linked multi-service mode)")
                    log("Client ID: ${clientRef?.actorId()?.serialNumber}")
                }
            } catch (e: Exception) {
                Log.e(TAG, "Connection failed", e)
                withContext(Dispatchers.Main) {
                    updateStatus("Connection failed")
                    connectButton.isEnabled = true
                    log("Error: ${e.message}")
                }
            }
        }
    }

    private fun disconnect() {
        updateStatus("Disconnecting...")
        disconnectButton.isEnabled = false
        messageInput.isEnabled = false
        sendButton.isEnabled = false
        sendFileButton.isEnabled = false

        lifecycleScope.launch {
            try {
                clientRef?.shutdown()
                clientRef?.awaitShutdown()
                clientRef = null

                withContext(Dispatchers.Main) {
                    updateStatus("Disconnected")
                    connectButton.isEnabled = true
                    log("Disconnected")
                }
            } catch (e: Exception) {
                Log.e(TAG, "Disconnect error", e)
                withContext(Dispatchers.Main) {
                    updateStatus("Disconnected")
                    connectButton.isEnabled = true
                    clientRef = null
                    log("Disconnect error: ${e.message}")
                }
            }
        }
    }

    private fun sendMessage() {
        val message = messageInput.text.toString().trim()
        if (message.isEmpty()) return

        val ref = clientRef
        if (ref == null) {
            log("Error: Not connected")
            return
        }

        messageInput.text.clear()
        log("📤 Sending Echo: $message")

        lifecycleScope.launch {
            try {
                val request = EchoRequest.newBuilder().setMessage(message).build()
                val responsePayload =
                        ref.call(
                                "echo.EchoService.Echo",
                                PayloadType.RPC_RELIABLE,
                                request.toByteArray(),
                                30000L
                        )
                val response = EchoResponse.parseFrom(responsePayload)
                Log.i(TAG, "📬 Echo Response: ${response.reply}")

                withContext(Dispatchers.Main) { log("📥 Echo: ${response.reply}") }
            } catch (e: Exception) {
                Log.e(TAG, "Echo send error", e)
                withContext(Dispatchers.Main) { log("❌ Echo error: ${e.message}") }
            }
        }
    }

    private fun sendFile() {
        val ref = clientRef
        if (ref == null) {
            log("Error: Not connected")
            return
        }

        log("📤 Starting stream transfer...")

        lifecycleScope.launch {
            try {
                val request =
                        ClientStartStreamRequest.newBuilder()
                                .setClientId("android-client")
                                .setStreamId("stream-${System.currentTimeMillis()}")
                                .setMessageCount(3)
                                .build()

                val responsePayload =
                        ref.call(
                                "data_stream_peer.StreamClient.StartStream",
                                PayloadType.RPC_RELIABLE,
                                request.toByteArray(),
                                60000L
                        )

                val response = ClientStartStreamResponse.parseFrom(responsePayload)
                Log.i(
                        TAG,
                        "📬 StartStream Response: accepted=${response.accepted}, message=${response.message}"
                )

                withContext(Dispatchers.Main) {
                    if (response.accepted) {
                        log("✅ Stream transfer started successfully")
                        log("📝 ${response.message}")
                    } else {
                        log("❌ Stream transfer rejected: ${response.message}")
                    }
                }
            } catch (e: Exception) {
                Log.e(TAG, "Stream transfer error", e)
                withContext(Dispatchers.Main) { log("❌ Stream transfer error: ${e.message}") }
            }
        }
    }

    private fun updateStatus(status: String) {
        statusText.text = "Status: $status"
    }

    private fun log(message: String) {
        val currentTime =
                java.text.SimpleDateFormat("HH:mm:ss", java.util.Locale.getDefault())
                        .format(java.util.Date())
        val logEntry = "[$currentTime] $message\n"
        logText.append(logEntry)
        scrollView.post { scrollView.fullScroll(ScrollView.FOCUS_DOWN) }
    }

    override fun onDestroy() {
        super.onDestroy()

        if (::networkMonitor.isInitialized) {
            networkMonitor.stopMonitoring()
        }

        lifecycleScope.launch {
            try {
                clientSystem?.close()
                clientSystem = null
            } catch (e: Exception) {
                Log.w(TAG, "Error during onDestroy cleanup: ${e.message}")
            }
        }
    }
}

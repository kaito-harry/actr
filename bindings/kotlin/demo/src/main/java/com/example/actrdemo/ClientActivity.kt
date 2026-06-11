package com.example.actrdemo

import android.content.ClipData
import android.content.ClipboardManager
import android.content.Context
import android.content.Intent
import android.os.Bundle
import android.os.Environment
import android.util.Log
import android.widget.Button
import android.widget.EditText
import android.widget.ScrollView
import android.widget.TextView
import android.widget.Toast
import androidx.appcompat.app.AppCompatActivity
import androidx.lifecycle.lifecycleScope
import com.example.MyUnifiedHandler
import com.example.UnifiedWorkload
import echo.Echo.EchoRequest
import echo.Echo.EchoResponse
import io.actor_rtc.actr.ActrType
import io.actor_rtc.actr.CleanupReason
import io.actor_rtc.actr.PayloadType
import io.actor_rtc.actr.dsl.*
import io.actorrtc.demo.R
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import local.StreamClientOuterClass.ClientStartStreamRequest
import local.StreamClientOuterClass.ClientStartStreamResponse
import java.io.File
import java.text.SimpleDateFormat
import java.util.Date
import java.util.Locale

class ClientActivity : AppCompatActivity() {
    companion object {
        private const val TAG = "ClientActivity"

        // Limit log buffer to avoid exceeding Android clipboard ~1MB transaction limit
        private const val MAX_LOG_CHARS = 50_000
    }

    private lateinit var statusText: TextView
    private lateinit var connectButton: Button
    private lateinit var disconnectButton: Button
    private lateinit var messageInput: EditText
    private lateinit var sendButton: Button
    private lateinit var sendFileButton: Button
    private lateinit var logText: TextView
    private lateinit var scrollView: ScrollView
    private lateinit var copyLogButton: Button
    private lateinit var downloadLogButton: Button
    private lateinit var clearLogButton: Button

    private var clientRef: ActrRef? = null
    private var clientSystem: ActrNode? = null
    private lateinit var networkMonitor: NetworkMonitor

    // Logcat reader - streams native actr library logs to the UI
    private lateinit var logcatReader: LogcatReader

    /** Parse ActrType from the [package] section of an actr.toml config file. */
    private fun parseActrTypeFromConfig(configPath: String): ActrType {
        val lines = java.io.File(configPath).readLines()
        var inPackage = false
        var name = ""
        var manufacturer = ""
        var version = ""

        for (line in lines) {
            val trimmed = line.trim()
            when {
                trimmed.startsWith("[package]") -> inPackage = true
                trimmed.startsWith("[") && inPackage -> break
                inPackage && trimmed.startsWith("name") ->
                    name = trimmed.substringAfter("=").trim().removeSurrounding("\"")
                inPackage && trimmed.startsWith("manufacturer") ->
                    manufacturer = trimmed.substringAfter("=").trim().removeSurrounding("\"")
                inPackage && trimmed.startsWith("version") ->
                    version = trimmed.substringAfter("=").trim().removeSurrounding("\"")
            }
        }

        require(manufacturer.isNotBlank() && name.isNotBlank() && version.isNotBlank()) {
            "Failed to parse [package] section from actr.toml"
        }

        return ActrType(manufacturer = manufacturer, name = name, version = version)
    }

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        setContentView(R.layout.activity_client)

        initViews()
        initLogcatReader() // Start early to capture actr library's early logs
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
                },
            )

        networkMonitor.startMonitoring()
    }

    private fun appendToLog(text: String) {
        // Auto-scroll only when user is at the bottom, avoiding forced layout spam
        val atBottom =
            scrollView.run {
                childCount > 0 && scrollY + height >= getChildAt(0).height - 20
            }
        logText.append(text)
        val excess = logText.length() - MAX_LOG_CHARS
        if (excess > 0) {
            logText.editableText.delete(0, excess)
        }
        if (atBottom) {
            scrollView.post { scrollView.fullScroll(ScrollView.FOCUS_DOWN) }
        }
    }

    private fun initLogcatReader() {
        logcatReader = LogcatReader { lines -> appendToLog(lines) }
        logcatReader.start()
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
        copyLogButton = findViewById(R.id.copyLogButton)
        downloadLogButton = findViewById(R.id.downloadLogButton)
        clearLogButton = findViewById(R.id.clearLogButton)
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

        copyLogButton.setOnClickListener {
            val text = logText.text.toString()
            if (text.isNotEmpty()) {
                val clipboard = getSystemService(Context.CLIPBOARD_SERVICE) as ClipboardManager
                clipboard.setPrimaryClip(ClipData.newPlainText("actr logs", text))
                Toast.makeText(this, "Logs copied to clipboard", Toast.LENGTH_SHORT).show()
            }
        }

        downloadLogButton.setOnClickListener { downloadLogs() }

        clearLogButton.setOnClickListener {
            logText.text = ""
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

                val actorType = parseActrTypeFromConfig(configPath)
                Log.i(TAG, "Actor type from config: ${actorType.manufacturer}:${actorType.name}:${actorType.version}")
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
                        30000L,
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
                    ClientStartStreamRequest
                        .newBuilder()
                        .setClientId("android-client")
                        .setSessionId("session-${System.currentTimeMillis()}")
                        .setMessageCount(3)
                        .build()

                val responsePayload =
                    ref.call(
                        "data_stream_peer.StreamClient.StartStream",
                        PayloadType.RPC_RELIABLE,
                        request.toByteArray(),
                        60000L,
                    )

                val response = ClientStartStreamResponse.parseFrom(responsePayload)
                Log.i(
                    TAG,
                    "📬 StartStream Response: accepted=${response.accepted}, message=${response.message}",
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

    private fun downloadLogs() {
        val text = logText.text.toString()
        if (text.isEmpty()) {
            Toast.makeText(this, "No logs to download", Toast.LENGTH_SHORT).show()
            return
        }
        try {
            val timestamp = SimpleDateFormat("yyyyMMdd_HHmmss", Locale.getDefault()).format(Date())
            val fileName = "actr_logs_$timestamp.txt"
            val dir = getExternalFilesDir(Environment.DIRECTORY_DOWNLOADS) ?: filesDir
            dir.mkdirs()
            val file = File(dir, fileName)
            file.writeText(text)
            Toast.makeText(this, "Logs saved: ${file.absolutePath}", Toast.LENGTH_LONG).show()

            // Also offer to share
            val shareIntent =
                Intent(Intent.ACTION_SEND).apply {
                    type = "text/plain"
                    putExtra(Intent.EXTRA_TEXT, text)
                    putExtra(Intent.EXTRA_SUBJECT, "actr Logs $timestamp")
                }
            startActivity(Intent.createChooser(shareIntent, "Share Logs"))
        } catch (e: Exception) {
            Toast.makeText(this, "Failed to save logs: ${e.message}", Toast.LENGTH_LONG).show()
        }
    }

    private fun updateStatus(status: String) {
        statusText.text = "Status: $status"
    }

    private fun log(message: String) {
        Log.i(TAG, message)
        val currentTime =
            SimpleDateFormat("HH:mm:ss", Locale.getDefault())
                .format(Date())
        appendToLog("[$currentTime] $message\n")
    }

    override fun onResume() {
        super.onResume()
        if (::networkMonitor.isInitialized) {
            networkMonitor.onAppForeground()
        }
    }

    override fun onPause() {
        super.onPause()
        if (::networkMonitor.isInitialized) {
            networkMonitor.onAppBackground()
        }
    }

    override fun onDestroy() {
        super.onDestroy()

        if (::networkMonitor.isInitialized) {
            networkMonitor.cleanupConnections(CleanupReason.APP_TERMINATING)
            networkMonitor.stopMonitoring()
        }

        // Stop logcat reader
        if (::logcatReader.isInitialized) {
            logcatReader.stop()
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

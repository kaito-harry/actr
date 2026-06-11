package com.example.actrdemo

import android.content.ClipData
import android.content.ClipboardManager
import android.content.Context
import android.content.Intent
import android.os.Bundle
import android.os.Environment
import android.util.Log
import android.widget.Button
import android.widget.ScrollView
import android.widget.TextView
import android.widget.Toast
import androidx.appcompat.app.AppCompatActivity
import androidx.lifecycle.lifecycleScope
import com.example.generated.EchoServiceDispatcher
import com.example.generated.EchoServiceHandler
import echo.Echo.EchoRequest
import echo.Echo.EchoResponse
import io.actor_rtc.actr.ActrType
import io.actor_rtc.actr.ContextBridge
import io.actor_rtc.actr.ErrorEventBridge
import io.actor_rtc.actr.RpcEnvelopeBridge
import io.actor_rtc.actr.WorkloadLifecycleBridge
import io.actor_rtc.actr.dsl.ActrNode
import io.actor_rtc.actr.dsl.ActrRef
import io.actor_rtc.actr.dsl.awaitShutdown
import io.actor_rtc.actr.dsl.dynamicWorkload
import io.actor_rtc.actr.dsl.linked
import io.actorrtc.demo.R
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import java.io.File
import java.text.SimpleDateFormat
import java.util.Date
import java.util.Locale

class ServerActivity : AppCompatActivity() {
    companion object {
        private const val TAG = "ServerActivity"

        // Limit log buffer to avoid exceeding Android clipboard ~1MB transaction limit
        private const val MAX_LOG_CHARS = 50_000
    }

    private lateinit var statusText: TextView
    private lateinit var startButton: Button
    private lateinit var stopButton: Button
    private lateinit var logText: TextView
    private lateinit var scrollView: ScrollView
    private lateinit var copyLogButton: Button
    private lateinit var downloadLogButton: Button
    private lateinit var clearLogButton: Button
    private var serverSystem: ActrNode? = null
    private var serverRef: ActrRef? = null

    // Logcat reader - streams native actr library logs to the UI
    private lateinit var logcatReader: LogcatReader

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        setContentView(R.layout.activity_server)

        statusText = findViewById(R.id.statusText)
        startButton = findViewById(R.id.startButton)
        stopButton = findViewById(R.id.stopButton)
        logText = findViewById(R.id.logText)
        scrollView = findViewById(R.id.scrollView)
        copyLogButton = findViewById(R.id.copyLogButton)
        downloadLogButton = findViewById(R.id.downloadLogButton)
        clearLogButton = findViewById(R.id.clearLogButton)

        initLogcatReader() // Start early to capture actr library's early logs

        startButton.setOnClickListener { startServer() }
        stopButton.setOnClickListener { stopServer() }

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

        log("Server activity created")
    }

    private fun startServer() {
        statusText.text = "Status: Starting linked EchoService"
        startButton.isEnabled = false
        log("Starting EchoService server...")

        lifecycleScope.launch {
            try {
                val configPath = copyAssetToInternalStorage("actr.toml")
                val actorType =
                    ActrType(manufacturer = "actrium", name = "EchoService", version = "1.0.0")
                val workload = dynamicWorkload(EchoServerWorkload())
                val system = linked(configPath, actorType, workload)
                val ref = system.start()

                serverSystem = system
                serverRef = ref

                withContext(Dispatchers.Main) {
                    statusText.text = "Status: Linked EchoService running"
                    stopButton.isEnabled = true
                    log("✅ EchoService started successfully")
                }
            } catch (e: Exception) {
                Log.e(TAG, "Failed to start linked EchoService", e)
                withContext(Dispatchers.Main) {
                    statusText.text = "Status: Start failed"
                    startButton.isEnabled = true
                    stopButton.isEnabled = false
                    log("❌ Start failed: ${e.message}")
                }
            }
        }
    }

    private fun stopServer() {
        stopButton.isEnabled = false
        log("Stopping EchoService server...")

        lifecycleScope.launch {
            try {
                serverRef?.shutdown()
                serverRef?.awaitShutdown()
                serverRef = null
                serverSystem = null
            } catch (e: Exception) {
                Log.w(TAG, "Linked EchoService stop failed: ${e.message}")
            } finally {
                withContext(Dispatchers.Main) {
                    statusText.text = "Status: Stopped"
                    startButton.isEnabled = true
                    log("EchoService stopped")
                }
            }
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

    private fun log(message: String) {
        Log.i(TAG, message)
        val currentTime =
            SimpleDateFormat("HH:mm:ss", Locale.getDefault())
                .format(Date())
        appendToLog("[$currentTime] $message\n")
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

    private class EchoServerWorkload :
        WorkloadLifecycleBridge,
        EchoServiceHandler {
        override suspend fun onStart(ctx: ContextBridge) {
            Log.i(TAG, "EchoServerWorkload.onStart")
        }

        override suspend fun onReady(ctx: ContextBridge) {
            Log.i(TAG, "EchoServerWorkload.onReady")
        }

        override suspend fun onStop(ctx: ContextBridge) {
            Log.i(TAG, "EchoServerWorkload.onStop")
        }

        override suspend fun onError(
            ctx: ContextBridge,
            event: ErrorEventBridge,
        ) {
            Log.e(TAG, "EchoServerWorkload.onError: $event")
        }

        override suspend fun dispatch(
            ctx: ContextBridge,
            envelope: RpcEnvelopeBridge,
        ): ByteArray = EchoServiceDispatcher.dispatch(this, ctx, envelope)

        override suspend fun echo(
            request: EchoRequest,
            ctx: ContextBridge,
        ): EchoResponse = EchoResponse.newBuilder().setReply("Echo: ${request.message}").build()
    }

    override fun onDestroy() {
        super.onDestroy()

        // Stop logcat reader
        if (::logcatReader.isInitialized) {
            logcatReader.stop()
        }

        serverRef?.shutdown()
    }
}

package com.example.actrdemo

import android.os.Bundle
import android.util.Log
import android.widget.Button
import android.widget.TextView
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
import java.io.File
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext

class ServerActivity : AppCompatActivity() {

    companion object {
        private const val TAG = "ServerActivity"
    }

    private lateinit var statusText: TextView
    private lateinit var startButton: Button
    private lateinit var stopButton: Button
    private var serverSystem: ActrNode? = null
    private var serverRef: ActrRef? = null

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        setContentView(R.layout.activity_server)

        statusText = findViewById(R.id.statusText)
        startButton = findViewById(R.id.startButton)
        stopButton = findViewById(R.id.stopButton)

        startButton.setOnClickListener { startServer() }
        stopButton.setOnClickListener { stopServer() }
    }

    private fun startServer() {
        statusText.text = "Status: Starting linked EchoService"
        startButton.isEnabled = false

        lifecycleScope.launch {
            try {
                val configPath = copyAssetToInternalStorage("actr.toml")
                val actorType =
                        ActrType(manufacturer = "acme", name = "EchoService", version = "1.0.0")
                val workload = dynamicWorkload(EchoServerWorkload())
                val system = linked(configPath, actorType, workload)
                val ref = system.start()

                serverSystem = system
                serverRef = ref

                withContext(Dispatchers.Main) {
                    statusText.text = "Status: Linked EchoService running"
                    stopButton.isEnabled = true
                }
            } catch (e: Exception) {
                Log.e(TAG, "Failed to start linked EchoService", e)
                withContext(Dispatchers.Main) {
                    statusText.text = "Status: Start failed"
                    startButton.isEnabled = true
                    stopButton.isEnabled = false
                }
            }
        }
    }

    private fun stopServer() {
        stopButton.isEnabled = false

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

    private class EchoServerWorkload : WorkloadLifecycleBridge, EchoServiceHandler {
        override suspend fun onStart(ctx: ContextBridge) {
            Log.i(TAG, "EchoServerWorkload.onStart")
        }

        override suspend fun onReady(ctx: ContextBridge) {
            Log.i(TAG, "EchoServerWorkload.onReady")
        }

        override suspend fun onStop(ctx: ContextBridge) {
            Log.i(TAG, "EchoServerWorkload.onStop")
        }

        override suspend fun onError(ctx: ContextBridge, event: ErrorEventBridge) {
            Log.e(TAG, "EchoServerWorkload.onError: $event")
        }

        override suspend fun dispatch(ctx: ContextBridge, envelope: RpcEnvelopeBridge): ByteArray {
            return EchoServiceDispatcher.dispatch(this, ctx, envelope)
        }

        override suspend fun echo(request: EchoRequest, ctx: ContextBridge): EchoResponse {
            return EchoResponse.newBuilder().setReply("Echo: ${request.message}").build()
        }
    }

    override fun onDestroy() {
        super.onDestroy()
        serverRef?.shutdown()
    }
}

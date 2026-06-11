package com.example.actrdemo

import android.os.Handler
import android.os.Looper
import android.util.Log

/**
 * Captures all logcat output tagged "actr" (native library logs) and streams them
 * to the UI via the [onLines] callback. Uses a daemon thread + batched main-thread
 * delivery to avoid flooding the UI thread.
 */
class LogcatReader(
    private val onLines: (String) -> Unit,
) {
    private var thread: Thread? = null
    private var process: java.lang.Process? = null
    private val mainHandler = Handler(Looper.getMainLooper())
    private val buffer = StringBuilder()
    private val lock = Any()

    private val flushRunnable =
        object : Runnable {
            override fun run() {
                val batch =
                    synchronized(lock) {
                        if (buffer.isEmpty()) {
                            null
                        } else {
                            buffer.toString().also { buffer.clear() }
                        }
                    }
                if (batch != null) onLines(batch)
                mainHandler.postDelayed(this, 100)
            }
        }

    fun start() {
        if (thread?.isAlive == true) return

        thread =
            Thread {
                try {
                    // Only capture "actr" tag, suppress GC/system noise
                    val pb = ProcessBuilder("logcat", "-v", "threadtime", "actr:V", "*:S")
                    pb.redirectErrorStream(true)
                    val proc = pb.start()
                    process = proc

                    proc.inputStream.bufferedReader(Charsets.UTF_8).use { reader ->
                        while (!Thread.currentThread().isInterrupted) {
                            val line = reader.readLine()
                            if (line == null) {
                                val exitCode =
                                    try {
                                        proc.exitValue()
                                    } catch (_: Exception) {
                                        -1
                                    }
                                val msg = "[LogcatReader] logcat exited with code=$exitCode, restarting in 2s..."
                                Log.w("LogcatReader", msg)
                                synchronized(lock) { buffer.append(msg).append('\n') }
                                break
                            }
                            synchronized(lock) { buffer.append(line).append('\n') }
                        }
                    }
                } catch (_: InterruptedException) {
                    // Normal shutdown
                } catch (e: Exception) {
                    Log.e("LogcatReader", "logcat error", e)
                    synchronized(lock) { buffer.append("[LogcatReader] error: ${e.message}\n") }
                }

                // Auto-restart after logcat process exits
                if (!Thread.currentThread().isInterrupted) {
                    try {
                        Thread.sleep(2000)
                    } catch (_: InterruptedException) {
                    }
                    if (!Thread.currentThread().isInterrupted) {
                        thread = null
                        start()
                    }
                }
            }.apply {
                name = "LogcatReader"
                isDaemon = true
            }

        thread!!.start()
        mainHandler.post(flushRunnable)
    }

    fun stop() {
        mainHandler.removeCallbacks(flushRunnable)
        try {
            process?.destroy()
        } catch (_: Exception) {
        }
        process = null
        thread?.interrupt()
        thread = null
    }
}

import SwiftUI
import UIKit

struct ContentView: View {
    @StateObject private var actrService = ActrService()
    @State private var streamChunkCountText = "3"
    @State private var exportedLogShareItem: LogShareItem?
    @State private var isExportingLog = false
    @State private var logExportError: String?

    private var streamChunkCount: Int? {
        let trimmedText = streamChunkCountText.trimmingCharacters(in: .whitespacesAndNewlines)
        guard let count = Int(trimmedText), count > 0 else {
            return nil
        }
        return count
    }

    var body: some View {
        NavigationStack {
            VStack(alignment: .leading, spacing: 16) {
                // Status
                HStack {
                    Circle()
                        .fill(actrService.isReady ? Color.green : Color.orange)
                        .frame(width: 10, height: 10)
                    Text(actrService.status)
                        .font(.footnote)
                        .foregroundStyle(actrService.isReady ? .green : .secondary)
                }

                // Run All button
                Button {
                    Task { await actrService.runAllProbes() }
                } label: {
                    HStack {
                        if actrService.isRunning {
                            ProgressView()
                                .tint(.white)
                        }
                        Text(actrService.isRunning ? "Running..." : "Run All")
                    }
                    .frame(maxWidth: .infinity)
                }
                .buttonStyle(.borderedProminent)
                .disabled(!actrService.isReady || actrService.isRunning || actrService.isSendingStream)

                // Stream sender
                VStack(alignment: .leading, spacing: 8) {
                    HStack(spacing: 8) {
                        TextField("Chunks", text: $streamChunkCountText)
                            .textFieldStyle(.roundedBorder)
                            .keyboardType(.numberPad)
                            .frame(width: 110)
                            .disabled(actrService.isSendingStream)

                        Button {
                            guard let count = streamChunkCount else {
                                return
                            }

                            Task {
                                await actrService.sendHelloStreamChunks(count: count)
                            }
                        } label: {
                            HStack {
                                if actrService.isSendingStream {
                                    ProgressView()
                                        .tint(.white)
                                }
                                Text(actrService.isSendingStream ? "Sending..." : "Send Stream")
                            }
                            .frame(maxWidth: .infinity)
                        }
                        .buttonStyle(.borderedProminent)
                        .disabled(!actrService.isReady || actrService.isRunning || actrService.isSendingStream || streamChunkCount == nil)
                    }

                    if !actrService.receivedEchoLines.isEmpty {
                        ScrollViewReader { proxy in
                            ScrollView {
                                LazyVStack(alignment: .leading, spacing: 2) {
                                    ForEach(Array(actrService.receivedEchoLines.enumerated()), id: \.offset) { idx, line in
                                        Text(receivedDisplayText(line))
                                            .font(.system(.caption, design: .monospaced))
                                            .foregroundStyle(.secondary)
                                            .id(idx)
                                    }
                                }
                            }
                            .frame(height: 120)
                            .onChange(of: actrService.receivedEchoLines.count) { _, newCount in
                                proxy.scrollTo(newCount - 1, anchor: .bottom)
                            }
                        }
                        .padding(8)
                        .background(Color(.systemGroupedBackground))
                        .clipShape(RoundedRectangle(cornerRadius: 8))
                    }
                }

                // Probe results
                if !actrService.results.isEmpty {
                    List(actrService.results) { result in
                        HStack {
                            Image(systemName: result.icon)
                                .foregroundStyle(result.passed ? .green : .red)
                            VStack(alignment: .leading, spacing: 2) {
                                Text(result.name)
                                    .font(.body)
                                Text(result.details)
                                    .font(.caption)
                                    .foregroundStyle(.secondary)
                            }
                            Spacer()
                            Text("\(result.durationMs)ms")
                                .font(.caption2)
                                .foregroundStyle(.secondary)
                        }
                        .padding(.vertical, 4)
                    }
                    .listStyle(.plain)
                    .frame(maxHeight: 260)
                }

                // Log output
                if !actrService.logLines.isEmpty {
                    HStack {
                        Spacer()
                        Button {
                            exportLog()
                        } label: {
                            if isExportingLog {
                                ProgressView()
                            } else {
                                Label("Download Log", systemImage: "square.and.arrow.up")
                            }
                        }
                        .buttonStyle(.bordered)
                        .disabled(isExportingLog)
                    }

                    if let logExportError {
                        Text(logExportError)
                            .font(.caption)
                            .foregroundStyle(.red)
                    }

                    ScrollViewReader { proxy in
                        ScrollView {
                            LazyVStack(alignment: .leading, spacing: 2) {
                                ForEach(Array(actrService.logLines.enumerated()), id: \.offset) { idx, line in
                                    Text(line)
                                        .font(.system(.caption, design: .monospaced))
                                        .foregroundStyle(line.contains("[PASS]") ? .green : line.contains("[FAIL]") ? .red : .primary)
                                        .id(idx)
                                }
                            }
                        }
                        .onChange(of: actrService.logLines.count) { _, newCount in
                            proxy.scrollTo(newCount - 1, anchor: .bottom)
                        }
                    }
                    .padding(8)
                    .background(Color(.systemGroupedBackground))
                    .clipShape(RoundedRectangle(cornerRadius: 8))
                }

                Spacer()
            }
            .padding()
            .navigationTitle("SwiftTsWorkloadApp")
        }
        .sheet(item: $exportedLogShareItem) { item in
            ActivityView(activityItems: [item.url])
        }
        .task {
            await actrService.startIfNeeded()
            NSLog("[SwiftTsWorkloadApp] startIfNeeded returned, shouldAutoRun=\(actrService.shouldAutoRun), autoStreamCount=\(String(describing: actrService.autoStreamCount)), isReady=\(actrService.isReady)")
            if actrService.shouldAutoRun {
                // Wait until ACTR node is ready, then run all probes
                while !actrService.isReady {
                    try? await Task.sleep(nanoseconds: 500_000_000)
                }
                try? await Task.sleep(nanoseconds: 1_000_000_000)
                await actrService.runAllProbes()
            }
            if let autoStreamCount = actrService.autoStreamCount {
                while !actrService.isReady {
                    try? await Task.sleep(nanoseconds: 500_000_000)
                }
                try? await Task.sleep(nanoseconds: 1_000_000_000)
                await actrService.sendHelloStreamChunks(count: autoStreamCount)
                if let autoResultFilename = actrService.autoResultFilename {
                    do {
                        try actrService.writeAutoResultFile(named: autoResultFilename)
                    } catch {
                        NSLog("[SwiftTsWorkloadApp] auto result file write failed: \(error)")
                    }
                }
            }
        }
    }

    private func receivedDisplayText(_ line: String) -> String {
        if line.hasPrefix("received:") {
            return line
        }
        return "received: \(line)"
    }

    private func exportLog() {
        isExportingLog = true
        logExportError = nil

        do {
            let url = try actrService.exportLogFile()
            exportedLogShareItem = LogShareItem(url: url)
        } catch {
            logExportError = "Log export failed: \(error.localizedDescription)"
        }

        isExportingLog = false
    }
}

private struct LogShareItem: Identifiable {
    let id = UUID()
    let url: URL
}

private struct ActivityView: UIViewControllerRepresentable {
    let activityItems: [Any]

    func makeUIViewController(context: Context) -> UIActivityViewController {
        UIActivityViewController(activityItems: activityItems, applicationActivities: nil)
    }

    func updateUIViewController(_ uiViewController: UIActivityViewController, context: Context) {}
}
import Dispatch
import ActrBindings
import Foundation
import Network
#if canImport(UIKit)
import UIKit
#endif
#if canImport(AppKit)
import AppKit
#endif

/// A high-level entry point for creating and starting a package-backed ACTR node.
public final class ActrNode: Sendable {
    private let inner: ActrBindings.ActrNode
    private let networkEventMonitor: NetworkEventMonitor
    private let appLifecycleMonitor: AppLifecycleMonitor
    private let retainedWorkload: DynamicWorkload?

    /// Creates a package-backed node from config and package file paths.
    public static func from(packageConfig configPath: String, packagePath: String) async throws -> ActrNode {
        let wrapper = try await ActrBindings.ActrNode.newFromPackageFile(
            configPath: configPath,
            packagePath: packagePath
        )
        let handle = try wrapper.createNetworkEventHandle()
        let monitor = NetworkEventMonitor(handle: handle)
        let lifecycleMonitor = AppLifecycleMonitor(handle: handle)
        return ActrNode(
            inner: wrapper,
            networkEventMonitor: monitor,
            appLifecycleMonitor: lifecycleMonitor,
            retainedWorkload: nil
        )
    }

    /// Creates a package-backed node from config and package file URLs.
    public static func from(packageConfig configURL: URL, packageURL: URL) async throws -> ActrNode {
        guard configURL.isFileURL else {
            throw ActrError.Config(msg: "packageConfig URL must be a file URL")
        }
        guard packageURL.isFileURL else {
            throw ActrError.Config(msg: "packageURL must be a file URL")
        }
        return try await from(packageConfig: configURL.path, packagePath: packageURL.path)
    }

    /// Creates a linked/static node from config, explicit actor identity, and a Swift-provided workload.
    public static func linked(config configPath: String, type actorType: ActrType, workload: DynamicWorkload) async throws -> ActrNode {
        let wrapper = try await ActrBindings.ActrNode.newFromLinkedWorkload(
            configPath: configPath,
            actorType: actorType,
            workload: workload
        )
        let handle = try wrapper.createNetworkEventHandle()
        let monitor = NetworkEventMonitor(handle: handle)
        let lifecycleMonitor = AppLifecycleMonitor(handle: handle)
        return ActrNode(
            inner: wrapper,
            networkEventMonitor: monitor,
            appLifecycleMonitor: lifecycleMonitor,
            retainedWorkload: workload
        )
    }

    /// Creates a linked/static node from a config file URL, explicit actor identity, and a Swift-provided workload.
    public static func linked(config configURL: URL, type actorType: ActrType, workload: DynamicWorkload) async throws -> ActrNode {
        guard configURL.isFileURL else {
            throw ActrError.Config(msg: "config URL must be a file URL")
        }
        return try await linked(config: configURL.path, type: actorType, workload: workload)
    }

    /// Starts the package-backed actor and returns a running reference.
    public func start() async throws -> ActrRef {
        let refWrapper = try await inner.start()
        return ActrRef(inner: refWrapper, retainedWorkload: retainedWorkload)
    }

    fileprivate init(
        inner: ActrBindings.ActrNode,
        networkEventMonitor: NetworkEventMonitor,
        appLifecycleMonitor: AppLifecycleMonitor,
        retainedWorkload: DynamicWorkload?
    ) {
        self.inner = inner
        self.networkEventMonitor = networkEventMonitor
        self.appLifecycleMonitor = appLifecycleMonitor
        self.retainedWorkload = retainedWorkload
    }
}

private final class NetworkEventMonitor: @unchecked Sendable {
    private let monitor: NWPathMonitor
    private let queue: DispatchQueue
    private let handle: NetworkEventHandleWrapper
    private var hasProcessedInitialPath = false
    private var lastStatus: NWPath.Status?
    private var lastIsWifi: Bool?
    private var lastIsCellular: Bool?

    init(handle: NetworkEventHandleWrapper) {
        self.handle = handle
        monitor = NWPathMonitor()
        queue = DispatchQueue(label: "actr.network.monitor")
        monitor.pathUpdateHandler = { [weak self] path in
            self?.process(path: path)
        }
        monitor.start(queue: queue)
    }

    deinit {
        monitor.cancel()
    }

    private func process(path: NWPath) {
        let status = path.status
        let isSatisfied = status == .satisfied
        let isWifi = path.usesInterfaceType(.wifi)
        let isCellular = path.usesInterfaceType(.cellular)
        let timestamp = formattedTimestamp()

        print("Network path update: time=\(timestamp), status=\(status), satisfied=\(isSatisfied), wifi=\(isWifi), cellular=\(isCellular)")

        if !hasProcessedInitialPath {
            print("Network initial path captured: time=\(timestamp), suppressing notify")
            hasProcessedInitialPath = true
            lastStatus = status
            if isSatisfied {
                lastIsWifi = isWifi
                lastIsCellular = isCellular
            } else {
                lastIsWifi = nil
                lastIsCellular = nil
            }
            return
        }

        if lastStatus != status {
            print("Network status changed: time=\(timestamp), \(lastStatus.map { String(describing: $0) } ?? "nil") -> \(status)")
            lastStatus = status

            if isSatisfied {
                lastIsWifi = isWifi
                lastIsCellular = isCellular
                print("📱 Network available: time=\(timestamp)")
                notifyAvailable()
                return
            }

            print("📱 Network lost: time=\(timestamp)")
            notifyLost()
            lastIsWifi = nil
            lastIsCellular = nil
            return
        }

        if isSatisfied {
            if lastIsWifi == nil || lastIsCellular == nil || lastIsWifi != isWifi || lastIsCellular != isCellular {
                lastIsWifi = isWifi
                lastIsCellular = isCellular
                print("📱 Network type changed: time=\(timestamp), WiFi=\(isWifi), Cellular=\(isCellular)")
                notifyTypeChanged(isWifi: isWifi, isCellular: isCellular)
                return
            }
        } else {
            lastIsWifi = nil
            lastIsCellular = nil
        }

        lastStatus = status
    }

    private func notifyAvailable() {
        Task { [handle] in
            _ = try? await handle.handleNetworkAvailable()
        }
    }

    private func notifyLost() {
        Task { [handle] in
            _ = try? await handle.handleNetworkLost()
        }
    }

    private func notifyTypeChanged(isWifi: Bool, isCellular: Bool) {
        Task { [handle] in
            _ = try? await handle.handleNetworkTypeChanged(isWifi: isWifi, isCellular: isCellular)
        }
    }

    private func formattedTimestamp() -> String {
        let formatter = ISO8601DateFormatter()
        formatter.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
        return formatter.string(from: Date())
    }
}

private final class AppLifecycleMonitor: @unchecked Sendable {
    private let handle: NetworkEventHandleWrapper
    private let queue: DispatchQueue
    private let threshold: TimeInterval
    private var observers: [NSObjectProtocol] = []
    private var backgroundedAt: Date?

    init(handle: NetworkEventHandleWrapper, threshold: TimeInterval = 30) {
        self.handle = handle
        self.queue = DispatchQueue(label: "actr.lifecycle.monitor")
        self.threshold = threshold
        print("AppLifecycleMonitor initialized: time=\(formattedTimestamp()), threshold=\(threshold)s")
        registerObservers()
    }

    deinit {
        observers.forEach { NotificationCenter.default.removeObserver($0) }
        print("AppLifecycleMonitor deinitialized: time=\(formattedTimestamp())")
    }

    private func registerObservers() {
#if canImport(UIKit)
        let center = NotificationCenter.default
        observers.append(center.addObserver(forName: UIApplication.didEnterBackgroundNotification, object: nil, queue: nil) { [weak self] _ in
            guard let self else { return }
            self.queue.async { [weak self] in
                self?.handleDidEnterBackground()
            }
        })
        observers.append(center.addObserver(forName: UIApplication.willEnterForegroundNotification, object: nil, queue: nil) { [weak self] _ in
            guard let self else { return }
            self.queue.async { [weak self] in
                self?.handleWillEnterForeground()
            }
        })
        print("AppLifecycleMonitor registered observers: time=\(formattedTimestamp()), platform=iOS")
#elseif canImport(AppKit)
        let center = NotificationCenter.default
        observers.append(center.addObserver(forName: NSApplication.didResignActiveNotification, object: nil, queue: nil) { [weak self] _ in
            guard let self else { return }
            self.queue.async { [weak self] in
                self?.handleDidEnterBackground()
            }
        })
        observers.append(center.addObserver(forName: NSApplication.didBecomeActiveNotification, object: nil, queue: nil) { [weak self] _ in
            guard let self else { return }
            self.queue.async { [weak self] in
                self?.handleWillEnterForeground()
            }
        })
        print("AppLifecycleMonitor registered observers: time=\(formattedTimestamp()), platform=macOS")
#else
        print("⚠️ AppLifecycleMonitor: No platform support available: time=\(formattedTimestamp())")
#endif
    }

    private func handleDidEnterBackground() {
        let timestamp = formattedTimestamp()
        if backgroundedAt == nil {
            backgroundedAt = Date()
            print("🔵 App entered background: time=\(timestamp)")
        } else {
            print("⚠️ App entered background (already backgrounded): time=\(timestamp)")
        }
    }

    private func handleWillEnterForeground() {
        let timestamp = formattedTimestamp()
        guard let backgroundedAt else {
            print("🟢 App entered foreground (no background timestamp): time=\(timestamp)")
            return
        }

        self.backgroundedAt = nil
        let duration = Date().timeIntervalSince(backgroundedAt)
        print("🟢 App entered foreground: time=\(timestamp), backgroundDuration=\(String(format: "%.2f", duration))s, threshold=\(threshold)s")

        if duration > threshold {
            print("🧹 Triggering connection cleanup: time=\(timestamp), reason=exceeded_threshold")
            Task { [handle] in
                do {
                    let result = try await handle.cleanupConnections()
                    print("✅ Connection cleanup completed: time=\(self.formattedTimestamp()), result=\(result)")
                } catch {
                    print("❌ Connection cleanup failed: time=\(self.formattedTimestamp()), error=\(error)")
                }
            }
        } else {
            print("⏭️ Skipping connection cleanup: time=\(timestamp), reason=below_threshold")
        }
    }

    private func formattedTimestamp() -> String {
        let formatter = ISO8601DateFormatter()
        formatter.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
        return formatter.string(from: Date())
    }
}

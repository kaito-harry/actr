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
    private let retainedObservers: RuntimeObservers?

    /// Creates a package-backed node from config and package file paths.
    public static func from(packageConfig configPath: String, packagePath: String, observers: RuntimeObservers? = nil) async throws -> ActrNode {
        do {
            let wrapper: ActrBindings.ActrNode
            if let observers {
                wrapper = try await ActrBindings.ActrNode.newFromPackageFileWithObservers(
                    configPath: configPath,
                    packagePath: packagePath,
                    observers: observers.bridge
                )
            } else {
                wrapper = try await ActrBindings.ActrNode.newFromPackageFile(
                    configPath: configPath,
                    packagePath: packagePath
                )
            }
            let handle = try wrapper.createNetworkEventHandle()
            let monitor = NetworkEventMonitor(handle: handle)
            let lifecycleMonitor = AppLifecycleMonitor(handle: handle, networkEventMonitor: monitor)
            return ActrNode(
                inner: wrapper,
                networkEventMonitor: monitor,
                appLifecycleMonitor: lifecycleMonitor,
                retainedWorkload: nil,
                retainedObservers: observers
            )
        } catch {
            throw ActrError(error: error)
        }
    }

    /// Creates a package-backed node from config and package file URLs.
    public static func from(packageConfig configURL: URL, packageURL: URL, observers: RuntimeObservers? = nil) async throws -> ActrNode {
        guard configURL.isFileURL else {
            throw ActrError.Config(msg: "packageConfig URL must be a file URL")
        }
        guard packageURL.isFileURL else {
            throw ActrError.Config(msg: "packageURL must be a file URL")
        }
        return try await from(packageConfig: configURL.path, packagePath: packageURL.path, observers: observers)
    }

    /// Creates a linked/static node from config, explicit actor identity, and a Swift-provided workload.
    public static func linked(config configPath: String, type actorType: ActrType, workload: DynamicWorkload) async throws -> ActrNode {
        do {
            let wrapper = try await ActrBindings.ActrNode.newFromLinkedWorkload(
                configPath: configPath,
                actorType: actorType.bridge,
                workload: workload.bridge
            )
            let handle = try wrapper.createNetworkEventHandle()
            let monitor = NetworkEventMonitor(handle: handle)
            let lifecycleMonitor = AppLifecycleMonitor(handle: handle, networkEventMonitor: monitor)
            return ActrNode(
                inner: wrapper,
                networkEventMonitor: monitor,
                appLifecycleMonitor: lifecycleMonitor,
                retainedWorkload: workload,
                retainedObservers: nil
            )
        } catch {
            throw ActrError(error: error)
        }
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
        do {
            let refWrapper = try await inner.start()
            return ActrRef(inner: refWrapper, retainedWorkload: retainedWorkload, retainedObservers: retainedObservers)
        } catch {
            throw ActrError(error: error)
        }
    }

    fileprivate init(
        inner: ActrBindings.ActrNode,
        networkEventMonitor: NetworkEventMonitor,
        appLifecycleMonitor: AppLifecycleMonitor,
        retainedWorkload: DynamicWorkload?,
        retainedObservers: RuntimeObservers?
    ) {
        self.inner = inner
        self.networkEventMonitor = networkEventMonitor
        self.appLifecycleMonitor = appLifecycleMonitor
        self.retainedWorkload = retainedWorkload
        self.retainedObservers = retainedObservers
    }
}

private final class NetworkEventMonitor: @unchecked Sendable {
    private let monitor: NWPathMonitor
    private let queue: DispatchQueue
    private let handle: NetworkEventHandleWrapper
    private var hasProcessedInitialPath = false
    private var lastStatus: NWPath.Status?
    private var lastTransport: NetworkTransportFlags?
    private var lastIsExpensive: Bool?
    private var lastIsConstrained: Bool?
    private var nextSequence: UInt64 = 1

    init(handle: NetworkEventHandleWrapper) {
        self.handle = handle
        monitor = NWPathMonitor()
        queue = DispatchQueue(label: "actr.network.monitor")
        monitor.pathUpdateHandler = { [weak self] path in
            self?.process(path: path, forceNotify: false)
        }
        monitor.start(queue: queue)
    }

    deinit {
        monitor.cancel()
    }

    func notifyCurrentPath() {
        queue.async { [weak self] in
            guard let self else { return }
            self.process(path: self.monitor.currentPath, forceNotify: false)
        }
    }

    private func process(path: NWPath, forceNotify: Bool) {
        let status = path.status
        let transport = transportFlags(for: path)
        let snapshot = makeSnapshot(for: path, transport: transport)
        let timestamp = formattedTimestamp()

        print("Network path update: time=\(timestamp), status=\(status), availability=\(snapshot.availability), wifi=\(transport.wifi), cellular=\(transport.cellular), ethernet=\(transport.ethernet), vpn=\(transport.vpn), other=\(transport.other), expensive=\(snapshot.isExpensive), constrained=\(snapshot.isConstrained)")

        if !hasProcessedInitialPath {
            print("Network initial path captured: time=\(timestamp), forceNotify=\(forceNotify)")
            hasProcessedInitialPath = true
            lastStatus = status
            lastTransport = transport
            lastIsExpensive = snapshot.isExpensive
            lastIsConstrained = snapshot.isConstrained
            if !forceNotify {
                return
            }
        }

        let pathChanged = forceNotify
            || lastStatus != status
            || lastTransport != transport
            || lastIsExpensive != snapshot.isExpensive
            || lastIsConstrained != snapshot.isConstrained
        guard pathChanged else {
            return
        }

        print("Network path changed: time=\(timestamp), sequence=\(snapshot.sequence), availability=\(snapshot.availability)")
        lastStatus = status
        lastTransport = transport
        lastIsExpensive = snapshot.isExpensive
        lastIsConstrained = snapshot.isConstrained
        notifyPathChanged(snapshot: snapshot)
    }

    private func makeSnapshot(for path: NWPath, transport: NetworkTransportFlags) -> NetworkSnapshot {
        defer { nextSequence += 1 }
        return NetworkSnapshot(
            sequence: nextSequence,
            availability: availability(for: path.status),
            transport: transport,
            isExpensive: path.isExpensive,
            isConstrained: path.isConstrained
        )
    }

    private func availability(for status: NWPath.Status) -> NetworkAvailability {
        switch status {
        case .satisfied:
            return .available
        case .unsatisfied:
            return .unavailable
        case .requiresConnection:
            return .unknown
        @unknown default:
            return .unknown
        }
    }

    private func transportFlags(for path: NWPath) -> NetworkTransportFlags {
        NetworkTransportFlags(
            wifi: path.usesInterfaceType(.wifi),
            cellular: path.usesInterfaceType(.cellular),
            ethernet: path.usesInterfaceType(.wiredEthernet),
            vpn: false,
            other: path.usesInterfaceType(.other) || path.usesInterfaceType(.loopback)
        )
    }

    private func notifyPathChanged(snapshot: NetworkSnapshot) {
        Task { [handle] in
            _ = try? await handle.handleNetworkPathChanged(snapshot: snapshot)
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
    private weak var networkEventMonitor: NetworkEventMonitor?
    private let queue: DispatchQueue
    private var observers: [NSObjectProtocol] = []
    private var backgroundedAt: Date?

    init(handle: NetworkEventHandleWrapper, networkEventMonitor: NetworkEventMonitor) {
        self.handle = handle
        self.networkEventMonitor = networkEventMonitor
        self.queue = DispatchQueue(label: "actr.lifecycle.monitor")
        print("AppLifecycleMonitor initialized: time=\(formattedTimestamp())")
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
            notifyLifecycleChanged(state: .background)
        } else {
            print("⚠️ App entered background (already backgrounded): time=\(timestamp)")
        }
    }

    private func handleWillEnterForeground() {
        let timestamp = formattedTimestamp()
        guard let backgroundedAt else {
            print("🟢 App entered foreground (no background timestamp): time=\(timestamp)")
            notifyLifecycleChanged(state: .foreground(backgroundDurationMs: 0))
            networkEventMonitor?.notifyCurrentPath()
            return
        }

        self.backgroundedAt = nil
        let duration = Date().timeIntervalSince(backgroundedAt)
        let durationMs = UInt64(max(0, duration * 1000).rounded())
        print("🟢 App entered foreground: time=\(timestamp), backgroundDurationMs=\(durationMs)")
        notifyLifecycleChanged(state: .foreground(backgroundDurationMs: durationMs))
        networkEventMonitor?.notifyCurrentPath()
    }

    private func notifyLifecycleChanged(state: AppLifecycleState) {
        Task { [handle] in
            _ = try? await handle.handleAppLifecycleChanged(state: state)
        }
    }

    private func formattedTimestamp() -> String {
        let formatter = ISO8601DateFormatter()
        formatter.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
        return formatter.string(from: Date())
    }
}

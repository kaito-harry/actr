import ActrBindings

/// Resolve the package's own ActrType from a manifest.toml file.
public func resolveManifestPackageActrType(manifestPath: String) throws -> ActrType {
    do {
        return try ActrType(
            bridge: ActrBindings.resolveManifestPackageActrType(manifestPath: manifestPath)
        )
    } catch {
        throw ActrError(error: error)
    }
}

/// Resolve a dependency ActrType from a manifest.toml file.
public func resolveManifestDependency(manifestPath: String, dependencyAlias: String) throws -> ActrType {
    do {
        return try ActrType(
            bridge: ActrBindings.resolveManifestDependency(
                manifestPath: manifestPath,
                dependencyAlias: dependencyAlias
            )
        )
    } catch {
        throw ActrError(error: error)
    }
}

/// Resolve all dependency aliases declared in a manifest.toml file.
public func resolveManifestDependencyAliasList(manifestPath: String) throws -> [String] {
    do {
        return try ActrBindings.resolveManifestDependencyAliasList(manifestPath: manifestPath)
    } catch {
        throw ActrError(error: error)
    }
}

/// Creates a linked-runtime workload from lifecycle/dispatch and optional observers.
public func dynamicWorkload(
    lifecycle: any Workload,
    signaling: (any SignalingObserver)? = nil,
    websocket: (any WebSocketObserver)? = nil,
    webrtc: (any WebRTCObserver)? = nil,
    credential: (any CredentialObserver)? = nil,
    mailbox: (any MailboxObserver)? = nil
) -> DynamicWorkload {
    DynamicWorkload(
        lifecycle: lifecycle,
        signaling: signaling,
        websocket: websocket,
        webrtc: webrtc,
        credential: credential,
        mailbox: mailbox
    )
}

/// Creates package-backed runtime observers without requiring workload dispatch callbacks.
public func runtimeObservers(
    signaling: (any SignalingObserver)? = nil,
    websocket: (any WebSocketObserver)? = nil,
    webrtc: (any WebRTCObserver)? = nil,
    credential: (any CredentialObserver)? = nil,
    mailbox: (any MailboxObserver)? = nil
) -> RuntimeObservers {
    RuntimeObservers(
        signaling: signaling,
        websocket: websocket,
        webrtc: webrtc,
        credential: credential,
        mailbox: mailbox
    )
}

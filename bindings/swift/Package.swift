// swift-tools-version: 6.0
import Foundation
import PackageDescription

// Binary distribution:
// - This monorepo package is for local development and CI validation only.
// - External SwiftPM consumers should use the published actr-swift package.
let env = ProcessInfo.processInfo.environment
let bindingsPath = env["ACTR_BINDINGS_PATH"] ?? "ActrBindings"
let overrideBinaryPath = env["ACTR_BINARY_PATH"]
let distBinaryPath = "dist/ActrFFI.xcframework"
let localBinaryPath = "ActrFFI.xcframework"

let manifestDir = URL(fileURLWithPath: #filePath).deletingLastPathComponent().path
let packageRootURL = URL(fileURLWithPath: manifestDir, isDirectory: true)
let distBinaryAbsolutePath = packageRootURL.appendingPathComponent(distBinaryPath).path
let localBinaryAbsolutePath = packageRootURL.appendingPathComponent(localBinaryPath).path

func binaryPathRelativeToPackageRoot(_ path: String) -> String? {
    if path.hasPrefix("/") {
        let prefix = manifestDir.hasSuffix("/") ? manifestDir : "\(manifestDir)/"
        guard path.hasPrefix(prefix) else { return nil }
        return String(path.dropFirst(prefix.count))
    }
    return path
}

let actrBinaryTarget: Target
if let overrideBinaryPath {
    if let relativeBinaryPath = binaryPathRelativeToPackageRoot(overrideBinaryPath) {
        actrBinaryTarget = .binaryTarget(
            name: "ActrFFILib",
            path: relativeBinaryPath
        )
    } else {
        fatalError("ACTR_BINARY_PATH must point inside the bindings/swift package root.")
    }
} else if FileManager.default.fileExists(atPath: distBinaryAbsolutePath) {
    actrBinaryTarget = .binaryTarget(
        name: "ActrFFILib",
        path: distBinaryPath
    )
} else if FileManager.default.fileExists(atPath: localBinaryAbsolutePath) {
    actrBinaryTarget = .binaryTarget(
        name: "ActrFFILib",
        path: localBinaryPath
    )
} else {
    fatalError("""
    Missing local ActrFFI.xcframework for repository-local Swift validation.
    Build it first with:
      ACTR_BINDINGS_PATH=dist/ActrBindings ACTR_BINARY_PATH=dist/ActrFFI.xcframework ./build-xcframework.sh
    Then run Swift commands with:
      ACTR_BINDINGS_PATH=dist/ActrBindings ACTR_BINARY_PATH=dist/ActrFFI.xcframework swift test
    External SwiftPM consumers should depend on the published actr-swift package instead.
    """)
}

let package = Package(
    name: "actr-swift",
    platforms: [
        .iOS(.v15),
        .macOS(.v12)
    ],
    products: [
        .library(
            name: "Actr",
            targets: ["Actr"]
        )
    ],
    dependencies: [
        .package(
            url: "https://github.com/apple/swift-protobuf.git",
            .upToNextMinor(from: "1.32.0")
        )
    ],
    targets: [
        actrBinaryTarget,
        .target(
            name: "ActrFFI",
            path: bindingsPath,
            sources: ["actrFFI.c"],
            publicHeadersPath: "include"
        ),
        .target(
            name: "ActrBindings",
            dependencies: ["ActrFFI", "ActrFFILib"],
            path: bindingsPath,
            exclude: ["actrFFI.c"],
            sources: ["Actr.swift"]
        ),
        .target(
            name: "Actr",
            dependencies: [
                "ActrFFI",
                "ActrBindings",
                "ActrFFILib",
                .product(name: "SwiftProtobuf", package: "swift-protobuf")
            ],
            linkerSettings: [
                .linkedFramework("SystemConfiguration")
            ]
        ),
        .testTarget(
            name: "ActrTests",
            dependencies: [
                "Actr",
                "ActrBindings",
                .product(name: "SwiftProtobuf", package: "swift-protobuf")
            ]
        )
    ]
)

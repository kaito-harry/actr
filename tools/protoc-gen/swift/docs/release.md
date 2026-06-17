# Release Guide

This project publishes a macOS arm64-only binary named `protoc-gen-actrframework-swift`.

## Versioning

- Tag format: `vX.Y.Z` (example: `v0.3.0`).
- Artifact name: `protoc-gen-actrframework-swift-macos-arm64.zip`.
- Checksum name: `protoc-gen-actrframework-swift-macos-arm64.zip.sha256`.

## Build locally

```bash
scripts/build-release.sh
```

Outputs:

- `dist/protoc-gen-actrframework-swift-macos-arm64.zip`
- `dist/protoc-gen-actrframework-swift-macos-arm64.zip.sha256`

## Publish

Releases are published from the repository-root workflow `.github/workflows/publish-protoc-plugins.yml`, triggered manually against an existing `Actrium/actr` release tag. All six protoc plugins (Rust, Swift, Kotlin, TypeScript, Python, Web) are built and published together every run.

1. Ensure the release tag already exists (created by the release train).
2. In GitHub Actions, run the **Publish Protoc Plugins** workflow with `tag` set to the release tag, e.g. `v0.3.11`.
3. The workflow validates the release exists, builds all six plugins in parallel, then uploads (or overwrites) the zip and checksum assets. It does **not** create tags or releases.

## Verification

1. Download the zip and checksum, then verify:
   ```bash
   shasum -a 256 -c protoc-gen-actrframework-swift-macos-arm64.zip.sha256
   unzip protoc-gen-actrframework-swift-macos-arm64.zip
   ```
2. Ensure the binary is on `PATH` and run `protoc`:
   ```bash
   cp protoc-gen-actrframework-swift /usr/local/bin/

   cat <<'EOF' > example.proto
   syntax = "proto3";

   package demo;

   service EchoService {
     rpc Echo (EchoRequest) returns (EchoResponse);
   }

   message EchoRequest {
     string message = 1;
   }

   message EchoResponse {
     string reply = 1;
   }
   EOF

   protoc --actrframework-swift_out=. example.proto
   ```
3. Confirm `example.actor.swift` is generated.

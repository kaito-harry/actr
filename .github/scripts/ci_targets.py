#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from fnmatch import fnmatch


TARGETS = (
    "rust_core",
    "ts_binding",
    "ts_workload",
    "python_codegen",
    "python_workload",
    "python_web_e2e",
    "swift_binding",
    "kotlin_binding",
    "web_binding",
    "release_related",
)

FULL_TRIGGER_PATTERNS = (
    ".github/actions/**",
    ".github/scripts/**",
    ".github/workflows/**",
    "Cargo.toml",
    "Cargo.lock",
    "rust-toolchain",
    "rust-toolchain.toml",
    "rustfmt.toml",
    "clippy.toml",
    "deny.toml",
    "scripts/release-train-cli-protoc.sh",
)


def run_git_diff(base_sha: str, head_sha: str) -> list[str]:
    result = subprocess.run(
        ["git", "diff", "--name-only", base_sha, head_sha],
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    return [line.strip() for line in result.stdout.splitlines() if line.strip()]


def matches_any(path: str, patterns: tuple[str, ...]) -> bool:
    return any(fnmatch(path, pattern) for pattern in patterns)


def set_all(targets: dict[str, bool], value: bool = True) -> None:
    for key in targets:
        targets[key] = value


def detect_targets(changed_files: list[str], full_run: bool) -> tuple[dict[str, bool], list[str]]:
    targets = {name: False for name in TARGETS}
    reasons: list[str] = []

    if full_run:
        set_all(targets, True)
        reasons.append("full_run")
        return targets, reasons

    for path in changed_files:
        if matches_any(path, FULL_TRIGGER_PATTERNS):
            set_all(targets, True)
            reasons.append(f"full_trigger:{path}")
            continue

        if path == "cli/tests/e2e_typescript_generated_echo_web.rs":
            targets["rust_core"] = True
            targets["ts_workload"] = True
            targets["web_binding"] = True
            reasons.append(f"typescript_workload_web_e2e:{path}")
            continue

        if path.startswith("core/"):
            targets["rust_core"] = True
            targets["ts_binding"] = True
            targets["swift_binding"] = True
            targets["web_binding"] = True
            targets["python_web_e2e"] = True
            if path == "core/framework/wit/actr-workload.wit":
                targets["python_workload"] = True
                targets["ts_workload"] = True
            reasons.append(f"core_dependency:{path}")
            continue

        if path == "cli/tests/e2e_python_web_echo.rs":
            targets["rust_core"] = True
            targets["python_web_e2e"] = True
            reasons.append(f"python_web_e2e:{path}")
            continue

        if path.startswith("cli/assets/web-runtime/"):
            targets["rust_core"] = True
            targets["web_binding"] = True
            reasons.append(f"web_runtime_asset:{path}")
            continue

        if path.startswith(("src/", "cli/", "bindings/ffi/")):
            targets["rust_core"] = True
            reasons.append(f"rust_workspace:{path}")
            continue

        if path.startswith("tools/protoc-gen/rust/"):
            targets["rust_core"] = True
            reasons.append(f"rust_codegen:{path}")
            continue

        if path.startswith("tools/protoc-gen/web/"):
            targets["rust_core"] = True
            targets["web_binding"] = True
            reasons.append(f"web_codegen:{path}")
            continue

        if path.startswith("bindings/typescript/actr-workload/"):
            targets["ts_workload"] = True
            reasons.append(f"typescript_workload:{path}")
            continue

        if path.startswith(("bindings/typescript/", "tools/protoc-gen/typescript/")):
            targets["ts_binding"] = True
            reasons.append(f"typescript:{path}")
            continue

        if path.startswith("bindings/python/actr-workload/"):
            targets["python_workload"] = True
            targets["python_web_e2e"] = True
            reasons.append(f"python_workload:{path}")
            reasons.append(f"python_web_e2e:{path}")
            continue

        if path.startswith("tools/protoc-gen/python/"):
            targets["python_codegen"] = True
            reasons.append(f"python_codegen:{path}")
            continue

        if path.startswith("examples/python/echo-workload/"):
            targets["python_workload"] = True
            targets["python_web_e2e"] = True
            reasons.append(f"python_workload:{path}")
            reasons.append(f"python_web_e2e:{path}")
            continue

        if path.startswith("examples/typescript/echo-workload/"):
            targets["ts_workload"] = True
            targets["web_binding"] = True
            reasons.append(f"typescript_workload_web_e2e:{path}")
            continue

        if path.startswith(("bindings/swift/", "tools/protoc-gen/swift/")):
            targets["swift_binding"] = True
            reasons.append(f"swift:{path}")
            continue

        if path.startswith(("bindings/kotlin/", "tools/protoc-gen/kotlin/")):
            targets["kotlin_binding"] = True
            reasons.append(f"kotlin:{path}")
            continue

        if path.startswith(
            (
                "bindings/web/examples/echo/start-python-mock.sh",
                "bindings/web/examples/echo/test-python-workload.js",
            )
        ):
            targets["web_binding"] = True
            targets["python_web_e2e"] = True
            reasons.append(f"web_binding:{path}")
            reasons.append(f"python_web_e2e:{path}")
            continue

        if path.startswith("bindings/web/"):
            targets["web_binding"] = True
            targets["python_web_e2e"] = True
            reasons.append(f"web_binding:{path}")
            reasons.append(f"python_web_e2e:{path}")
            continue

    if any(targets.values()):
        targets["release_related"] = targets["release_related"] or any(
            target for name, target in targets.items() if name != "release_related"
        )

    return targets, reasons


def write_output(name: str, value: str) -> None:
    output_path = os.environ.get("GITHUB_OUTPUT")
    if not output_path:
        return

    with open(output_path, "a", encoding="utf-8") as output:
        output.write(f"{name}={value}\n")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--event-name", required=True)
    parser.add_argument("--base-sha", default="")
    parser.add_argument("--head-sha", default="")
    parser.add_argument("--manual-full", default="false")
    args = parser.parse_args()

    event_name = args.event_name
    manual_full = args.manual_full.lower() == "true"
    full_run = manual_full or event_name != "pull_request"

    if full_run:
        changed_files: list[str] = []
    else:
        if not args.base_sha or not args.head_sha:
            print("Missing base/head SHA for pull_request diff", file=sys.stderr)
            return 1
        changed_files = run_git_diff(args.base_sha, args.head_sha)

    targets, reasons = detect_targets(changed_files, full_run=full_run)

    write_output("mode", "full" if full_run else "affected")
    write_output("changed_files_json", json.dumps(changed_files))
    write_output("reasons_json", json.dumps(reasons))

    for name, value in targets.items():
        write_output(name, "true" if value else "false")

    summary = {
        "mode": "full" if full_run else "affected",
        "changed_files": changed_files,
        "reasons": reasons,
        "targets": targets,
    }
    print(json.dumps(summary, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

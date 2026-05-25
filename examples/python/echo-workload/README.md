<!-- SPDX-License-Identifier: Apache-2.0 -->

# echo-workload (Python)

Minimal actr workload authored in Python, compiled to a `wasm32-wasip2`
Component Model module, and packaged as a signed `.actr`.

This example uses the local `actr-workload` authoring package. Its
`build` extra pins `componentize-py==0.23.0` and wraps the
componentize-py commands used for bindings generation and
componentization. The WIT contract comes from the repo-wide source of
truth at `core/framework/wit/actr-workload.wit`.

The generated bindings module is `actr_workload_bindings`; the user
source imports the authoring package as `actr_workload`, so generated
code does not shadow the package used by workload authors.

## What It Does

- Implements `dispatch(envelope) -> result<list<u8>, actr-error>` by
  echoing the inbound payload prefixed with `"echo: "`.
- Implements `on-start`, `on-ready`, `on-stop`, and `on-error` as
  fallible no-ops returning `Ok(())`.
- Implements the twelve observation hooks for signaling, transport,
  credential, and mailbox events as infallible no-ops.

## Toolchain Requirements

| Tool              | Version | Purpose                                      |
|-------------------|---------|----------------------------------------------|
| `python3`         | >= 3.11 | host interpreter that runs componentize-py    |
| `pip`             | >= 23   | install `actr-workload[build]`               |
| `componentize-py` | 0.23.0  | WIT bindings and Component bundling          |
| `wasm-tools`      | >= 1.219| Component metadata verification              |
| `actr` CLI        | current | package the generated component              |
| `wasm-pack`       | 0.13.1  | regenerate CLI web runtime assets if missing |

`componentize-py` downloads a prebuilt CPython WASM interpreter on first
use. First-run builds require network access.

## Build

```bash
./build.sh
```

The script:

1. Creates `.venv/`.
2. Installs `../../../bindings/python/actr-workload[build]`, which pins
   `componentize-py==0.23.0`.
3. Runs `actr-workload bindings bindings --world-module actr_workload_bindings`.
4. Runs `actr-workload componentize workload --bindings-dir bindings`,
   with the local `actr-workload/src` directory on the componentizer
   Python path.
5. Runs `wasm-tools component wit` against the output and checks that
   the `actr:workload` interfaces appear in the metadata.

The output component is:

```text
dist/echo-python-0.1.0-wasm32-wasip2.wasm
```

## Packaging

```bash
./build.sh package
```

The package step runs:

```bash
cargo run --manifest-path "${ACTR_ROOT}/Cargo.toml" -p actr-cli -- \
  build --no-compile -m manifest.toml --key "${SIGNING_KEY}"
```

`manifest.toml` declares the generated wasm component as
`wasm32-wasip2`. Compilation is handled by componentize-py, so actr
packaging uses `--no-compile`.

If `ACTR_SIGNING_KEY` is set, the script uses that key. Otherwise it
generates a local development key at `dist/dev-key.json` before
packaging.

If the generated CLI web runtime assets are missing, `build.sh package`
regenerates them with:

```bash
bash bindings/web/scripts/sync-cli-assets.sh --build
```

## Files

- `workload.py` — the workload class.
- `requirements.txt` — local editable install of `actr-workload[build]`.
- `build.sh` — venv, bindings, componentize, verify, and package flow.
- `manifest.toml` — actr packaging metadata.

## License

Apache-2.0 — see workspace [LICENSE](../../../LICENSE).

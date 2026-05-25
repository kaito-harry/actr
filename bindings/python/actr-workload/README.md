<!-- SPDX-License-Identifier: Apache-2.0 -->

# actr-workload

`actr-workload` is the Python authoring package for actr workload
Components. It provides:

- A lightweight `Workload` `Protocol` for user code.
- A small CLI/API wrapper around `componentize-py`.
- Default WIT resolution against the repository source of truth at
  `core/framework/wit/actr-workload.wit`.

The build extra pins the currently supported componentizer:

```bash
python -m pip install 'actr-workload[build]'
componentize-py --version  # componentize-py 0.23.0
```

## WIT Resolution

The repo tracks exactly one workload WIT source:

```text
core/framework/wit/actr-workload.wit
```

`actr-workload` resolves WIT in this order:

1. An explicit `--wit PATH` argument or API `wit=` argument.
2. The `ACTR_WORKLOAD_WIT` environment variable.
3. The nearest checked-out repo WIT at `core/framework/wit/actr-workload.wit`.
4. A packaged resource copied from that repo WIT when building a wheel or sdist.

The packaged resource is only an artifact fallback for `pip install`
ergonomics. It is not a second tracked source copy.

## Generate Bindings

```bash
actr-workload bindings bindings
```

This runs:

```bash
componentize-py \
  -w actr-workload-guest \
  -d <resolved actr-workload.wit> \
  --world-module actr_workload_bindings \
  bindings bindings
```

Generated bindings are written under the `actr_workload_bindings`
module so they do not shadow the authoring package named
`actr_workload`.

Use `--wit PATH` to override the resolved WIT file.

## Componentize

```bash
actr-workload componentize workload \
  -o dist/echo-python-0.1.0-wasm32-wasip2.wasm \
  --project-dir . \
  --bindings-dir bindings
```

This runs `componentize-py componentize` with both the project directory
and generated bindings directory on the Python import path.

## Python API

```python
from actr_workload import Workload as WorkloadProtocol


class Workload(WorkloadProtocol):
    def dispatch(self, envelope) -> bytes:
        return bytes(envelope.payload)
```

The package intentionally does not implement runtime hosting. Host
execution belongs to the actr runtime; this package only supports Python
workload authoring and build-time componentization.

Importing `actr_workload.Workload` is guest-safe: build helpers are loaded
only when `actr-workload` CLI commands or the build API are used.

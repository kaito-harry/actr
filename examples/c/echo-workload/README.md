# C echo workload

<!-- SPDX-License-Identifier: Apache-2.0 -->

An actr workload authored in C, compiled to a wasm32-wasip2 Component that
implements the [`actr-workload-guest`](../../../core/framework/wit/actr-workload.wit)
world.

This example proves the non-Rust authoring path for the actr Component Model
contract: you can implement the sixteen lifecycle/transport/mailbox hooks plus
the `dispatch` RPC entry directly in C, using wit-bindgen's canonical-ABI
bindings.

## Semantics

`dispatch` echoes the inbound `rpc-envelope.payload` back with a literal
`echo: ` prefix. All observation hooks are implemented as infallible no-ops
(they free their owned record arguments per the canonical ABI and return).

## Required toolchain

The build pipeline is:

```
wit-bindgen c  →  clang (wasm32-wasip2)  →  wasm-component-ld  →  wasm-tools
```

Versions pinned repo-wide (see `AGENTS.md`):

| Tool              | Minimum version | Notes                                               |
|-------------------|-----------------|-----------------------------------------------------|
| `wit-bindgen`     | 0.57.1          | provides the `c` backend                            |
| `wasi-sdk`        | 24              | supplies `clang`, `wasi-libc` headers, builtins     |
| `wasm-component-ld` | 0.5.26        | must be on `$PATH`; `clang --target=wasm32-wasip2` invokes it automatically |
| `wasm-tools`      | 1.253.0         | used for the `component wit` verification step     |

On Linux, the conventional install locations are:

```bash
# wasi-sdk: extract the tarball and symlink to /opt/wasi-sdk
# https://github.com/WebAssembly/wasi-sdk/releases

# Rust-installable tooling (cargo >= 1.75):
cargo install wit-bindgen-cli --version 0.57.1
cargo install wasm-component-ld --version 0.5.26
cargo install wasm-tools --version 1.253.0
```

## Build

```bash
make              # bindings + compile + verify
# — or step-by-step —
make bindings     # regenerate gen/ from ../../../core/framework/wit/actr-workload.wit
make component    # produce echo.wasm
make verify       # print the exported world via wasm-tools
```

Override `WASI_SDK` if installed elsewhere:

```bash
make WASI_SDK=$HOME/toolchains/wasi-sdk-24
```

## Expected `wasm-tools component wit` output

Once the build succeeds on a machine with wasi-sdk installed, the verification
step should print a world shape equivalent to:

```wit
package root:component;

world root {
    import actr:workload/host@0.1.0;
    export actr:workload/workload@0.1.0;
}
```

i.e. the component imports the actr host interface and exports the actr
workload interface — exactly the shape the actr runtime (core/hyper) expects.

## Build status on this commit

As of the commit that introduced this example, the environment where the
example was authored lacked wasi-sdk, so the `clang → wasm-component-ld` leg
was not executed end-to-end. What **was** verified:

- `wit-bindgen c --world actr-workload-guest` successfully emits
  `gen/actr_workload_guest.{h,c}` + `gen/actr_workload_guest_component_type.o`
  from the repository WIT file.
- `src/main.c` implements the full set of exported symbols that the generated
  header declares.

The remaining `clang + wasm-component-ld + wasm-tools` steps are mechanical
given a working wasi-sdk install and are documented above.

## Memory ownership cheatsheet (canonical ABI)

- Inbound record/variant parameters (`rpc-envelope`, `peer-event`,
  `error-event`) are **owned by the guest**: call the generated `*_free`
  helper on them before returning or the host leaks the allocation.
- The outbound `list<u8>` from `dispatch` is allocated by the guest and
  handed to the host; use `malloc` (matched by the runtime's cabi_realloc
  free path).
- Hooks with only scalar event payloads (`credential-event`,
  `backpressure-event`) have no owned resources — nothing to free.

## Files

```
examples/c/echo-workload/
├── README.md          # this file
├── Makefile           # drives wit-bindgen + clang + wasm-tools
├── manifest.toml      # actr build metadata
├── .gitignore         # hides gen/ + build artifacts
└── src/main.c         # workload implementation
```

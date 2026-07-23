# SPDX-License-Identifier: Apache-2.0
#
# Build an actr workload authored in C as a wasm32-wasip2 Component.
#
# Pipeline:
#   1. wit-bindgen c            — generate async canonical-ABI bindings from
#                                 the V2 actr-workload WIT for the guest side
#   2. clang (wasi-sdk)         — compile C + generated bindings to a
#                                 wasm32-wasip1 reactor core module
#   3. wasm-tools component new — adapt Preview 1 and create the Component
#   4. wasm-tools component wit — verify the produced component exports the
#                                 actr-workload-guest world exactly
#
# Required tools (versions pinned by this example and CI):
#   wit-bindgen         = 0.59.0
#   wasm-tools          = 1.253.0
#   clang + wasi-sdk    = 24
#   Wasmtime reactor adapter = 46.0.1
#
# Override WASI_SDK on the command line if installed elsewhere, e.g.
#   make WASI_SDK=/opt/wasi-sdk-24
#
# WASI_ADAPTER must name Wasmtime's wasi_snapshot_preview1.reactor.wasm.

WIT_FILE      := ../../../core/framework/wit-v2/actr-workload.wit
WORLD         := actr-workload-guest-v2
GEN_DIR       := gen
BUILD_DIR     := build
DIST_DIR      := dist
CORE_WASM     := $(BUILD_DIR)/echo-c.core.wasm
OUT           := $(DIST_DIR)/echo-c-0.1.0-wasm32-wasip2.wasm
WIT_DUMP      := $(DIST_DIR)/echo-c.wit.txt

WASI_SDK      ?= /opt/wasi-sdk
WASI_CLANG    ?= $(WASI_SDK)/bin/clang
WASI_SYSROOT  ?= $(WASI_SDK)/share/wasi-sysroot
WASI_ADAPTER  ?= wasi_snapshot_preview1.reactor.wasm

WIT_BINDGEN   ?= wit-bindgen
WASM_TOOLS    ?= wasm-tools

# If WASI_CLANG points at a wasi-sdk clang, it already knows its sysroot;
# otherwise forward one explicitly when building. Users of a generic clang
# can override SYSROOT_FLAG to empty.
ifeq ($(WASI_CLANG),$(WASI_SDK)/bin/clang)
SYSROOT_FLAG :=
else
SYSROOT_FLAG := --sysroot=$(WASI_SYSROOT)
endif

CFLAGS := -O2 -Wall -Wextra --target=wasm32-wasip1 -mexec-model=reactor \
	$(SYSROOT_FLAG) -I$(GEN_DIR)

GEN_SRCS := $(GEN_DIR)/actr_workload_guest_v2.c
GEN_HDRS := $(GEN_DIR)/actr_workload_guest_v2.h
GEN_COMP := $(GEN_DIR)/actr_workload_guest_v2_component_type.o
GEN_STAMP := $(GEN_DIR)/.generated

.PHONY: all bindings component verify clean

all: verify

bindings: $(GEN_STAMP)

$(GEN_STAMP): $(WIT_FILE)
	@mkdir -p $(GEN_DIR)
	$(WIT_BINDGEN) c --world $(WORLD) --out-dir $(GEN_DIR) $(WIT_FILE)
	@touch $@

$(CORE_WASM): src/main.c $(GEN_STAMP)
	@mkdir -p $(BUILD_DIR)
	$(WASI_CLANG) $(CFLAGS) \
		-o $@ \
		src/main.c $(GEN_SRCS) $(GEN_COMP)

$(OUT): $(CORE_WASM) $(WASI_ADAPTER)
	@mkdir -p $(DIST_DIR)
	$(WASM_TOOLS) component new \
		--adapt $(WASI_ADAPTER) \
		$(CORE_WASM) \
		-o $@

component: $(OUT)

verify: component
	@echo "==> wasm-tools component wit $(OUT)"
	$(WASM_TOOLS) component wit $(OUT) | tee $(WIT_DUMP)
	grep -q "actr:workload/host@0.2.0" $(WIT_DUMP)
	grep -q "actr:workload/workload@0.2.0" $(WIT_DUMP)
	$(WASM_TOOLS) validate $(OUT)

clean:
	rm -rf $(GEN_DIR) $(BUILD_DIR) $(DIST_DIR)

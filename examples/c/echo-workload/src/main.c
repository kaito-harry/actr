// SPDX-License-Identifier: Apache-2.0
//
// C implementation of the async actr:workload@0.2.0 guest contract.
//
// Every export completes immediately, but still uses wit-bindgen's async-lift
// entrypoints and explicit task-return helpers. The invocation context is
// owned by the guest and must be released on every path.

#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "actr_workload_guest_v2.h"

static const char kEchoPrefix[] = "echo: ";

static exports_actr_workload_workload_result_void_actr_error_t ok_void(void) {
    exports_actr_workload_workload_result_void_actr_error_t result = {0};
    result.is_err = false;
    return result;
}

static actr_workload_guest_v2_callback_code_t callback_exit(
    actr_workload_guest_v2_event_t *event) {
    (void)event;
    return ACTR_WORKLOAD_GUEST_V2_CALLBACK_CODE_EXIT;
}

actr_workload_guest_v2_callback_code_t
exports_actr_workload_workload_dispatch(
    exports_actr_workload_workload_rpc_envelope_t *envelope,
    exports_actr_workload_workload_invocation_ctx_t *ctx) {
    exports_actr_workload_workload_result_list_u8_actr_error_t result = {0};
    const size_t prefix_len = sizeof(kEchoPrefix) - 1;
    const size_t payload_len = envelope->payload.len;
    const size_t total_len = prefix_len + payload_len;
    uint8_t *out = (uint8_t *)malloc(total_len);

    if (out == NULL) {
        result.is_err = true;
        result.val.err.tag = ACTR_WORKLOAD_TYPES_ACTR_ERROR_INTERNAL;
        actr_workload_guest_v2_string_dup(
            &result.val.err.val.internal,
            "C echo workload could not allocate its response");
    } else {
        memcpy(out, kEchoPrefix, prefix_len);
        if (payload_len > 0) {
            memcpy(out + prefix_len, envelope->payload.ptr, payload_len);
        }
        result.is_err = false;
        result.val.ok.ptr = out;
        result.val.ok.len = total_len;
    }

    exports_actr_workload_workload_rpc_envelope_free(envelope);
    exports_actr_workload_workload_invocation_ctx_free(ctx);
    exports_actr_workload_workload_dispatch_return(result);
    return ACTR_WORKLOAD_GUEST_V2_CALLBACK_CODE_EXIT;
}

actr_workload_guest_v2_callback_code_t
exports_actr_workload_workload_dispatch_callback(
    actr_workload_guest_v2_event_t *event) {
    return callback_exit(event);
}

#define DEFINE_FALLIBLE_CONTEXT_HOOK(name)                                  \
    actr_workload_guest_v2_callback_code_t                                  \
        exports_actr_workload_workload_##name(                              \
            exports_actr_workload_workload_invocation_ctx_t *ctx) {         \
        exports_actr_workload_workload_invocation_ctx_free(ctx);            \
        exports_actr_workload_workload_##name##_return(ok_void());          \
        return ACTR_WORKLOAD_GUEST_V2_CALLBACK_CODE_EXIT;                   \
    }                                                                       \
    actr_workload_guest_v2_callback_code_t                                  \
        exports_actr_workload_workload_##name##_callback(                   \
            actr_workload_guest_v2_event_t *event) {                        \
        return callback_exit(event);                                        \
    }

DEFINE_FALLIBLE_CONTEXT_HOOK(on_start)
DEFINE_FALLIBLE_CONTEXT_HOOK(on_ready)
DEFINE_FALLIBLE_CONTEXT_HOOK(on_stop)

actr_workload_guest_v2_callback_code_t
exports_actr_workload_workload_on_error(
    exports_actr_workload_workload_error_event_t *event,
    exports_actr_workload_workload_invocation_ctx_t *ctx) {
    exports_actr_workload_workload_error_event_free(event);
    exports_actr_workload_workload_invocation_ctx_free(ctx);
    exports_actr_workload_workload_on_error_return(ok_void());
    return ACTR_WORKLOAD_GUEST_V2_CALLBACK_CODE_EXIT;
}

actr_workload_guest_v2_callback_code_t
exports_actr_workload_workload_on_error_callback(
    actr_workload_guest_v2_event_t *event) {
    return callback_exit(event);
}

#define DEFINE_INFALLIBLE_CONTEXT_HOOK(name)                                \
    actr_workload_guest_v2_callback_code_t                                  \
        exports_actr_workload_workload_##name(                              \
            exports_actr_workload_workload_invocation_ctx_t *ctx) {         \
        exports_actr_workload_workload_invocation_ctx_free(ctx);            \
        exports_actr_workload_workload_##name##_return();                   \
        return ACTR_WORKLOAD_GUEST_V2_CALLBACK_CODE_EXIT;                   \
    }                                                                       \
    actr_workload_guest_v2_callback_code_t                                  \
        exports_actr_workload_workload_##name##_callback(                   \
            actr_workload_guest_v2_event_t *event) {                        \
        return callback_exit(event);                                        \
    }

DEFINE_INFALLIBLE_CONTEXT_HOOK(on_signaling_connecting)
DEFINE_INFALLIBLE_CONTEXT_HOOK(on_signaling_connected)
DEFINE_INFALLIBLE_CONTEXT_HOOK(on_signaling_disconnected)

#define DEFINE_PEER_EVENT_HOOK(name)                                        \
    actr_workload_guest_v2_callback_code_t                                  \
        exports_actr_workload_workload_##name(                              \
            exports_actr_workload_workload_peer_event_t *event,             \
            exports_actr_workload_workload_invocation_ctx_t *ctx) {         \
        exports_actr_workload_workload_peer_event_free(event);              \
        exports_actr_workload_workload_invocation_ctx_free(ctx);            \
        exports_actr_workload_workload_##name##_return();                   \
        return ACTR_WORKLOAD_GUEST_V2_CALLBACK_CODE_EXIT;                   \
    }                                                                       \
    actr_workload_guest_v2_callback_code_t                                  \
        exports_actr_workload_workload_##name##_callback(                   \
            actr_workload_guest_v2_event_t *event) {                        \
        return callback_exit(event);                                        \
    }

DEFINE_PEER_EVENT_HOOK(on_websocket_connecting)
DEFINE_PEER_EVENT_HOOK(on_websocket_connected)
DEFINE_PEER_EVENT_HOOK(on_websocket_disconnected)
DEFINE_PEER_EVENT_HOOK(on_webrtc_connecting)
DEFINE_PEER_EVENT_HOOK(on_webrtc_connected)
DEFINE_PEER_EVENT_HOOK(on_webrtc_disconnected)

#define DEFINE_SCALAR_EVENT_HOOK(name, event_type)                           \
    actr_workload_guest_v2_callback_code_t                                  \
        exports_actr_workload_workload_##name(                              \
            event_type *event,                                              \
            exports_actr_workload_workload_invocation_ctx_t *ctx) {         \
        (void)event;                                                        \
        exports_actr_workload_workload_invocation_ctx_free(ctx);            \
        exports_actr_workload_workload_##name##_return();                   \
        return ACTR_WORKLOAD_GUEST_V2_CALLBACK_CODE_EXIT;                   \
    }                                                                       \
    actr_workload_guest_v2_callback_code_t                                  \
        exports_actr_workload_workload_##name##_callback(                   \
            actr_workload_guest_v2_event_t *event) {                        \
        return callback_exit(event);                                        \
    }

DEFINE_SCALAR_EVENT_HOOK(
    on_credential_renewed,
    exports_actr_workload_workload_credential_event_t)
DEFINE_SCALAR_EVENT_HOOK(
    on_credential_expiring,
    exports_actr_workload_workload_credential_event_t)
DEFINE_SCALAR_EVENT_HOOK(
    on_mailbox_backpressure,
    exports_actr_workload_workload_backpressure_event_t)

actr_workload_guest_v2_callback_code_t
exports_actr_workload_workload_on_data_chunk(
    exports_actr_workload_workload_data_chunk_t *chunk,
    exports_actr_workload_workload_actr_id_t *sender,
    exports_actr_workload_workload_invocation_ctx_t *ctx) {
    exports_actr_workload_workload_data_chunk_free(chunk);
    exports_actr_workload_workload_actr_id_free(sender);
    exports_actr_workload_workload_invocation_ctx_free(ctx);
    exports_actr_workload_workload_on_data_chunk_return(ok_void());
    return ACTR_WORKLOAD_GUEST_V2_CALLBACK_CODE_EXIT;
}

actr_workload_guest_v2_callback_code_t
exports_actr_workload_workload_on_data_chunk_callback(
    actr_workload_guest_v2_event_t *event) {
    return callback_exit(event);
}

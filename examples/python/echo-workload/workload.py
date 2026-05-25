# SPDX-License-Identifier: Apache-2.0
#
# Python implementation of an actr workload, compiled to a wasm32-wasip2
# Component via actr-workload and componentize-py.

from actr_workload import Workload as WorkloadProtocol


_ECHO_PREFIX = b"echo: "


class Workload(WorkloadProtocol):
    """Echo workload implementation for the actr-workload-guest world."""

    def dispatch(self, envelope) -> bytes:
        payload = envelope.payload if envelope.payload is not None else b""
        return _ECHO_PREFIX + bytes(payload)

    def on_start(self) -> None:
        return None

    def on_ready(self) -> None:
        return None

    def on_stop(self) -> None:
        return None

    def on_error(self, event) -> None:
        return None

    def on_signaling_connecting(self) -> None:
        return None

    def on_signaling_connected(self) -> None:
        return None

    def on_signaling_disconnected(self) -> None:
        return None

    def on_websocket_connecting(self, event) -> None:
        return None

    def on_websocket_connected(self, event) -> None:
        return None

    def on_websocket_disconnected(self, event) -> None:
        return None

    def on_webrtc_connecting(self, event) -> None:
        return None

    def on_webrtc_connected(self, event) -> None:
        return None

    def on_webrtc_disconnected(self, event) -> None:
        return None

    def on_credential_renewed(self, event) -> None:
        return None

    def on_credential_expiring(self, event) -> None:
        return None

    def on_mailbox_backpressure(self, event) -> None:
        return None


__all__ = ["Workload"]

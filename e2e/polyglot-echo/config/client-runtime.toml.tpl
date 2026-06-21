edition = 1

[signaling]
url = "ws://127.0.0.1:__HTTP_PORT__/signaling/ws"

[ais_endpoint]
# Mount AIS at `/ais/*`; see server-runtime.toml.tpl for the rationale.
url = "http://127.0.0.1:__HTTP_PORT__/ais"

[deployment]
realm_id = __REALM_ID__

[discovery]
visible = true

[observability]
filter_level = "info"
tracing_enabled = false

[webrtc]
force_relay = false
stun_urls = ["stun:127.0.0.1:__ICE_PORT__"]
turn_urls = ["turn:127.0.0.1:__ICE_PORT__"]

[acl]

# Allow inbound RPCs from the EchoService at any version we publish in
# this run. Matches the manufacturer used by setup.sh (defaults to
# `polyglot`).
[[acl.rules]]
permission = "allow"
type = "__SERVICE_TYPE__"

[[trust]]
kind = "static"
pubkey_b64 = "__MFR_PUBKEY__"

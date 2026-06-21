edition = 1

[package]
path = "__PACKAGE_PATH__"

[signaling]
url = "ws://127.0.0.1:__HTTP_PORT__/signaling/ws"

[ais_endpoint]
# Mount AIS at the `/ais/*` prefix; the bare-root form parses with a
# trailing slash and yields `http://host//register` once Hyper formats
# `{endpoint}/register`. mock-actrix exposes the same routes under both
# `/` and `/ais/*`, so pinning the prefix here is safe.
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

# Polyglot client drivers all enter Hyper through `Node::from_config_file`,
# which synthesises a placeholder `local:Client:0.0.0` actr_type for the
# linked attachment. Allow-listing that single triple covers every language
# driver in this scenario; per-language identity is a follow-up.
[[acl.rules]]
permission = "allow"
type = "local:Client:0.0.0"

[[trust]]
kind = "static"
pubkey_b64 = "__MFR_PUBKEY__"

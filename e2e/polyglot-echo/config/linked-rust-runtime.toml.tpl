edition = 1

[signaling]
url = "ws://127.0.0.1:__HTTP_PORT__/signaling/ws"

[ais_endpoint]
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

# Client drivers enter Hyper through `Node::from_config_file` /
# `ActrNode.fromConfig`, which synthesise the placeholder
# `local:Client:0.0.0` actr_type for the linked attachment. Allow-listing
# that single triple covers every client driver in this scenario.
[[acl.rules]]
permission = "allow"
type = "local:Client:0.0.0"

[[trust]]
kind = "static"
pubkey_b64 = "__MFR_PUBKEY__"

# actr.toml - SwiftTsWorkloadApp linked runtime configuration
#
# SwiftTsWorkloadApp uses linked mode through ActrNode.linked() and does not use
# `actr build`. This file is read directly as runtime configuration, so
# package, binary, and build sections are intentionally omitted.
#
# The actor type is defined in Swift code:
#     ActrType(manufacturer: "__MANUFACTURER__", name: "SwiftTsWorkloadApp", version: "0.1.0")

[signaling]
url = "ws://__HOST__:__HTTP_PORT__/signaling/ws"

[ais_endpoint]
url = "http://__HOST__:__HTTP_PORT__/ais"

[deployment]
# Replace this with the REALM_ID returned by actrix CreateRealm/Admin UI.
realm_id = __REALM_ID__
realm_secret = "__REALM_SECRET__"

[discovery]
visible = true

[observability]
filter_level = "info"
tracing_enabled = true
tracing_endpoint = "http://localhost:4317"
tracing_service_name = "swift-ts-workload-ios"

[webrtc]
force_relay = false
stun_urls = ["stun:__HOST__:__ICE_PORT__"]
turn_urls = ["turn:__HOST__:__ICE_PORT__"]

[acl]

[[acl.rules]]
permission = "allow"
type = "__MANUFACTURER__:DuplexEchoService:1.0.0"

[[acl.rules]]
permission = "allow"
type = "__MANUFACTURER__:SwiftTsWorkloadApp:0.1.0"

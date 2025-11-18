# Actrix Agent Notes

## Key serialization invariant
- AIS and KS share the exact same rule: every secp256k1 public key must come from `public_key.serialize_compressed()` yielding a **33-byte compressed** buffer, and only that byte array may be Base64-encoded, stored, or sent over any API.
- Generation/persistence: all write paths such as `crates/ks/src/storage/{sqlite,redis,postgres}.rs` and `crates/ais/src/issuer.rs::refresh_key_internal` have to call `serialize_compressed()` before encoding; otherwise downstream 33-byte guards will immediately fail with “Unsupported public key length: 65”.
- Validation/readback: both KS clients (`crates/ks/src/client.rs` and `crates/ks/src/grpc_client.rs`) enforce that the Base64 payload decodes to exactly 33 bytes; any other length must trigger an immediate failure.
- Any change touching the `KeyStorage` table or public APIs must preserve this compressed-form invariant so AIS caches, KS clients, and external services remain interoperable and debuggable.

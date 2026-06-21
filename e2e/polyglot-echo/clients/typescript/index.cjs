// polyglot-echo — TypeScript bindings driver (CommonJS).
//
// Counterpart to clients/rust/src/main.rs but talks to mock-actrix through
// `bindings/typescript`. The script loads the napi binding directly from
// the in-tree `dist/` output so the scenario doesn't have to npm-install
// against a published version of @actrium/actr.
//
// Inputs (positional CLI args, all required):
//   --actr-toml <path>        runtime config rendered by setup.sh
//   --service-type <triple>   "manufacturer:name:version" to discover
//   --message <string>        payload to echo
//
// On success the driver prints `[Received reply] <reply>` and exits 0.
// On any failure it prints to stderr and exits non-zero.

'use strict';

const fs = require('node:fs');
const path = require('node:path');

const REPO_ROOT = path.resolve(__dirname, '..', '..', '..', '..');
const BINDINGS_DIR = path.join(REPO_ROOT, 'bindings', 'typescript');
const DIST_ENTRY = path.join(BINDINGS_DIR, 'dist', 'index.js');

if (!fs.existsSync(DIST_ENTRY)) {
  console.error(
    `bindings/typescript dist not built at ${DIST_ENTRY}. ` +
      'Run `npm install && npm run build` in bindings/typescript first.',
  );
  process.exit(2);
}

const { ActrNode } = require(DIST_ENTRY);

const ECHO_ROUTE = 'echo.EchoService.Echo';
const RPC_PAYLOAD_TYPE_RELIABLE = 0; // PayloadType.RpcReliable
const RPC_TIMEOUT_MS = 15000;

// ── proto codec (minimal, by-hand) ─────────────────────────────────────
//
// Two messages, lifted from the `echo.proto` shared by every driver:
//   EchoRequest  { string message = 1; }
//   EchoResponse { string reply = 1; uint64 timestamp = 2; }
//
// We only encode the request and decode the reply field; timestamp is
// ignored. Hand-rolling avoids a dependency on protobufjs and keeps the
// driver dependency-free.

function encodeVarint(value) {
  let v = value >>> 0;
  const out = [];
  while (v >= 0x80) {
    out.push((v & 0x7f) | 0x80);
    v >>>= 7;
  }
  out.push(v);
  return Buffer.from(out);
}

function decodeVarint(buf, offset) {
  let result = 0n;
  let shift = 0n;
  let i = 0;
  while (offset + i < buf.length) {
    const byte = BigInt(buf[offset + i]);
    result |= (byte & 0x7fn) << shift;
    i += 1;
    if ((byte & 0x80n) === 0n) {
      return { value: result, length: i };
    }
    shift += 7n;
  }
  throw new Error('truncated varint');
}

function encodeEchoRequest(message) {
  const bytes = Buffer.from(message, 'utf8');
  return Buffer.concat([
    Buffer.from([0x0a]), // (field=1 << 3) | wire-type 2 (length-delimited)
    encodeVarint(bytes.length),
    bytes,
  ]);
}

function decodeEchoResponseReply(buf) {
  let offset = 0;
  let reply = '';
  while (offset < buf.length) {
    const tag = decodeVarint(buf, offset);
    offset += tag.length;
    const fieldNumber = Number(tag.value) >> 3;
    const wireType = Number(tag.value) & 0x07;

    if (wireType === 2) {
      const len = decodeVarint(buf, offset);
      offset += len.length;
      const end = offset + Number(len.value);
      const slice = buf.subarray(offset, end);
      offset = end;
      if (fieldNumber === 1) {
        reply = slice.toString('utf8');
      }
    } else if (wireType === 0) {
      const v = decodeVarint(buf, offset);
      offset += v.length;
      // field 2 is timestamp (uint64) — ignored
    } else {
      throw new Error(`unsupported wire type ${wireType}`);
    }
  }
  return reply;
}

// ── arg parsing ────────────────────────────────────────────────────────

function parseArgs(argv) {
  const out = {
    actrToml: null,
    serviceType: null,
    message: 'polyglot-typescript',
  };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === '--actr-toml') {
      out.actrToml = argv[++i];
    } else if (a === '--service-type') {
      out.serviceType = argv[++i];
    } else if (a === '--message') {
      out.message = argv[++i];
    } else if (!a.startsWith('--')) {
      out.message = a;
    } else {
      throw new Error(`unknown argument: ${a}`);
    }
  }
  if (!out.actrToml) throw new Error('missing --actr-toml');
  if (!out.serviceType) throw new Error('missing --service-type');
  const parts = out.serviceType.split(':');
  if (parts.length !== 3) {
    throw new Error(
      `--service-type must be 'manufacturer:name:version', got ${out.serviceType}`,
    );
  }
  out.actrType = {
    manufacturer: parts[0],
    name: parts[1],
    version: parts[2],
  };
  return out;
}

// ── main ───────────────────────────────────────────────────────────────

async function main() {
  const args = parseArgs(process.argv.slice(2));

  // ActrNode.fromConfig wants `manifest.toml`; the sibling `actr.toml`
  // is loaded automatically. Setup.sh writes runtime config to
  // $RUN_DIR/client-runtime.toml; symlink it next to our manifest.
  const manifestPath = path.join(__dirname, 'manifest.toml');
  const runtimeLinkPath = path.join(__dirname, 'actr.toml');
  try {
    fs.unlinkSync(runtimeLinkPath);
  } catch (e) {
    if (e.code !== 'ENOENT') throw e;
  }
  fs.symlinkSync(args.actrToml, runtimeLinkPath);

  const node = await ActrNode.fromConfig(manifestPath);
  const ref = await node.start();

  const targets = await ref.discover(args.actrType, 1);
  if (!targets || targets.length === 0) {
    throw new Error(`no candidates discovered for ${args.serviceType}`);
  }
  const target = targets[0];

  const requestPayload = encodeEchoRequest(args.message);
  const responseBytes = await ref.call(
    target,
    ECHO_ROUTE,
    RPC_PAYLOAD_TYPE_RELIABLE,
    requestPayload,
    RPC_TIMEOUT_MS,
  );
  const reply = decodeEchoResponseReply(responseBytes);
  console.log(`[Received reply] ${reply}`);

  // shutdown + wait for completion (no arbitrary sleep)
  await ref.stop();
  process.exit(0);
}

main().catch((err) => {
  console.error('TS driver failed:', err && err.stack ? err.stack : err);
  process.exit(1);
});

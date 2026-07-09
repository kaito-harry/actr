/* Actor-RTC Service Worker entry — wasm-bindgen guest path.
 *
 * Loads a wasm-bindgen guest bundle (produced by `tools/wit-compile-web` →
 * `actr-web-abi` + `wasm-pack --target no-modules`) and bridges it into the
 * sw-host runtime. This is the only browser dispatch path post Option U
 * Phase 8 (the previous Component Model + jco variant was deleted; see
 * `bindings/web/docs/option-u-wit-compile-web.zh.md` §11).
 *
 * Required `runtimeConfig` fields:
 *   package_url       - URL of the `.actr` workload package
 *   runtime_wasm_url  - URL of the SW runtime WASM (sw-host wasm-pack output)
 *   trust             - array of `TrustAnchor` entries
 *
 * Companion artefact convention:
 *   `<package_url>.wbg/guest.js` and `<package_url>.wbg/guest_bg.wasm` must
 *   be served alongside the .actr; the CLI mounts them when the companion
 *   directory exists.
 */

/* global wasm_bindgen */

let SW_BROADCAST = null;

// ── Console interception: forward WASM logs to main page ──
//
// Identical to actor.sw.js; the WBG path still emits Rust `log::info!` from
// sw-host, so this broadcaster stays the same.
(function () {
  const _origInfo = console.info;
  const _origWarn = console.warn;
  const _origError = console.error;
  const _origLog = console.log;

  function extractMessage(args) {
    return Array.from(args)
      .filter(
        (a) => typeof a === 'string' && !/^\s*(color|background|font-weight|padding)\s*:/.test(a)
      )
      .join(' ')
      .replace(/%c/g, '')
      .trim();
  }

  function broadcast(data) {
    self.clients
      .matchAll({ type: 'window' })
      .then((clients) => {
        for (const client of clients) {
          client.postMessage(data);
        }
      })
      .catch(() => {
        /* ignore */
      });
  }
  SW_BROADCAST = broadcast;

  console.info = function (...args) {
    _origInfo.apply(console, args);
    const msg = extractMessage(args);
    if (msg.includes('📨') && msg.includes('Echo request')) {
      const m = msg.match(/message='([^']*)'/);
      broadcast({ type: 'echo_event', event: 'request', detail: m ? m[1] : '', ts: Date.now() });
    } else if (msg.includes('📤') && msg.includes('Echo response')) {
      const m = msg.match(/reply='([^']*)'/);
      broadcast({ type: 'echo_event', event: 'response', detail: m ? m[1] : '', ts: Date.now() });
    }
    if (
      msg.includes('[SW]') ||
      msg.includes('[WBG]') ||
      msg.includes('EchoService') ||
      msg.includes('Echo') ||
      msg.includes('Registering') ||
      msg.includes('SendEcho') ||
      msg.includes('Scheduler') ||
      msg.includes('Dispatcher') ||
      msg.includes('HostGate') ||
      msg.includes('PeerGate')
    ) {
      broadcast({ type: 'sw_log', level: 'info', message: msg, ts: Date.now() });
    }
  };

  console.warn = function (...args) {
    _origWarn.apply(console, args);
    const msg = extractMessage(args);
    if (msg.length > 0) {
      broadcast({ type: 'sw_log', level: 'warn', message: msg, ts: Date.now() });
    }
  };

  console.error = function (...args) {
    _origError.apply(console, args);
    const msg = extractMessage(args);
    broadcast({ type: 'sw_log', level: 'error', message: msg, ts: Date.now() });
    if (msg.includes('Echo') || msg.includes('handle_request') || msg.includes('service')) {
      broadcast({ type: 'echo_event', event: 'error', detail: msg, ts: Date.now() });
    }
  };

  console.log = function (...args) {
    _origLog.apply(console, args);
    const msg = extractMessage(args);
    if (
      msg.includes('[EchoService]') ||
      msg.includes('[SW]') ||
      msg.includes('[WBG]') ||
      msg.includes('[SendEcho]') ||
      msg.includes('[WebRTC]')
    ) {
      broadcast({ type: 'sw_log', level: 'info', message: msg, ts: Date.now() });
    }
  };
})();

/** @type {import('@actrium/actr-web').SwRuntimeConfig | null} */
let RUNTIME_CONFIG = null;

let wasmReady = false;
let wsProbeDone = false;

const clientPorts = new Map();
const browserToSwClient = new Map();
let staleCleanupTimer = null;

async function cleanupStaleClients() {
  if (!wasmReady) return;
  try {
    const activeWindows = await self.clients.matchAll({ type: 'window' });
    const activeIds = new Set(activeWindows.map((c) => c.id));
    for (const [browserId, swClientId] of browserToSwClient.entries()) {
      if (!activeIds.has(browserId)) {
        console.log('[SW] Cleaning up stale client:', swClientId, '(browser:', browserId, ')');
        browserToSwClient.delete(browserId);
        clientPorts.delete(swClientId);
        try {
          await wasm_bindgen.unregister_client(swClientId);
        } catch (e) {
          console.warn('[SW] unregister_client error for', swClientId, ':', e);
        }
      }
    }
  } catch (e) {
    console.warn('[SW] cleanupStaleClients error:', e);
  }
}

function scheduleStaleClientCleanup(delayMs = 0) {
  if (staleCleanupTimer) {
    clearTimeout(staleCleanupTimer);
  }
  staleCleanupTimer = setTimeout(() => {
    staleCleanupTimer = null;
    cleanupStaleClients();
  }, delayMs);
}

function emitSwLog(level, message, detail) {
  for (const port of clientPorts.values()) {
    try {
      port.postMessage({
        type: 'webrtc_event',
        payload: {
          eventType: 'sw_log',
          data: { level, message, detail },
        },
      });
    } catch (_) {
      /* port may be closed */
    }
  }
}

// ─────────────────────────────────────────────────────────────────────────
// Schema adapters between sw-host (camelCase) and actr-web-abi guest
// (kebab-case, from serde with `#[serde(rename = "...")]`).
//
// sw-host's `actr_id_to_js` emits `{ realm: { realmId }, serialNumber,
// type: { manufacturer, name, version } }` (camelCase, the WBG guest's
// `serde-wasm-bindgen` shape).
// actr-web-abi `ActrId` deserialises from `{ realm: { "realm-id" },
// "serial-number", type: {...} }`.
// ─────────────────────────────────────────────────────────────────────────

function actrIdCamelToKebab(id) {
  if (id == null) return id;
  return {
    realm: { 'realm-id': id.realm && id.realm.realmId },
    'serial-number': id.serialNumber,
    // `type` is a WIT-reserved-ish name; both sides keep the key as `type`.
    type: id.type,
  };
}

function actrIdKebabToCamel(id) {
  if (id == null) return id;
  return {
    realm: { realmId: id.realm && id.realm['realm-id'] },
    serialNumber: id['serial-number'],
    type: id.type,
  };
}

// ─────────────────────────────────────────────────────────────────────────
// Install the 8 `actrHost*` globals the wasm-bindgen guest imports.
//
// The guest's `.wasm` imports these by bare global name (wasm-pack
// `--target no-modules` resolves them from the enclosing global scope —
// see `echo_*_guest_wbg.js` `__wbg_actrHostCallRaw_*` entries).
//
// Each one is a thin proxy onto `wasm_bindgen.host_*_async` etc. from
// `actr_sw_host.js`, with argument/result reshaping to bridge the
// serde-wasm-bindgen (kebab) <-> hand-written Reflect (camel) gap.
// ─────────────────────────────────────────────────────────────────────────

function installActrHostGlobals() {
  // γ-unified (Option U Phase 6 §3.4/§3.6): every `actrHost*` global now
  // accepts `requestId` as the first argument, threading it into the
  // sw-host `host_*_async` wasm-bindgen imports so the `DISPATCH_CTXS`
  // HashMap can look up the per-dispatch `RuntimeContext` keyed on that
  // id. This lets multiple concurrent dispatches share the single-threaded
  // JS bridge without clobbering each other's runtime context (TD-003).

  self.actrHostCall = async function (requestId, target, routeKey, payload) {
    // actr-web-abi `call` passes `Dest` variant as `{ peer: {...} }` or
    // `"host"`/`"workload"`; sw-host's `host_call_async` reads `{ tag, val }`.
    // The WBG path is only exercised by the echo client which uses
    // `call_raw`, but keep the shape future-proof.
    let destCamel;
    if (target === 'host' || target === 'workload') {
      destCamel = { tag: target };
    } else if (target && target.peer) {
      destCamel = { tag: 'peer', val: actrIdKebabToCamel(target.peer) };
    } else {
      throw new Error('[WBG] actrHostCall: unknown dest shape ' + JSON.stringify(target));
    }
    const u8 = payload instanceof Uint8Array ? payload : new Uint8Array(payload);
    try {
      const reply = await wasm_bindgen.host_call_async(requestId, destCamel, routeKey, u8);
      return { ok: Array.from(reply) };
    } catch (e) {
      // Host rejects with Error carrying `actrErrorTag`; fold into the
      // guest-expected `Result::Err(ActrError)` shape.
      return { err: mapHostErrorToActr(e) };
    }
  };

  self.actrHostCallRaw = async function (requestId, target, routeKey, payload) {
    // `target` is an `ActrId` (kebab) from the guest.
    const targetCamel = actrIdKebabToCamel(target);
    const u8 = payload instanceof Uint8Array ? payload : new Uint8Array(payload);
    try {
      const reply = await wasm_bindgen.host_call_raw_async(requestId, targetCamel, routeKey, u8);
      // `reply` is a `Uint8Array` from Rust; guest expects Vec<u8> —
      // serde-wasm-bindgen deserialises Array / Uint8Array / numbers
      // buffer — Uint8Array works directly, but the outer Result
      // needs the `Ok` tag matching `#[serde(rename = "ok")]`?
      // Actually generated `ActrError` variants use kebab tags but
      // `Result<T, E>` is serde's default which emits `{ Ok: ... }`
      // (capitalized). Keep capitalization exact.
      return { Ok: reply };
    } catch (e) {
      return { Err: mapHostErrorToActr(e) };
    }
  };

  self.actrHostDiscover = async function (requestId, targetType) {
    // `targetType` is `ActrType { manufacturer, name, version }` —
    // identical shape on both sides.
    try {
      const id = await wasm_bindgen.host_discover_async(requestId, targetType);
      return { Ok: actrIdCamelToKebab(id) };
    } catch (e) {
      return { Err: mapHostErrorToActr(e) };
    }
  };

  self.actrHostGetCallerId = async function (requestId) {
    try {
      const id = wasm_bindgen.host_get_caller_id(requestId);
      if (id == null || id === undefined) return null;
      return actrIdCamelToKebab(id);
    } catch (e) {
      console.warn('[WBG] host_get_caller_id threw:', e);
      return null;
    }
  };

  self.actrHostGetRequestId = async function (requestId) {
    try {
      return wasm_bindgen.host_get_request_id(requestId);
    } catch (e) {
      console.warn('[WBG] host_get_request_id threw:', e);
      return '';
    }
  };

  self.actrHostGetSelfId = async function (requestId) {
    const id = wasm_bindgen.host_get_self_id(requestId);
    return actrIdCamelToKebab(id);
  };

  self.actrHostLogMessage = async function (requestId, level, message) {
    wasm_bindgen.host_log_message(requestId, level, message);
  };

  self.actrHostTell = async function (requestId, target, routeKey, payload) {
    let destCamel;
    if (target === 'host' || target === 'workload') {
      destCamel = { tag: target };
    } else if (target && target.peer) {
      destCamel = { tag: 'peer', val: actrIdKebabToCamel(target.peer) };
    } else {
      throw new Error('[WBG] actrHostTell: unknown dest shape ' + JSON.stringify(target));
    }
    const u8 = payload instanceof Uint8Array ? payload : new Uint8Array(payload);
    try {
      await wasm_bindgen.host_tell_async(requestId, destCamel, routeKey, u8);
      return { Ok: null };
    } catch (e) {
      return { Err: mapHostErrorToActr(e) };
    }
  };

  console.log('[SW][WBG] actrHost* globals installed (γ-unified, request_id-threaded)');
}

function mapHostErrorToActr(e) {
  // sw-host's `actr_error_to_js` attaches `actrErrorTag` + message.
  // actr-web-abi `ActrError` variant serde tags are kebab-case; we
  // return the matching `{ "kebab-tag": payload }` shape.
  const tag = (e && e.actrErrorTag) || 'internal';
  const msg = (e && e.message) || String(e);
  // `timed-out` is a unit variant; serde emits it as a bare string.
  if (tag === 'timed-out') return 'timed-out';
  if (tag === 'connection-not-ready') {
    const retryAfterMs = Number.isFinite(e && e.actrRetryAfterMs) ? e.actrRetryAfterMs : null;
    return { 'connection-not-ready': { 'retry-after-ms': retryAfterMs } };
  }
  // `dependency-not-found` carries a record payload; we don't have the
  // fields cleanly separated here, so fall back to `internal` to keep
  // the result deserialisable.
  if (tag === 'dependency-not-found') {
    return { internal: msg };
  }
  return { [tag]: msg };
}

// ─────────────────────────────────────────────────────────────────────────
// Main bootstrap: load sw-host, load WBG guest, bridge them.
// ─────────────────────────────────────────────────────────────────────────

async function loadWithGuestBridge(packageUrl, runtimeWasmUrl) {
  emitSwLog('info', 'guest_bridge_start', { packageUrl, runtimeWasmUrl });

  // ── 1. Load sw-host WASM + JS glue (unchanged from CM path) ──
  const jsUrl = runtimeWasmUrl.replace(/_bg\.wasm$/, '.js');
  const jsResp = await fetch(jsUrl, { cache: 'no-store' });
  if (!jsResp.ok) {
    throw new Error('[SW] Failed to fetch runtime JS glue: ' + jsResp.status);
  }
  const jsText = await jsResp.text();
  const patchedText = jsText.replace('let wasm_bindgen =', 'self.wasm_bindgen =');
  (0, eval)(patchedText);
  emitSwLog('info', 'guest_bridge_runtime_js_loaded', jsText.length);

  await wasm_bindgen({ module_or_path: runtimeWasmUrl });
  wasm_bindgen.init_global();
  emitSwLog('info', 'guest_bridge_runtime_ready', null);

  // ── 2. Install actrHost* globals BEFORE guest instantiation ──
  // The guest wasm resolves imports at instantiate time; globals must
  // exist up-front.
  installActrHostGlobals();

  // ── 3. Verify + extract .actr (keeps the mandatory-verify contract) ──
  const resp = await fetch(packageUrl, { cache: 'no-store' });
  if (!resp.ok) {
    throw new Error('[SW] Failed to fetch .actr package: ' + resp.status);
  }
  const buffer = await resp.arrayBuffer();
  emitSwLog('info', 'guest_bridge_actr_size', buffer.byteLength);

  const trustJson = JSON.stringify(
    RUNTIME_CONFIG && Array.isArray(RUNTIME_CONFIG.trust) ? RUNTIME_CONFIG.trust : []
  );
  try {
    // Phase 4c: the `.actr` package for the WBG variant wraps the
    // wasm-bindgen core wasm; we still call verify_and_extract to honour
    // signing, but we intentionally ignore `extracted.binary` because the
    // actual module we load is the companion `guest_bg.wasm` shipped in
    // the `.wbg/` sibling directory (see §5 below). The core wasm inside
    // the .actr is identical; we just need the JS glue from the sibling
    // bundle to drive it with wasm-bindgen conventions.
    wasm_bindgen.verify_and_extract_actr_package(new Uint8Array(buffer), trustJson);
    emitSwLog('info', 'guest_bridge_verify_ok', null);
  } catch (verifyError) {
    emitSwLog('error', 'guest_bridge_verify_failed', String(verifyError));
    throw verifyError;
  }

  // ── 4. Resolve wbg bundle URL ──
  const wbgJsUrl =
    (RUNTIME_CONFIG && RUNTIME_CONFIG.wbg_module_url) ||
    packageUrl.replace(/\.actr$/, '') + '.wbg/guest.js';
  emitSwLog('info', 'guest_bridge_guest_js_url', wbgJsUrl);

  // ── 5. Fetch guest JS glue, rewrite top-level `let wasm_bindgen` to
  // write to a dedicated global (avoid clobbering sw-host's).
  const wbgResp = await fetch(wbgJsUrl, { cache: 'no-store' });
  if (!wbgResp.ok) {
    throw new Error('[SW] Failed to fetch WBG guest JS: ' + wbgResp.status);
  }
  const wbgSrc = await wbgResp.text();
  // wasm-pack `--target no-modules` emits `let wasm_bindgen = (function(exports) {...})({});`.
  // Rewrite to `self.actrGuestBindgen = ...` so the glue coexists with
  // sw-host (also keyed to `self.wasm_bindgen`).
  const patchedGuestSrc = wbgSrc.replace(/^\s*let\s+wasm_bindgen\s*=/m, 'self.actrGuestBindgen =');
  if (patchedGuestSrc === wbgSrc) {
    throw new Error(
      '[SW] WBG guest JS does not match expected `let wasm_bindgen =` preamble; refusing to eval'
    );
  }
  (0, eval)(patchedGuestSrc);
  if (typeof self.actrGuestBindgen !== 'function') {
    throw new Error('[SW] actrGuestBindgen init not installed after eval');
  }

  // ── 6. Instantiate guest wasm ──
  const wbgWasmUrl = wbgJsUrl.replace(/\.js$/, '_bg.wasm');
  emitSwLog('info', 'guest_bridge_guest_wasm_url', wbgWasmUrl);
  await self.actrGuestBindgen({ module_or_path: wbgWasmUrl });
  emitSwLog('info', 'guest_bridge_guest_instantiated', Object.keys(self.actrGuestBindgen));

  // ── 7. Build dispatchFn that adapts sw-host envelope (camelCase) to
  // the actr-web-abi guest (kebab-case + Vec<u8>).
  const dispatchFn = async (envelope) => {
    // sw-host builds `{ requestId, routeKey, payload: Uint8Array }`.
    // actr-web-abi `RpcEnvelope` uses `request-id`, `route-key`, `payload`.
    const kebabEnv = {
      'request-id': envelope.requestId,
      'route-key': envelope.routeKey,
      // serde-wasm-bindgen reads Vec<u8> from Uint8Array fine, but
      // also accepts plain arrays. Uint8Array is the natural form.
      payload: envelope.payload,
    };
    const result = await self.actrGuestBindgen.dispatch(kebabEnv);
    // actr-web-abi `dispatch` returns `Result<Vec<u8>, ActrError>` —
    // serde-wasm-bindgen encodes `Ok(bytes)` as `{ Ok: [...] }`.
    if (result && Object.prototype.hasOwnProperty.call(result, 'Ok')) {
      const ok = result.Ok;
      if (ok instanceof Uint8Array) return ok;
      if (Array.isArray(ok)) return new Uint8Array(ok);
      throw new Error('[WBG] dispatch Ok was not bytes-like: ' + typeof ok);
    }
    if (result && Object.prototype.hasOwnProperty.call(result, 'Err')) {
      throw new Error('[WBG] guest dispatch returned Err: ' + JSON.stringify(result.Err));
    }
    // Older serde shapes or direct Uint8Array — accept as last resort.
    if (result instanceof Uint8Array) return result;
    throw new Error('[WBG] guest dispatch returned unexpected shape: ' + JSON.stringify(result));
  };

  wasm_bindgen.register_guest_workload(dispatchFn);
  emitSwLog('info', 'guest_bridge_ready', 'WBG workload registered');
}

async function ensureWasmReady() {
  if (wasmReady) return;

  if (!RUNTIME_CONFIG) {
    throw new Error('[SW] Cannot load WASM: RUNTIME_CONFIG not yet received');
  }

  const packageUrl = RUNTIME_CONFIG.package_url;
  const runtimeWasmUrl = RUNTIME_CONFIG.runtime_wasm_url;

  try {
    if (!wsProbeDone) {
      wsProbeDone = true;
      try {
        emitSwLog('info', 'ws_probe_start', RUNTIME_CONFIG.signaling_url);
        const probe = new WebSocket(RUNTIME_CONFIG.signaling_url);
        probe.binaryType = 'arraybuffer';
        probe.onopen = () => {
          emitSwLog('info', 'ws_probe_open', null);
          probe.close();
        };
        probe.onerror = () => {
          emitSwLog('error', 'ws_probe_error', null);
        };
        probe.onclose = (event) => {
          emitSwLog('info', 'ws_probe_close', {
            code: event.code,
            reason: event.reason,
            wasClean: event.wasClean,
          });
        };
      } catch (error) {
        emitSwLog('error', 'ws_probe_throw', String(error));
      }
    }

    if (!runtimeWasmUrl || !packageUrl) {
      throw new Error('[SW] RUNTIME_CONFIG requires both `runtime_wasm_url` and `package_url`');
    }
    await loadWithGuestBridge(packageUrl, runtimeWasmUrl);

    wasmReady = true;
    emitSwLog('info', 'wasm_ready', null);
  } catch (error) {
    console.error(
      '[SW] WASM init failed:',
      error && error.message ? error.message : String(error),
      'name=' + (error && error.name),
      'stack=' + (error && error.stack)
    );
    emitSwLog('error', 'wasm_init_failed', {
      error: error && error.message ? error.message : String(error),
      name: error && error.name ? error.name : undefined,
      stack: error && error.stack ? error.stack : undefined,
      packageUrl: packageUrl || null,
      runtimeWasmUrl: runtimeWasmUrl || null,
    });
    throw error;
  }
}

self.addEventListener('install', (event) => {
  console.log('[SW] installing (WBG variant)...');
  event.waitUntil(self.skipWaiting());
});

self.addEventListener('activate', (event) => {
  console.log('[SW] activated (WBG variant)');
  event.waitUntil(self.clients.claim());
});

self.addEventListener('message', async (event) => {
  if (event.data && event.data.type === 'PING') {
    if (event.source && event.source.postMessage) {
      event.source.postMessage({ type: 'PONG' });
    }
    return;
  }

  if (event.data && event.data.type === 'CLIENT_UNREGISTER') {
    const clientId = event.data.clientId;
    if (!clientId) return;
    try {
      await ensureWasmReady();
      await wasm_bindgen.unregister_client(clientId);
    } catch (e) {
      console.warn('[SW] top-level unregister_client error for', clientId, ':', e);
    }
    clientPorts.delete(clientId);
    for (const [browserId, swClientId] of browserToSwClient.entries()) {
      if (swClientId === clientId) {
        browserToSwClient.delete(browserId);
      }
    }
    emitSwLog('info', 'client_unregistered', { clientId, variant: 'wbg', source: 'top-level' });
    return;
  }

  if (!event.data || event.data.type !== 'DOM_PORT_INIT') {
    return;
  }

  const port = event.data.port;
  const clientId = event.data.clientId;
  if (!port || !clientId) return;

  if (event.data.runtimeConfig && !RUNTIME_CONFIG) {
    RUNTIME_CONFIG = event.data.runtimeConfig;
  }

  const browserId = event.source && event.source.id;
  if (browserId) {
    const previousClientId = browserToSwClient.get(browserId);
    if (previousClientId && previousClientId !== clientId) {
      console.log(
        '[SW] browser client remapped, unregistering previous client:',
        previousClientId,
        'browser:',
        browserId
      );
      const previousPort = clientPorts.get(previousClientId);
      if (previousPort) {
        try {
          previousPort.close();
        } catch (_) {
          /* ignore */
        }
        clientPorts.delete(previousClientId);
      }
      try {
        await ensureWasmReady();
        await wasm_bindgen.unregister_client(previousClientId);
      } catch (e) {
        console.warn('[SW] remap unregister_client error for', previousClientId, ':', e);
      }
    }
  }

  clientPorts.set(clientId, port);
  if (browserId) {
    browserToSwClient.set(browserId, clientId);
  }

  cleanupStaleClients();
  scheduleStaleClientCleanup(1500);

  console.log('[SW] port initialized (WBG) for client:', clientId, 'total:', clientPorts.size);

  if (event.source && event.source.postMessage) {
    event.source.postMessage({ type: 'sw_ack', message: 'port_ready' });
  }

  emitSwLog('info', 'sw_env', {
    clientId,
    variant: 'wbg',
    location: self.location ? self.location.href : null,
    totalClients: clientPorts.size,
  });

  let portMessageChain = Promise.resolve();

  async function processPortMessage(message) {
    try {
      await ensureWasmReady();
    } catch (error) {
      console.error('[SW] WASM not ready:', error);
      return;
    }

    if (!message || !message.type) return;

    switch (message.type) {
      case 'control':
        try {
          await wasm_bindgen.handle_dom_control(clientId, message.payload);
        } catch (error) {
          console.error('[SW] handle_dom_control failed:', error);
          emitSwLog('error', 'handle_dom_control_failed', String(error));
        }
        break;

      case 'webrtc_event':
        try {
          await wasm_bindgen.handle_dom_webrtc_event(clientId, message.payload);
        } catch (error) {
          console.error('[SW] handle_dom_webrtc_event failed:', error);
          emitSwLog('error', 'handle_dom_webrtc_event_failed', String(error));
        }
        break;

      case 'fast_path_data':
        try {
          await wasm_bindgen.handle_dom_fast_path(clientId, message.payload);
        } catch (error) {
          console.error('[SW] handle_dom_fast_path failed:', error);
          emitSwLog('error', 'handle_dom_fast_path_failed', String(error));
        }
        break;

      case 'register_datachannel_port':
        try {
          const dcPort = message.payload.port;
          const dcPeerId = message.payload.peerId;
          if (dcPort && dcPeerId) {
            await wasm_bindgen.register_datachannel_port(clientId, dcPeerId, dcPort);
          } else {
            console.warn('[SW] register_datachannel_port: missing port or peerId');
          }
        } catch (error) {
          console.error('[SW] register_datachannel_port failed:', error);
          emitSwLog('error', 'register_datachannel_port_failed', String(error));
        }
        break;

      case 'unregister_client':
        try {
          await wasm_bindgen.unregister_client(clientId);
          clientPorts.delete(clientId);
          for (const [browserId, swClientId] of browserToSwClient.entries()) {
            if (swClientId === clientId) {
              browserToSwClient.delete(browserId);
            }
          }
          emitSwLog('info', 'client_unregistered', { clientId, variant: 'wbg' });
        } catch (error) {
          console.error('[SW] unregister_client failed:', error);
          emitSwLog('error', 'unregister_client_failed', { clientId, error: String(error) });
        }
        break;

      default:
        console.log('[SW] unknown message type:', message.type);
        break;
    }
  }

  port.onmessage = (portEvent) => {
    const message = portEvent.data;
    portMessageChain = portMessageChain
      .then(() => processPortMessage(message))
      .catch((error) => {
        console.error('[SW] port message pipeline failed:', error);
        emitSwLog('error', 'port_message_pipeline_failed', String(error));
      });
  };

  port.start();

  ensureWasmReady().then(async () => {
    try {
      if (!RUNTIME_CONFIG) {
        console.error('[SW] RUNTIME_CONFIG not received from main thread');
        return;
      }
      await wasm_bindgen.register_client(clientId, RUNTIME_CONFIG, port);
      console.log('[SW] Client registered (WBG):', clientId);
      emitSwLog('info', 'client_registered', { clientId, variant: 'wbg' });
    } catch (error) {
      console.error('[SW] register_client failed:', error);
      emitSwLog('error', 'register_client_failed', { clientId, error: String(error) });
    }
  });
});

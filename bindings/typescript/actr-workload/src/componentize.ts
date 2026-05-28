import { mkdir, mkdtemp, rm, stat, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, resolve } from 'node:path';

import { build } from 'esbuild';

import { runJco } from './bindings.js';
import { resolveWorkloadWit } from './paths.js';

export interface ComponentizeOptions {
  output: string;
  projectDir?: string;
  bindingsDir?: string;
  wit?: string;
}

function toPublicEnvelope(): string {
  return `{
    method: envelope?.routeKey ?? envelope?.method ?? '',
    payload: envelope?.payload,
    correlationId: envelope?.requestId ?? envelope?.correlationId,
  }`;
}

function shimSource(entry: string): string {
  return `
import userWorkload from ${JSON.stringify(entry)};

function activeWorkload() {
  if (!userWorkload || typeof userWorkload.dispatch !== 'function') {
    throw new Error('The workload entry must default-export defineWorkload({ dispatch(...) { ... } }).');
  }
  return userWorkload;
}

function toUint8Array(value) {
  if (value instanceof Uint8Array) {
    return value;
  }
  if (value instanceof ArrayBuffer) {
    return new Uint8Array(value);
  }
  if (ArrayBuffer.isView(value)) {
    return new Uint8Array(value.buffer, value.byteOffset, value.byteLength);
  }
  if (value && typeof value.length === 'number') {
    return Uint8Array.from(value);
  }
  return new Uint8Array();
}

function errorMessage(event) {
  if (typeof event === 'string') {
    return event;
  }
  if (event?.context) {
    return event.context;
  }
  if (event?.source) {
    return JSON.stringify(event.source);
  }
  return 'ACTR workload error';
}

export const workload = {
  async dispatch(envelope) {
    const result = await activeWorkload().dispatch(${toPublicEnvelope()});
    return toUint8Array(result);
  },

  async onStart() {
    await activeWorkload().onStart?.();
  },

  async onReady() {
    await activeWorkload().onReady?.();
  },

  async onStop() {
    await activeWorkload().onStop?.();
  },

  async onError(event) {
    await activeWorkload().onError?.(errorMessage(event));
  },

  onSignalingConnecting() {},
  onSignalingConnected() {},
  onSignalingDisconnected() {},
  onWebsocketConnecting(_event) {},
  onWebsocketConnected(_event) {},
  onWebsocketDisconnected(_event) {},
  onWebrtcConnecting(_event) {},
  onWebrtcConnected(_event) {},
  onWebrtcDisconnected(_event) {},
  onCredentialRenewed(_event) {},
  onCredentialExpiring(_event) {},
  onMailboxBackpressure(_event) {},
};
`;
}

export async function componentize(
  entry: string,
  options: ComponentizeOptions,
): Promise<void> {
  const wit = resolveWorkloadWit(options.wit);
  const projectDir = resolve(options.projectDir ?? '.');
  const entryPath = resolve(projectDir, entry);
  const output = resolve(options.output);
  const tempDir = await mkdtemp(resolve(tmpdir(), 'actr-workload-ts-'));
  const ownsBindingsDir = options.bindingsDir === undefined;
  const bindingsDir = options.bindingsDir
    ? resolve(projectDir, options.bindingsDir)
    : resolve(tempDir, 'bindings');
  const shimPath = resolve(tempDir, 'entry-shim.js');
  const bundlePath = resolve(tempDir, 'bundle.mjs');

  try {
    await mkdir(dirname(output), { recursive: true });
    await writeFile(shimPath, shimSource(entryPath), 'utf8');
    await build({
      entryPoints: [shimPath],
      outfile: bundlePath,
      bundle: true,
      format: 'esm',
      platform: 'node',
      target: 'es2022',
      absWorkingDir: projectDir,
      sourcemap: false,
      logLevel: 'silent',
    });

    await runJco([
      'componentize',
      bundlePath,
      '--wit',
      wit,
      '-n',
      'actr-workload-guest',
      '--disable',
      'http',
      'fetch-event',
      '-o',
      output,
      '--debug-bindings-dir',
      bindingsDir,
    ]);
    await assertOutputFile(output);
  } finally {
    await rm(tempDir, { recursive: true, force: true });
    if (ownsBindingsDir) {
      await rm(bindingsDir, { recursive: true, force: true });
    }
  }
}

async function assertOutputFile(output: string): Promise<void> {
  try {
    const outputStat = await stat(output);
    if (outputStat.isFile() && outputStat.size > 0) {
      return;
    }
  } catch {
    // Report a single command-level error below.
  }

  throw new Error(`jco componentize completed without writing ${output}.`);
}

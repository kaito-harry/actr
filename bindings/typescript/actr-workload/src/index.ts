import {
  registerStream as hostRegisterStream,
  sendDataChunk as hostSendDataChunk,
  unregisterStream as hostUnregisterStream,
} from 'actr:workload/host@0.1.0';

export type PayloadBytes = Uint8Array | ArrayBuffer | ArrayLike<number>;

export interface RpcEnvelope {
  method: string;
  payload?: Uint8Array;
  contentType?: string;
  correlationId?: string;
  deadlineMs?: bigint;
}

export interface Realm {
  realmId: number;
}

export interface ActrType {
  manufacturer: string;
  name: string;
  version: string;
}

export interface ActrId {
  realm: Realm;
  serialNumber: bigint | number;
  type: ActrType;
}

export interface MetadataEntry {
  key: string;
  value: string;
}

export interface DataChunk {
  streamId: string;
  sequence: bigint | number;
  payload: PayloadBytes;
  metadata?: MetadataEntry[];
  timestampMs?: bigint | number;
}

export type Dest = 'host' | 'workload' | { peer: ActrId };

type WitActrId = Omit<ActrId, 'serialNumber'> & {
  serialNumber: bigint;
};

type WitDest =
  { tag: 'host' } | { tag: 'workload' } | { tag: 'peer'; val: WitActrId };

type WitPayloadType = { tag: PayloadType };

type WitDataChunk = Omit<
  DataChunk,
  'sequence' | 'payload' | 'metadata' | 'timestampMs'
> & {
  sequence: bigint;
  payload: Uint8Array;
  metadata: MetadataEntry[];
  timestampMs?: bigint;
};

export const PayloadType = {
  RpcReliable: 'rpc-reliable',
  RpcSignal: 'rpc-signal',
  StreamReliable: 'stream-reliable',
  StreamLatencyFirst: 'stream-latency-first',
  MediaRtp: 'media-rtp',
} as const;

export type PayloadType = (typeof PayloadType)[keyof typeof PayloadType];

export type StreamCallback = (
  chunk: DataChunk,
  sender: ActrId,
) => void | Promise<void>;

export interface Workload {
  dispatch(
    envelope: RpcEnvelope,
  ): Uint8Array | ArrayBuffer | Promise<Uint8Array | ArrayBuffer>;
  onStart?(): void | Promise<void>;
  onReady?(): void | Promise<void>;
  onStop?(): void | Promise<void>;
  onError?(message: string): void | Promise<void>;
  onDataChunk?(chunk: DataChunk, sender: ActrId): void | Promise<void>;
}

export function defineWorkload(workload: Workload): Workload {
  return workload;
}

const streamCallbacks = new Map<string, StreamCallback>();

export function toUint8Array(value: PayloadBytes): Uint8Array {
  if (value instanceof Uint8Array) {
    return value;
  }
  if (value instanceof ArrayBuffer) {
    return new Uint8Array(value);
  }
  if (ArrayBuffer.isView(value)) {
    return new Uint8Array(value.buffer, value.byteOffset, value.byteLength);
  }
  return Uint8Array.from(value);
}

function toWitDest(dest: Dest): WitDest {
  if (dest === 'host') {
    return { tag: 'host' };
  }
  if (dest === 'workload') {
    return { tag: 'workload' };
  }
  return {
    tag: 'peer',
    val: {
      ...dest.peer,
      serialNumber: BigInt(dest.peer.serialNumber),
    },
  };
}

function toWitDataChunk(chunk: DataChunk): WitDataChunk {
  return {
    streamId: chunk.streamId,
    sequence: BigInt(chunk.sequence),
    payload: toUint8Array(chunk.payload),
    metadata: chunk.metadata ?? [],
    timestampMs:
      chunk.timestampMs === undefined ? undefined : BigInt(chunk.timestampMs),
  };
}

export async function registerStream(
  streamId: string,
  callback: StreamCallback,
): Promise<void> {
  streamCallbacks.set(streamId, callback);
  await hostRegisterStream(streamId);
}

export async function unregisterStream(streamId: string): Promise<void> {
  streamCallbacks.delete(streamId);
  await hostUnregisterStream(streamId);
}

export async function sendDataChunk(
  target: Dest,
  chunk: DataChunk,
  payloadType: PayloadType,
): Promise<void> {
  await hostSendDataChunk(toWitDest(target), toWitDataChunk(chunk), {
    tag: payloadType,
  } satisfies WitPayloadType);
}

export async function __dispatchDataChunk(
  chunk: DataChunk,
  sender: ActrId,
): Promise<void> {
  const callback = streamCallbacks.get(chunk.streamId);
  if (!callback) {
    throw new Error(`No stream callback registered for ${chunk.streamId}`);
  }
  await callback(chunk, sender);
}

import { create } from '@bufbuild/protobuf';
import {
  PayloadType,
  defineWorkload,
  registerStream,
  sendDataStream,
  unregisterStream,
  type ActrId,
  type DataStream,
  type MetadataEntry,
} from '@actrium/actr-workload';

import type {
  EchoRequest,
  EchoResponse,
  FinishDuplexStreamRequest,
  FinishDuplexStreamResponse,
  StartDuplexStreamRequest,
  StartDuplexStreamResponse,
} from './generated/duplex_echo_pb.js';
import {
  EchoResponseSchema,
  FinishDuplexStreamResponseSchema,
  StartDuplexStreamResponseSchema,
  StreamPayloadMode,
} from './generated/duplex_echo_pb.js';
import type { DuplexEchoServiceHandler } from './generated/local_workload.js';
import { DuplexEchoServiceDispatcher } from './generated/local_workload.js';

const textDecoder = new TextDecoder();
const textEncoder = new TextEncoder();

type SessionState = {
  sessionId: string;
  clientToServiceStreamId: string;
  serviceToClientStreamId: string;
  payloadType: PayloadType;
  clientChunksReceived: number;
  serviceChunksSent: number;
};

const sessionsById = new Map<string, SessionState>();
const sessionIdByClientStreamId = new Map<string, string>();

function payloadTypeFor(mode: StreamPayloadMode): PayloadType {
  if (mode === StreamPayloadMode.STREAM_LATENCY_FIRST) {
    return PayloadType.StreamLatencyFirst;
  }
  return PayloadType.StreamReliable;
}

function payloadText(payload: Uint8Array | ArrayBuffer | ArrayLike<number>): string {
  const bytes =
    payload instanceof Uint8Array
      ? payload
      : payload instanceof ArrayBuffer
        ? new Uint8Array(payload)
        : Uint8Array.from(payload);
  return textDecoder.decode(bytes);
}

class DuplexEchoServiceHandlerImpl implements DuplexEchoServiceHandler {
  async echo(request: EchoRequest): Promise<EchoResponse> {
    console.log(`[DuplexEchoService] recv Echo message="${request.message}"`);
    return create(EchoResponseSchema, { message: `echo:${request.message}` });
  }

  async startDuplexStream(
    request: StartDuplexStreamRequest,
  ): Promise<StartDuplexStreamResponse> {
    const payloadType = payloadTypeFor(request.payloadMode);
    const serviceToClientStreamId = `s2c-${request.sessionId}`;

    const state: SessionState = {
      sessionId: request.sessionId,
      clientToServiceStreamId: request.clientToServiceStreamId,
      serviceToClientStreamId,
      payloadType,
      clientChunksReceived: 0,
      serviceChunksSent: 0,
    };

    await registerStream(request.clientToServiceStreamId, async (chunk, sender) => {
      await this.onClientChunk(chunk, sender);
    });

    sessionsById.set(request.sessionId, state);
    sessionIdByClientStreamId.set(request.clientToServiceStreamId, request.sessionId);

    return create(StartDuplexStreamResponseSchema, {
      sessionId: request.sessionId,
      acceptedClientToServiceStreamId: request.clientToServiceStreamId,
      serviceToClientStreamId,
      status: `registered:${request.clientToServiceStreamId}`,
    });
  }

  async finishDuplexStream(
    request: FinishDuplexStreamRequest,
  ): Promise<FinishDuplexStreamResponse> {
    const state = sessionsById.get(request.sessionId);
    await unregisterStream(request.clientToServiceStreamId);
    sessionIdByClientStreamId.delete(request.clientToServiceStreamId);
    if (state) {
      sessionsById.delete(request.sessionId);
    }
    return create(FinishDuplexStreamResponseSchema, {
      sessionId: request.sessionId,
      clientChunksReceived: state?.clientChunksReceived ?? 0,
      serviceChunksSent: state?.serviceChunksSent ?? 0,
      status: `unregistered:${request.clientToServiceStreamId}`,
    });
  }

  private async onClientChunk(chunk: DataStream, sender: ActrId): Promise<void> {
    const sessionId = sessionIdByClientStreamId.get(chunk.streamId);
    const state = sessionId ? sessionsById.get(sessionId) : undefined;
    if (!state) {
      console.log(`[DuplexEchoService] drop chunk, no session stream=${chunk.streamId}`);
      return;
    }

    const text = payloadText(chunk.payload);
    state.clientChunksReceived += 1;
    state.serviceChunksSent += 1;

    const ackSequence = BigInt(chunk.sequence) + 1000n;
    const ackPayload = `echo:${text}`;
    const ackMetadata: MetadataEntry[] = [
      { key: 'session_id', value: state.sessionId },
      { key: 'direction', value: 'service-to-client' },
      { key: 'ack_for_sequence', value: String(chunk.sequence) },
      { key: 'source_stream_id', value: chunk.streamId },
    ];

    await sendDataStream(
      { actor: sender },
      {
        streamId: state.serviceToClientStreamId,
        sequence: ackSequence,
        payload: textEncoder.encode(ackPayload),
        metadata: ackMetadata,
        timestampMs: BigInt(Date.now()),
      },
      state.payloadType,
    );
  }
}

const dispatcher = new DuplexEchoServiceDispatcher(new DuplexEchoServiceHandlerImpl());

export default defineWorkload({
  async onStart(): Promise<void> {
    console.log('[DuplexEchoService] workload started');
  },
  async onStop(): Promise<void> {
    console.log('[DuplexEchoService] workload stopped');
  },
  async dispatch(envelope): Promise<Uint8Array> {
    return dispatcher.dispatch(envelope);
  },
});

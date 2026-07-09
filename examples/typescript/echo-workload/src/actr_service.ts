import {
  PayloadType,
  defineWorkload,
  registerStream,
  sendDataChunk,
  toUint8Array,
  unregisterStream,
} from '@actrium/actr-workload';
import { create } from '@bufbuild/protobuf';

import type {
  EchoRequest,
  EchoResponse,
  StreamPrepareRequest,
  StreamPrepareResponse,
  StreamReleaseRequest,
  StreamReleaseResponse,
} from './generated/echo_pb.js';
import {
  EchoResponseSchema,
  StreamPrepareResponseSchema,
  StreamReleaseResponseSchema,
} from './generated/echo_pb.js';
import type { EchoServiceHandler } from './generated/echo_workload.js';
import { EchoServiceDispatcher } from './generated/echo_workload.js';

const textDecoder = new TextDecoder();
const textEncoder = new TextEncoder();

class EchoServiceHandlerImpl implements EchoServiceHandler {
  echo(request: EchoRequest): EchoResponse {
    console.log(`Received Echo request: ${request.message}`);

    return create(EchoResponseSchema, {
      reply: request.message,
      timestamp: BigInt(Math.floor(Date.now() / 1000)),
    });
  }

  async prepareStream(
    request: StreamPrepareRequest,
  ): Promise<StreamPrepareResponse> {
    const inboundStreamId = request.inboundStreamId;
    const replyStreamId = request.replyStreamId;
    const replyMessage = request.replyMessage;

    await registerStream(inboundStreamId, async (chunk, sender) => {
      const incoming = textDecoder.decode(toUint8Array(chunk.payload));

      await sendDataChunk(
        { peer: sender },
        {
          streamId: replyStreamId,
          sequence: BigInt(chunk.sequence) + 1n,
          payload: textEncoder.encode(`${replyMessage}: ${incoming}`),
          metadata: [{ key: 'echo-runtime', value: 'typescript-wasm' }],
          timestampMs: BigInt(Date.now()),
        },
        PayloadType.StreamReliable,
      );

      await unregisterStream(inboundStreamId);
    });

    return create(StreamPrepareResponseSchema, {
      status: `registered:${inboundStreamId}`,
    });
  }

  async releaseStream(
    request: StreamReleaseRequest,
  ): Promise<StreamReleaseResponse> {
    await unregisterStream(request.streamId);
    return create(StreamReleaseResponseSchema, {
      status: `unregistered:${request.streamId}`,
    });
  }
}

const dispatcher = new EchoServiceDispatcher(new EchoServiceHandlerImpl());

export default defineWorkload({
  async onStart(): Promise<void> {
    console.log('Generated TypeScript EchoService workload started');
  },

  async onStop(): Promise<void> {
    console.log('Generated TypeScript EchoService workload stopped');
  },

  async dispatch(envelope): Promise<Uint8Array> {
    return dispatcher.dispatch(envelope);
  },
});

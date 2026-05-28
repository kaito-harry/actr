import { defineWorkload } from '@actrium/actr-workload';
import { create } from '@bufbuild/protobuf';

import type { EchoRequest, EchoResponse } from './generated/echo_pb.js';
import { EchoResponseSchema } from './generated/echo_pb.js';
import type { EchoServiceHandler } from './generated/echo_workload.js';
import {
  EchoServiceDispatcher,
} from './generated/echo_workload.js';

class EchoServiceHandlerImpl implements EchoServiceHandler {
  echo(request: EchoRequest): EchoResponse {
    console.log(`Received Echo request: ${request.message}`);

    return create(EchoResponseSchema, {
      reply: request.message,
      timestamp: BigInt(Math.floor(Date.now() / 1000)),
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

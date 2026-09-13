import { createServer, nodeServer } from './server.js'
import { create } from '@bufbuild/protobuf'
import { CountResponseSchema, EchoRequestSchema, EchoResponseSchema, HealthResponseSchema, FailResponseSchema } from './easyrpc/conformance/v1/conformance_pb.js'
import { ConformanceServiceHandlers } from './easyrpc/conformance/v1/conformance_easyrpc.js'

const port = Number(process.env.PORT) || 18899
const impl: any = {
  health: async () => create(HealthResponseSchema, { ok: true, name: 'conformance' }),
  echo: async (req: any) => create(EchoResponseSchema, { output: 'echo:' + req.input }),
  count: async () => {
    return {
      async *[Symbol.asyncIterator]() {
        for (let i = 0; i < 3; i++) yield create(CountResponseSchema, { index: i } as any)
      },
    }
  },
  fail: async () => create(FailResponseSchema, { ok: true }),
}
const handlers = ConformanceServiceHandlers(impl)
const dispatch = createServer([
  { path: '/v1/health', name: 'Health', serverStream: false },
  { path: '/v1/echo', name: 'Echo', serverStream: false },
  { path: '/v1/count', name: 'Count', serverStream: true },
], handlers)

// Conformance server: HTTP/1 push server (streams are written + flushed
// frame-by-frame). h2c is available via the library's http2Server export.
nodeServer(dispatch).listen(port, '127.0.0.1', () => console.log('ts on', port))

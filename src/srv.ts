import { createServer, nodeServer } from './server.js'
import { create } from '@bufbuild/protobuf'
import { CountResponseSchema, EchoRequestSchema, EchoResponseSchema, HealthResponseSchema, FailResponseSchema, StreamFailResponseSchema, EchoMetaResponseSchema, BigResponseSchema, StreamFailDetailsResponseSchema } from './easyrpc/conformance/v1/conformance_pb.js'
import { ConformanceServiceHandlers, methodSpecs } from './easyrpc/conformance/v1/conformance_easyrpc.js'
import { RPCError, Headers } from './protocol.js'

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
  streamFail: async (in_: any) => ({
    async *[Symbol.asyncIterator]() {
      for (let i = 0; i < (in_.emitBefore ?? 0); i++) yield create(StreamFailResponseSchema, { index: i } as any)
      throw new RPCError(Number(in_.code ?? 13), String(in_.message ?? 'boom'))
    },
  }),
  echoMeta: async (in_: any, md?: Headers) =>
    create(EchoMetaResponseSchema, { input: in_.input, meta: { 'x-test': md?.['x-test']?.[0] ?? '' } } as any),
  big: async (in_: any) => create(BigResponseSchema, { size: in_.size } as any),
  failDetails: async (in_: any) => {
    throw new RPCError(Number(in_.code ?? 8), String(in_.message ?? 'limited'), [
      { type: String(in_.detailType ?? 't/x'), value: new TextEncoder().encode(String(in_.detailText ?? 'd')) },
    ])
  },
  streamFailDetails: async (in_: any) => ({
    async *[Symbol.asyncIterator]() {
      for (let i = 0; i < (in_.emitBefore ?? 0); i++) yield create(StreamFailDetailsResponseSchema, { index: i } as any)
      throw new RPCError(Number(in_.code ?? 13), String(in_.message ?? 'boom'), [
        { type: String(in_.detailType ?? 't/s'), value: new TextEncoder().encode(String(in_.detailText ?? 'sd')) },
      ])
    },
  }),
}
const handlers = ConformanceServiceHandlers(impl)
// Route straight off the generated specs so the server always serves the
// full conformance surface (no hand-maintained path list).
const dispatch = createServer(methodSpecs as any, handlers)

// Conformance server: HTTP/1 push server (streams are written + flushed
// frame-by-frame). h2c is available via the library's http2Server export.
nodeServer(dispatch).listen(port, '127.0.0.1', () => console.log('ts on', port))

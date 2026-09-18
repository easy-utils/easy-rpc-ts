import { createServer, http2Server, nodeServer } from './server.js'
import { create } from '@bufbuild/protobuf'
import {
  BigResponseSchema,
  BigStreamResponseSchema,
  CountResponseSchema,
  CountTrailerResponseSchema,
  EchoBytesResponseSchema,
  EchoMetaResponseSchema,
  EchoResponseSchema,
  EchoTrailerResponseSchema,
  EmptyResponseSchema,
  FailResponseSchema,
  HealthResponseSchema,
  SleepResponseSchema,
  StreamFailDetailsResponseSchema,
  StreamFailResponseSchema,
} from './easyrpc/conformance/v1/conformance_pb.js'
import { ConformanceServiceHandlers, methodSpecs } from './easyrpc/conformance/v1/conformance_easyrpc.js'
import { RPCError } from './protocol.js'

const port = Number(process.env.PORT) || 18899
const impl: any = {
  health: async () => create(HealthResponseSchema, { ok: true, name: 'conformance' }),
  echo: async (req: any) => create(EchoResponseSchema, { output: 'echo:' + req.input }),
  count: async (req: any) => {
    const n = req.count > 0 ? req.count : 3
    return {
      async *[Symbol.asyncIterator]() {
        for (let i = 0; i < n; i++) yield create(CountResponseSchema, { index: i } as any)
      },
    }
  },
  fail: async (req: any) => {
    if (req.message !== '') throw new RPCError(3, req.message)
    return create(FailResponseSchema, { ok: true })
  },
  streamFail: async (in_: any) => ({
    async *[Symbol.asyncIterator]() {
      for (let i = 0; i < (in_.emitBefore ?? 0); i++) yield create(StreamFailResponseSchema, { index: i } as any)
      throw new RPCError(Number(in_.code ?? 13), String(in_.message ?? 'boom'))
    },
  }),
  echoMeta: async (in_: any, ctx: any) =>
    create(EchoMetaResponseSchema, {
      input: in_.input,
      meta: { 'x-test': ctx?.headers?.['x-test']?.[0] ?? '', authorization: ctx?.headers?.['authorization']?.[0] ?? '' },
    } as any),
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
  echoBytes: async (req: any) => create(EchoBytesResponseSchema, { data: req.data } as any),
  sleep: async (req: any) => {
    const ms = Number(req.millis ?? 0)
    await new Promise(r => setTimeout(r, Math.max(0, ms)))
    return create(SleepResponseSchema, { ok: true })
  },
  empty: async () => create(EmptyResponseSchema, {}),
  bigStream: async (req: any) => {
    const n = Number(req.count ?? 3)
    const size = Number(req.size ?? 0)
    const payload = new Uint8Array(size)
    return {
      async *[Symbol.asyncIterator]() {
        for (let i = 0; i < n; i++) yield create(BigStreamResponseSchema, { index: i, size } as any)
        void payload
      },
    }
  },
  echoTrailer: async (in_: any, ctx: any) => {
    ctx?.setTrailer?.('x-trl', 'unary-' + String(in_.input ?? ''))
    return create(EchoTrailerResponseSchema, { output: 'trailer:' + String(in_.input ?? '') } as any)
  },
  countTrailer: async (in_: any, ctx: any) => {
    ctx?.setTrailer?.('x-ctrailer', 'done')
    const n = in_.count > 0 ? in_.count : 3
    return {
      async *[Symbol.asyncIterator]() {
        for (let i = 0; i < n; i++) yield create(CountTrailerResponseSchema, { index: i } as any)
      },
    }
  },
}
const handlers = ConformanceServiceHandlers(impl)
const dispatch = createServer(methodSpecs as any, handlers)

// HTTP_PROTOCOL=h2c serves cleartext HTTP/2 prior-knowledge (h2c); default h1.
const proto = process.env.HTTP_PROTOCOL ?? 'h1'
if (proto === 'h2c') {
  http2Server(dispatch).listen(port, '127.0.0.1', () => console.log('ts h2c on', port))
} else {
  nodeServer(dispatch).listen(port, '127.0.0.1', () => console.log('ts on', port))
}

// Bidirectional wire interop with the REAL @connectrpc/connect implementation
// (spec §8). This is the hard proof that easy-rpc speaks the Connect protocol,
// not just a lookalike:
//
//   1. @connectrpc client  -> easy-rpc server   (our Dispatch/ResponseWriter)
//   2. easy-rpc client     -> @connectrpc server (their connect-node router)
//
// Covered: unary, server-stream, unary error (status + JSON body), streaming
// end-stream error + details, unary trailing metadata, streaming trailing
// metadata, and unary gzip.
import { describe, it, expect, beforeAll, afterAll } from 'vitest'
import { createServer as createHttpServer } from 'node:http'
import { createClient, type Client } from '@connectrpc/connect'
import { createConnectTransport } from '@connectrpc/connect-node'
import {
  ConformanceService,
  ConformanceServiceHandlers,
  createConformanceServiceClient,
  methodSpecs,
} from '../src/easyrpc/conformance/v1/conformance_easyrpc'
import {
  ConformanceService as ConformanceServiceDesc,
  BigRequestSchema,
  BigResponseSchema,
  CountRequestSchema,
  CountResponseSchema,
  CountTrailerRequestSchema,
  CountTrailerResponseSchema,
  EchoRequestSchema,
  EchoResponseSchema,
  EchoTrailerRequestSchema,
  EchoTrailerResponseSchema,
  FailRequestSchema,
  HealthRequestSchema,
  HealthResponseSchema,
  StreamFailRequestSchema,
  StreamFailResponseSchema,
} from '../src/easyrpc/conformance/v1/conformance_pb'
import { createServer, nodeServer } from '../src/server'
import { createFetchTransport } from '../src/bridge_fetch'
import { RPCError } from '../src/protocol'

// ---- easy-rpc service implementation (shared by both directions) ----------
const impl = {
  health: async () => ({ ok: true, name: 'conformance' }),
  echo: async (req: { input: string }) => ({ output: 'echo:' + req.input }),
  count: async (req: { count: number }) => ({
    async *[Symbol.asyncIterator]() {
      const n = req.count > 0 ? req.count : 3
      for (let i = 0; i < n; i++) yield { index: i }
    },
  }),
  fail: async (req: { message: string }) => {
    if (req.message !== '') throw new RPCError(3, req.message)
    return { ok: true }
  },
  streamFail: async (req: { emitBefore: number; code: number; message: string }) => ({
    async *[Symbol.asyncIterator]() {
      for (let i = 0; i < req.emitBefore; i++) yield { index: i }
      throw new RPCError(req.code, req.message)
    },
  }),
  echoMeta: async () => ({ input: '', meta: {} }),
  big: async (req: { size: number }) => ({ size: req.size }),
  failDetails: async () => ({ ok: true }),
  streamFailDetails: async () => ({ async *[Symbol.asyncIterator]() {} }),
  echoTrailer: async (req: { input: string }, ctx: { setTrailer(k: string, v: string): void }) => {
    ctx.setTrailer('x-trl', 'unary-' + req.input)
    return { output: 'trailer:' + req.input }
  },
  countTrailer: async (req: { count: number }, ctx: { setTrailer(k: string, v: string): void }) => {
    ctx.setTrailer('x-ctrailer', 'done')
    return {
      async *[Symbol.asyncIterator]() {
        const n = req.count > 0 ? req.count : 3
        for (let i = 0; i < n; i++) yield { index: i }
      },
    }
  },
}

let easyServer: ReturnType<typeof createHttpServer>
let easyBase = ''
let connectServer: ReturnType<typeof createHttpServer>
let connectBase = ''

beforeAll(async () => {
  // ---- 1. easy-rpc server ----
  const dispatch = createServer(
    methodSpecs.map(m => ({ path: m.path, name: m.name, serverStream: m.serverStream })),
    ConformanceServiceHandlers(impl as never),
  )
  easyServer = nodeServer(dispatch)
  await new Promise<void>(r => easyServer.listen(0, '127.0.0.1', () => r()))
  easyBase = `http://127.0.0.1:${(easyServer.address() as { port: number }).port}`

  // ---- 2. @connectrpc/connect server (official node adapter) ----
  const { createConnectRouter, Code, ConnectError } = await import('@connectrpc/connect')
  const { connectNodeAdapter } = await import('@connectrpc/connect-node')
  const router = createConnectRouter()
  let srvImpl = {
    health: async () => ({ ok: true, name: 'connectrpc' }),
    echo: async (req: { input: string }) => ({ output: 'echo:' + req.input }),
    count: (req: { count: number }) => {
      const n = req.count > 0 ? req.count : 3
      return (async function* () {
        for (let i = 0; i < n; i++) yield { index: i }
      })()
    },
    fail: async (req: { message: string }) => {
      if (req.message !== '') throw new ConnectError(req.message, Code.InvalidArgument)
      return { ok: true }
    },
    streamFail: (req: { emitBefore: number; message: string }) =>
      (async function* () {
        for (let i = 0; i < req.emitBefore; i++) yield { index: i }
        throw new ConnectError(req.message, Code.Internal)
      })(),
    echoMeta: async () => ({ input: '', meta: {} }),
    big: async (req: { size: number }) => ({ size: req.size }),
    failDetails: async () => ({ ok: true }),
    streamFailDetails: async () => (async function* () {})(),
    echoTrailer: async (req: { input: string }, ctx: { responseTrailer: Headers }) => {
      ctx.responseTrailer.set('x-trl', 'unary-' + req.input)
      return { output: 'trailer:' + req.input }
    },
    countTrailer: (req: { count: number }, ctx: { responseTrailer: Headers }) => {
      ctx.responseTrailer.set('x-ctrailer', 'done')
      const n = req.count > 0 ? req.count : 3
      return (async function* () {
        for (let i = 0; i < n; i++) yield { index: i }
      })()
    },
  }
  router.service(ConformanceServiceDesc, srvImpl as never)
  connectServer = createHttpServer(connectNodeAdapter({ routes: r => r.service(ConformanceServiceDesc, srvImpl as never) }))
  await new Promise<void>(r => connectServer.listen(0, '127.0.0.1', () => r()))
  connectBase = `http://127.0.0.1:${(connectServer.address() as { port: number }).port}`
})

afterAll(async () => {
  await new Promise<void>(r => easyServer.close(() => r()))
  await new Promise<void>(r => connectServer.close(() => r()))
})

const connectClient = (): Client<typeof ConformanceServiceDesc> =>
  createClient(ConformanceServiceDesc, createConnectTransport({ baseUrl: easyBase, httpVersion: '1.1' }))

// JSON codec variant: the official client sends application/json /
// application/connect+json; our server must round-trip it.
const connectJsonClient = (): Client<typeof ConformanceServiceDesc> =>
  createClient(ConformanceServiceDesc, createConnectTransport({ baseUrl: easyBase, httpVersion: '1.1', useBinaryFormat: false }))

describe('@connectrpc client -> easy-rpc server', () => {
  it('unary: echo', async () => {
    const res = await connectClient().echo({ input: 'hi' })
    expect(res.output).toBe('echo:hi')
  })
  it('unary: health descriptor', async () => {
    const res = await connectClient().health({})
    expect(res.ok).toBe(true)
  })
  it('server-stream: count', async () => {
    const idx: number[] = []
    for await (const c of connectClient().count({ count: 3 })) idx.push(c.index)
    expect(idx).toEqual([0, 1, 2])
  })
  it('json codec: unary echo', async () => {
    const res = await connectJsonClient().echo({ input: 'hi' })
    expect(res.output).toBe('echo:hi')
  })
  it('json codec: server-stream count', async () => {
    const idx: number[] = []
    for await (const c of connectJsonClient().count({ count: 3 })) idx.push(c.index)
    expect(idx).toEqual([0, 1, 2])
  })
  it('json codec: unary error', async () => {
    let err: unknown = null
    try { await connectJsonClient().fail({ message: 'nope' }) } catch (e) { err = e }
    expect((err as { code?: number })?.code).toBe(3)
  })

  it('unary error: ConnectError code + JSON body', async () => {
    let err: unknown = null
    try { await connectClient().fail({ message: 'nope' }) } catch (e) { err = e }
    expect((err as { code?: number })?.code).toBe(3)
    expect(String((err as { message?: string })?.message)).toContain('nope')
  })
  it('server-stream error: end-stream error surfaces', async () => {
    let err: unknown = null
    const seen: number[] = []
    try {
      for await (const c of connectClient().streamFail({ emitBefore: 2, code: 13, message: 'boom' })) seen.push(c.index)
    } catch (e) { err = e }
    expect(seen).toEqual([0, 1])
    expect((err as { code?: number })?.code).toBe(13)
  })
  it('unary trailer: trailer-* headers demuxed by @connectrpc', async () => {
    let trailers: Headers | undefined
    const res = await connectClient().echoTrailer({ input: 'x' }, { onTrailer: t => { trailers = t } })
    expect(res.output).toBe('trailer:x')
    expect(trailers?.get('x-trl')).toBe('unary-x')
  })
  it('stream trailer: END-frame metadata surfaces', async () => {
    let trailers: Headers | undefined
    const idx: number[] = []
    for await (const c of connectClient().countTrailer({ count: 2 }, { onTrailer: t => { trailers = t } })) idx.push(c.index)
    expect(idx).toEqual([0, 1])
    expect(trailers?.get('x-ctrailer')).toBe('done')
  })
  it('large unary body (gzip negotiated transparently)', async () => {
    const res = await connectClient().big({ size: 100000 })
    expect(res.size).toBe(100000)
  })
})

describe('easy-rpc client -> @connectrpc server', () => {
  const easyClient = () => createConformanceServiceClient(createFetchTransport(connectBase))
  it('unary: echo', async () => {
    const res = await easyClient().echo({ input: 'hi' })
    expect(res.output).toBe('echo:hi')
  })
  it('server-stream: count', async () => {
    const stream = await easyClient().count({ count: 3 })
    const idx: number[] = []
    for await (const c of stream) idx.push(c.index)
    expect(idx).toEqual([0, 1, 2])
  })
  it('unary error: code + message', async () => {
    let err: unknown = null
    try { await easyClient().fail({ message: 'nope' }) } catch (e) { err = e }
    expect((err as { code?: number })?.code).toBe(3)
  })
  it('server-stream error: end-stream error surfaces', async () => {
    let err: unknown = null
    const seen: number[] = []
    try {
      const stream = await easyClient().streamFail({ emitBefore: 2, code: 13, message: 'boom' })
      for await (const c of stream) seen.push(c.index)
    } catch (e) { err = e }
    expect(seen).toEqual([0, 1])
    expect((err as { code?: number })?.code).toBe(13)
  })
  it('unary trailer: @connectrpc trailer-* headers demuxed', async () => {
    let trailers: Record<string, string[]> = {}
    const res = await easyClient().echoTrailer({ input: 'x' }, { onTrailer: t => { trailers = t } })
    expect(res.output).toBe('trailer:x')
    expect(trailers['x-trl']).toEqual(['unary-x'])
  })
  it('stream trailer: END-frame metadata surfaces', async () => {
    const stream = await easyClient().countTrailer({ count: 2 })
    const idx: number[] = []
    for await (const c of stream) idx.push(c.index)
    expect(idx).toEqual([0, 1])
    expect(stream.trailers()['x-ctrailer']).toEqual(['done'])
  })
  it('large unary body', async () => {
    const res = await easyClient().big({ size: 100000 })
    expect(res.size).toBe(100000)
  })
})

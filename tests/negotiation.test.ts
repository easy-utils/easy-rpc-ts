import { describe, it, expect, afterAll, beforeAll } from 'vitest'
import { create } from '@bufbuild/protobuf'
import {
  createNodeTransport, createHttp1Transport, createMetadataTransport, createFetchTransport,
} from '../src/index'
import { createConformanceServiceClient } from '../src/easyrpc/conformance/v1/conformance_easyrpc'
import {
  EchoRequestSchema, CountRequestSchema,
} from '../src/easyrpc/conformance/v1/conformance_pb'
import { spawn } from 'node:child_process'

let child: ReturnType<typeof spawn>
let base = 'http://127.0.0.1:24001'
const client = (transport: any) => createConformanceServiceClient(transport)

beforeAll(async () => {
  child = spawn('/tmp/opencode/matrix/srv-go', [], { env: { ...process.env, PORT: '24001' }, stdio: 'ignore' })
  await new Promise(r => setTimeout(r, 1500))
})
afterAll(() => { child?.kill('SIGKILL') })

describe('node http2 (server-to-server RPC) -> Go h1+h2c', () => {
  it('echo unary proto over h2c', async () => {
    // base is provided via transport options (server-to-server RPC)
    const tr = createNodeTransport({ protocol: 'h2', base })
    const res = await client(tr).echo(create(EchoRequestSchema, { input: 'hi' }))
    expect(res.output).toBe('echo:hi')
  })
  it('count server-stream over h2c', async () => {
    const tr = createNodeTransport({ protocol: 'h2', base })
    const iter = await client(tr).count(create(CountRequestSchema, { count: 3 }))
    const idx: number[] = []
    for await (const chunk of iter) idx.push(chunk.index)
    expect(idx).toEqual([0, 1, 2])
  })
})

describe('http1 fallback bridge (h1)', () => {
  it('echo unary proto via explicit http1', async () => {
    const tr = createHttp1Transport(undefined, base)
    const res = await client(tr).echo(create(EchoRequestSchema, { input: 'hi' }))
    expect(res.output).toBe('echo:hi')
  })
  it('count server-stream via explicit http1 (incremental, END frame)', async () => {
    // Regression: the h1 bridge used to buffer the whole response through the
    // unary path (a competing 'data' consumer), so the stream yielded zero
    // frames and failed with "stream ended without END frame".
    const tr = createHttp1Transport(undefined, base)
    const iter = await client(tr).count(create(CountRequestSchema, { count: 3 }))
    const idx: number[] = []
    for await (const chunk of iter) idx.push(chunk.index)
    expect(idx).toEqual([0, 1, 2])
  })
  it('count server-stream via auto mode against an h1-only peer', async () => {
    // 'auto' probes the h2 handshake, sees the h1-only Go server, and must
    // fall back to the h1 bridge for streams too.
    const tr = createNodeTransport({ protocol: 'auto', base })
    const iter = await client(tr).count(create(CountRequestSchema, { count: 3 }))
    const idx: number[] = []
    for await (const chunk of iter) idx.push(chunk.index)
    expect(idx).toEqual([0, 1, 2])
  })
})

describe('metadata / auth via headers', () => {
  it('attaches authorization header via createMetadataTransport', async () => {
    const auth = createMetadataTransport({ authorization: ['Bearer trust-me'] }, createNodeTransport({ protocol: 'h2', base }))
    const res = await client(auth).echo(create(EchoRequestSchema, { input: 'hi' }))
    expect(res.output).toBe('echo:hi')
  })
  it('per-call metadata via client method arg', async () => {
    const tr = createNodeTransport({ protocol: 'h2', base })
    const res = await client(tr).echo(create(EchoRequestSchema, { input: 'hi' }), 'proto', { authorization: ['Bearer x'] })
    expect(res.output).toBe('echo:hi')
  })
})

describe('fetch bridge (web) -> Go', () => {
  it('echo unary proto via fetch', async () => {
    const res = await client(createFetchTransport(base)).echo(create(EchoRequestSchema, { input: 'hi' }))
    expect(res.output).toBe('echo:hi')
  })
})

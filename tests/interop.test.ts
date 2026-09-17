import { describe, it, expect } from 'vitest'
import { createFetchTransport, createMetadataTransport } from '../src'
import { createConformanceServiceClient } from '../src/easyrpc/conformance/v1/conformance_easyrpc'
import { create } from '@bufbuild/protobuf'
import { EchoRequestSchema, CountRequestSchema, StreamFailRequestSchema, EchoMetaRequestSchema } from '../src/easyrpc/conformance/v1/conformance_pb'

const base = process.env.EASY_RPC_BASE ?? 'http://127.0.0.1:18888'
const client = () => createConformanceServiceClient(createFetchTransport(base))
describe('TS -> interop', () => {
  it('echo unary proto', async () => {
    const res = await client().echo(create(EchoRequestSchema, { input: 'hi' }))
    expect(res.output).toBe('echo:hi')
  })
  it('echo unary json', async () => {
    const res = await createConformanceServiceClient(createFetchTransport(base), 'json').echo(create(EchoRequestSchema, { input: 'hi' }))
    expect(res.output).toBe('echo:hi')
  })
  it('count server-stream', async () => {
    const iter = await client().count(create(CountRequestSchema, { count: 3 }))
    const idx: number[] = []
    for await (const chunk of iter) idx.push(chunk.index)
    expect(idx).toEqual([0,1,2])
  })
})

describe('TS -> interop (phase C cases)', () => {
  it('stream-fail: data frames then end-stream error surfaces', async () => {
    const iter = await client().streamFail(create(StreamFailRequestSchema, { emitBefore: 2, code: 13, message: 'boom' }))
    const seen: number[] = []
    let err: unknown = null
    try {
      for await (const c of iter) seen.push(c.index)
    } catch (e) { err = e }
    expect(seen).toEqual([0, 1])
    expect((err as { code?: number } | null)?.code).toBe(13)
  })
  it('echo-meta: request metadata is visible server-side', async () => {
    const c = createMetadataTransport({ 'x-test': ['abc'] }, createFetchTransport(base))
    const res = await createConformanceServiceClient(c).echoMeta(create(EchoMetaRequestSchema, { input: 'hi' }))
    expect(res.input).toBe('hi')
    expect(res.meta['x-test']).toBe('abc')
  })
})

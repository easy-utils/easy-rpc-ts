import { describe, it, expect } from 'vitest'
import { createFetchTransport, createMetadataTransport } from '../src'
import { createConformanceServiceClient } from '../src/easyrpc/conformance/v1/conformance_easyrpc'
import { create } from '@bufbuild/protobuf'
import { EchoRequestSchema, CountRequestSchema, StreamFailRequestSchema, EchoMetaRequestSchema, FailDetailsRequestSchema, StreamFailDetailsRequestSchema } from '../src/easyrpc/conformance/v1/conformance_pb'

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

describe('TS -> interop (error details, spec §4.1)', () => {
  it('failDetails: unary error carries structured details', async () => {
    let err: unknown = null
    try {
      await client().failDetails(create(FailDetailsRequestSchema, {
        code: 8, message: 'limited',
        detailType: 'type.googleapis.com/google.rpc.RetryInfo', detailText: 'retry:5s',
      }))
    } catch (e) { err = e }
    const re = err as { code?: number; message?: string; details?: { type: string; value: Uint8Array }[] } | null
    expect(re?.code).toBe(8)
    expect(re?.message).toBe('limited')
    expect(re?.details?.length).toBe(1)
    expect(re?.details?.[0]?.type).toBe('type.googleapis.com/google.rpc.RetryInfo')
    expect(new TextDecoder().decode(re?.details?.[0]?.value ?? new Uint8Array())).toBe('retry:5s')
  })

  it('streamFailDetails: frames then end-stream error with details', async () => {
    const iter = await client().streamFailDetails(create(StreamFailDetailsRequestSchema, {
      emitBefore: 2, code: 13, message: 'boom', detailType: 't/stream', detailText: 'sd',
    }))
    const seen: number[] = []
    let err: unknown = null
    try {
      for await (const c of iter) seen.push(c.index)
    } catch (e) { err = e }
    expect(seen).toEqual([0, 1])
    const re = err as { code?: number; details?: { type: string; value: Uint8Array }[] } | null
    expect(re?.code).toBe(13)
    expect(re?.details?.[0]?.type).toBe('t/stream')
    expect(new TextDecoder().decode(re?.details?.[0]?.value ?? new Uint8Array())).toBe('sd')
  })
})

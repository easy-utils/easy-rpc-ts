import { describe, it, expect } from 'vitest'
import { createFetchTransport } from '../src'
import { createConformanceServiceClient } from '../src/easyrpc/conformance/v1/conformance_easyrpc'
import { create } from '@bufbuild/protobuf'
import { EchoRequestSchema, EchoResponseSchema, CountRequestSchema } from '../src/easyrpc/conformance/v1/conformance_pb'

const base = 'http://127.0.0.1:18888'

describe('TS -> Go interop', () => {
  it('echo unary proto', async () => {
    const c = createConformanceServiceClient(createFetchTransport(base))
    const res = await c.echo(create(EchoRequestSchema, { input: 'hi' }))
    expect(res.output).toBe('echo:hi')
  })
  it('echo unary json', async () => {
    const c = createConformanceServiceClient(createFetchTransport(base), 'json')
    const res = await c.echo(create(EchoRequestSchema, { input: 'hi' }))
    expect(res.output).toBe('echo:hi')
  })
  it('count server-stream', async () => {
    const c = createConformanceServiceClient(createFetchTransport(base))
    const iter = await c.count(create(CountRequestSchema, { count: 3 }))
    const idx: number[] = []
    for await (const chunk of iter) idx.push(chunk.index)
    expect(idx).toEqual([0,1,2])
  })
})

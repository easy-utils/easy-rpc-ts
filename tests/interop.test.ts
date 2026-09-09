import { describe, it, expect } from 'vitest'
import { createFetchTransport } from '../src'
import { createConformanceServiceClient } from '../src/easyrpc/conformance/v1/conformance_easyrpc'
import { create } from '@bufbuild/protobuf'
import { EchoRequestSchema, CountRequestSchema } from '../src/easyrpc/conformance/v1/conformance_pb'

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

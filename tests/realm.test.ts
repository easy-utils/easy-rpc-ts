import { describe, it, expect } from 'vitest'
import { create } from '@bufbuild/protobuf'
import { createDefaultTransport } from '../src'
import { createConformanceServiceClient } from '../src/easyrpc/conformance/v1/conformance_easyrpc'
import { EchoRequestSchema, CountRequestSchema } from '../src/easyrpc/conformance/v1/conformance_pb'

const base = process.env.EASY_RPC_BASE ?? 'http://127.0.0.1:18888'
describe('realm default transport', () => {
  it('node realm echo+count', async () => {
    const c = createConformanceServiceClient(createDefaultTransport(base, 'node'))
    const r = await c.echo(create(EchoRequestSchema, { input: 'hi' }))
    expect(r.output).toBe('echo:hi')
    const it = await c.count(create(CountRequestSchema, { count: 3 }))
    const idx: number[] = []
    for await (const x of it) idx.push(x.index)
    expect(idx).toEqual([0,1,2])
  })
})

// Cross-language interop: TS generated client -> Go conformance server.
// Requires the Go server running on 127.0.0.1:18888 (run ./cmd/conformance-server).
import { describe, it, expect } from 'vitest'
import { createFetchTransport } from '../src'
import { createConformanceServiceClient } from '../src/easyrpc/conformance/v1/conformance_easyrpc'
import { create } from '@bufbuild/protobuf'
import { EchoRequestSchema, CountRequestSchema } from '../src/easyrpc/conformance/v1/conformance_pb'

const base = 'http://127.0.0.1:18888'

describe('TS -> Go interop', () => {
  it('echo unary (REST path /v1/echo)', async () => {
    const c = createConformanceServiceClient(createFetchTransport(base, fetch))
    const res = await c.echo(create(EchoRequestSchema, { input: 'hi' }))
    expect(res.output).toBe('echo:hi')
  })

  it('count server-stream (REST path /v1/count)', async () => {
    const c = createConformanceServiceClient(createFetchTransport(base, fetch))
    const iter = await c.count(create(CountRequestSchema, { count: 3 }))
    const idx: number[] = []
    for await (const chunk of iter) {
      idx.push(chunk.index)
    }
    expect(idx).toEqual([0, 1, 2])
  })
})

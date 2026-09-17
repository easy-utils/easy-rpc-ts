// Composition-root injection parity: connect({transport}) wraps a custom
// adapter with the SAME built-in interceptors as a mode-picked one.
import { describe, it, expect } from 'vitest'
import { connect } from '../src/connect'
import type { Transport, Request, Response, Stream } from '../src/protocol'

function fakeTransport(seen: { headers: Record<string, string[]>[] }): Transport {
  return {
    async send(req: Request): Promise<Response> {
      seen.headers.push(req.headers)
      return { status: 200, headers: {}, body: new Uint8Array(0) }
    },
    async openStream(): Promise<Stream> { throw new Error('unused') },
  }
}

describe('connect() custom transport injection', () => {
  it('uses the injected adapter and still applies metadata + deadline', async () => {
    const seen: { headers: Record<string, string[]>[] } = { headers: [] }
    const t = connect({ baseUrl: 'http://x', token: 'sekret', timeoutMs: 1500, transport: fakeTransport(seen) })
    await t.send({ url: '/x', method: 'POST', headers: {}, body: new Uint8Array(0) })
    const h = seen.headers[0]!
    expect(h['authorization']).toEqual(['Bearer sekret'])
    expect(h['connect-timeout-ms']).toEqual(['1500'])
  })

  it('no token/timeout => no extra headers, adapter untouched', async () => {
    const seen: { headers: Record<string, string[]>[] } = { headers: [] }
    const t = connect({ baseUrl: 'http://x', transport: fakeTransport(seen) })
    await t.send({ url: '/x', method: 'POST', headers: { a: ['b'] }, body: new Uint8Array(0) })
    expect(seen.headers[0]).toEqual({ a: ['b'] })
  })
})

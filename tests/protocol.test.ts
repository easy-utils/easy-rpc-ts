import { describe, it, expect } from 'vitest'
import { frame, readFrames, httpStatus, RPCError } from '../src/protocol'

describe('framing', () => {
  it('round-trips a frame', async () => {
    const f = frame(new Uint8Array([1, 2, 3]), false)
    const it = readFrames((async function* () { yield f })())
    const first = await it.next()
    expect(first.value.end).toBe(false)
    expect([...first.value.payload]).toEqual([1, 2, 3])
  })

  it('detects end-stream flag', async () => {
    const f = frame(new Uint8Array([9]), true)
    const it = readFrames((async function* () { yield f })())
    const first = await it.next()
    expect(first.value.end).toBe(true)
  })
})

describe('status mapping', () => {
  it('maps connect codes to http', () => {
    expect(httpStatus(3)).toBe(400)
    expect(httpStatus(5)).toBe(404)
    expect(httpStatus(16)).toBe(401)
  })
})

describe('interceptors', () => {
  it('metadata + timeout interceptors compose', async () => {
    const { createInterceptorTransport, metadataInterceptor, timeoutInterceptor } = await import('../src/protocol')
    const seen: Record<string, string[]> = {}
    const fake = {
      async send(req: { headers: Record<string, string[]> }) {
        Object.assign(seen, req.headers)
        return { status: 200, headers: {}, body: new Uint8Array(0) }
      },
      async openStream() { throw new Error('unused') },
    }
    const t = createInterceptorTransport(
      [metadataInterceptor({ 'x-test': ['abc'] }), timeoutInterceptor(250)],
      fake as never,
    )
    await t.send({ url: '/x', method: 'POST', headers: {}, body: undefined })
    expect(seen['x-test']).toEqual(['abc'])
    expect(seen['connect-timeout-ms']).toEqual(['250'])
  })
})

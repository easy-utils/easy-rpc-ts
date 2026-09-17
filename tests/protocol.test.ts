import { describe, it, expect } from 'vitest'
import http from 'node:http'
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

describe('max message + protocol version', () => {
  it('frame rejects an over-limit payload (ResourceExhausted=8)', async () => {
    const { frame, DEFAULT_MAX_MESSAGE_BYTES } = await import('../src/protocol')
    expect(() => frame(new Uint8Array(DEFAULT_MAX_MESSAGE_BYTES + 1))).toThrow()
    const ok = frame(new Uint8Array(16))
    expect(ok.length).toBe(21)
  })

  it('server rejects an unsupported protocol version', async () => {
    const { createServer, nodeServer } = await import('../src/server')
    const { readFrames, decodeErrorJson } = await import('../src/protocol')
    const dispatch = createServer(
      [{ path: '/t.Echo', name: 'Echo', serverStream: false }],
      { unary: { Echo: async () => new Uint8Array(1) }, stream: {} } as never,
    )
    const server = nodeServer(dispatch)
    await new Promise<void>(r => server.listen(0, () => r()))
    const port = (server.address() as { port: number }).port
    const status = await new Promise<number>(resolve => {
      const req = http.request(
        { host: '127.0.0.1', port, path: '/t.Echo', method: 'POST',
          headers: { 'content-type': 'application/proto', 'connect-protocol-version': '999' } },
        res => { res.resume(); resolve(res.statusCode ?? 0) },
      )
      req.end()
    })
    server.close()
    expect(status).toBe(501) // unimplemented
  })
})

describe('local deadline cancels the adapter', () => {
  it('timeout interceptor aborts the request (no adapter support needed)', async () => {
    const { createInterceptorTransport, timeoutInterceptor, RPCError } = await import('../src/protocol')
    // An adapter that never resolves the request, but honours the signal.
    const fake = {
      async send(req: { signal?: AbortSignal }) {
        return await new Promise((_res, rej) => {
          req.signal?.addEventListener('abort', () => rej(req.signal!.reason))
        })
      },
      async openStream() { throw new Error('unused') },
    }
    const t = createInterceptorTransport([timeoutInterceptor(50)], fake as never)
    const started = Date.now()
    let err: unknown
    try {
      await t.send({ url: '/x', method: 'POST', headers: {}, body: undefined })
    } catch (e) { err = e }
    expect(Date.now() - started).toBeLessThan(500)
    expect(err).toBeInstanceOf(RPCError)
    expect((err as { code: number }).code).toBe(4)
  })
})

describe('connect composition root', () => {
  it('installs metadata + deadline and is adapter-agnostic', async () => {
    const { connect } = await import('../src/connect')
    const { RPCError } = await import('../src/protocol')
    // node mode with an unroutable port: the deadline must fire locally.
    const t = connect({ baseUrl: 'http://127.0.0.1:1', token: 'abc', mode: 'h1', timeoutMs: 80 })
    const started = Date.now()
    let err: unknown
    try {
      await t.send({ url: '/x', method: 'POST', headers: {}, body: new Uint8Array(0) })
    } catch (e) { err = e }
    expect(Date.now() - started).toBeLessThan(2000)
    expect(err).toBeDefined()
  })
})

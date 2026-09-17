// easy-rpc TS: server-stream is TRULY incremental. This test would fail against
// any implementation that buffers the whole stream before responding: it reads
// the raw socket and asserts the first frame arrives long before the stream
// ends.
import { describe, it, expect } from 'vitest'
import http from 'node:http'
import { createServer, nodeServer } from '../src/server'
import { readFrames } from '../src/protocol'

function delay(ms: number): Promise<void> {
  return new Promise(r => setTimeout(r, ms))
}

describe('server-stream incrementality', () => {
  it('flushes each frame as it is produced', async () => {
    const handlers = {
      unary: {},
      stream: {
        Slow: async (_input: Uint8Array, _kind: string, emit: (d: Uint8Array, e: boolean) => Promise<void>) => {
          for (let i = 0; i < 3; i++) {
            await emit(new Uint8Array([i]), false)
            await delay(250)
          }
          await emit(new Uint8Array(0), true)
        },
      },
    }
    const dispatch = createServer(
      [{ path: '/t.Slow', name: 'Slow', serverStream: true }],
      handlers as never,
    )
    const server = nodeServer(dispatch)
    await new Promise<void>(r => server.listen(0, () => r()))
    const port = (server.address() as { port: number }).port

    const arrivals: number[] = []
    const start = Date.now()
    const payloads: number[] = []
    await new Promise<void>((resolve, reject) => {
      const req = http.request(
        { host: '127.0.0.1', port, path: '/t.Slow', method: 'POST', headers: { 'content-type': 'application/connect+proto' } },
        res => {
          const source = (async function* () {
            for await (const c of res) yield c as Uint8Array
          })()
          void (async () => {
            for await (const f of readFrames(source)) {
              if (f.end) break
              arrivals.push(Date.now() - start)
              payloads.push(f.payload[0] ?? -1)
            }
            resolve()
          })().catch(reject)
        },
      )
      req.on('error', reject)
      req.end()
    })
    server.close()

    expect(payloads).toEqual([0, 1, 2])
    // First frame must arrive well before the stream ends (~750ms of delays).
    expect(arrivals[0]).toBeLessThan(200)
    expect(arrivals[arrivals.length - 1]).toBeGreaterThan(400)
  })

  it('streams without buffering: a never-yielding stream keeps the response open', async () => {
    // If the server buffered, the response would either hang forever or emit
    // nothing; we assert the first frame is visible while the handler is still
    // running (it is stuck awaiting, having emitted once).
    let release: () => void = () => {}
    const gate = new Promise<void>(r => (release = r))
    const handlers = {
      unary: {},
      stream: {
        Held: async (_i: Uint8Array, _k: string, emit: (d: Uint8Array, e: boolean) => Promise<void>) => {
          await emit(new Uint8Array([7]), false)
          await gate
          await emit(new Uint8Array(0), true)
        },
      },
    }
    const dispatch = createServer([{ path: '/t.Held', name: 'Held', serverStream: true }], handlers as never)
    const server = nodeServer(dispatch)
    await new Promise<void>(r => server.listen(0, () => r()))
    const port = (server.address() as { port: number }).port

    const firstAt = await new Promise<number>((resolve, reject) => {
      const start = Date.now()
      const req = http.request(
        { host: '127.0.0.1', port, path: '/t.Held', method: 'POST', headers: { 'content-type': 'application/connect+proto' } },
        res => {
          res.once('data', () => resolve(Date.now() - start))
          res.resume()
        },
      )
      req.on('error', reject)
      req.end()
    })
    release()
    server.close()
    expect(firstAt).toBeLessThan(200)
  })
})

describe('end-stream error (Connect JSON)', () => {
  it('encodes/decodes the Connect error shape', async () => {
    const { encodeEndStream, decodeEndStream } = await import('../src/protocol')
    const bytes = encodeEndStream(16, 'missing bearer token')
    const decoded = decodeEndStream(bytes)
    expect(decoded).toEqual({ code: 16, message: 'missing bearer token' })
    expect(decodeEndStream(new Uint8Array(0))).toBeNull()
  })

  it('throws on an error END frame (client surfaces it)', async () => {
    const handlers = {
      unary: {},
      stream: {
        Boom: async (_i: Uint8Array, _k: string, emit: (d: Uint8Array, e: boolean) => Promise<void>) => {
          await emit(new Uint8Array([1]), false)
          throw new Error('mid-stream failure')
        },
      },
    }
    const dispatch = createServer([{ path: '/t.Boom', name: 'Boom', serverStream: true }], handlers as never)
    const server = nodeServer(dispatch)
    await new Promise<void>(r => server.listen(0, () => r()))
    const port = (server.address() as { port: number }).port
    const payloads: number[] = []
    let caught: unknown = null
    try {
      await new Promise<void>((resolve, reject) => {
        const req = http.request(
          { host: '127.0.0.1', port, path: '/t.Boom', method: 'POST', headers: { 'content-type': 'application/connect+proto' } },
          res => {
            const source = (async function* () { for await (const c of res) yield c as Uint8Array })()
            void (async () => {
              for await (const f of readFrames(source)) {
                if (f.end) {
                  const { decodeEndStream } = await import('../src/protocol')
                  const e = decodeEndStream(f.payload)
                  if (e) { reject(new (await import('../src/protocol')).RPCError(e.code, e.message)); return }
                  resolve(); return
                }
                payloads.push(f.payload[0] ?? -1)
              }
              resolve()
            })().catch(reject)
          },
        )
        req.on('error', reject)
        req.end()
      })
    } catch (e) { caught = e }
    server.close()
    expect(payloads).toEqual([1])
    expect(caught).toBeInstanceOf((await import('../src/protocol')).RPCError)
    expect((caught as { code: number }).code).toBe(13)
  })
})

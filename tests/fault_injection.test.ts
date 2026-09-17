// Fault injection (spec §4.2 M8/M10 end-to-end): a mock server emits
// malformed stream bodies; the client MUST surface errors, never partial
// payloads, never raw compressed bytes. Mirrored in Go/Rust/Python (socket
// level) and Kotlin/Dart/C#/Swift (protocol level).
import { describe, it, expect, afterEach } from 'vitest'
import http from 'node:http'
import { gzipSync } from 'node:zlib'
import { createFetchTransport } from '../src'
import { frame, RPCError, type Bytes, type Stream } from '../src/protocol'

const servers: http.Server[] = []

function serve(body: Buffer, opts: { status?: number } = {}): Promise<string> {
  return new Promise((resolve) => {
    const srv = http.createServer((_req, res) => {
      res.writeHead(opts.status ?? 200, { 'content-type': 'application/connect+proto' })
      res.end(body)
    })
    srv.listen(0, '127.0.0.1', () => resolve(`http://127.0.0.1:${(srv.address() as { port: number }).port}`))
    servers.push(srv)
  })
}

const msg = (i: number): Bytes => new TextEncoder().encode(JSON.stringify({ index: i }))

async function open(t: { openStream(req: unknown): Promise<Stream> }, base: string): Promise<Stream> {
  return t.openStream({ url: `${base}/x`, method: 'POST', headers: { 'content-type': ['application/connect+proto'] } })
}

const collect = async (st: Stream): Promise<number[]> => {
  const out: number[] = []
  for await (const p of st) out.push(JSON.parse(new TextDecoder().decode(p)).index)
  return out
}

afterEach(() => { for (const s of servers.splice(0)) s.close() })

describe('fault injection: malformed stream bodies', () => {
  it('F1 mid-frame truncation at EOF errors (M8)', async () => {
    const base = await serve(Buffer.concat([frame(msg(0)), frame(msg(1)).subarray(0, 6)]))
    await expect(collect(await open(createFetchTransport(), base))).rejects.toBeInstanceOf(RPCError)
  })

  it('F2 abrupt close at a frame boundary (no END frame) errors', async () => {
    const base = await serve(Buffer.concat([frame(msg(0)), frame(msg(1))]))
    const st = await open(createFetchTransport(), base)
    let err: unknown = null
    const got: number[] = []
    try { for await (const p of st) got.push(JSON.parse(new TextDecoder().decode(p)).index) } catch (e) { err = e }
    expect(got).toEqual([0, 1])
    expect(err).toBeInstanceOf(RPCError)
    expect((err as RPCError).code).toBe(13)
  })

  it('F3 garbage END payload is a clean end, no throw (M2)', async () => {
    const base = await serve(Buffer.concat([frame(msg(0)), frame(new Uint8Array([0xff, 0xfe, 0x42]), true)]))
    expect(await collect(await open(createFetchTransport(), base))).toEqual([0])
  })

  it('F4 corrupt gzip payload errors, never yields raw bytes (M10)', async () => {
    const corrupt = Buffer.from([0x1f, 0x8b, 0x08, 0x00, 0xde, 0xad, 0xbe, 0xef])
    const header = Buffer.alloc(5)
    header[0] = 0x01
    header.writeUInt32BE(corrupt.length, 1)
    const base = await serve(Buffer.concat([header, corrupt, frame(new Uint8Array(0), true)]))
    await expect(collect(await open(createFetchTransport(), base))).rejects.toThrow()
  })

  it('F5 frames split across arbitrary chunk boundaries reassemble', async () => {
    const body = Buffer.concat([frame(msg(0)), frame(msg(1)), frame(new Uint8Array(0), true)])
    const srv = http.createServer((_req, res) => {
      res.writeHead(200, { 'content-type': 'application/connect+proto' })
      let i = 0
      const timer = setInterval(() => {
        if (i >= body.length) { clearInterval(timer); res.end(); return }
        res.write(body.subarray(i, i + 3))
        i += 3
      }, 1)
    })
    const base = await new Promise<string>((resolve) => {
      srv.listen(0, '127.0.0.1', () => resolve(`http://127.0.0.1:${(srv.address() as { port: number }).port}`))
    })
    servers.push(srv)
    expect(await collect(await open(createFetchTransport(), base))).toEqual([0, 1])
  })

  it('F6 valid gzip frame still decodes (M10 positive)', async () => {
    const gz = gzipSync(Buffer.from(JSON.stringify({ index: 7 })))
    const header = Buffer.alloc(5)
    header[0] = 0x01
    header.writeUInt32BE(gz.length, 1)
    const base = await serve(Buffer.concat([header, gz, frame(new Uint8Array(0), true)]))
    expect(await collect(await open(createFetchTransport(), base))).toEqual([7])
  })
})

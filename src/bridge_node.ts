// node bridge using the built-in http2 module. It implements the Transport
// interface for server-to-server RPC:
//   - cleartext (http://)  -> h2c prior-knowledge
//   - with ALPN or explicit http2 -> h2
//   - falls back to HTTP/1.1 when the endpoint only speaks h1.
// Bridge-only: the protocol logic (frames, headers, content-type) stays in core.
import type { Request, Response, Stream, Transport, Headers as HeadersT } from './protocol.js'
import { readFrames, RPCError, streamPayloads, decodeErrorJson } from './protocol.js'
import http2 from 'node:http2'
import http from 'node:http'

/** Options controlling h2/h2c/h1 negotiation for the node bridge. */
export interface NodeTransportOptions {
  /** Preferred protocol. 'h2' uses http2.connect (h2c for http://, h2 for https://).
   *  'auto' also falls back to http/1.1 if h2 handshake/connect fails. */
  protocol?: 'h2' | 'h2c' | 'h1' | 'auto'
  /** Agent for http/1.1 fallback (optional). */
  httpAgent?: http.Agent
  /** Base URL (e.g. http://localhost:8080) to join relative RPC paths. */
  base?: string
}

function join(base: string | undefined, u: string): string {
  if (u.startsWith('http://') || u.startsWith('https://')) return u
  return (base ? base.replace(/\/+$/, '') : '') + (u.startsWith('/') ? u : '/' + u)
}

/** Build a Node http2-based Transport. */
export function createNodeTransport(opts: NodeTransportOptions = {}): Transport {
  const protocol = opts.protocol ?? 'h2'

  // HTTP/2 cleartext (h2c, http:// URL) or secure (h2, https:// URL).
  function connect(url: string): http2.ClientHttp2Session {
    const target = join(opts.base, url)
    const u = new URL(target.startsWith('http') ? target : 'http://' + target)
    const session = http2.connect(u.protocol === 'https:' ? u.href : `http://${u.host}`)
    session.on('error', () => { /* surfaced via stream error */ })
    return session
  }

  async function doSend(req: Request): Promise<Response> {
    const session = connect(req.url)
    const stream = session.request(headersFor(req, false, opts.base))
    const chunks: Uint8Array[] = []
    const status = await new Promise<number>((resolve, reject) => {
      stream.on('response', (headers: http2.IncomingHttpHeaders) => {
        if (Number(headers[':status'] ?? 200) >= 300) {
          reject(new RPCError(13, 'http error'))
        }
      })
      stream.on('data', (c: Uint8Array) => chunks.push(c))
      stream.on('end', () => resolve(200))
      stream.on('error', reject)
      stream.end(req.body ?? new Uint8Array(0))
    })
    session.close()
    return { status, headers: {}, body: concatAll(chunks) }
  }

  function doOpenStream(req: Request): Promise<Stream> {
    const session = connect(req.url)
    const stream = session.request(headersFor(req, true, opts.base))
    stream.end(req.body ?? new Uint8Array(0))
    const chunks = (async function* () {
      for await (const c of stream) yield c as Uint8Array
    })()
    const framed = readFrames(chunks)
    return Promise.resolve({
      async *[Symbol.asyncIterator]() {
        yield* streamPayloads(framed)
      },
      cancel() {
        try { stream.close(); session.close() } catch { /* noop */ }
      },
    })
  }

  // HTTP/1.1 fallback (used when 'auto' or explicit 'h1').
  const h1Transport: Transport = createHttp1Transport(opts.httpAgent)

  return {
    async send(req: Request): Promise<Response> {
      if (protocol === 'h1') return h1Transport.send(req)
      try {
        return await doSend(req)
      } catch (e) {
        if (protocol === 'auto') return h1Transport.send(req)
        throw e
      }
    },
    async openStream(req: Request): Promise<Stream> {
      if (protocol === 'h1') return h1Transport.openStream(req)
      try {
        return await doOpenStream(req)
      } catch (e) {
        if (protocol === 'auto') return h1Transport.openStream(req)
        throw e
      }
    },
  }
}

/** HTTP/1.1 fallback bridge (plain node:http). Used for 'h1' and 'auto'. */
export function createHttp1Transport(agent?: http.Agent, base = ''): Transport {
  return {
    async send(req: Request): Promise<Response> {
      const res = await httpRequest(req, agent, base)
      const headers: HeadersT = {}
      for (const [k, v] of Object.entries(res.headers)) headers[k] = [v]
      return {
        status: res.status,
        headers,
        body: res.body,
        error: decodeErrorJson(res.status, headers, res.body) ?? undefined,
      }
    },
    async openStream(req: Request): Promise<Stream> {
      const res = await httpRequest(req, agent, base)
      const source = (async function* () {
        for await (const c of res.raw) yield c
      })()
      const framed = readFrames(source)
      return {
        async *[Symbol.asyncIterator]() {
          yield* streamPayloads(framed)
        },
        cancel() { /* http1 closes on end */ },
      }
    },
  }
}

function httpRequest(req: Request, agent?: http.Agent, base = ''): Promise<{ status: number; headers: Record<string, string>; body: Uint8Array; raw: AsyncIterable<Uint8Array> }> {
  const u = new URL(join(base, req.url))
  return new Promise((resolve, reject) => {
    const preq = http.request({
      hostname: u.hostname,
      port: u.port || (u.protocol === 'https:' ? 443 : 80),
      path: u.pathname + u.search,
      method: req.method,
      headers: headersToRecord(req.headers),
      agent,
    }, (resp) => {
      const chunks: Uint8Array[] = []
      const raw: AsyncIterable<Uint8Array> = (async function* () {
        for await (const c of resp) yield c as Uint8Array
      })()
      resp.on('data', (c: Uint8Array) => chunks.push(c))
      resp.on('end', () => {
        resolve({
          status: resp.statusCode ?? 0,
          headers: resp.headers as Record<string, string>,
          body: concatAll(chunks),
          raw,
        })
      })
    })
    preq.on('error', reject)
    preq.end(req.body ?? new Uint8Array(0))
  })
}

function headersFor(req: Request, stream: boolean, base = ''): http2.OutgoingHttpHeaders {
  const out: Record<string, string | string[]> = { ':method': req.method }
  const u = new URL(join(base, req.url))
  out[':path'] = u.pathname + u.search
  for (const [k, v] of Object.entries(req.headers)) {
    if (k.startsWith(':')) continue
    out[k] = v.length === 1 ? (v[0] ?? '') : v
  }
  if (!out['content-type']) out['content-type'] = stream ? 'application/connect+proto' : 'application/proto'
  if (!out['accept']) out['accept'] = stream ? 'application/connect+proto' : 'application/proto'
  return out
}

function headersToRecord(h: Request['headers']): Record<string, string> {
  const out: Record<string, string> = {}
  for (const [k, v] of Object.entries(h)) out[k] = v.join(',')
  return out
}

function concatAll(chunks: Uint8Array[]): Uint8Array {
  const total = chunks.reduce((a, c) => a + c.length, 0)
  const out = new Uint8Array(total)
  let off = 0
  for (const c of chunks) { out.set(c, off); off += c.length }
  return out
}

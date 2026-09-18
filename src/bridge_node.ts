// node bridge using the built-in http2 module. It implements the Transport
// interface for server-to-server RPC:
//   - cleartext (http://)  -> h2c prior-knowledge
//   - with ALPN or explicit http2 -> h2
//   - falls back to HTTP/1.1 when the endpoint only speaks h1.
// Bridge-only: the protocol logic (frames, headers, content-type) stays in core.
import type { Request, Response, Stream, Transport, Headers as HeadersT } from './protocol.js'
import { gzipDecompress } from './compression.js'
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
    // A session error is surfaced through the request stream; keep a no-op
    // listener so a failed h2 preface (e.g. an h1-only peer) never crashes the
    // process as an unhandled 'error' event.
    session.on('error', () => { /* surfaced via stream error */ })
    return session
  }

  /**
   * Wait until the peer confirms it speaks HTTP/2 (SETTINGS frame). Used in
   * `auto` mode BEFORE issuing a request, so an h1-only peer falls back to the
   * h1 bridge deterministically instead of failing mid-stream.
   */
  function h2Ready(session: http2.ClientHttp2Session, timeoutMs = 2000): Promise<void> {
    return new Promise((resolve, reject) => {
      const done = (fn: () => void) => {
        clearTimeout(timer)
        session.off('remoteSettings', onSettings)
        session.off('error', onError)
        session.off('close', onClose)
        fn()
      }
      const onSettings = () => done(resolve)
      const onError = () => done(() => reject(new RPCError(14, 'h2 handshake failed')))
      const onClose = () =>
        done(() => reject(new RPCError(14, 'h2 session closed before SETTINGS')))
      const timer = setTimeout(
        () => done(() => reject(new RPCError(14, 'h2 handshake timeout'))),
        timeoutMs,
      )
      session.once('remoteSettings', onSettings)
      session.once('error', onError)
      session.once('close', onClose)
    })
  }

  async function doSend(req: Request): Promise<Response> {
    const session = connect(req.url)
    const stream = session.request(headersFor(req, false, opts.base))
    if (req.signal !== undefined) {
      const kill = () => { try { stream.close(); session.close() } catch { /* noop */ } }
      if (req.signal.aborted) kill()
      else req.signal.addEventListener('abort', kill, { once: true })
    }
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
    if (req.signal !== undefined) {
      const kill = () => { try { stream.close(); session.close() } catch { /* noop */ } }
      if (req.signal.aborted) kill()
      else req.signal.addEventListener('abort', kill, { once: true })
    }
    stream.end(req.body ?? new Uint8Array(0))
    const chunks = (async function* () {
      for await (const c of stream) yield c as Uint8Array
    })()
    const framed = readFrames(chunks, undefined, gzipDecompress)
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
  const h1Transport: Transport = createHttp1Transport(opts.httpAgent, opts.base)

  /**
   * Auto mode: probe the h2 handshake first (SETTINGS). If the peer is h1-only
   * the probe fails/aborts and we use the h1 bridge — importantly for streams,
   * BEFORE any request is issued, so no mid-stream error escapes. The result is
   * memoized per origin so a stable peer costs one probe, not one per call.
   */
  const h2Probe = new Map<string, boolean>()
  async function autoH2Available(req: Request): Promise<boolean> {
    const target = join(opts.base, req.url)
    const u = new URL(target.startsWith('http') ? target : 'http://' + target)
    const origin = `${u.protocol}//${u.host}`
    const known = h2Probe.get(origin)
    if (known !== undefined) return known
    const session = http2.connect(u.protocol === 'https:' ? u.href : `http://${u.host}`)
    session.on('error', () => { /* handled by h2Ready */ })
    let ok = false
    try {
      await h2Ready(session)
      session.close()
      ok = true
    } catch {
      try { session.destroy() } catch { /* noop */ }
      ok = false
    }
    h2Probe.set(origin, ok)
    return ok
  }

  return {
    async send(req: Request): Promise<Response> {
      if (protocol === 'h1') return h1Transport.send(req)
      if (protocol === 'auto' && !(await autoH2Available(req))) {
        return h1Transport.send(req)
      }
      try {
        return await doSend(req)
      } catch (e) {
        if (protocol === 'auto') return h1Transport.send(req)
        throw e
      }
    },
    async openStream(req: Request): Promise<Stream> {
      if (protocol === 'h1') return h1Transport.openStream(req)
      if (protocol === 'auto' && !(await autoH2Available(req))) {
        return h1Transport.openStream(req)
      }
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
      const err = decodeErrorJson(res.status, headers, res.body)
      return {
        status: res.status,
        headers,
        body: res.body,
        ...(err !== null ? { error: err } : {}),
      }
    },
    async openStream(req: Request): Promise<Stream> {
      const res = await httpStream(req, agent, base)
      const framed = readFrames(res.raw, undefined, gzipDecompress)
      return {
        async *[Symbol.asyncIterator]() {
          yield* streamPayloads(framed)
        },
        cancel() { res.destroy() },
      }
    },
  }
}

/**
 * HTTP/1.1 streaming request: resolves as soon as response HEADERS arrive, with
 * the raw response readable as the SOLE consumer (no buffering, no competing
 * 'data' listener). Server-streams are consumed incrementally here — the
 * unary `httpRequest` below buffers the whole body and must never be used for
 * a stream (doing so drains the socket before the caller reads it).
 */
function httpStream(
  req: Request,
  agent?: http.Agent,
  base = '',
): Promise<{ status: number; headers: Record<string, string>; raw: AsyncIterable<Uint8Array>; destroy: () => void }> {
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
      const raw = resp as unknown as AsyncIterable<Uint8Array>
      resolve({
        status: resp.statusCode ?? 0,
        headers: resp.headers as Record<string, string>,
        raw,
        destroy: () => { try { resp.destroy(); preq.destroy() } catch { /* noop */ } },
      })
    })
    preq.on('error', reject)
    if (req.signal !== undefined) {
      if (req.signal.aborted) { preq.destroy(req.signal.reason as Error); return }
      req.signal.addEventListener('abort', () => preq.destroy(req.signal?.reason as Error), { once: true })
    }
    preq.end(req.body ?? new Uint8Array(0))
  })
}

function httpRequest(req: Request, agent?: http.Agent, base = ''): Promise<{ status: number; headers: Record<string, string>; body: Uint8Array }> {
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
      resp.on('data', (c: Uint8Array) => chunks.push(c))
      resp.on('end', () => {
        resolve({
          status: resp.statusCode ?? 0,
          headers: resp.headers as Record<string, string>,
          body: concatAll(chunks),
        })
      })
    })
    preq.on('error', reject)
    if (req.signal !== undefined) {
      if (req.signal.aborted) { preq.destroy(req.signal.reason as Error); return }
      req.signal.addEventListener('abort', () => preq.destroy(req.signal?.reason as Error), { once: true })
    }
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
  if (!out['connect-accept-encoding']) out['connect-accept-encoding'] = 'gzip'
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

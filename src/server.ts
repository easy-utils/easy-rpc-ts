// easy-rpc TS server core: ASGI-style dispatch + JSON/proto content
// negotiation. This module is server-only and may import Node built-ins; the
// client entry (index.ts) does NOT re-export it.
//
// The dispatch core is PUSH-based: it decodes an RPC request and writes the
// response into a `ResponseWriter`. Server-stream responses are written and
// flushed frame-by-frame — never buffered. Runtime adapters (node:http,
// node:http2, fetch/Web) implement the writer for their transport.
import type { Bytes, Headers, Request, Response, ResponseWriter, ServerDispatch } from './protocol.js'
import { httpStatus, RPCError, frame, encodeEndStream } from './protocol.js'
import { type ContentKind, type ServiceHandlers, detectKind } from './protocol.js'
import nodeHttp from 'node:http'
import nodeHttp2 from 'node:http2'

export type { ContentKind, ServiceHandlers, ResponseWriter } from './protocol.js'
export { detectKind } from './protocol.js'

export interface MethodSpec2 { path: string; name: string; serverStream: boolean }

function contentFor(kind: ContentKind): string { return kind === 'json' ? 'application/json' : 'application/proto' }
function streamContentFor(kind: ContentKind): string { return kind === 'json' ? 'application/connect+json' : 'application/connect+proto' }

/** Server dispatch: pushes a response into `w`. */
export type ServerHandler = ServerDispatch

/** Build a push-based dispatch function from method specs + handler tables. */
export function createServer(methods: MethodSpec2[], handlers: ServiceHandlers): ServerHandler {
  return async (req: Request, w: ResponseWriter): Promise<void> => {
    const kind = detectKind(req)
    const pathname = new URL(req.url.startsWith('http') ? req.url : 'http://localhost' + req.url).pathname
    const body = req.body ?? new Uint8Array(0)

    // `Spec` matching (streaming methods are POST-only).
    const spec = methods.find(m => m.path === pathname && (!m.serverStream || req.method === 'POST'))
    if (!spec) return fail(w, new RPCError(5, 'not found'), kind)

    if (spec.serverStream) {
      const h = handlers.stream[spec.name]
      if (!h) return fail(w, new RPCError(5, 'no handler'), kind)
      // Stream: HTTP status is always 200; errors go into the END frame.
      w.status(200)
      w.header('content-type', streamContentFor(kind))
      let wroteEnd = false
      const emit = async (data: Bytes, end: boolean): Promise<void> => {
        if (wroteEnd) return
        if (end) {
          wroteEnd = true
          await w.write(frame(new Uint8Array(0), true))
          return
        }
        await w.write(frame(data, false))
      }
      try {
        await h(body, kind, emit)
      } catch (e) {
        // Propagate the failure in the END frame (HTTP stays 200).
        const err = e instanceof RPCError ? e : new RPCError(13, String(e))
        if (!wroteEnd) {
          wroteEnd = true
          await w.write(frame(encodeEndStream(err.code, err.message), true))
        }
        await w.finish()
        return
      }
      if (!wroteEnd) await w.write(frame(new Uint8Array(0), true))
      await w.finish()
      return
    }

    // Unary: resolve fully BEFORE writing so a thrown error can set a real status.
    const h = handlers.unary[spec.name]
    if (!h) return fail(w, new RPCError(5, 'no handler'), kind)
    let out: Bytes
    try {
      out = await h(body, kind)
    } catch (e) {
      return fail(w, e instanceof RPCError ? e : new RPCError(13, String(e)), kind)
    }
    w.status(200)
    w.header('content-type', contentFor(kind))
    await w.write(out)
    await w.finish()
  }
}

/** Emit a non-200 error response (before any stream body has been written). */
async function fail(w: ResponseWriter, err: RPCError, kind: ContentKind): Promise<void> {
  w.status(httpStatus(err.code))
  w.header('content-type', kind === 'json' ? 'application/json' : 'text/plain')
  await w.write(new TextEncoder().encode(err.message))
  await w.finish()
}

export function concat(chunks: Bytes[]): Bytes {
  let len = 0; for (const c of chunks) len += c.length
  const out = new Uint8Array(len); let off = 0
  for (const c of chunks) { out.set(c, off); off += c.length }
  return out
}

// ---- writers ----

/** A ResponseWriter over a Node http.ServerResponse. Each `write` is flushed so
 *  streaming responses reach the client immediately. */
function nodeWriter(res: nodeHttp.ServerResponse): ResponseWriter {
  let status = 200
  const headers: Record<string, string> = {}
  let started = false
  const start = () => {
    if (started) return
    started = true
    res.writeHead(status, headers)
    res.flushHeaders?.()
  }
  return {
    status(code) { status = code },
    header(name, value) { headers[name] = value },
    async write(chunk) {
      start()
      await new Promise<void>(resolve => {
        if (res.write(Buffer.from(chunk))) return resolve()
        res.once('drain', () => resolve())
      })
      ;(res as unknown as { flush?: () => void }).flush?.()
    },
    async finish() {
      start()
      await new Promise<void>(resolve => res.end(() => resolve()))
    },
  }
}

/** Node HTTP/1.1 listener: dispatch + push, chunked + flushed. */
export function nodeServer(handler: ServerHandler): nodeHttp.Server {
  return nodeHttp.createServer((req, res) => {
    const readBody = new Promise<Bytes>(resolve => {
      const a: Uint8Array[] = []
      req.on('data', c => a.push(c as Uint8Array))
      req.on('end', () => resolve(concat(a as Bytes[])))
    })
    void (async () => {
      const body = await readBody
      const headers: Headers = {}
      for (const [k, v] of Object.entries(req.headers)) {
        if (v === undefined) continue
        headers[k] = Array.isArray(v) ? v : [v]
      }
      const w = nodeWriter(res)
      try {
        await handler(
          { url: req.url ?? '/', method: req.method ?? 'POST', headers, body },
          w,
        )
      } catch {
        // A writer-level failure: the response may already be streaming, so we
        // can only end it. Log-free: the caller owns observability.
        if (!res.headersSent) res.writeHead(500)
        res.end()
      }
    })()
  })
}

/** HTTP/2 server bridge (cleartext h2c + optional TLS h2). Same push contract,
 *  one stream per request, each frame written and flushed immediately. */
export function http2Server(handler: ServerHandler, secure = false): nodeHttp2.Http2Server | nodeHttp2.Http2SecureServer {
  const server = secure
    ? nodeHttp2.createSecureServer({})
    : nodeHttp2.createServer()
  server.on('stream', (stream: nodeHttp2.ServerHttp2Stream, headers) => {
    void (async () => {
      const body = await readHttp2Body(stream)
      const h: Headers = {}
      for (const [k, v] of Object.entries(headers)) {
        if (k.startsWith(':')) continue
        h[k] = Array.isArray(v) ? v as string[] : [v as string]
      }
      const method = String(headers[':method'] ?? 'GET')
      const url = String(headers[':path'] ?? '/')
      let status = 200
      const outHeaders: Record<string, string> = {}
      let started = false
      const start = () => {
        if (started) return
        started = true
        stream.respond({ ':status': status, ...outHeaders })
      }
      const w: ResponseWriter = {
        status(code) { status = code },
        header(name, value) { outHeaders[name] = value },
        async write(chunk) {
          start()
          await new Promise<void>(resolve => stream.write(Buffer.from(chunk), () => resolve()))
        },
        async finish() {
          start()
          await new Promise<void>(resolve => stream.end(() => resolve()))
        },
      }
      try {
        await handler({ url, method, headers: h, body }, w)
      } catch {
        if (!started) stream.respond({ ':status': 500 })
        stream.end()
      }
    })()
  })
  return server
}

function readHttp2Body(stream: nodeHttp2.ServerHttp2Stream): Promise<Bytes> {
  return new Promise(resolve => { const a: Uint8Array[] = []; stream.on('data', c => a.push(c as Uint8Array)); stream.on('end', () => resolve(concat(a as Bytes[]))) })
}

// ---- buffered adapter (for non-streaming callers / tests) ----

/** Run the push dispatch and collect the result into a core `Response`. This is
 *  a convenience for tests and non-streaming integrations — the streaming
 *  adapters above do NOT use it. */
export async function dispatchToResponse(handler: ServerHandler, req: Request): Promise<Response> {
  const chunks: Bytes[] = []
  let status = 200
  const headers: Headers = {}
  await handler(req, {
    status(code) { status = code },
    header(name, value) { headers[name] = [value] },
    async write(chunk) { chunks.push(chunk) },
    async finish() {},
  })
  return { status, headers, body: concat(chunks) }
}

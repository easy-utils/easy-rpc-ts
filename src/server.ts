// easy-rpc TS server core: ASGI-style dispatch + JSON/proto content
// negotiation. This module is server-only and may import Node built-ins; the
// client entry (index.ts) does NOT re-export it.
import type { Bytes, Headers, Request, Response } from './protocol.js'
import { httpStatus, RPCError, frame } from './protocol.js'
import { type ContentKind, type ServiceHandlers, detectKind } from './protocol.js'
import nodeHttp2 from 'node:http2'

export type { ContentKind, ServiceHandlers } from './protocol.js'
export { detectKind } from './protocol.js'

export interface MethodSpec2 { path: string; name: string; serverStream: boolean }

function contentFor(kind: ContentKind): string { return kind === 'json' ? 'application/json' : 'application/proto' }
function streamContentFor(kind: ContentKind): string { return kind === 'json' ? 'application/connect+json' : 'application/connect+proto' }

export type ServerHandler = (req: Request) => Promise<Response>

export function createServer(methods: MethodSpec2[], handlers: ServiceHandlers): ServerHandler {
  return async (req: Request): Promise<Response> => {
    const kind = detectKind(req)
    const pathname = new URL(req.url.startsWith('http') ? req.url : 'http://localhost' + req.url).pathname
    const spec = methods.find(m => m.path === pathname && (!m.serverStream || req.method === 'POST'))
    if (!spec) return { status: 404, headers: { 'content-type': [contentFor(kind)] }, body: new Uint8Array(0), error: new RPCError(5, 'not found') }
    const body = req.body ?? new Uint8Array(0)
    if (spec.serverStream) {
      const h = handlers.stream[spec.name]
      if (!h) return { status: 404, headers: {}, body: new Uint8Array(0), error: new RPCError(5, 'no handler') }
      const parts: Bytes[] = []
      await h(body, kind, async (data, end) => { if (end) return; parts.push(frame(data)) })
      const bytes = concat(parts)
      return { status: 200, headers: { 'content-type': [streamContentFor(kind)] }, body: bytes }
    }
    const h = handlers.unary[spec.name]
    if (!h) return { status: 404, headers: {}, body: new Uint8Array(0), error: new RPCError(5, 'no handler') }
    try { return { status: 200, headers: { 'content-type': [contentFor(kind)] }, body: await h(body, kind) } }
    catch (e) { const err = e instanceof RPCError ? e : new RPCError(13, String(e)); return { status: httpStatus(err.code), headers: {}, body: new Uint8Array(0), error: err } }
  }
}

export function concat(chunks: Bytes[]): Bytes {
  let len = 0; for (const c of chunks) len += c.length
  const out = new Uint8Array(len); let off = 0
  for (const c of chunks) { out.set(c, off); off += c.length }
  return out
}

/** HTTP/2 server bridge (cleartext h2c + optional TLS h2) for server-to-server
 * RPC. It speaks the same Connect wire as the pure app, but over http2.
 * This is the only Node-native server adapter kept; for h1 use toWebHandler
 * with any fetch-compatible runtime (Hono / Bun / CF Workers / Deno). */
export function http2Server(handler: ServerHandler, secure = false): nodeHttp2.Http2Server | nodeHttp2.Http2SecureServer {
  const server = secure
    ? nodeHttp2.createSecureServer({})
    : nodeHttp2.createServer()
  server.on('stream', async (stream, headers) => {
    const body = await readHttp2Body(stream as nodeHttp2.ServerHttp2Stream)
    const h: Headers = {}
    for (const [k, v] of Object.entries(headers)) {
      if (k.startsWith(':')) continue
      h[k] = Array.isArray(v) ? v as string[] : [v as string]
    }
    const method = String(headers[':method'] ?? 'GET')
    const url = String(headers[':path'] ?? '/')
    const out = await handler({ url, method, headers: h, body })
    ;(stream as nodeHttp2.ServerHttp2Stream).respond({ ':status': out.status, 'content-type': out.headers['content-type']?.[0] ?? 'application/proto' })
    stream.end(out.body)
  })
  return server
}

function readHttp2Body(stream: nodeHttp2.ServerHttp2Stream): Promise<Bytes> {
  return new Promise(resolve => { const a: Uint8Array[] = []; stream.on('data', c => a.push(c as Uint8Array)); stream.on('end', () => resolve(concat(a as unknown as Bytes[]))) })
}

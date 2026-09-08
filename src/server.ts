// easy-rpc TS server core + JSON/proto content negotiation for client.
import type { Bytes, Headers, Request, Response } from './protocol'
import { httpStatus, RPCError, frame } from './protocol'

type Fastify = unknown

export type ContentKind = 'proto' | 'json'

export interface MethodSpec2 { path: string; name: string; serverStream: boolean }

/** A typed service handler table. */
export interface ServiceHandlers {
  unary: Record<string, (input: Bytes, kind: ContentKind) => Promise<Bytes>>
  stream: Record<string, (input: Bytes, kind: ContentKind, emit: (data: Bytes, end: boolean) => Promise<void>) => Promise<void>>
}

export function detectKind(req: Request, resHeaders: Headers): ContentKind {
  const ct = req.headers['content-type']?.[0] ?? ''
  const ac = req.headers['accept']?.[0] ?? ''
  if (ct.startsWith('application/json') || ac.startsWith('application/json')) return 'json'
  return 'proto'
}

function contentFor(kind: ContentKind): string {
  return kind === 'json' ? 'application/json' : 'application/proto'
}
function streamContentFor(kind: ContentKind): string {
  return kind === 'json' ? 'application/connect+json' : 'application/connect+proto'
}

export interface ServerHandler {
  (req: Request): Promise<Response>
}

/**
 * Build an http.RequestListener-compatible handler (works with node:http or a
 * fetch-style adaptor). Dispatch by method specs to typed handlers.
 */
export function createServer(
  methods: MethodSpec2[],
  handlers: ServiceHandlers,
  opts: { reqToIncoming: (req: Request) => BodyLike } = { reqToIncoming: (r) => r as unknown as BodyLike },
): ServerHandler {
  return async (req: Request): Promise<Response> => {
    const kind = detectKind(req, req.headers)
    const spec = methods.find((m) => m.path === new URL(req.url).pathname && (!m.serverStream || req.method === 'POST'))
    if (!spec) return { status: 404, headers: { 'content-type': [contentFor(kind)] }, body: new Uint8Array(0), error: new RPCError(5, 'not found') }
    const body = req.body ?? new Uint8Array(0)
    if (spec.serverStream) {
      const h = handlers.stream[spec.name]
      if (!h) return { status: 404, headers: {}, body: new Uint8Array(0), error: new RPCError(5, 'no handler') }
      const parts: Bytes[] = []
      await h(body, kind, async (data, end) => { parts.push(data); if (end) throw new Error('__end') })
      // fuse into one response body (server-stream over HTTP/1.1 chunked);
      // for real streaming we emit frames; here we concatenate frames.
      const bytes = concat(parts)
      // Wrap into a stream-like connect payload: caller reads frames.
      return { status: 200, headers: { 'content-type': [streamContentFor(kind)] }, body: bytes }
    }
    const h = handlers.unary[spec.name]
    if (!h) return { status: 404, headers: {}, body: new Uint8Array(0), error: new RPCError(5, 'no handler') }
    try {
      const out = await h(body, kind)
      return { status: 200, headers: { 'content-type': [contentFor(kind)] }, body: out }
    } catch (e) {
      const err = e instanceof RPCError ? e : new RPCError(13, String(e))
      return { status: httpStatus(err.code), headers: {}, body: new Uint8Array(0), error: err }
    }
  }
}

export function concat(chunks: Bytes[]): Bytes {
  let len = 0
  for (const c of chunks) len += c.length
  const out = new Uint8Array(len)
  let off = 0
  for (const c of chunks) { out.set(c, off); off += c.length }
  return out
}

interface BodyLike { [k: string]: unknown }

/** Node http listener: adapt raw IncomingMessage to a Request, run handler. */
export function nodeServer(handler: ServerHandler) {
  // eslint-disable-next-line @typescript-eslint/no-var-requires
  const http = require('node:http') as typeof import('node:http')
  return http.createServer(async (reqRes, res) => {
    const reqObj = reqRes as import('node:http').IncomingMessage & { method?: string; url?: string; headers?: Record<string, string | string[]> }
    const body = await readBody(reqObj)
    const href = reqObj.url ?? '/'
    const headers: Headers = {}
    for (const [k, v] of Object.entries(reqObj.headers ?? {})) headers[k] = Array.isArray(v) ? v as string[] : [v as string]
    const out = await handler({
      url: href, method: reqObj.method ?? 'GET', headers, body,
    })
    res.writeHead(out.status, toNodeHeaders(out.headers))
    res.end(out.body)
  })
}

function readBody(req: import('node:http').IncomingMessage): Promise<Bytes> {
  return new Promise((resolve) => {
    const chunks: Uint8Array[] = []
    req.on('data', (c) => chunks.push(c))
    req.on('end', () => resolve(concat(chunks as unknown as Bytes[])))
  })
}

function toNodeHeaders(h: Headers): Record<string, string> {
  const out: Record<string, string> = {}
  for (const [k, v] of Object.entries(h)) out[k] = v.join(',')
  return out
}

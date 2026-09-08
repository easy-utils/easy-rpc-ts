// easy-rpc TS server core + JSON/proto content negotiation.
import type { Bytes, Headers, Request, Response } from './protocol.js'
import { httpStatus, RPCError, frame } from './protocol.js'
import nodeHttp from 'node:http'

export type ContentKind = 'proto' | 'json'

export interface MethodSpec2 { path: string; name: string; serverStream: boolean }

export interface ServiceHandlers {
  unary: Record<string, (input: Bytes, kind: ContentKind) => Promise<Bytes>>
  stream: Record<string, (input: Bytes, kind: ContentKind, emit: (data: Bytes, end: boolean) => Promise<void>) => Promise<void>>
}

export function detectKind(req: Request): ContentKind {
  const ct = req.headers['content-type']?.[0] ?? ''
  const ac = req.headers['accept']?.[0] ?? ''
  if (ct.startsWith('application/json') || ac.startsWith('application/json')) return 'json'
  return 'proto'
}
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

export type NodeListener = ReturnType<typeof nodeHttp.createServer>

export function nodeServer(handler: ServerHandler): NodeListener {
  return nodeHttp.createServer(async (req, res) => {
    const body = await readBody(req)
    const headers: Headers = {}
    for (const [k, v] of Object.entries(req.headers)) headers[k] = Array.isArray(v) ? v as string[] : [v as string]
    const out = await handler({ url: req.url ?? '/', method: req.method ?? 'GET', headers, body })
    res.writeHead(out.status, toNodeHeaders(out.headers))
    res.end(out.body)
  })
}

function readBody(req: import('node:http').IncomingMessage): Promise<Bytes> {
  return new Promise(resolve => { const a: Uint8Array[] = []; req.on('data', c => a.push(c)); req.on('end', () => resolve(concat(a as unknown as Bytes[]))) })
}
function toNodeHeaders(h: Headers): Record<string, string> { const o: Record<string,string> = {}; for (const [k,v] of Object.entries(h)) o[k] = v.join(','); return o }

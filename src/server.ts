// easy-rpc TS server core: push-based dispatch for the Connect wire subset
// (unary + server-stream, proto only, POST only). This module is server-only
// and may import Node built-ins; the client entry (index.ts) does NOT re-export
// it.
//
// The dispatch core is PUSH-based: it decodes an RPC request and writes the
// response into a `ResponseWriter`. Server-stream responses are written and
// flushed frame-by-frame — never buffered. Runtime adapters (node:http,
// node:http2, fetch/Web) implement the writer for their transport.
import type {
  Bytes,
  HandlerContext,
  Headers,
  Request,
  Response,
  ResponseWriter,
  ServerDispatch,
  ServiceHandlers,
} from './protocol.js'
import {
  CONTENT_TYPE_STREAM,
  CONTENT_TYPE_UNARY,
  CONNECT_PROTOCOL_VERSION,
  COMPRESS_MIN_BYTES,
  DEFAULT_MAX_MESSAGE_BYTES,
  ENCODING_GZIP,
  HEADER_ACCEPT_ENCODING,
  HEADER_CONTENT_ENCODING,
  HEADER_PROTOCOL_VERSION,
  HEADER_STREAM_ACCEPT_ENCODING,
  HEADER_TIMEOUT,
  RPCError,
  encodeEndStream,
  encodeErrorJson,
  frame,
  httpStatus,
  
  muxTrailers,
  parseTimeout,
} from './protocol.js'
import { gzipCompress, gzipDecompress } from './compression.js'
import nodeHttp from 'node:http'
import nodeHttp2 from 'node:http2'

export type { ServiceHandlers, ResponseWriter } from './protocol.js'

export interface MethodSpec2 { path: string; name: string; serverStream: boolean }

/** Server dispatch: pushes a response into `w`. */
export type ServerHandler = ServerDispatch

/** Build a push-based dispatch function from method specs + handler tables. */
export function createServer(
  methods: MethodSpec2[],
  handlers: ServiceHandlers,
  opts: { maxMessageBytes?: number } = {},
): ServerHandler {
  const maxBytes = opts.maxMessageBytes ?? DEFAULT_MAX_MESSAGE_BYTES
  return async (req: Request, w: ResponseWriter): Promise<void> => {
    const pathname = new URL(req.url.startsWith('http') ? req.url : 'http://localhost' + req.url).pathname
    const body = req.body ?? new Uint8Array(0)
    const ct = req.headers['content-type']?.[0] ?? ''

    // Protocol version: reject an explicitly-unsupported version (absent is ok).
    const pv = req.headers[HEADER_PROTOCOL_VERSION]?.[0]
    if (pv !== undefined && pv !== '' && pv !== CONNECT_PROTOCOL_VERSION) {
      return fail(w, new RPCError(12, `unsupported connect-protocol-version: ${pv}`))
    }

    const spec = methods.find(m => m.path === pathname)
    if (!spec) return fail(w, new RPCError(5, 'not found'))

    // proto-only (spec §2): unary uses application/proto, server-stream uses
    // application/connect+proto. JSON content types are rejected with 415.
    const expected = spec.serverStream ? CONTENT_TYPE_STREAM : CONTENT_TYPE_UNARY
    const got = (ct.split(';')[0] ?? '').trim().toLowerCase()
    if (got !== expected) {
      return fail(w, new RPCError(3, `unsupported content-type: ${ct || '(none)'} (expected ${expected})`), 415)
    }
    if (body.length > maxBytes) {
      return fail(w, new RPCError(8, `request too large: ${body.length} > ${maxBytes}`))
    }

    // Deadline: the Connect timeout header bounds the whole call.
    const timeoutMs = parseTimeout(req.headers[HEADER_TIMEOUT]?.[0])
    const timedOut = (): RPCError => new RPCError(4, 'deadline exceeded')

    // Trailing metadata set by the handler.
    const trailers: Headers = {}
    const ctx: HandlerContext = {
      headers: req.headers,
      setTrailer(key: string, value: string) {
        const k = key.toLowerCase()
        const cur = trailers[k]
        if (cur === undefined) trailers[k] = [value]
        else cur.push(value)
      },
    }

    if (spec.serverStream) {
      const h = handlers.stream[spec.name]
      if (!h) return fail(w, new RPCError(5, 'no handler'))
      // Stream request body is ENVELOPED (spec §3.2): one data frame carrying
      // the single request message. Unframe it before dispatch.
      let reqBody: Bytes
      try {
        reqBody = await readSingleFrame(body, maxBytes)
      } catch (e) {
        return fail(w, e instanceof RPCError ? e : new RPCError(13, String(e)))
      }
      // Stream: HTTP status is always 200; errors go into the END frame.
      w.status(200)
      w.header('content-type', CONTENT_TYPE_STREAM)
      const wantsGzip = (req.headers[HEADER_STREAM_ACCEPT_ENCODING] ?? []).some(
        (v) => v.split(',').map((s) => s.trim()).includes(ENCODING_GZIP),
      )
      let wroteEnd = false
      const emit = async (data: Bytes, end: boolean): Promise<void> => {
        if (wroteEnd) return
        if (end) {
          wroteEnd = true
          await w.write(frame(encodeEndStream(0, '', trailers), true))
          return
        }
        if (wantsGzip && data.length >= COMPRESS_MIN_BYTES) {
          await w.write(frame(gzipCompress(data), false, DEFAULT_MAX_MESSAGE_BYTES, true))
        } else {
          await w.write(frame(data, false))
        }
      }
      try {
        if (timeoutMs > 0) {
          let timer: ReturnType<typeof setTimeout> | undefined
          const deadline = new Promise<never>((_, rej) => {
            timer = setTimeout(() => rej(timedOut()), timeoutMs)
          })
          try {
            await Promise.race([h(reqBody, ctx, emit), deadline])
          } finally {
            if (timer !== undefined) clearTimeout(timer)
          }
        } else {
          await h(reqBody, ctx, emit)
        }
      } catch (e) {
        // Propagate the failure in the END frame (HTTP stays 200).
        const err = e instanceof RPCError ? e : new RPCError(13, String(e))
        if (!wroteEnd) {
          wroteEnd = true
          await w.write(frame(encodeEndStream(err.code, err.message, trailers, err.details), true))
        }
        await w.finish()
        return
      }
      if (!wroteEnd) await w.write(frame(encodeEndStream(0, '', trailers), true))
      await w.finish()
      return
    }

    // Unary: resolve fully BEFORE writing so a thrown error can set a real status.
    const h = handlers.unary[spec.name]
    if (!h) return fail(w, new RPCError(5, 'no handler'))
    let out: Bytes
    try {
      out = timeoutMs > 0 ? await Promise.race([
        h(body, ctx),
        new Promise<never>((_, rej) => setTimeout(() => rej(timedOut()), timeoutMs)),
      ]) : await h(body, ctx)
    } catch (e) {
      return fail(w, e instanceof RPCError ? e : new RPCError(13, String(e)), undefined, trailers)
    }
    w.status(200)
    w.header('content-type', CONTENT_TYPE_UNARY)
    // Unary: compress the whole body when the client accepts gzip (spec §3.5).
    const wantsGzip = acceptsUnaryGzip(req.headers)
    if (wantsGzip && out.length >= COMPRESS_MIN_BYTES) {
      w.header(HEADER_CONTENT_ENCODING, ENCODING_GZIP)
      out = gzipCompress(out)
    }
    for (const [k, v] of Object.entries(muxTrailers({}, trailers))) {
      w.header(k, v[0] ?? '')
    }
    await w.write(out)
    await w.finish()
  }
}

function acceptsUnaryGzip(headers: Headers): boolean {
  return (headers[HEADER_ACCEPT_ENCODING] ?? []).some(
    (v) => v.split(',').map((s) => s.trim()).includes(ENCODING_GZIP),
  )
}

/** Read exactly one frame (the enveloped server-stream request message).
 *  Returns the payload. Throws on truncation / size violation. */
async function readSingleFrame(body: Bytes, maxBytes: number): Promise<Bytes> {
  if (body.length < 5) {
    throw new RPCError(13, `stream request: truncated frame header (${body.length} bytes)`)
  }
  const flags = body[0] ?? 0
  const len = new DataView(body.buffer, body.byteOffset, body.byteLength).getUint32(1, false)
  if (len > maxBytes) throw new RPCError(8, `frame too large: ${len} > ${maxBytes}`)
  if (body.length < 5 + len) {
    throw new RPCError(13, `stream request: truncated frame (want ${5 + len}, have ${body.length})`)
  }
  if ((flags & 0x01) !== 0) {
    // Compressed request frame (optional). Decompress.
    return gzipDecompress(body.slice(5, 5 + len))
  }
  return body.slice(5, 5 + len)
}

/** Emit a non-200 error response (before any stream body has been written).
 *  Unary errors use the Connect HTTP-status + JSON body shape; trailers are
 *  muxed as `trailer-*` headers. */
async function fail(w: ResponseWriter, err: RPCError, statusOverride?: number, trailers: Headers = {}): Promise<void> {
  w.status(statusOverride ?? httpStatus(err.code))
  w.header('content-type', 'application/json')
  for (const [k, v] of Object.entries(muxTrailers({}, trailers))) {
    w.header(k, v[0] ?? '')
  }
  await w.write(encodeErrorJson(err.code, err.message, err.details))
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
        await handler({ url: req.url ?? '/', headers, body }, w)
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
        await handler({ url, headers: h, body }, w)
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

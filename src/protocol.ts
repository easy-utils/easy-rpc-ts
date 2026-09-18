// easy-rpc TypeScript core: zero-runtime-bindings Transport interface + the
// Connect wire protocol subset (unary + server-stream, proto messages only).
// Bridges (fetch/node) adapt a concrete HTTP runtime to Transport; protocol
// logic (framing, errors, trailers, content-type) is runtime-agnostic.

/** Multi-value headers. */
export type Bytes = Uint8Array<ArrayBufferLike>
export type Headers = Record<string, string[]>

/** Merge fixed metadata headers into a request's headers (auth/tenant/token).
 * Call-provided values win; this is the documented way to attach credentials. */
export function withMetadata(metadata: Headers, req: Request): Request {
  const merged: Headers = { ...(metadata ?? {}) }
  for (const [k, v] of Object.entries(req.headers)) merged[k] = v
  return { ...req, headers: merged }
}

/**
 * A call interceptor. It wraps the next unary/stream invocation and may mutate
 * the request (attach auth/metadata), observe the response, short-circuit, or
 * impose a deadline. `next` performs the actual transport call.
 */
export interface Interceptor {
  unary?(req: Request, next: (req: Request) => Promise<Response>): Promise<Response>
  stream?(req: Request, next: (req: Request) => Promise<Stream>): Promise<Stream>
}

/** Apply interceptors to a Transport (outermost first, like a middleware chain). */
export function createInterceptorTransport(
  interceptors: Interceptor[],
  transport: Transport,
): Transport {
  const chain = (
    req: Request,
    call: (r: Request) => Promise<Response>,
  ): Promise<Response> => {
    const dispatch = (i: number, r: Request): Promise<Response> => {
      const ic = interceptors[i]
      if (ic === undefined) return call(r)
      const next = (nr: Request): Promise<Response> => dispatch(i + 1, nr)
      return ic.unary ? ic.unary(r, next) : next(r)
    }
    return dispatch(0, req)
  }

  const streamChain = (
    req: Request,
    call: (r: Request) => Promise<Stream>,
  ): Promise<Stream> => {
    const dispatch = (i: number, r: Request): Promise<Stream> => {
      const ic = interceptors[i]
      if (ic === undefined) return call(r)
      const next = (nr: Request): Promise<Stream> => dispatch(i + 1, nr)
      return ic.stream ? ic.stream(r, next) : next(r)
    }
    return dispatch(0, req)
  }

  return {
    send: (req) => chain(req, (r) => transport.send(r)),
    openStream: (req) => streamChain(req, (r) => transport.openStream(r)),
  }
}

/** Built-in interceptor: attach fixed metadata (auth/tenant/token) to every call. */
export function metadataInterceptor(metadata: Headers): Interceptor {
  return {
    unary: (req, next) => next(withMetadata(metadata, req)),
    stream: (req, next) => next(withMetadata(metadata, req)),
  }
}

/**
 * Built-in interceptor: impose a per-call deadline.
 *
 * It sets the Connect `Connect-Timeout-Ms` header (server-side deadline) AND a
 * local AbortSignal that adapters honour, so the client cancels even when the
 * server cannot enforce the deadline. The signal is cleared on completion.
 */
export function timeoutInterceptor(timeoutMs: number): Interceptor {
  const withDeadline = (req: Request): { req: Request; done: () => void } => {
    if (timeoutMs <= 0) return { req, done: () => {} }
    const ctrl = new AbortController()
    const timer = setTimeout(() => ctrl.abort(new RPCError(4, 'deadline exceeded')), timeoutMs)
    return {
      req: { ...withTimeout(req, timeoutMs), signal: ctrl.signal },
      done: () => clearTimeout(timer),
    }
  }
  return {
    async unary(req, next) {
      const { req: r, done } = withDeadline(req)
      try {
        return await next(r)
      } finally {
        done()
      }
    },
    async stream(req, next) {
      const { req: r, done } = withDeadline(req)
      try {
        return await next(r)
      } finally {
        done()
      }
    },
  }
}

/** A normalized RPC request. All calls are POST (no method field). */
export interface Request {
  url: string
  headers: Headers
  body: Bytes | undefined
  /** Local cancellation channel. Adapters that support abort (fetch signal,
   *  node request destroy, h2 stream close) honour it; others ignore it. */
  signal?: AbortSignal
}

/** A normalized unary response. */
export interface Response {
  status: number
  headers: Headers
  body: Bytes
  /** Trailing metadata (demuxed from `trailer-*` response headers by the bridge). */
  trailers?: Headers
  error?: RPCError
}

/**
 * A structured error detail (spec §4.1, aligned with Connect Error Details /
 * gRPC google.rpc status details). `type` is a type URL; `value` is opaque
 * bytes (typically an encoded protobuf message).
 */
export interface ErrorDetail {
  type: string
  value: Bytes
}

// Runtime-agnostic base64 (no btoa/Buffer dependency; details are small).
const B64 = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/'
function base64Encode(b: Bytes): string {
  let out = ''
  for (let i = 0; i < b.length; i += 3) {
    const n = (b[i]! << 16) | ((b[i + 1] ?? 0) << 8) | (b[i + 2] ?? 0)
    out += (B64[n >> 18] ?? '') + (B64[(n >> 12) & 63] ?? '') + (i + 1 < b.length ? B64[(n >> 6) & 63] ?? '' : '=') + (i + 2 < b.length ? B64[n & 63] ?? '' : '=')
  }
  return out
}
function base64Decode(s: string): Bytes {
  const clean = s.replace(/[^A-Za-z0-9+/]/g, '')
  const out = new Uint8Array((clean.length * 3) >> 2)
  let o = 0
  for (let i = 0; i < clean.length; i += 4) {
    const idx = [0, 1, 2, 3].map((k) => B64.indexOf(clean[i + k] ?? ''))
    const n = (idx[0]! << 18) | (idx[1]! << 12) | ((idx[2]! < 0 ? 0 : idx[2]!) << 6) | (idx[3]! < 0 ? 0 : idx[3]!)
    out[o++] = (n >> 16) & 0xff
    if (idx[2]! >= 0) out[o++] = (n >> 8) & 0xff
    if (idx[3]! >= 0) out[o++] = n & 0xff
  }
  return out.subarray(0, o)
}

/** Wire-level error with a Connect code. */
export class RPCError extends Error {
  constructor(
    public code: number,
    message: string,
    /** Optional structured details (spec §4.1); opaque to the wire layer. */
    public details?: ErrorDetail[],
  ) {
    super(message)
    this.name = 'RPCError'
  }
}

/** Server-stream response: async iterable of raw message bytes, plus the
 *  trailing metadata (available after the iterator completes) and cancel(). */
export interface Stream extends AsyncIterable<Bytes> {
  /** Trailing metadata from the END frame. Empty until the stream has ended. */
  trailers(): Headers
  cancel(): void
}

/** The core interface a bridge must implement. */
export interface Transport {
  send(req: Request): Promise<Response>
  openStream(req: Request): Promise<Stream>
}

/** Client-side typed server-stream: message iterator + trailing metadata. */
export interface ServerStream<T> extends AsyncIterable<T> {
  /** Trailing metadata from the END frame. Empty until the stream has ended. */
  trailers(): Headers
  cancel(): void
}

/** Maps Connect code to HTTP status. */
export function httpStatus(code: number): number {
  switch (code) {
    case 1: return 499
    case 3: return 400
    case 4: return 504
    case 5: return 404
    case 6: return 409
    case 7: return 403
    case 8: return 429
    case 9: return 400
    case 10: return 409
    case 11: return 400
    case 12: return 501
    case 14: return 503
    case 16: return 401
    default: return 500
  }
}

/** Connect error-code names (wire-stable strings used on the JSON error
 *  payloads / end-stream). Mirrors @connectrpc/connect. */
const CODE_NAMES: Record<number, string> = {
  0: 'ok', 1: 'canceled', 2: 'unknown', 3: 'invalid_argument',
  4: 'deadline_exceeded', 5: 'not_found', 6: 'already_exists',
  7: 'permission_denied', 8: 'resource_exhausted', 9: 'failed_precondition',
  10: 'aborted', 11: 'out_of_range', 12: 'unimplemented', 13: 'internal',
  14: 'unavailable', 15: 'data_loss', 16: 'unauthenticated',
}
const CODE_BY_NAME: Record<string, number> = Object.fromEntries(
  Object.entries(CODE_NAMES).map(([n, s]) => [s, Number(n)]),
)

/** Connect code -> stable lowercase name (e.g. 5 -> "not_found"). */
export function codeToString(code: number): string {
  return CODE_NAMES[code] ?? CODE_NAMES[2]!
}

/** Stable lowercase name -> Connect code (unknown -> 2). */
export function codeFromString(name: string): number {
  return CODE_BY_NAME[name] ?? 2
}

/** Connect unary error body (JSON). The HTTP status carries the class; the
 *  body carries the exact code name + message + optional details. */
export interface ConnectErrorBody {
  code: string
  message: string
  details?: { type: string; value: string }[]
}

/** Serialize details to their wire shape (base64 value strings). */
function encodeDetails(details: ErrorDetail[]): { type: string; value: string }[] {
  return details.map((d) => ({ type: d.type, value: base64Encode(d.value) }))
}

/** Parse the wire details array; skips malformed entries (matrix M7). */
function decodeDetails(v: unknown): ErrorDetail[] | undefined {
  if (!Array.isArray(v)) return undefined
  const out: ErrorDetail[] = []
  for (const el of v) {
    if (typeof el !== 'object' || el === null) continue
    const t = (el as { type?: unknown }).type
    const val = (el as { value?: unknown }).value
    if (typeof t !== 'string' || t === '' || typeof val !== 'string' || val === '') continue
    // Strict base64: invalid chars / bad padding => skip the entry (M7).
    if (!/^[A-Za-z0-9+/]*={0,2}$/.test(val) || (val.length & 3) !== 0) continue
    out.push({ type: t, value: base64Decode(val) })
  }
  return out.length > 0 ? out : undefined
}

/** Build the Connect unary error body from a code + message. */
export function encodeErrorJson(code: number, message: string, details?: ErrorDetail[]): Bytes {
  const body: ConnectErrorBody = { code: codeToString(code), message }
  if (details !== undefined && details.length > 0) body.details = encodeDetails(details)
  return new TextEncoder().encode(JSON.stringify(body))
}

/** Parse a Connect unary error body. Tolerates the legacy plain-text body and
 *  the lossy `connect-code`/`connect-error` headers for backward compatibility.
 *  Returns null when the response is not an error. */
export function decodeErrorJson(
  status: number,
  headers: Headers,
  body: Bytes,
): RPCError | null {
  if (status < 300) return null
  // Prefer the exact code in the response header (legacy servers), then the
  // Connect JSON body, then fall back to the lossy status mapping.
  const hdrCode = headers['connect-code']?.[0]
  if (hdrCode !== undefined) {
    const c = Number.parseInt(hdrCode, 10)
    if (Number.isFinite(c)) {
      // The header carries the exact code; the JSON body (when present) may
      // still carry details — merge them (details never travel in headers).
      let details: ErrorDetail[] | undefined
      if (body.length > 0) {
        try {
          const o = JSON.parse(new TextDecoder().decode(body)) as ConnectErrorBody
          if (o !== null && typeof o === 'object') details = decodeDetails(o.details)
        } catch { /* not JSON */ }
      }
      return new RPCError(c, headers['connect-error']?.[0] ?? '', details)
    }
  }
  if (body.length > 0) {
    try {
      const obj = JSON.parse(new TextDecoder().decode(body)) as ConnectErrorBody
      if (obj !== null && typeof obj === 'object' && typeof obj.code === 'string') {
        return new RPCError(
          codeFromString(obj.code),
          typeof obj.message === 'string' ? obj.message : '',
          decodeDetails(obj.details),
        )
      }
    } catch {
      /* fall through */
    }
  }
  const text = body.length > 0 ? new TextDecoder().decode(body) : ''
  return new RPCError(connectFromStatus(status), text)
}

function connectFromStatus(status: number): number {
  switch (status) {
    case 400: return 3
    case 404: return 5
    case 403: return 7
    case 401: return 16
    case 429: return 8
    case 503: return 14
    case 409: return 10
    case 504: return 4
    case 501: return 12
    case 499: return 1
    default: return 13
  }
}

// ---- framing ----
const FLAG_COMPRESSED = 0x01
const FLAG_END_STREAM = 0x02

/** Default maximum message/frame payload size (Connect's read/writeMaxBytes default). */
export const DEFAULT_MAX_MESSAGE_BYTES = 4 * 1024 * 1024

/** The Connect protocol-version header. */
export const HEADER_PROTOCOL_VERSION = 'connect-protocol-version'

/** Compression negotiation headers (Connect naming). */
export const HEADER_STREAM_ACCEPT_ENCODING = 'connect-accept-encoding'
export const HEADER_STREAM_CONTENT_ENCODING = 'connect-content-encoding'
export const HEADER_ACCEPT_ENCODING = 'accept-encoding'
export const HEADER_CONTENT_ENCODING = 'content-encoding'
export const ENCODING_GZIP = 'gzip'
/** Compress only messages at or above this size (Connect's compressMinBytes). */
export const COMPRESS_MIN_BYTES = 1024

/** Current Connect protocol version we speak. */
export const CONNECT_PROTOCOL_VERSION = '1'

/** Content types for the two shapes (proto only). */
export const CONTENT_TYPE_UNARY = 'application/proto'
export const CONTENT_TYPE_STREAM = 'application/connect+proto'

/** Encode a single streaming frame. */
export function frame(
  payload: Bytes,
  endStream = false,
  maxBytes = DEFAULT_MAX_MESSAGE_BYTES,
  compressed = false,
): Bytes {
  if (payload.length > maxBytes) throw new RPCError(8, `message too large: ${payload.length} > ${maxBytes}`)
  const flags = (endStream ? FLAG_END_STREAM : 0) | (compressed ? FLAG_COMPRESSED : 0)
  const out = new Uint8Array(5 + payload.length)
  out[0] = flags
  const dv = new DataView(out.buffer)
  dv.setUint32(1, payload.length, false)
  out.set(payload, 5)
  return out
}

/** Read a frame from an async iterator of byte chunks. Yields {payload, end}. */
export async function* readFrames(
  chunks: AsyncIterable<Bytes>,
  maxBytes = DEFAULT_MAX_MESSAGE_BYTES,
  /** Decompress a flagged payload (identity when omitted). */
  decompress: (payload: Bytes) => Bytes = (p) => p,
): AsyncGenerator<{ payload: Bytes; end: boolean }, void> {
  // accumulate raw bytes
  let acc = new Uint8Array(0) as Bytes
  for await (const c of chunks) {
    acc = concat(acc, c)
    for (;;) {
      if (acc.length < 5) break
      const flags = acc[0] ?? 0
      const len = new DataView(acc.buffer, acc.byteOffset, acc.byteLength).getUint32(1, false)
      if (len > maxBytes) throw new RPCError(8, `frame too large: ${len} > ${maxBytes}`)
      if (acc.length < 5 + len) break
      let payload: Bytes = acc.slice(5, 5 + len)
      acc = acc.slice(5 + len)
      if ((flags & FLAG_COMPRESSED) !== 0) payload = decompress(payload)
      yield { payload, end: (flags & FLAG_END_STREAM) !== 0 }
    }
  }
  // A partial frame at EOF means the body was truncated mid-frame (matrix M8):
  // treat it as corruption, never as a clean end.
  if (acc.length > 0) throw new RPCError(13, `truncated frame: ${acc.length} trailing bytes`)
}

function concat(a: Bytes, b: Bytes): Bytes {
  const out = new Uint8Array(a.length + b.length) as Bytes
  out.set(a, 0)
  out.set(b, a.length)
  return out
}

// ---- deadline (Connect-Timeout-Ms) ----

/** The Connect request-timeout header. */
export const HEADER_TIMEOUT = 'connect-timeout-ms'

/** Attach a deadline to a request (header + local enforcement). */
export function withTimeout(req: Request, timeoutMs: number): Request {
  if (timeoutMs <= 0) return req
  return {
    ...req,
    headers: { ...req.headers, [HEADER_TIMEOUT]: [String(Math.ceil(timeoutMs))] },
  }
}

/** Parse the Connect timeout header into milliseconds (0 = no deadline). */
export function parseTimeout(value: string | undefined): number {
  if (value === undefined) return 0
  const n = Number(value)
  return Number.isFinite(n) && n > 0 ? Math.ceil(n) : 0
}

/** Run `run` under an abort signal that fires after timeoutMs (0 = none). */
export function deadlineSignal(timeoutMs: number): { signal?: AbortSignal; cancel: () => void } {
  if (timeoutMs <= 0) return { cancel: () => {} }
  const ctrl = new AbortController()
  const t = setTimeout(() => ctrl.abort(new RPCError(4, 'deadline exceeded')), timeoutMs)
  return { signal: ctrl.signal, cancel: () => clearTimeout(t) }
}

/** Build the default gRPC-style path for a method. */
export function urlFor(pkg: string, service: string, method: string): string {
  return `/${pkg}.${service}/${method}`
}

/** MethodSpec mirrors what the generator emits (no HTTP verb / body binding:
 *  easy-rpc v2 is POST-only with the gRPC-style path). */
export interface MethodSpec {
  service: string
  name: string
  path: string
  clientStream: boolean
  serverStream: boolean
}

/** ServiceDesc is the runtime descriptor for a generated service. */
export interface ServiceDesc {
  typeName: string
  methods: MethodSpec[]
}

// ---- content type ----

/** True when the header value is the unary proto content type. */
export function isUnaryContentType(v: string): boolean {
  return v.split(';')[0]!.trim().toLowerCase() === CONTENT_TYPE_UNARY
}

/** True when the header value is the streaming proto content type. */
export function isStreamContentType(v: string): boolean {
  return v.split(';')[0]!.trim().toLowerCase() === CONTENT_TYPE_STREAM
}

// ---- trailer mux/demux (unary: `trailer-` response-header prefix) ----

const TRAILER_PREFIX = 'trailer-'

/** Split response headers into (headers, trailers) by the `trailer-` prefix.
 *  Case-insensitive, matching Connect. */
export function demuxTrailers(headers: Headers): { headers: Headers; trailers: Headers } {
  const h: Headers = {}
  const t: Headers = {}
  for (const [k, v] of Object.entries(headers)) {
    const lk = k.toLowerCase()
    if (lk.startsWith(TRAILER_PREFIX)) {
      t[lk.slice(TRAILER_PREFIX.length)] = v
    } else {
      h[k] = v
    }
  }
  return { headers: h, trailers: t }
}

/** Merge a trailer map into response headers using the `trailer-` prefix. */
export function muxTrailers(headers: Headers, trailers: Headers): Headers {
  const out: Headers = { ...headers }
  for (const [k, v] of Object.entries(trailers)) {
    out[`${TRAILER_PREFIX}${k.toLowerCase()}`] = v
  }
  return out
}

// ---- server dispatch surface ----

/**
 * Per-RPC context handed to generated handlers. Provides the request metadata
 * and a channel to set trailing metadata.
 */
export interface HandlerContext {
  /** Request metadata (HTTP headers). */
  readonly headers: Headers
  /** Set a trailing-metadata entry. For unary RPCs it is emitted as a
   *  `trailer-<key>` response header; for server-streams it is carried in the
   *  END frame's end-stream JSON `metadata`. */
  setTrailer(key: string, value: string): void
}

export interface ServiceHandlers {
  unary: Record<string, (input: Bytes, ctx: HandlerContext) => Promise<Bytes>>
  stream: Record<string, (input: Bytes, ctx: HandlerContext, emit: (data: Bytes, end: boolean) => Promise<void>) => Promise<void>>
}

/**
 * Server-side response sink. The dispatch core PUSHES bytes into a writer
 * instead of returning a buffered body, so server-stream responses are written
 * frame-by-frame and flushed by the runtime adapter as they are produced.
 *
 * Connect semantics: a server-stream response is always HTTP 200; a failure is
 * carried in the END frame, never as an HTTP status. Unary responses are fully
 * resolved before `status`/`write` are called, so a thrown error still surfaces
 * as a real non-200 status.
 */
export interface ResponseWriter {
  /** Set the HTTP status (called before the first `write`). */
  status(code: number): void
  /** Set a response header (called before the first `write`). */
  header(name: string, value: string): void
  /** Write raw response bytes (already framed for stream methods). */
  write(chunk: Bytes): Promise<void>
  /** Finish the response. */
  finish(): Promise<void>
}

/** Server dispatch function: decodes the request, runs the handler, pushes the
 *  response into the writer. */
export type ServerDispatch = (req: Request, w: ResponseWriter) => Promise<void>

/** Method shape the server dispatches on (all POST, path-keyed). */
export interface ServerMethodSpec {
  path: string
  name: string
  serverStream: boolean
}

/** Encode an error/trailer into a stream END frame payload (Connect end-stream
 *  JSON). A clean end with no metadata still serializes as `{}` — Connect's
 *  parser requires valid JSON on the END frame. */
export function encodeEndStream(
  code: number,
  message: string,
  metadata?: Headers,
  details?: ErrorDetail[],
): Bytes {
  const obj: {
    error?: { code: string; message: string; details?: { type: string; value: string }[] }
    metadata?: Headers
  } = {}
  if (code !== 0) {
    obj.error = { code: codeToString(code), message }
    if (details !== undefined && details.length > 0) obj.error.details = encodeDetails(details)
  }
  if (metadata !== undefined) {
    const md: Headers = {}
    for (const [k, v] of Object.entries(metadata)) if (v.length > 0) md[k] = v
    if (Object.keys(md).length > 0) obj.metadata = md
  }
  return new TextEncoder().encode(JSON.stringify(obj))
}

/** Decode an END frame payload (Connect end-stream JSON). Returns null for a
 *  clean end (empty payload) or malformed input. */
export function decodeEndStream(payload: Bytes): {
  code: number
  message: string
  metadata?: Headers
  details?: ErrorDetail[]
} | null {
  if (payload.length === 0) return null
  let obj: unknown
  try {
    obj = JSON.parse(new TextDecoder().decode(payload))
  } catch {
    return null
  }
  if (typeof obj !== 'object' || obj === null) return null
  const err = (obj as { error?: unknown }).error
  const md = (obj as { metadata?: unknown }).metadata
  const metadata: Headers | undefined =
    typeof md === 'object' && md !== null
      ? (md as Headers)
      : undefined
  if (typeof err !== 'object' || err === null) {
    return metadata !== undefined ? { code: 0, message: '', metadata } : null
  }
  const name = (err as { code?: unknown }).code
  const message = (err as { message?: unknown }).message
  const details = decodeDetails((err as { details?: unknown }).details)
  return {
    code: typeof name === 'string' ? codeFromString(name) : 2,
    message: typeof message === 'string' ? message : '',
    ...(details !== undefined ? { details } : {}),
    ...(metadata !== undefined ? { metadata } : {}),
  }
}

/**
 * Turn a framed response into a payload stream, capturing trailing metadata.
 * A non-empty END payload with `error` throws; `metadata` is captured for
 * `trailers()`. A stream that ends WITHOUT an END frame is truncated (F2).
 */
export function streamPayloads(
  framed: AsyncIterable<{ payload: Bytes; end: boolean }>,
): { payloads: AsyncGenerator<Bytes, void>; trailers: () => Headers } {
  let trailers: Headers = {}
  const payloads = (async function* (): AsyncGenerator<Bytes, void> {
    let sawEnd = false
    for await (const f of framed) {
      if (f.end) {
        sawEnd = true
        const es = decodeEndStream(f.payload)
        if (es !== null) {
          if (es.metadata !== undefined) trailers = es.metadata
          if (es.code !== 0) throw new RPCError(es.code, es.message, es.details)
        }
        return
      }
      yield f.payload
    }
    if (!sawEnd) throw new RPCError(13, 'stream ended without END frame')
  })()
  return { payloads, trailers: () => trailers }
}

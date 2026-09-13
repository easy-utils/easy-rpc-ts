// easy-rpc TypeScript core: zero-runtime-bindings Transport interface + the
// Connect wire protocol (unary + server-stream). Bridges (fetch/node) adapt a
// concrete HTTP runtime to Transport; protocol logic is runtime-agnostic.

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

/** MetadataTransport decorates a Transport with fixed metadata headers. */
export function createMetadataTransport(metadata: Headers, transport: Transport): Transport {
  return {
    async send(req: Request): Promise<Response> {
      return transport.send(withMetadata(metadata, req))
    },
    async openStream(req: Request): Promise<Stream> {
      return transport.openStream(withMetadata(metadata, req))
    },
  }
}

/** A normalized RPC request. */
export interface Request {
  url: string
  method: string // GET / POST / ...
  headers: Headers
  body: Bytes | undefined
}

/** A normalized response. */
export interface Response {
  status: number
  headers: Headers
  body: Bytes
  trailers?: Headers
  error?: RPCError
}

/** Wire-level error with a Connect code. */
export class RPCError extends Error {
  constructor(public code: number, message: string) {
    super(message)
    this.name = 'RPCError'
  }
}

/** Server-stream response: async iterable of raw message bytes + cancel(). */
export interface Stream extends AsyncIterable<Bytes> {
  cancel(): void
}

/** The core interface a bridge must implement. */
export interface Transport {
  send(req: Request): Promise<Response>
  openStream(req: Request): Promise<Stream>
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

/** Encode a single streaming frame. */
export function frame(payload: Bytes, endStream = false): Bytes {
  const flags = endStream ? FLAG_END_STREAM : 0
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
): AsyncGenerator<{ payload: Bytes; end: boolean }, void> {
  // accumulate raw bytes
  let acc = new Uint8Array(0) as Bytes
  for await (const c of chunks) {
    acc = concat(acc, c)
    for (;;) {
      if (acc.length < 5) break
      const flags = acc[0] ?? 0
      const len = new DataView(acc.buffer, acc.byteOffset, acc.byteLength).getUint32(1, false)
      if (len > 64 * 1024 * 1024) throw new RPCError(13, 'frame too large')
      if (acc.length < 5 + len) break
      const payload = acc.slice(5, 5 + len)
      acc = acc.slice(5 + len)
      yield { payload, end: (flags & FLAG_END_STREAM) !== 0 }
    }
  }
}

function concat(a: Bytes, b: Bytes): Bytes {
  const out = new Uint8Array(a.length + b.length) as Bytes
  out.set(a, 0)
  out.set(b, a.length)
  return out
}

/** Build the default gRPC-style path for a method. */
export function urlFor(pkg: string, service: string, method: string): string {
  return `/${pkg}.${service}/${method}`
}

/** MethodSpec mirrors what the generator emits. */
export interface MethodSpec {
  service: string
  name: string
  path: string
  httpMethod: string
  clientStream: boolean
  serverStream: boolean
  body: string
}

/** ServiceDesc is the runtime descriptor for a generated service. */
export interface ServiceDesc {
  typeName: string
  methods: MethodSpec[]
}

// ---- JSON helpers ----
const enc = new TextEncoder()
const dec = new TextDecoder()
export function toBytes(v: unknown): Bytes {
  return enc.encode(typeof v === 'string' ? v : JSON.stringify(v))
}
export function fromBytesToJson(b: Bytes): unknown {
  return JSON.parse(dec.decode(b))
}

// ---- content negotiation (shared by client + server; no runtime deps) ----
export type ContentKind = 'proto' | 'json'

export interface ServiceHandlers {
  unary: Record<string, (input: Bytes, kind: ContentKind) => Promise<Bytes>>
  stream: Record<string, (input: Bytes, kind: ContentKind, emit: (data: Bytes, end: boolean) => Promise<void>) => Promise<void>>
}

/**
 * Server-side response sink. The dispatch core PUSHES bytes into a writer
 * instead of returning a buffered body, so server-stream responses are written
 * frame-by-frame and flushed by the runtime adapter (node:http, node:http2,
 * fetch/Web, ...) as they are produced. This is the whole point of the easy-rpc
 * server model: never buffer a stream.
 *
 * Connect semantics: a server-stream response is always HTTP 200; a failure is
 * carried in the END frame, never as an HTTP status. Unary responses are fully
 * resolved before `status`/`write` are called, so a thrown error still surfaces
 * as a real non-200 status (the core maps it before the first write).
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

/** Method shape the server dispatches on. */
export interface ServerMethodSpec {
  path: string
  name: string
  serverStream: boolean
}

export function detectKind(req: Request): ContentKind {
  const ct = req.headers['content-type']?.[0] ?? ''
  const ac = req.headers['accept']?.[0] ?? ''
  if (ct.startsWith('application/json') || ac.startsWith('application/json')) return 'json'
  return 'proto'
}

/** Encode an error into a stream END frame payload (`<code byte>\x00<message>`),
 *  matching the Go/Rust/Python encoders. Clients that understand it surface the
 *  error; clients that don't still see a clean END. */
export function encodeEndStream(code: number, message: string): Bytes {
  const msg = new TextEncoder().encode(message)
  const out = new Uint8Array(2 + msg.length)
  out[0] = code & 0xff
  out[1] = 0
  out.set(msg, 2)
  return out
}

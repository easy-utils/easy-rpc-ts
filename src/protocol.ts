// easy-rpc TypeScript core: zero-runtime-bindings Transport interface + the
// Connect wire protocol (unary + server-stream). Bridges (fetch/node) adapt a
// concrete HTTP runtime to Transport; protocol logic is runtime-agnostic.

/** Multi-value headers. */
export type Bytes = Uint8Array<ArrayBufferLike>
export type Headers = Record<string, string[]>

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
    case 3: return 400
    case 5: return 404
    case 7: return 403
    case 8: return 429
    case 16: return 401
    case 14: return 503
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
      const flags = acc[0]
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

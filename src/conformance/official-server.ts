// Official ConnectRPC conformance server (easy-rpc), server-under-test mode.
//
// Reads a size-prefixed ServerCompatRequest from stdin, starts an easy-rpc
// HTTP server on an ephemeral port implementing the official
// connectrpc.conformance.v1.ConformanceService, then writes the
// ServerCompatResponse to stdout. This lets the real `connectconformance`
// runner validate our Connect subset (proto codec, unary + server-stream).
//
// By design we do NOT implement ClientStream / BidiStream (easy-rpc has no
// client/bidi streaming). The harness config skips those stream types.
import { create as createMsg, fromBinary, toBinary, type MessageInitShape } from '@bufbuild/protobuf'
import { anyPack, type Any } from '@bufbuild/protobuf/wkt'
import {
  ConformanceServiceHandlers,
  methodSpecs,
  type ConformanceServiceServiceImpl,
} from '../connectrpc/conformance/v1/service_easyrpc.js'
import {
  ServerCompatRequestSchema,
  ServerCompatResponseSchema,
} from '../connectrpc/conformance/v1/server_compat_pb.js'
import {
  ConformancePayloadSchema,
  ConformancePayload_RequestInfoSchema,
  HeaderSchema,
  UnaryRequestSchema,
  ServerStreamRequestSchema,
  type ConformancePayload,
  type Error as ConfError,
  type Header,
  type UnaryRequest,
  type ServerStreamRequest,
  type IdempotentUnaryRequest,
  type IdempotentUnaryResponse,
} from '../connectrpc/conformance/v1/service_pb.js'
import { RPCError, type Headers, type HandlerContext } from '../protocol.js'
import { UnaryResponseSchema, ServerStreamResponseSchema } from '../connectrpc/conformance/v1/service_pb.js'
import { HTTPVersion } from '../connectrpc/conformance/v1/config_pb.js'
import { createServer, http2Server, nodeServer } from '../server.js'

const HEADER_TIMEOUT = 'connect-timeout-ms'

// ---- size-prefixed protobuf stdin/stdout framing (testing_servers.md) ----
function takeSizePrefixed(buf: Uint8Array<ArrayBuffer>): { msg: Uint8Array<ArrayBuffer>; rest: Uint8Array<ArrayBuffer> } | null {
  if (buf.length < 4) return null
  const len = new DataView(buf.buffer, buf.byteOffset, buf.byteLength).getUint32(0, false)
  if (buf.length < 4 + len) return null
  return { msg: buf.slice(4, 4 + len) as Uint8Array<ArrayBuffer>, rest: buf.slice(4 + len) as Uint8Array<ArrayBuffer> }
}
function writeSizePrefixed(bytes: Uint8Array): void {
  const out = new Uint8Array(4 + bytes.length)
  new DataView(out.buffer).setUint32(0, bytes.length, false)
  out.set(bytes, 4)
  process.stdout.write(Buffer.from(out))
}

function toProtoHeaders(h: Headers): Header[] {
  const out: Header[] = []
  for (const [k, vs] of Object.entries(h)) {
    for (const v of vs) out.push(createMsg(HeaderSchema, { name: k, value: [v] }))
  }
  return out
}
function applyHeaders(list: Header[], add: (k: string, v: string) => void): void {
  for (const h of list) for (const v of h.value) add(h.name, v)
}
function makeRequestInfo(headers: Headers, requestAny: Any[]): ConformancePayload['requestInfo'] {
  const raw = headers[HEADER_TIMEOUT]?.[0]
  const n = raw !== undefined ? Number(raw) : Number.NaN
  const timeoutMs = Number.isFinite(n) && n >= 0 ? BigInt(Math.trunc(n)) : undefined
  return createMsg(ConformancePayload_RequestInfoSchema, {
    requestHeaders: toProtoHeaders(headers),
    requests: requestAny,
    ...(timeoutMs !== undefined ? { timeoutMs } : {}),
  })
}
function toRpcError(err: ConfError, info: ConformancePayload['requestInfo'] | undefined): RPCError {
  // Connect's wire `type` is the BARE type name (the part after the last '/'),
  // not the full type URL (see connect-go typeNameForURL).
  const bare = (url: string) => url.slice(url.lastIndexOf('/') + 1)
  const details = err.details.map((a) => ({ type: bare(a.typeUrl), value: a.value }))
  // The runner expects the request info echoed in error details.
  if (info !== undefined) details.push({ type: 'connectrpc.conformance.v1.ConformancePayload.RequestInfo', value: toBinary(ConformancePayload_RequestInfoSchema, info) })
  return new RPCError(err.code as unknown as number, err.message ?? '', details.length > 0 ? details : undefined)
}

async function startServer(reqBytes: Uint8Array): Promise<void> {
  const req = fromBinary(ServerCompatRequestSchema, reqBytes)

  const impl: ConformanceServiceServiceImpl = {
    async unary(r: UnaryRequest, ctx: HandlerContext): Promise<MessageInitShape<typeof UnaryResponseSchema>> {
      return doUnaryAsync(r, ctx)
    },
    async idempotentUnary(r: IdempotentUnaryRequest, ctx: HandlerContext): Promise<IdempotentUnaryResponse> {
      return doUnaryAsync(r as unknown as UnaryRequest, ctx) as unknown as IdempotentUnaryResponse
    },
    serverStream(r: ServerStreamRequest, ctx: HandlerContext): AsyncIterable<MessageInitShape<typeof ServerStreamResponseSchema>> {
      return doServerStream(r, ctx)
    },
    async clientStream(): Promise<never> {
      throw new RPCError(12, 'client streaming is not supported by easy-rpc')
    },
    bidiStream(): AsyncIterable<never> {
      throw new RPCError(12, 'bidi streaming is not supported by easy-rpc')
    },
    async unimplemented(): Promise<never> {
      throw new RPCError(12, 'unimplemented')
    },
  }

  const handlers = ConformanceServiceHandlers(impl)
  const dispatch = createServer(
    methodSpecs.map((m) => ({ path: m.path, name: m.name, serverStream: m.serverStream })),
    handlers,
  )
  // HTTP/1.1 and cleartext HTTP/2 (h2c) cannot share one socket in Node, so
  // pick the server by the requested minimum version. The runner uses
  // HTTP/2 prior-knowledge for h2c cases.
  const wantsH2 = req.httpVersion === HTTPVersion.HTTP_VERSION_2
  const server = wantsH2 ? http2Server(dispatch) : nodeServer(dispatch)
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', () => resolve()))
  const port = (server.address() as { port: number }).port

  const resp = createMsg(ServerCompatResponseSchema, { host: '127.0.0.1', port, pemCert: new Uint8Array(0) as Uint8Array<ArrayBuffer> })
  writeSizePrefixed(toBinary(ServerCompatResponseSchema, resp))
  void req
}

async function delay(ms: number): Promise<void> {
  if (ms > 0) await new Promise((r) => setTimeout(r, ms))
}

async function doUnaryAsync(r: UnaryRequest, ctx: HandlerContext): Promise<MessageInitShape<typeof UnaryResponseSchema>> {
  const resp = doUnary(r, ctx)
  if (r.responseDefinition !== undefined) await delay(r.responseDefinition.responseDelayMs)
  return resp
}

function doUnary(r: UnaryRequest, ctx: HandlerContext): MessageInitShape<typeof UnaryResponseSchema> {
  const reqAny = anyPack(UnaryRequestSchema, r)
  const info = makeRequestInfo(ctx.headers, [reqAny])
  const def = r.responseDefinition
  if (def === undefined) {
    return { payload: createMsg(ConformancePayloadSchema, { requestInfo: info }) }
  }
  applyHeaders(def.responseHeaders, (k, v) => ctx.setHeader(k, v))
  applyHeaders(def.responseTrailers, (k, v) => ctx.setTrailer(k, v))
  if (def.response.case === 'error') {
    throw toRpcError(def.response.value, info)
  }
  const data = def.response.case === 'responseData' ? def.response.value : (new Uint8Array(0) as Uint8Array<ArrayBuffer>)
  return { payload: createMsg(ConformancePayloadSchema, { requestInfo: info, data }) }
}

async function* doServerStream(r: ServerStreamRequest, ctx: HandlerContext): AsyncIterable<MessageInitShape<typeof ServerStreamResponseSchema>> {
  const reqAny = anyPack(ServerStreamRequestSchema, r)
  const info = makeRequestInfo(ctx.headers, [reqAny])
  const def = r.responseDefinition
  if (def === undefined) return
  applyHeaders(def.responseHeaders, (k, v) => ctx.setHeader(k, v))
  applyHeaders(def.responseTrailers, (k, v) => ctx.setTrailer(k, v))
  let first = true
  for (const data of def.responseData) {
    await delay(def.responseDelayMs)
    yield { payload: createMsg(ConformancePayloadSchema, first ? { requestInfo: info, data } : { data }) }
    first = false
  }
  if (def.error !== undefined) throw toRpcError(def.error, first ? info : undefined)
}

let acc: Uint8Array<ArrayBuffer> = new Uint8Array(0)
process.stdin.on('data', (chunk: Buffer) => {
  acc = new Uint8Array([...acc, ...chunk]) as Uint8Array<ArrayBuffer>
  for (;;) {
    const one = takeSizePrefixed(acc)
    if (one === null) break
    acc = one.rest
    void startServer(one.msg)
  }
})
process.stdin.resume()

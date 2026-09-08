// Server-side dispatch helpers for easy-rpc TS.
import type { Request, Response } from './protocol'
import { httpStatus, RPCError, frame } from './protocol'

export type Handler<Req = unknown, Res = unknown> = (req: Req) => Res | Promise<Res>
export type StreamHandler<Req = unknown, Res = unknown> = (req: Req) => AsyncIterable<Res> | Promise<AsyncIterable<Res>>

export interface ServiceImplementation {
  [method: string]: Handler | StreamHandler
}

export async function dispatchUnary<T, R>(
  svc: ServiceImplementation,
  method: string,
  requestBytes: Uint8Array,
  decode: (b: Uint8Array) => T,
  encode: (v: R) => Uint8Array,
): Promise<Response> {
  const h = svc[method]
  if (!h) return { status: 404, headers: {}, body: new Uint8Array(0), error: new RPCError(5, 'not found') }
  try {
    const out = await (h as Handler<T, R>)(decode(requestBytes))
    return { status: 200, headers: { 'content-type': ['application/proto'] }, body: encode(out) }
  } catch (e) {
    const err = e instanceof RPCError ? e : new RPCError(13, String(e))
    return { status: httpStatus(err.code), headers: {}, body: new Uint8Array(0), error: err }
  }
}

// ---- frame streaming helpers for server handlers ----
export async function* writeFrames(iter: AsyncIterable<Uint8Array>): AsyncGenerator<Uint8Array, void> {
  for await (const msg of iter) {
    yield frame(msg)
  }
}

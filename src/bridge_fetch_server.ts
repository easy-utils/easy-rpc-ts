// Web-standard Request/Response adapter for the easy-rpc push server.
//
// The easy-rpc dispatch core pushes bytes into a `ResponseWriter`. On a fetch
// runtime this adapter bridges that push into a ReadableStream: the stream is
// enqueued chunk-by-chunk (and closed at finish), so server-stream responses
// flow to the client without buffering. Status/headers are captured before the
// first chunk (the core guarantees they are set first).
import type { Request as CoreRequest, ResponseWriter, ServerDispatch } from './protocol.js'
import { Bytes, RPCError } from './protocol.js'

export type WebHandler = (req: Request) => Promise<Response>

export function toWebHandler(dispatch: ServerDispatch): WebHandler {
  return async (req: Request): Promise<Response> => {
    const body = new Uint8Array(await req.arrayBuffer()) as Bytes
    const headers: CoreRequest['headers'] = {}
    req.headers.forEach((v, k) => {
      headers[k] = headers[k] ? [...headers[k], v] : [v]
    })
    const core: CoreRequest = {
      url: new URL(req.url).pathname,
      headers,
      body,
    }

    let status = 200
    const outHeaders = new Headers()
    const holder: { c: ReadableStreamDefaultController<Uint8Array> | null } = { c: null }
    const stream = new ReadableStream<Uint8Array>({
      start(controller) { holder.c = controller },
      cancel() { holder.c = null },
    })
    let closed = false

    const w: ResponseWriter = {
      status(code) { status = code },
      header(name, value) { outHeaders.set(name, value) },
      async write(chunk) { holder.c?.enqueue(chunk as Uint8Array) },
      async finish() {
        if (closed) return
        closed = true
        holder.c?.close()
      },
    }

    // Run the dispatch; any error after status/headers were captured still
    // closes the stream (the core maps handler errors itself).
    try {
      await dispatch(core, w)
    } catch (e) {
      const err = e instanceof RPCError ? e : new RPCError(13, String(e))
      if (!closed) {
        closed = true
        holder.c?.error(err)
      }
    }
    return new Response(stream, { status, headers: outHeaders })
  }
}

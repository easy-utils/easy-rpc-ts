// Web-standard Request/Response adapter for the easy-rpc server app.
//
// ASGI-style: the easy-rpc app is `(CoreRequest) => Promise<CoreResponse>` using
// core (non-Web) types. toWebHandler adapts it to the Web standard
// `Request`/`Response` (WHATWG Fetch), so ANY fetch-compatible server/framework
// can serve it: Hono, Bun, Cloudflare Workers, Deno, etc. This is the primary
// server entry point; http2Server (node:http2, h2c) remains the only Node-native
// adapter (for cleartext HTTP/2). For plain HTTP/1, use toWebHandler + Hono.
import type { Request as CoreRequest, Response as CoreResponse } from './protocol.js'
import { Bytes } from './protocol.js'

export type WebHandler = (req: Request) => Promise<Response>

export function toWebHandler(app: (req: CoreRequest) => Promise<CoreResponse>): WebHandler {
  return async (req: Request): Promise<Response> => {
    const body = new Uint8Array(await req.arrayBuffer()) as Bytes
    const headers: CoreRequest['headers'] = {}
    req.headers.forEach((v, k) => {
      headers[k] = headers[k] ? [...headers[k], v] : [v]
    })
    const core: CoreRequest = {
      url: new URL(req.url).pathname,
      method: req.method,
      headers,
      body,
    }
    const res: CoreResponse = await app(core)
    const h = new Headers()
    for (const [k, vs] of Object.entries(res.headers)) {
      for (const v of vs) h.append(k, v)
    }
    return new Response(res.body as unknown as BodyInit, { status: res.status, headers: h })
  }
}

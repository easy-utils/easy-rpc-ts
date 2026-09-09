// Standard fetch Request/Response adapter for the easy-rpc server app.
//
// ASGI-style: the easy-rpc app is `(Request) => Promise<Response>` using core
// (non-Web) types. bindFetch adapts it to the Web standard `Request`/`Response`
// so ANY fetch-compatible server/framework can serve it (Hono, Bun,
// Cloudflare Workers, deno, etc.). This replaces nodeServer/http2Server as the
// "any backend" entry point.
import type { Request as CoreRequest, Response as CoreResponse } from './protocol.js'
import { Bytes } from './protocol.js'

export type AppHandler = (req: CoreRequest) => Promise<CoreResponse>

export function bindFetch(app: AppHandler): (req: Request, ...rest: any[]) => Promise<Response> {
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

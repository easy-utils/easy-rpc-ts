// fetch bridge for browsers. Adapts window.fetch to the Transport interface.
import type { Request, Response, Stream, Transport } from './protocol'
import { readFrames } from './protocol'

/** Build a fetch-based Transport. */
export function createFetchTransport(baseUrl = '', fetchFn: typeof fetch = fetch): Transport {
  const join = (u: string) => (baseUrl ? baseUrl.replace(/\/+$/, '') + u : u)
  return {
    async send(req: Request): Promise<Response> {
      const res = await fetchFn(join(req.url), {
        method: req.method,
        headers: headersToFetch(req.headers),
        body: req.body as unknown as BodyInit | undefined,
      })
      const body = new Uint8Array(await res.arrayBuffer())
      return { status: res.status, headers: fromFetchHeaders(res.headers), body }
    },
    async openStream(req: Request): Promise<Stream> {
      const res = await fetchFn(join(req.url), {
        method: req.method,
        headers: headersToFetch(req.headers),
        body: req.body as unknown as BodyInit | undefined,
      })
      if (!res.body) return { async *[Symbol.asyncIterator]() {}, cancel() {} }
      const reader = res.body.getReader()
      const source = (async function* () {
        // readBytes as Uint8Array chunks
        for (;;) {
          const { done, value } = await reader.read()
          if (done) return
          yield value
        }
      })()
      const framed = readFrames(source)
      return {
        async *[Symbol.asyncIterator]() {
          for await (const f of framed) {
            if (f.end) return
            yield f.payload
          }
        },
        cancel() {
          void reader.cancel()
        },
      }
    },
  }
}

function headersToFetch(h: Request['headers']): Record<string, string> {
  const out: Record<string, string> = {}
  for (const [k, v] of Object.entries(h)) out[k] = v.join(',')
  return out
}

function fromFetchHeaders(h: Headers): Request['headers'] {
  const out: Request['headers'] = {}
  h.forEach((v, k) => {
    out[k] = out[k] ? [...out[k], v] : [v]
  })
  return out
}

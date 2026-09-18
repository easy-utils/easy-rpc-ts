// fetch bridge for browsers. Adapts window.fetch to the Transport interface.
// All easy-rpc calls are POST (spec §0); the bridge always uses POST.
import type { Request, Response, Stream, Transport } from './protocol.js'
import { gzipDecompress } from './compression.js'
import { readFrames, streamPayloads, decodeErrorJson, demuxTrailers, HEADER_ACCEPT_ENCODING, HEADER_CONTENT_ENCODING, ENCODING_GZIP } from './protocol.js'

/** Build a fetch-based Transport. */
export function createFetchTransport(baseUrl = '', fetchFn: typeof fetch = fetch): Transport {
  const join = (u: string) => (baseUrl ? baseUrl.replace(/\/+$/, '') + u : u)
  const sendHeaders = (h: Request['headers']): Record<string, string> => {
    const out: Record<string, string> = {}
    for (const [k, v] of Object.entries(h)) out[k] = v.join(',')
    return out
  }
  return {
    async send(req: Request): Promise<Response> {
      const res = await fetchFn(join(req.url), {
        method: 'POST',
        headers: { ...sendHeaders(req.headers), [HEADER_ACCEPT_ENCODING]: ENCODING_GZIP },
        body: req.body as unknown as BodyInit | null,
        ...(req.signal !== undefined ? { signal: req.signal } : {}),
      })
      let body: Uint8Array = new Uint8Array(await res.arrayBuffer())
      const allHeaders = fromFetchHeaders(res.headers)
      const { headers, trailers } = demuxTrailers(allHeaders)
      if ((headers[HEADER_CONTENT_ENCODING]?.[0] ?? '') === ENCODING_GZIP && body.length > 0) {
        body = gzipDecompress(body)
      }
      const error = decodeErrorJson(res.status, headers, body)
      return { status: res.status, headers, body, trailers, ...(error !== null ? { error } : {}) }
    },
    async openStream(req: Request): Promise<Stream> {
      const res = await fetchFn(join(req.url), {
        method: 'POST',
        headers: { ...sendHeaders(req.headers), 'connect-accept-encoding': 'gzip' },
        body: req.body as unknown as BodyInit | null,
        ...(req.signal !== undefined ? { signal: req.signal } : {}),
      })
      if (!res.body) {
        const empty = streamPayloads((async function* () {})())
        return { async *[Symbol.asyncIterator]() { yield* empty.payloads }, trailers: empty.trailers, cancel() {} }
      }
      const reader = res.body.getReader()
      const source = (async function* () {
        for (;;) {
          const { done, value } = await reader.read()
          if (done) return
          yield value
        }
      })()
      const framed = readFrames(source, undefined, gzipDecompress)
      const sp = streamPayloads(framed)
      return {
        async *[Symbol.asyncIterator]() {
          yield* sp.payloads
        },
        trailers: sp.trailers,
        cancel() {
          void reader.cancel()
        },
      }
    },
  }
}

function fromFetchHeaders(h: Headers): Request['headers'] {
  const out: Request['headers'] = {}
  h.forEach((v, k) => {
    out[k] = out[k] ? [...out[k], v] : [v]
  })
  return out
}

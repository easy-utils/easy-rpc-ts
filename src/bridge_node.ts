// node bridge using built-in http2 (h2c/h2 via node:http2) or the fetch global
// on Node 18+. Adapts to the Transport interface.
import type { Request, Response, Stream, Transport } from './protocol'
import { readFrames } from './protocol'

/** Build a Node http2-based Transport (cleartext h2c / ALPN h2). */
export function createNodeTransport(): Transport {
  // Node >=18 has undici fetch which supports h2 via ALPN but cleartext h2c is
  // limited. For robustness we use http2.connect for both.
  const { connect } = nodeHttp2()
  return {
    async send(req: Request): Promise<Response> {
      const res = await requestOnce(req, connect)
      return res
    },
    async openStream(req: Request): Promise<Stream> {
      const session = connect(req.url)
      const stream = session.request(headersFor(req))
      const chunks = (async function* () {
        stream.on('data', (c: Uint8Array) => (push as any)(c))
        stream.on('end', () => (push as any)(null))
      })()
      // We drive it manually with a queue.
      const queue: Uint8Array[] = []
      let done = false
      const waiters: ((v: Uint8Array | null) => void)[] = []
      const push = (v: Uint8Array | null) => {
        if (v === null) {
          done = true
        } else {
          queue.push(v)
        }
        const w = waiters.shift()
        if (w) w(done ? null : queue.shift() ?? null)
      }
      stream.on('data', (c: Uint8Array) => push(c))
      stream.on('end', () => push(null))
      stream.on('error', () => push(null))
      stream.end(req.body ?? new Uint8Array(0))

      const source: AsyncIterable<Uint8Array> = (async function* () {
        // read top-level http2 stream 'data' chunks by subscribing push
        // A simpler approach: re-read stream as async iterable is not direct.
        yield* nodeStreamToAsync(stream)
      })()

      const framed = readFrames(source)
      const streamObj: Stream = {
        async *[Symbol.asyncIterator]() {
          for await (const f of framed) {
            if (f.end) return
            yield f.payload
          }
        },
        cancel() {
          try { session.close() } catch { /* noop */ }
        },
      }
      return streamObj
    },
  }
}

function nodeStreamToAsync(stream: any): AsyncGenerator<Uint8Array> {
  // Build an async queue from events.
  const queue: Uint8Array[] = []
  let done = false
  let error: unknown
  const waiters: ((v: IteratorResult<Uint8Array>) => void)[] = []
  const wake = () => {
    while (waiters.length) {
      const w = waiters.shift()!
      if (error) { w({ done: true, value: undefined as any }); continue }
      if (queue.length) { w({ done: false, value: queue.shift()! }); continue }
      if (done) { w({ done: true, value: undefined as any }); continue }
      break
    }
  }
  stream.on('data', (c: Uint8Array) => { queue.push(c); wake() })
  stream.on('end', () => { done = true; wake() })
  stream.on('error', (e: unknown) => { error = e; done = true; wake() })
  return (async function* () {
    for (;;) {
      if (queue.length) yield queue.shift()!
      else if (done) return
      else await new Promise<void>((resolve) => {
        waiters.push((r) => { void r; resolve() })
      })
    }
  })()
}

async function requestOnce(req: Request, connect: any): Promise<Response> {
  const session = connect(req.url)
  const stream = session.request(headersFor(req))
  const chunks: Uint8Array[] = []
  await new Promise<void>((resolve, reject) => {
    stream.on('data', (c: Uint8Array) => chunks.push(c))
    stream.on('end', resolve)
    stream.on('error', reject)
    stream.end(req.body ?? new Uint8Array(0))
  })
  session.close()
  return { status: 200, headers: {}, body: concatAll(chunks) }
}

function headersFor(req: Request): Record<string, string> {
  const out: Record<string, string> = {}
  for (const [k, v] of Object.entries(req.headers)) out[k] = v.join(',')
  out[':method'] = req.method
  out[':path'] = new URL(req.url).pathname + new URL(req.url).search
  return out
}

function concatAll(chunks: Uint8Array[]): Uint8Array {
  const total = chunks.reduce((a, c) => a + c.length, 0)
  const out = new Uint8Array(total)
  let off = 0
  for (const c of chunks) { out.set(c, off); off += c.length }
  return out
}

function nodeHttp2() {
  // eslint-disable-next-line @typescript-eslint/no-var-requires
  return require('node:http2') as { connect: any }
}

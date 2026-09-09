import { createServer } from './server.js'
import { create } from '@bufbuild/protobuf'
import { CountResponseSchema, EchoRequestSchema, EchoResponseSchema, HealthResponseSchema, FailResponseSchema } from './easyrpc/conformance/v1/conformance_pb.js'
import nodeHttp from 'node:http'

const port = Number(process.env.PORT) || 18899
const impl: any = {
  health: async () => create(HealthResponseSchema, { ok: true, name: 'conformance' }),
  echo: async (req: any) => create(EchoResponseSchema, { output: 'echo:' + req.input }),
  count: async () => {
    return {
      async *[Symbol.asyncIterator]() {
        for (let i = 0; i < 3; i++) yield create(CountResponseSchema, { index: i } as any)
      },
    }
  },
  fail: async () => create(FailResponseSchema, { ok: true }),
}
import { ConformanceServiceHandlers } from './easyrpc/conformance/v1/conformance_easyrpc.js'
const handlers = ConformanceServiceHandlers(impl)
const serverHandler = createServer([
  { path: '/v1/health', name: 'Health', serverStream: false },
  { path: '/v1/echo', name: 'Echo', serverStream: false },
  { path: '/v1/count', name: 'Count', serverStream: true },
], handlers)

// Conformance server: serve HTTP/1 on one port (the interop matrix clients use
// fetch/h1). h2c is available via the library's http2Server export.
nodeHttp.createServer(async (req, res) => {
  const body = await new Promise<Uint8Array>(resolve => { const a: Uint8Array[] = []; req.on('data', c => a.push(c)); req.on('end', () => resolve(Buffer.concat(a.map(x => Buffer.from(x))))) })
  const headers: Record<string, string[]> = {}
  for (const [k, v] of Object.entries(req.headers)) headers[k] = Array.isArray(v) ? v as string[] : [v as string]
  const out = await serverHandler({ url: req.url ?? '/', method: req.method ?? 'GET', headers, body: body as any })
  const h: Record<string, string> = {}
  for (const [k, vs] of Object.entries(out.headers)) h[k] = vs.join(',')
  res.writeHead(out.status, h)
  res.end(out.body)
}).listen(port, '127.0.0.1', () => console.log('ts on', port))

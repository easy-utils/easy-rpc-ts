import { describe, it, expect } from 'vitest'
import { create } from '@bufbuild/protobuf'
import { createServer, dispatchToResponse } from '../src/server'
import { createConformanceServiceClient, ConformanceServiceHandlers, methodSpecs } from '../src/easyrpc/conformance/v1/conformance_easyrpc'
import { EchoRequestSchema, EchoResponseSchema, CountRequestSchema, CountResponseSchema, EchoBytesRequestSchema, EchoBytesResponseSchema, FailDetailsRequestSchema } from '../src/easyrpc/conformance/v1/conformance_pb'
import { RPCError, decodeErrorJson, type Request, type Response } from '../src/protocol'
import type { ServiceHandlers } from '../src/protocol'

// In-process transport: routes client Requests straight into the server
// dispatch (no sockets), so the JSON codec path is exercised end-to-end.
function inProcess(handler: ReturnType<typeof createServer>) {
  return {
    async send(req: Request): Promise<Response> {
      const res = await dispatchToResponse(handler, req)
      const err = decodeErrorJson(res.status, res.headers, res.body)
      return err ? { ...res, error: err } : res
    },
    async openStream(req: Request) {
      // server-stream returns a buffered body of frames; the client Stream just
      // yields nothing extra here (tests use unary + explicit frame checks).
      const res = await dispatchToResponse(handler, req)
      return {
        async *[Symbol.asyncIterator]() { /* server-stream JSON covered in conformance */ },
        trailers: () => res.trailers,
        cancel() {},
      }
    },
  }
}

const impl = {
  echo: async (req: any) => ({ output: 'echo:' + req.input }),
  count: async (req: any) => ({ async *[Symbol.asyncIterator]() { const n = req.count > 0 ? req.count : 3; for (let i = 0; i < n; i++) yield { index: i } } }),
  echoBytes: async (req: any) => ({ data: req.data }),
  failDetails: async () => { throw new RPCError(8, 'limited', [{ type: 't/x', value: new Uint8Array([1, 2, 3]) }]) },
} as unknown as Parameters<typeof ConformanceServiceHandlers>[0]

describe('JSON codec (in-process)', () => {
  const handlers: ServiceHandlers = ConformanceServiceHandlers(impl)
  const dispatch = createServer(methodSpecs.map(m => ({ path: m.path, name: m.name, serverStream: m.serverStream })), handlers)
  const client = createConformanceServiceClient(inProcess(dispatch) as any)

  it('unary JSON round-trip', async () => {
    const res = await client.echo(create(EchoRequestSchema, { input: 'hi' }), { kind: 'json' })
    expect(res.output).toBe('echo:hi')
  })

  it('unary JSON error carries details', async () => {
    let err: any = null
    try { await client.failDetails(create(FailDetailsRequestSchema, { code: 8 }), { kind: 'json' }) } catch (e) { err = e }
    expect(err?.code).toBe(8)
    expect(err?.details?.[0]?.type).toBe('t/x')
  })

  it('bytes round-trip in JSON (base64)', async () => {
    const data = new Uint8Array([0, 1, 2, 255, 254, 128])
    const res = await client.echoBytes(create(EchoBytesRequestSchema, { data }), { kind: 'json' })
    expect(Array.from(res.data)).toEqual(Array.from(data))
  })

  it('content-type is application/json for JSON unary', async () => {
    let seen = ''
    const spy = {
      async send(req: Request): Promise<Response> {
        seen = req.headers['content-type']?.[0] ?? ''
        return dispatchToResponse(dispatch, req)
      },
      async openStream(req: Request) { return { async *[Symbol.asyncIterator]() {}, trailers: () => ({}), cancel() {} } },
    }
    await createConformanceServiceClient(spy as any).echo(create(EchoRequestSchema, { input: 'x' }), { kind: 'json' })
    expect(seen).toBe('application/json')
  })

  it('default codec remains proto', async () => {
    let seen = ''
    const spy = {
      async send(req: Request): Promise<Response> {
        seen = req.headers['content-type']?.[0] ?? ''
        return dispatchToResponse(dispatch, req)
      },
      async openStream(req: Request) { return { async *[Symbol.asyncIterator]() {}, trailers: () => ({}), cancel() {} } },
    }
    await createConformanceServiceClient(spy as any).echo(create(EchoRequestSchema, { input: 'x' }))
    expect(seen).toBe('application/proto')
  })
})

// easy-rpc TS: a full cross-language conformance test.
// It demonstrates unary (Echo) + server-stream (Count) against a Go server
// via the fetch transport (node global fetch), decoding with generated schemas.
import { describe, it, expect } from 'vitest'
import { createFetchTransport } from '../src'
import { readFrames } from '../src/protocol'
import { toBinary, fromBinary } from '@bufbuild/protobuf'
import {
  EchoRequestSchema, EchoResponseSchema,
  CountRequestSchema, CountResponseSchema,
} from '../src/easyrpc/conformance/v1/conformance_pb'
import { create } from '@bufbuild/protobuf'

// A tiny in-memory http server acting as the "Go" peer (Node http + http2 not
// needed; use node:http for plain chunked stream which matches connect).
function makeServer() {
  const http = require('node:http')
  return http.createServer((req, res) => {
    if (req.url === '/easyrpc.conformance.v1.ConformanceService/Echo') {
      let chunks = []
      req.on('data', c => chunks.push(c))
      req.on('end', () => {
        res.writeHead(200, { 'content-type': 'application/proto' })
        res.end(Buffer.concat(chunks)) // echo back
      })
    } else if (req.url === '/easyrpc.conformance.v1.ConformanceService/Count') {
      res.writeHead(200, { 'content-type': 'application/connect+proto' })
      // send 3 frames
      for (let i = 0; i < 3; i++) {
        const msg = toBinary(CountResponseSchema, create(CountResponseSchema, { index: i }))
        const frame = new Uint8Array(5 + msg.length)
        frame[0] = 0
        new DataView(frame.buffer).setUint32(1, msg.length, false)
        frame.set(msg, 5)
        res.write(Buffer.from(frame))
      }
      res.end()
    } else {
      res.writeHead(404)
      res.end()
    }
  })
}

describe('conformance cross-lang', () => {
  it('echo unary', async () => {
    const server = makeServer()
    await new Promise(r => server.listen(0, r))
    const port = server.address().port
    const t = createFetchTransport(fetch)
    const res = await t.send({
      url: `http://127.0.0.1:${port}/easyrpc.conformance.v1.ConformanceService/Echo`,
      method: 'POST',
      headers: { 'content-type': ['application/proto'] },
      body: toBinary(EchoRequestSchema, create(EchoRequestSchema, { input: 'hi' })),
    })
    const out = fromBinary(EchoResponseSchema, res.body)
    expect(out.output).toBe('hi')
    server.close()
  })

  it('count server-stream frames decode', async () => {
    const server = makeServer()
    await new Promise(r => server.listen(0, r))
    const port = server.address().port
    const t = createFetchTransport(fetch)
    const stream = await t.openStream({
      url: `http://127.0.0.1:${port}/easyrpc.conformance.v1.ConformanceService/Count`,
      method: 'POST',
      headers: { 'content-type': ['application/connect+proto'] },
      body: toBinary(CountRequestSchema, create(CountRequestSchema, { count: 3 })),
    })
    const idx: number[] = []
    for await (const chunk of stream) {
      idx.push(fromBinary(CountResponseSchema, chunk).index)
    }
    expect(idx).toEqual([0, 1, 2])
    server.close()
  })
})


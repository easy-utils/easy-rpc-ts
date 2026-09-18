// Error-path matrix (spec §4.2 M1–M13) + Error Details round-trip (§4.1).
// These tests construct inputs directly against the protocol functions — no
// server needed — and are mirrored in every language implementation.
import { describe, it, expect } from 'vitest'
import {
  encodeEndStream,
  decodeEndStream,
  encodeErrorJson,
  decodeErrorJson,
  streamPayloads,
  demuxTrailers,
  muxTrailers,
  frame,
  readFrames,
  RPCError,
  type ErrorDetail,
  type Headers,
} from '../src/protocol'

const enc = (s: string) => new TextEncoder().encode(s)
const detail: ErrorDetail = {
  type: 'type.googleapis.com/google.rpc.RetryInfo',
  value: new Uint8Array([1, 2, 3, 250]),
}

describe('matrix: end-stream decode', () => {
  // M1: empty payload => clean end
  it('M1 empty payload is a clean end', () => {
    expect(decodeEndStream(new Uint8Array(0))).toBeNull()
  })
  // M2: garbage bytes => treated as clean, no throw
  it('M2 non-JSON payload is not an error', () => {
    expect(decodeEndStream(new Uint8Array([0xff, 0xfe, 0x00, 0x42]))).toBeNull()
  })
  // M3: error without code/message => unknown code, empty message
  it('M3 {"error":{}} maps to code 2 / empty message', () => {
    const r = decodeEndStream(enc('{"error":{}}'))
    expect(r?.code).toBe(2)
    expect(r?.message).toBe('')
  })
  // M4: unknown code name => 2
  it('M4 unknown code name maps to 2', () => {
    const r = decodeEndStream(enc('{"error":{"code":"nope","message":"m"}}'))
    expect(r?.code).toBe(2)
    expect(r?.message).toBe('m')
  })
  // M5: unknown JSON fields are ignored
  it('M5 unknown fields ignored', () => {
    const r = decodeEndStream(enc('{"error":{"code":"not_found","message":"m"},"x":1}'))
    expect(r?.code).toBe(5)
  })
  // M6: details are decoded (base64 -> bytes)
  it('M6 details round-trip', () => {
    const payload = encodeEndStream(8, 'rate limited', undefined, [detail])
    const r = decodeEndStream(payload)
    expect(r?.code).toBe(8)
    expect(r?.details).toEqual([detail])
  })
  // M7: malformed detail entries are skipped, never crash
  it('M7 malformed details entries skipped', () => {
    const r = decodeEndStream(
      enc('{"error":{"code":"resource_exhausted","details":[{"type":"t","value":"!!!"},{"value":"x"},{"type":"ok"},{"type":"t2","value":"AQID"}]}}'),
    )
    expect(r?.details).toEqual([{ type: 't2', value: new Uint8Array([1, 2, 3]) }])
  })

  it('details omitted when empty (v1.0 byte-for-byte)', () => {
    const payload = new TextDecoder().decode(encodeEndStream(5, 'gone'))
    expect(payload).toBe('{"error":{"code":"not_found","message":"gone"}}')
  })
})

describe('matrix: framing', () => {
  // M8: truncated frame must error, not silently yield partial payload
  it('M8 truncated frame errors', async () => {
    const full = frame(new Uint8Array(10), false)
    const truncated = full.subarray(0, full.length - 4)
    const frames = readFrames((async function* () { yield truncated })())
    await expect(frames.next()).rejects.toThrow()
  })
  // M9: oversized frame is rejected
  it('M9 oversized frame rejected', async () => {
    const huge = new Uint8Array(5 + 4 * 1024 * 1024 + 1)
    new DataView(huge.buffer).setUint32(1, 4 * 1024 * 1024 + 1)
    const frames = readFrames((async function* () { yield huge })(), 4 * 1024 * 1024)
    await expect(frames.next()).rejects.toThrow(/resource_exhausted|exceeds|too large/i)
  })
})

describe('matrix: unary error decode', () => {
  // M11: legacy fallbacks — header first, then status mapping
  it('M11 legacy connect-code header wins', () => {
    const e = decodeErrorJson(429, { 'connect-code': ['7'] }, enc('plain text'))
    expect(e).toBeInstanceOf(RPCError)
    expect(e?.code).toBe(7)
  })
  it('M11b plain-text body falls back to status mapping', () => {
    const e = decodeErrorJson(503, {} as Headers, enc('busy'))
    expect(e?.code).toBe(14)
    expect(e?.message).toBe('busy')
  })
  it('unary details round-trip', () => {
    const body = encodeErrorJson(8, 'limited', [detail])
    const e = decodeErrorJson(429, {} as Headers, body)
    expect(e?.code).toBe(8)
    expect(e?.details).toEqual([detail])
  })
  it('unary body without details stays v1.0-shaped', () => {
    const body = new TextDecoder().decode(encodeErrorJson(5, 'x'))
    expect(JSON.parse(body)).toEqual({ code: 'not_found', message: 'x' })
  })
})

describe('matrix: stream throws end-stream error with details', () => {
  it('M6b streamPayloads surfaces details on the thrown RPCError', async () => {
    const src = (async function* () {
      yield { payload: encodeEndStream(8, 'rl', undefined, [detail]), end: true }
    })()
    const sp = streamPayloads(src)
    await expect(sp.payloads.next()).rejects.toMatchObject({ code: 8, details: [detail] })
  })
})

// M14: END frame metadata only => clean end + trailers (spec §3.3).
describe('matrix: streaming trailing metadata', () => {
  it('M14 END metadata becomes trailers on a clean end', async () => {
    const src = (async function* () {
      yield { payload: new TextEncoder().encode('hi'), end: false }
      yield { payload: encodeEndStream(0, '', { 'x-trl': ['v1', 'v2'] }), end: true }
    })()
    const sp = streamPayloads(src)
    const out: Uint8Array[] = []
    for await (const p of sp.payloads) out.push(p)
    expect(out.length).toBe(1)
    expect(sp.trailers()).toEqual({ 'x-trl': ['v1', 'v2'] })
  })
})

// M15: unary `trailer-*` response headers are demuxed (spec §3.3).
describe('matrix: unary trailer demux', () => {
  it('M15 trailer- prefixed headers become trailers', () => {
    const { headers, trailers } = demuxTrailers({
      'content-type': ['application/proto'],
      'trailer-x-trl': ['a'],
      'Trailer-Y': ['b'],
    })
    expect(headers).toEqual({ 'content-type': ['application/proto'] })
    expect(trailers).toEqual({ 'x-trl': ['a'], y: ['b'] })
  })
  it('M15b muxTrailers prefixes entries', () => {
    expect(muxTrailers({ a: ['1'] }, { X: ['2'] })).toEqual({ a: ['1'], 'trailer-x': ['2'] })
  })
})

// M12/M13 (local + server deadline) are covered in stream.test.ts / server tests.
describe('matrix: deadline codes', () => {
  it('M12/M13 deadline maps to code 4', () => {
    expect(decodeErrorJson(504, {} as Headers, encodeErrorJson(4, 'deadline exceeded'))?.code).toBe(4)
  })
})

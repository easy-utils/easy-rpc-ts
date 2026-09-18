// Wire golden-vector conformance: the protocol layer (transport-independent)
// must reproduce the spec's byte-level vectors exactly. This is the only check
// that can catch a same-source bug shared by two implementations.
//
// Vectors: easy-rpc-spec/conformance/wire-vectors.json (generated + verified by
// the TS reference; all languages consume the SAME file).
import { describe, it, expect } from 'vitest'
import { readFileSync } from 'node:fs'
import { resolve } from 'node:path'
import {
  frame,
  encodeEndStream,
  decodeEndStream,
  encodeErrorJson,
  demuxTrailers,
  muxTrailers,
  codeToString,
  codeFromString,
  httpStatus,
  DEFAULT_MAX_MESSAGE_BYTES,
} from '../src/protocol'

const V = JSON.parse(
  readFileSync(resolve(process.cwd(), '../easy-rpc-spec/conformance/wire-vectors.json'), 'utf8'),
) as {
  frames: { name: string; encode: { payloadHex: string; end: boolean; compressed: boolean }; bytesHex: string }[]
  endStream: {
    name: string
    encode?: { code: number; message: string; metadata?: Record<string, string[]> }
    decode: { bytesHex: string }
    bytesHex?: string
    code: number
    message: string
    details: null
    metadata: Record<string, string[]> | null
  }[]
  unaryError: { name: string; encode: { code: number; message: string; details?: { type: string; valueHex: string }[] }; bytesHex: string }[]
  trailerHeaders: {
    name: string
    demux?: Record<string, string[]>
    headers?: Record<string, string[]>
    trailers?: Record<string, string[]>
    mux?: { headers: Record<string, string[]>; trailers: Record<string, string[]> }
    result?: Record<string, string[]>
  }[]
  codeNames: { code: number; name: string; http: number }[]
}

const hex = (b: Uint8Array) => Buffer.from(b).toString('hex')
const bytes = (h: string) => new Uint8Array(Buffer.from(h, 'hex'))

describe('wire vectors: frames', () => {
  for (const f of V.frames) {
    it(f.name, () => {
      // Bytes are byte-exact: the envelope is [flags][len32be][payload]. The
      // compressed case only sets the flag bit (the payload is pre-compressed).
      const raw = frame(bytes(f.encode.payloadHex), f.encode.end, DEFAULT_MAX_MESSAGE_BYTES, false)
      if (f.encode.compressed) raw[0] = (raw[0] ?? 0) | 0x01
      expect(hex(raw)).toBe(f.bytesHex)
    })
  }
})

describe('wire vectors: end-stream', () => {
  for (const e of V.endStream) {
    it(`${e.name} decode`, () => {
      const got = decodeEndStream(bytes(e.decode.bytesHex))
      if (e.code === 0 && e.message === '' && e.details === null && e.metadata === null) {
        // clean end: null or a zero value
        if (got !== null) {
          expect(got.code).toBe(0)
          expect(Object.keys(got.metadata ?? {})).toHaveLength(0)
        }
      } else {
        expect(got).not.toBeNull()
        expect(got!.code).toBe(e.code)
        expect(got!.message).toBe(e.message)
        if (e.metadata) expect(got!.metadata).toEqual(e.metadata)
      }
    })
    if (e.encode && e.bytesHex) {
      it(`${e.name} encode`, () => {
        const got = encodeEndStream(e.encode!.code, e.encode!.message, undefined, e.encode!.metadata)
        // JSON payloads are compared SEMANTICALLY (key order is not significant).
        expect(JSON.parse(new TextDecoder().decode(got))).toEqual(
          JSON.parse(new TextDecoder().decode(bytes(e.bytesHex!))),
        )
      })
    }
  }
})

describe('wire vectors: unary error json', () => {
  for (const u of V.unaryError) {
    it(u.name, () => {
      const details = (u.encode.details ?? []).map((d) => ({ type: d.type, value: bytes(d.valueHex) }))
      const got = encodeErrorJson(u.encode.code, u.encode.message, details.length ? details : undefined)
      expect(JSON.parse(new TextDecoder().decode(got))).toEqual(
        JSON.parse(new TextDecoder().decode(bytes(u.bytesHex))),
      )
    })
  }
})

describe('wire vectors: trailer mux/demux', () => {
  for (const t of V.trailerHeaders) {
    it(t.name, () => {
      if (t.demux) {
        const r = demuxTrailers(t.demux)
        expect(r.headers).toEqual(t.headers)
        expect(r.trailers).toEqual(t.trailers)
      }
      if (t.mux) {
        expect(muxTrailers(t.mux.headers, t.mux.trailers)).toEqual(t.result)
      }
    })
  }
})

describe('wire vectors: code/http mapping', () => {
  for (const c of V.codeNames) {
    it(`code ${c.code} -> ${c.name}`, () => {
      expect(codeToString(c.code)).toBe(c.name)
      expect(codeFromString(c.name)).toBe(c.code)
      if (c.code !== 0) expect(httpStatus(c.code)).toBe(c.http)
    })
  }
})

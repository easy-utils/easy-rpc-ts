// Optional gzip support. Core framing stays dependency-free; this module is
// imported only by runtimes that can provide deflate (node:zlib, etc).
import { deflateRawSync, inflateRawSync } from 'node:zlib'
import type { Bytes } from './protocol.js'

/** gzip-compress bytes (raw deflate). */
export function gzipCompress(data: Bytes): Bytes {
  return new Uint8Array(deflateRawSync(Buffer.from(data)))
}

/** gzip-decompress bytes (raw deflate). */
export function gzipDecompress(data: Bytes): Bytes {
  return new Uint8Array(inflateRawSync(Buffer.from(data)))
}

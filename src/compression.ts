// Optional gzip support. Core framing stays dependency-free; this module is
// imported only by runtimes that can provide deflate (node:zlib, etc).
//
// Wire format: RFC-1952 gzip (the wrapper), matching every other easy-rpc
// implementation (Go compress/gzip, Python gzip, Rust flate2 GzEncoder, ...).
// Pre-0.5.1 this used raw deflate, which was NOT interoperable with the other
// languages' compressed frames.
import { gzipSync, gunzipSync } from 'node:zlib'
import type { Bytes } from './protocol.js'

/** gzip-compress bytes (RFC-1952 wrapper, like every other language). */
export function gzipCompress(data: Bytes): Bytes {
  return new Uint8Array(gzipSync(Buffer.from(data)))
}

/** gzip-decompress bytes. THROWS on corrupt input (fault matrix M10): a
 * corrupt compressed frame is a protocol error, never raw bytes. */
export function gzipDecompress(data: Bytes): Bytes {
  return new Uint8Array(gunzipSync(Buffer.from(data)))
}

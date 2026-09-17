// Composition root for TypeScript: pick an adapter by `mode` and install the
// standard interceptors (metadata/deadline) plus any user interceptors. This is
// the ONE place that knows about concrete adapters; everything else depends on
// the `Transport` interface.
import type { Headers, Interceptor, Transport } from './protocol.js'
import {
  createInterceptorTransport,
  metadataInterceptor,
  timeoutInterceptor,
} from './protocol.js'
import { createFetchTransport } from './bridge_fetch.js'
import { createNodeTransport, createHttp1Transport } from './bridge_node.js'

/** Adapter modes. `auto` picks by environment. */
export type Mode = 'auto' | 'fetch' | 'node' | 'h1'

export interface ConnectOptions {
  baseUrl: string
  /** Bearer token attached to every call (as metadata). */
  token?: string
  /** Adapter mode. Default: `auto`. */
  mode?: Mode
  /** Per-call deadline in ms (0/undefined = none). */
  timeoutMs?: number
  /** Extra interceptors, applied after the built-ins (closest to the adapter). */
  interceptors?: Interceptor[]
  /** Adapter-specific knobs. */
  node?: { protocol?: 'h2' | 'h2c' | 'h1' | 'auto'; httpAgent?: import('node:http').Agent }
}

function adapterFor(mode: Mode, opts: ConnectOptions): Transport {
  const base = opts.baseUrl.replace(/\/+$/, '')
  if (mode === 'fetch') return createFetchTransport(base)
  if (mode === 'h1') return createHttp1Transport(opts.node?.httpAgent, base)
  if (mode === 'node') return createNodeTransport({ base, protocol: opts.node?.protocol ?? 'h2' })
  // auto: browser => fetch; node => http2 with h1 fallback.
  if (typeof window !== 'undefined') return createFetchTransport(base)
  return createNodeTransport({ base, protocol: opts.node?.protocol ?? 'auto' })
}

/**
 * Build a ready-to-use Transport: adapter(mode) wrapped with the built-in
 * metadata + deadline interceptors (when configured) and any user interceptors.
 * Swap `mode` and the interceptors are unchanged — that is the whole point.
 */
export function connect(opts: ConnectOptions): Transport {
  const mode: Mode = opts.mode ?? 'auto'
  const ics: Interceptor[] = []
  const md: Headers = {}
  if (opts.token !== undefined && opts.token !== '') {
    md['authorization'] = [`Bearer ${opts.token}`]
  }
  if (Object.keys(md).length > 0) ics.push(metadataInterceptor(md))
  if (opts.timeoutMs !== undefined && opts.timeoutMs > 0) {
    ics.push(timeoutInterceptor(opts.timeoutMs))
  }
  for (const ic of opts.interceptors ?? []) ics.push(ic)
  return createInterceptorTransport(ics, adapterFor(mode, opts))
}

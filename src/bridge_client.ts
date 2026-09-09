// Bridge selector for TypeScript.
//
// Two realms:
//   - "web":   use fetch. On browsers fetch already covers HTTP/1, HTTP/2 and
//              HTTP/3, with no extra dependency.
//   - "node":  use node:http2 with a node:http fallback -> HTTP/1 + HTTP/2 (h2
//              and h2c). Node core does NOT support HTTP/3; that is a browser-
//              only capability in easy-rpc TS.
//
// createDefaultTransport chooses by environment (browser=web, else node).
import type { Transport } from './protocol.js'
import { createFetchTransport } from './bridge_fetch.js'
import { createNodeTransport } from './bridge_node.js'

export type ClientRealm = 'web' | 'node'

export function createDefaultTransport(base = '', realm?: ClientRealm): Transport {
  const r: ClientRealm = realm ?? (typeof window !== 'undefined' ? 'web' : 'node')
  if (r === 'web') return createFetchTransport(base)
  return createNodeTransport({ base })
}

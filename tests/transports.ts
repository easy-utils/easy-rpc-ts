// Transport selector for conformance tests. Reads EASY_RPC_TRANSPORT and
// returns the matching easy-rpc transport; unset => the current default.
//
// Unified transport vocabulary (spec §7.1):
//   ts: fetch | node | h1        go: std | auto       rust: reqwest | hyper
//   python: httpx | auto         kotlin: okhttp | cio  csharp: h1 | h2 | h3
//   swift: urlsession | ahc      dart: io | http2
import {
  createFetchTransport,
  createNodeTransport,
  createHttp1Transport,
  type Transport,
} from '../src/index'

export function conformanceTransport(base: string): { transport: Transport; name: string } {
  const name = process.env.EASY_RPC_TRANSPORT ?? 'fetch'
  switch (name) {
    case 'fetch':
      return { transport: createFetchTransport(base), name }
    case 'node':
      return { transport: createNodeTransport({ base }), name }
    case 'h1':
      return { transport: createHttp1Transport(undefined, base), name }
    case 'auto':
      // Negotiate: try h2 then fall back to h1 (works against h1-only peers).
      return { transport: createNodeTransport({ base, protocol: 'auto' }), name }
    default:
      throw new Error(`unknown EASY_RPC_TRANSPORT=${name} (ts: fetch|node|h1|auto)`)
  }
}

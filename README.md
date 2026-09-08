# easy-rpc-ts

Zero-runtime-bindings easy-rpc core for TypeScript. Provides:
- a `Transport` interface (unary + server-stream),
- the Connect wire protocol (framing, error codes, Content-Type),
- `createFetchTransport` (browser) and `createNodeTransport` (h2/h2c) bridges,
- server dispatch helpers,
- self-generated `*_easyrpc.ts` (MethodSpecs) via `protoc-gen-easyrpc-ts`.

Messages come from official `protoc-gen-es`; no connectrpc dependency.

```ts
import { createFetchTransport } from '@easy-utils/easy-rpc'
const t = createFetchTransport()
const resp = await t.send({ url: '/v1/echo', method: 'POST', headers: {}, body: new TextEncoder().encode('hi') })
```

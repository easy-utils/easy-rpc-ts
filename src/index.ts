// easy-rpc TS client entry: protocol + client bridges only. Server helpers
// (createServer / http2Server / toWebHandler) are intentionally NOT re-exported
// here — import them from the `easy-rpc/server` subpath so client bundles do
// not pull Node server built-ins.
export * from './protocol.js'
export { createFetchTransport } from './bridge_fetch.js'
export { createNodeTransport, createHttp1Transport } from './bridge_node.js'
export { createDefaultTransport } from './bridge_client.js'
export { connect, type Mode, type ConnectOptions } from './connect.js'
export { createMetadataTransport } from './protocol.js'

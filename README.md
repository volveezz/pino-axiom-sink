# pino-axiom-sink

A pino destination that ships NDJSON to Axiom without leaking memory.

`@axiomhq/pino` runs in a worker and encodes each batch through `CompressionStream`, which retains `ArrayBuffer`s V8 never reclaims, so RSS climbs to ~1 GB under sustained load and stays pinned. This writes each line straight into a keep-alive HTTPS connection on the main thread: no worker, no batch copy. Memory tracks only in-flight bytes and returns to baseline, flat ~53 MB at typical load and ~135 MB through a 1M-event burst, never growing with event count.

## Use

```ts
import pino from 'pino';
import { createAxiomDestination } from 'pino-axiom-sink';

const logger = pino({ level: 'info' }, createAxiomDestination({ dataset, token }));
```

With nestjs-pino:

```ts
return { pinoHttp: [{ level: 'info' }, createAxiomDestination({ dataset, token })] };
```

Call `dest.flush()` on shutdown to drain in-flight batches.

## Options

`dataset` and `token` are required. `host` defaults to `https://api.axiom.co`. `maxBytes` (default 256 MiB) caps un-acked bytes: past it a stalled sink drops new lines instead of growing memory.

## Build

```sh
npm install
npm run build
```

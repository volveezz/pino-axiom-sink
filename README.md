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

Call `dest.flush()` on shutdown to drain in-flight batches. It rejects once with any delivery failures accumulated since the previous flush.

## Options

`dataset` and `token` are required. `host` defaults to `https://api.axiom.co`; include an ingest base path in this URL when using an edge deployment. `maxBytes` defaults to 8 MiB and caps unacknowledged bytes. `timeoutMs` defaults to 10 seconds and bounds the complete delivery, including retries; each of the four attempts gets a quarter of it, so a stalled response is retried rather than consuming the whole budget. A batch Axiom committed but acknowledged too late can land twice. `onError` receives each terminal batch failure and the first capacity drop in each pressure episode.

Each non-empty `write()` must contain one NDJSON event. Multi-event batches stay within 1 MiB and 10,000 events; a single event may be larger than 1 MiB. Capacity drops deliberately return `true` to keep Pino non-blocking; observe them through `onError` or the next `flush()` rejection. Both Promise-based `destination.flush()` and Pino's callback-based `logger.flush(callback)` are supported.

## Build

```sh
npm install
npm run build
```

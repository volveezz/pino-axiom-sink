import * as http from 'node:http';
import * as https from 'node:https';

export interface AxiomDestinationOptions {
  dataset: string;
  token: string;
  host?: string;
  maxBytes?: number;
}

export interface AxiomDestination {
  write(line: string): boolean;
  flush(): Promise<void>;
}

const MAX_RETRIES = 3;
const sleep = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms).unref());

/**
 * Main-thread Axiom log sink for pino
 *
 * The obvious path (a pino transport worker) encodes batches through
 * CompressionStream, which retains external ArrayBuffers V8 never reclaims; this
 * sink stays on the main thread and writes each line straight to a keep-alive
 * socket so freed bytes stay GC-visible
 */
export function createAxiomDestination(opts: AxiomDestinationOptions): AxiomDestination {
  const origin = new URL(opts.host ?? 'https://api.axiom.co');
  const request = origin.protocol === 'http:' ? http.request : https.request;
  const Agent = origin.protocol === 'http:' ? http.Agent : https.Agent;
  const agent = new Agent({ keepAlive: true, maxSockets: 4 });
  const path = `/v1/datasets/${encodeURIComponent(opts.dataset)}/ingest?timestamp-field=time`;
  const headers = { authorization: `Bearer ${opts.token}`, 'content-type': 'application/x-ndjson' };
  const maxBytes = opts.maxBytes ?? 256 * (1 << 20);
  const flushIntervalMs = 250;
  const maxBatchBytes = 1 << 20;

  let parts: string[] = [];
  let pendingBytes = 0;
  let unackedBytes = 0;
  let timer: ReturnType<typeof setTimeout> | null = null;
  const inFlight = new Set<Promise<void>>();

  function write(line: string): boolean {
    if (unackedBytes + pendingBytes > maxBytes) return true; // a stalled sink drops here rather than growing memory without bound
    parts.push(line);
    pendingBytes += Buffer.byteLength(line);
    if (pendingBytes >= maxBatchBytes) flushNow();
    else if (timer === null) {
      timer = setTimeout(flushNow, flushIntervalMs);
      timer.unref();
    }
    return true;
  }

  function flushNow(): void {
    if (timer !== null) {
      clearTimeout(timer);
      timer = null;
    }
    if (parts.length === 0) return;

    const batch = parts;
    const bytes = pendingBytes;
    parts = [];
    pendingBytes = 0;
    unackedBytes += bytes;

    const p = post(batch).finally(() => {
      unackedBytes -= bytes;
      inFlight.delete(p);
    });
    inFlight.add(p);
  }

  async function post(batch: string[], attempt = 0): Promise<void> {
    const { ok, retriable, retryAfterMs } = await once(batch);
    if (ok || !retriable || attempt >= MAX_RETRIES) return; // 4xx and exhausted retries drop; a reset connection, 429, or 5xx is a transient blip worth retrying
    await sleep(retryAfterMs ?? Math.min(250 * 2 ** attempt, 5000));
    return post(batch, attempt + 1);
  }

  function once(batch: string[]): Promise<{ ok: boolean; retriable: boolean; retryAfterMs: number | null }> {
    return new Promise((resolve) => {
      const req = request({ protocol: origin.protocol, hostname: origin.hostname, port: origin.port, path, method: 'POST', agent, headers }, (res) => {
        const status = res.statusCode ?? 0;
        const retryAfterMs = parseRetryAfter(res);
        res.resume();
        res.on('end', () => resolve({ ok: status >= 200 && status < 300, retriable: status === 429 || status >= 500, retryAfterMs }));
      });
      req.on('error', () => resolve({ ok: false, retriable: true, retryAfterMs: null }));
      for (let i = 0; i < batch.length; i++) req.write(batch[i]); // write lines individually; joining them restores the batch copy this sink avoids
      req.end();
    });
  }

  async function flush(): Promise<void> {
    flushNow();
    while (inFlight.size > 0) await Promise.allSettled([...inFlight]);
  }

  return { write, flush };
}

function parseRetryAfter(res: http.IncomingMessage): number | null {
  const reset = (res.headers['x-ratelimit-reset'] ?? res.headers['retry-after']) as string | undefined;
  if (!reset) return null;
  const n = Number(reset);
  if (!Number.isFinite(n)) return null;
  return n > 1e6 ? Math.max(0, n * 1000 - Date.now()) : n * 1000; // x-ratelimit-reset is epoch seconds, retry-after is a delta
}

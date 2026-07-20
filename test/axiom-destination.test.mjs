/**
 * Contract tests for bounded delivery, retries, provider responses and failure recovery
 */
import assert from 'node:assert/strict';
import http from 'node:http';
import { afterEach, test } from 'node:test';
import pino from 'pino';
import { AxiomSinkError, createAxiomDestination } from '../dist/index.js';

const servers = [];

afterEach(async () => {
  await Promise.all(
    servers.splice(0).map(
      (server) =>
        new Promise((resolve) => {
          server.closeAllConnections();
          server.close(resolve);
        }),
    ),
  );
});

async function serve(handler) {
  const server = http.createServer((request, response) => void handler(request, response));
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  servers.push(server);
  const address = server.address();
  assert(address && typeof address === 'object');
  return `http://127.0.0.1:${address.port}`;
}

async function readEvents(request) {
  let body = '';
  for await (const chunk of request) body += chunk;
  return body.trimEnd().split('\n').filter(Boolean);
}

function respond(response, ingested, failed = 0) {
  response.writeHead(200, { 'content-type': 'application/json' });
  response.end(JSON.stringify({ ingested, failed, failures: [] }));
}

function expectSinkError(code) {
  return (error) => {
    assert(error instanceof AxiomSinkError);
    assert.equal(error.code, code);
    return true;
  };
}

test('splits batches at 10,000 events independently of bytes', async () => {
  const batchSizes = [];
  const host = await serve(async (request, response) => {
    const events = await readEvents(request);
    batchSizes.push(events.length);
    respond(response, events.length);
  });
  const destination = createAxiomDestination({ dataset: 'logs', token: 'token', host });

  for (let index = 0; index < 10_001; index++) destination.write('{}\n');
  await destination.flush();

  assert.deepEqual(batchSizes.sort((left, right) => right - left), [10_000, 1]);
});

test('honors edge ingest paths and flushes before the byte boundary', async () => {
  const requests = [];
  const host = await serve(async (request, response) => {
    const events = await readEvents(request);
    requests.push({ url: request.url, bytes: Buffer.byteLength(events.join('\n')) });
    respond(response, events.length);
  });
  const destination = createAxiomDestination({ dataset: 'edge logs', token: 'token', host: `${host}/v1/ingest` });
  const line = `${JSON.stringify({ payload: 'x'.repeat(600_000) })}\n`;

  destination.write(line);
  destination.write(line);
  await destination.flush();

  assert.deepEqual(requests.map(({ url }) => url), [
    '/v1/ingest/edge%20logs?timestamp-field=time',
    '/v1/ingest/edge%20logs?timestamp-field=time',
  ]);
  assert(requests.every(({ bytes }) => bytes < 1 << 20));
});

test('admits the exact byte limit and reports one capacity drop per pressure episode', async () => {
  const reported = [];
  const host = await serve(async (request, response) => respond(response, (await readEvents(request)).length));
  const destination = createAxiomDestination({
    dataset: 'logs',
    token: 'token',
    host,
    maxBytes: 4,
    onError: (error) => reported.push(error),
  });

  assert.equal(destination.write('{}\n'), true);
  assert.equal(destination.write('x'), true);
  assert.equal(destination.write('oversized'), true);
  await assert.rejects(destination.flush(), expectSinkError('buffer_full'));
  assert.deepEqual(reported.map((error) => error.code), ['buffer_full']);
  await destination.flush();
});

test('rejects empty writes without allocating batches or requests', async () => {
  let requests = 0;
  const reported = [];
  const host = await serve(async (request, response) => {
    requests++;
    respond(response, (await readEvents(request)).length);
  });
  const destination = createAxiomDestination({
    dataset: 'logs',
    token: 'token',
    host,
    onError: (error) => reported.push(error),
  });

  for (let index = 0; index < 20_000; index++) destination.write('');
  await assert.rejects(destination.flush(), expectSinkError('invalid_event'));
  assert.equal(requests, 0);
  assert.deepEqual(reported.map((error) => error.code), ['invalid_event']);
});

test('retries network, rate-limit and server failures before succeeding', async () => {
  let requests = 0;
  const host = await serve(async (request, response) => {
    await readEvents(request);
    requests++;
    if (requests === 1) return request.socket.destroy();
    if (requests === 2) {
      response.writeHead(429, { 'retry-after': '0' });
      return response.end('rate limited');
    }
    if (requests === 3) {
      response.writeHead(503);
      return response.end('unavailable');
    }
    respond(response, 1);
  });
  const destination = createAxiomDestination({ dataset: 'logs', token: 'token', host });

  destination.write('{}\n');
  await destination.flush();

  assert.equal(requests, 4);
});

test('does not retry terminal HTTP failures and recovers after flush reports them', async () => {
  let requests = 0;
  const host = await serve(async (request, response) => {
    await readEvents(request);
    requests++;
    if (requests === 1) {
      response.writeHead(401);
      return response.end('invalid token');
    }
    respond(response, 1);
  });
  const destination = createAxiomDestination({ dataset: 'logs', token: 'token', host });

  destination.write('{}\n');
  await assert.rejects(destination.flush(), expectSinkError('http_status'));
  assert.equal(requests, 1);
  destination.write('{}\n');
  await destination.flush();
  assert.equal(requests, 2);
});

test('rejects malformed, partial and oversized successful responses', async (context) => {
  const cases = [
    ['malformed', '{', 'invalid_response'],
    ['mismatched count', JSON.stringify({ ingested: 2, failed: 0 }), 'invalid_response'],
    ['partial ingest', JSON.stringify({ ingested: 0, failed: 1, failures: [{}] }), 'ingest_rejected'],
    ['oversized', 'x'.repeat(70_000), 'invalid_response'],
  ];

  for (const [name, body, code] of cases) {
    await context.test(name, async () => {
      const host = await serve(async (request, response) => {
        await readEvents(request);
        response.writeHead(200, { 'content-type': 'application/json' });
        response.end(body);
      });
      const destination = createAxiomDestination({ dataset: 'logs', token: 'token', host });
      destination.write('{}\n');
      await assert.rejects(destination.flush(), (error) => {
        assert(expectSinkError(code)(error));
        assert((error.responseBody?.length ?? 0) <= 65_536);
        return true;
      });
    });
  }
});

test('supports Pino callback flushing and contains observer failures', async () => {
  const reported = [];
  const host = await serve(async (request) => {
    await readEvents(request);
  });
  const destination = createAxiomDestination({
    dataset: 'logs',
    token: 'token',
    host,
    timeoutMs: 40,
    onError: (error) => {
      reported.push(error);
      throw new Error('observer failed');
    },
  });
  const logger = pino({}, destination);

  logger.info({ action: 'timeout_contract_test' });
  const error = await new Promise((resolve) => logger.flush(resolve));
  assert(expectSinkError('delivery_timeout')(error));
  assert.equal(reported.length, 1);
});

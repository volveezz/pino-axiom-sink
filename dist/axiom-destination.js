/**
 * Bounded main-thread NDJSON delivery to Axiom for Pino and direct event producers
 */
import * as http from 'node:http';
import * as https from 'node:https';
export class AxiomSinkError extends Error {
    code;
    eventCount;
    attempts;
    statusCode;
    responseBody;
    name = 'AxiomSinkError';
    constructor(message, code, eventCount, attempts, statusCode, responseBody, options) {
        super(message, options);
        this.code = code;
        this.eventCount = eventCount;
        this.attempts = attempts;
        this.statusCode = statusCode;
        this.responseBody = responseBody;
    }
}
const MAX_ATTEMPTS = 4;
const MAX_BATCH_BYTES = 1 << 20;
const MAX_BATCH_EVENTS = 10_000;
const MAX_RESPONSE_BYTES = 64 << 10;
const MAX_RETRY_DELAY_MS = 5_000;
const MAX_RETAINED_FAILURES = 16;
const DEFAULT_MAX_BYTES = 8 << 20;
const DEFAULT_TIMEOUT_MS = 10_000;
const FLUSH_INTERVAL_MS = 250;
export function createAxiomDestination(options) {
    const origin = new URL(options.host ?? 'https://api.axiom.co');
    const request = origin.protocol === 'http:' ? http.request : https.request;
    const Agent = origin.protocol === 'http:' ? http.Agent : https.Agent;
    const agent = new Agent({ keepAlive: true, maxSockets: 4 });
    const dataset = encodeURIComponent(options.dataset);
    const basePath = origin.pathname === '/' ? `/v1/datasets/${dataset}/ingest` : `${origin.pathname.replace(/\/$/, '')}/${dataset}`;
    const path = `${basePath}?timestamp-field=time`;
    const headers = { authorization: `Bearer ${options.token}`, 'content-type': 'application/x-ndjson' };
    const maxBytes = positiveOption('maxBytes', options.maxBytes ?? DEFAULT_MAX_BYTES);
    const timeoutMs = positiveOption('timeoutMs', options.timeoutMs ?? DEFAULT_TIMEOUT_MS);
    const attemptTimeoutMs = timeoutMs / MAX_ATTEMPTS;
    let parts = [];
    let pendingBytes = 0;
    let unackedBytes = 0;
    let timer = null;
    let bufferFullReported = false;
    let invalidWriteReported = false;
    let omittedFailures = 0;
    const retainedFailures = [];
    const inFlight = new Set();
    function report(error) {
        if (retainedFailures.length < MAX_RETAINED_FAILURES)
            retainedFailures.push(error);
        else
            omittedFailures++;
        try {
            options.onError?.(error);
        }
        catch { }
    }
    function write(line) {
        const lineBytes = Buffer.byteLength(line);
        if (lineBytes === 0) {
            if (!invalidWriteReported) {
                invalidWriteReported = true;
                report(new AxiomSinkError('Axiom sink received an empty event', 'invalid_event', 1, 0));
            }
            return true;
        }
        invalidWriteReported = false;
        const bufferedBytes = unackedBytes + pendingBytes;
        if (bufferedBytes + lineBytes > maxBytes) {
            if (!bufferFullReported) {
                bufferFullReported = true;
                report(new AxiomSinkError('Axiom sink buffer is full; events are being dropped', 'buffer_full', 1, 0));
            }
            return true;
        }
        if (bufferFullReported && bufferedBytes + lineBytes <= maxBytes / 2)
            bufferFullReported = false;
        if (parts.length > 0 && pendingBytes + lineBytes > MAX_BATCH_BYTES)
            flushNow();
        parts.push(line);
        pendingBytes += lineBytes;
        if (pendingBytes >= MAX_BATCH_BYTES || parts.length >= MAX_BATCH_EVENTS)
            flushNow();
        else if (timer === null) {
            timer = setTimeout(flushNow, FLUSH_INTERVAL_MS);
            timer.unref();
        }
        return true;
    }
    function flushNow() {
        if (timer !== null) {
            clearTimeout(timer);
            timer = null;
        }
        if (parts.length === 0)
            return;
        const batch = parts;
        const bytes = pendingBytes;
        parts = [];
        pendingBytes = 0;
        unackedBytes += bytes;
        const delivery = post(batch)
            .catch((error) => report(asSinkError(error, batch.length)))
            .finally(() => {
            unackedBytes -= bytes;
            inFlight.delete(delivery);
        });
        inFlight.add(delivery);
    }
    async function post(batch) {
        const deadline = Date.now() + timeoutMs;
        let attempts = 0;
        let lastRequestError;
        while (attempts < MAX_ATTEMPTS) {
            const remainingMs = deadline - Date.now();
            if (remainingMs <= 0) {
                throw new AxiomSinkError(`Axiom delivery exceeded ${timeoutMs}ms`, 'delivery_timeout', batch.length, attempts, undefined, undefined, lastRequestError ? { cause: lastRequestError } : undefined);
            }
            attempts++;
            const result = await once(batch, Math.min(remainingMs, attemptTimeoutMs));
            if (result.type === 'response') {
                if (result.statusCode >= 200 && result.statusCode < 300) {
                    validateIngestResponse(result, batch.length, attempts);
                    return;
                }
                if (result.statusCode !== 429 && result.statusCode < 500) {
                    throw responseError('Axiom rejected the ingest request', 'http_status', result, batch.length, attempts);
                }
                if (attempts >= MAX_ATTEMPTS) {
                    throw responseError('Axiom ingest retries were exhausted', 'http_status', result, batch.length, attempts);
                }
                await retryDelay(deadline, attempts, result.retryAfterMs);
                continue;
            }
            lastRequestError = result.error;
            if (attempts >= MAX_ATTEMPTS) {
                throw new AxiomSinkError('Axiom request retries were exhausted', 'request_failed', batch.length, attempts, undefined, undefined, { cause: result.error });
            }
            await retryDelay(deadline, attempts, null);
        }
    }
    function once(batch, requestTimeoutMs) {
        return new Promise((resolve) => {
            let settled = false;
            let deadlineTimer;
            const settle = (result) => {
                if (settled)
                    return;
                settled = true;
                clearTimeout(deadlineTimer);
                resolve(result);
            };
            const req = request({
                protocol: origin.protocol,
                hostname: origin.hostname,
                port: origin.port,
                path,
                method: 'POST',
                agent,
                headers,
            }, (res) => {
                const chunks = [];
                let capturedBytes = 0;
                let bodyTruncated = false;
                res.on('data', (chunk) => {
                    if (capturedBytes >= MAX_RESPONSE_BYTES) {
                        bodyTruncated = true;
                        return;
                    }
                    const captured = chunk.subarray(0, MAX_RESPONSE_BYTES - capturedBytes);
                    chunks.push(captured);
                    capturedBytes += captured.length;
                    if (captured.length < chunk.length)
                        bodyTruncated = true;
                });
                res.on('end', () => settle({
                    type: 'response',
                    statusCode: res.statusCode ?? 0,
                    body: Buffer.concat(chunks, capturedBytes).toString('utf8'),
                    bodyTruncated,
                    retryAfterMs: parseRetryAfter(res),
                }));
                res.on('error', (error) => settle({ type: 'request_error', error }));
            });
            deadlineTimer = setTimeout(() => req.destroy(new Error(`Axiom request exceeded ${requestTimeoutMs}ms`)), requestTimeoutMs);
            deadlineTimer.unref();
            req.on('error', (error) => settle({ type: 'request_error', error }));
            for (const line of batch)
                req.write(line);
            req.end();
        });
    }
    async function flushAsync() {
        flushNow();
        while (inFlight.size > 0)
            await Promise.all([...inFlight]);
        if (retainedFailures.length === 0)
            return;
        const failures = retainedFailures.splice(0);
        const omitted = omittedFailures;
        omittedFailures = 0;
        if (omitted > 0) {
            failures.push(new AxiomSinkError(`${omitted} additional Axiom delivery failures omitted`, 'request_failed', 0, 0));
        }
        if (failures.length === 1)
            throw failures[0];
        throw new AggregateError(failures, `${failures.length} Axiom delivery failures`);
    }
    function flush(callback) {
        const operation = flushAsync();
        if (!callback)
            return operation;
        void operation.then(() => {
            try {
                callback();
            }
            catch { }
        }, (error) => {
            try {
                callback(error);
            }
            catch { }
        });
    }
    return { write, flush };
    async function retryDelay(deadline, attempts, retryAfterMs) {
        const delayMs = Math.min(retryAfterMs ?? 250 * 2 ** (attempts - 1), MAX_RETRY_DELAY_MS, Math.max(0, deadline - Date.now()));
        if (delayMs <= 0)
            return;
        await new Promise((resolve) => {
            const retryTimer = setTimeout(resolve, delayMs);
            retryTimer.unref();
        });
    }
}
function validateIngestResponse(result, eventCount, attempts) {
    let response;
    try {
        response = result.bodyTruncated ? null : JSON.parse(result.body);
    }
    catch {
        response = null;
    }
    if (!isRecord(response) || !isCount(response.ingested) || !isCount(response.failed) || response.ingested + response.failed !== eventCount) {
        throw responseError('Axiom returned an invalid ingest response', 'invalid_response', result, eventCount, attempts);
    }
    if (response.failed > 0) {
        throw responseError('Axiom rejected events from the ingest batch', 'ingest_rejected', result, eventCount, attempts);
    }
}
function responseError(message, code, result, eventCount, attempts) {
    return new AxiomSinkError(message, code, eventCount, attempts, result.statusCode, result.body);
}
function asSinkError(error, eventCount) {
    return error instanceof AxiomSinkError
        ? error
        : new AxiomSinkError('Unexpected Axiom sink failure', 'request_failed', eventCount, 0, undefined, undefined, {
            cause: error,
        });
}
function positiveOption(name, value) {
    if (!Number.isFinite(value) || value <= 0)
        throw new RangeError(`${name} must be a positive number`);
    return value;
}
function isRecord(value) {
    return typeof value === 'object' && value !== null;
}
function isCount(value) {
    return Number.isSafeInteger(value) && value >= 0;
}
function parseRetryAfter(response) {
    const reset = response.headers['x-ratelimit-reset'];
    const resetSeconds = Number(Array.isArray(reset) ? reset[0] : reset);
    if (Number.isFinite(resetSeconds))
        return Math.max(0, resetSeconds * 1000 - Date.now());
    const retryAfter = response.headers['retry-after'];
    const value = Array.isArray(retryAfter) ? retryAfter[0] : retryAfter;
    if (!value)
        return null;
    const deltaSeconds = Number(value);
    if (Number.isFinite(deltaSeconds))
        return Math.max(0, deltaSeconds * 1000);
    const date = Date.parse(value);
    return Number.isFinite(date) ? Math.max(0, date - Date.now()) : null;
}
//# sourceMappingURL=axiom-destination.js.map
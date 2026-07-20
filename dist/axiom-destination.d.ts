export type AxiomSinkErrorCode = 'buffer_full' | 'delivery_timeout' | 'http_status' | 'ingest_rejected' | 'invalid_event' | 'invalid_response' | 'request_failed';
export declare class AxiomSinkError extends Error {
    readonly code: AxiomSinkErrorCode;
    readonly eventCount: number;
    readonly attempts: number;
    readonly statusCode?: number | undefined;
    readonly responseBody?: string | undefined;
    readonly name = "AxiomSinkError";
    constructor(message: string, code: AxiomSinkErrorCode, eventCount: number, attempts: number, statusCode?: number | undefined, responseBody?: string | undefined, options?: ErrorOptions);
}
export interface AxiomDestinationOptions {
    dataset: string;
    token: string;
    host?: string;
    maxBytes?: number;
    timeoutMs?: number;
    onError?: (error: AxiomSinkError) => void;
}
export interface AxiomDestination {
    write(line: string): boolean;
    flush(): Promise<void>;
    flush(callback: (error?: Error) => void): void;
}
export declare function createAxiomDestination(options: AxiomDestinationOptions): AxiomDestination;
//# sourceMappingURL=axiom-destination.d.ts.map
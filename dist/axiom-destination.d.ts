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
/**
 * Main-thread Axiom log sink for pino
 *
 * The obvious path (a pino transport worker) encodes batches through
 * CompressionStream, which retains external ArrayBuffers V8 never reclaims; this
 * sink stays on the main thread and writes each line straight to a keep-alive
 * socket so freed bytes stay GC-visible
 */
export declare function createAxiomDestination(opts: AxiomDestinationOptions): AxiomDestination;
//# sourceMappingURL=axiom-destination.d.ts.map
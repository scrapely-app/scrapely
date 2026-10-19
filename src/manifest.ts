/**
 * Scrapely Actor Manifest
 * Defines the structure of package.json
 */

export interface ScrapelyManifest {
    /** Actor name (required) */
    name: string;
    
    /** Actor version (required) */
    version: string;
    
    /** Entry point file (required) - e.g., "main.ts", "index.js" */
    entrypoint: string;
    
    /** Maximum memory in MB (optional, default: 512) */
    maxMemory?: number;
    
    /** Maximum timeout in seconds (optional, default: 300) */
    maxTimeout?: number;
    
    /** Actor description (optional) */
    description?: string;
    
    /** Author (optional) */
    author?: string;
    
    /** Additional metadata (optional) */
    [key: string]: unknown;
}

/** Default values for optional fields (can be overridden via env vars) */
export const DEFAULT_MAX_MEMORY = parseInt(process.env.SCRAPELY_DEFAULT_MAX_MEMORY || '512', 10); // MB
export const DEFAULT_MAX_TIMEOUT = parseInt(process.env.SCRAPELY_DEFAULT_MAX_TIMEOUT || '1800', 10); // seconds (30 minutes)

/**
 * Validate a manifest object
 * Returns list of errors, empty if valid
 */
export function validateManifest(manifest: unknown): string[] {
    const errors: string[] = [];
    
    if (!manifest || typeof manifest !== 'object') {
        errors.push('Manifest must be a JSON object');
        return errors;
    }
    
    const m = manifest as Record<string, unknown>;
    
    // Required fields
    if (typeof m.name !== 'string' || m.name.trim() === '') {
        errors.push('Missing required field: name (string)');
    }
    
    if (typeof m.version !== 'string' || m.version.trim() === '') {
        errors.push('Missing required field: version (string)');
    }
    
    if (typeof m.entrypoint !== 'string' || m.entrypoint.trim() === '') {
        errors.push('Missing required field: entrypoint (string)');
    }
    
    // Optional fields with type validation
    if (m.maxMemory !== undefined && typeof m.maxMemory !== 'number') {
        errors.push('Field maxMemory must be a number');
    }
    
    if (m.maxTimeout !== undefined && typeof m.maxTimeout !== 'number') {
        errors.push('Field maxTimeout must be a number');
    }
    
    if (m.description !== undefined && typeof m.description !== 'string') {
        errors.push('Field description must be a string');
    }
    
    if (m.author !== undefined && typeof m.author !== 'string') {
        errors.push('Field author must be a string');
    }
    
    return errors;
}
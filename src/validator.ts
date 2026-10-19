/**
 * Scrapely Actor Package Validator
 * Validates actor package format
 */

import * as fs from 'fs';
import * as path from 'path';
import { ScrapelyManifest, validateManifest, DEFAULT_MAX_MEMORY, DEFAULT_MAX_TIMEOUT } from './manifest.js';

export interface ValidationResult {
    valid: boolean;
    errors: string[];
    manifest?: ScrapelyManifest;
    packageJson?: Record<string, unknown>;
}

/**
 * Validate an actor package directory
 * Checks for: package.json, package.json, entrypoint file
 */
export function validatePackage(packageDir: string): ValidationResult {
    const errors: string[] = [];
    let manifest: ScrapelyManifest | undefined;
    let packageJson: Record<string, unknown> | undefined;

    // Check directory exists
    if (!fs.existsSync(packageDir)) {
        return { valid: false, errors: [`Directory not found: ${packageDir}`] };
    }

    if (!fs.statSync(packageDir).isDirectory()) {
        return { valid: false, errors: [`Not a directory: ${packageDir}`] };
    }

    // Check package.json
    const manifestPath = path.join(packageDir, 'package.json');
    if (!fs.existsSync(manifestPath)) {
        errors.push('Missing package.json');
    } else {
        try {
            const content = fs.readFileSync(manifestPath, 'utf-8');
            const parsed = JSON.parse(content);
            const manifestErrors = validateManifest(parsed);
            
            if (manifestErrors.length > 0) {
                errors.push(...manifestErrors.map(e => `package.json: ${e}`));
            } else {
                manifest = parsed as ScrapelyManifest;
            }
        } catch (err) {
            errors.push(`package.json: Invalid JSON - ${(err as Error).message}`);
        }
    }

    // Check package.json
    const packageJsonPath = path.join(packageDir, 'package.json');
    if (!fs.existsSync(packageJsonPath)) {
        errors.push('Missing package.json');
    } else {
        try {
            const content = fs.readFileSync(packageJsonPath, 'utf-8');
            packageJson = JSON.parse(content);
        } catch (err) {
            errors.push(`package.json: Invalid JSON - ${(err as Error).message}`);
        }
    }

    // Check entrypoint file exists
    if (manifest?.entrypoint) {
        const entrypointPath = path.join(packageDir, manifest.entrypoint);
        if (!fs.existsSync(entrypointPath)) {
            errors.push(`Entrypoint not found: ${manifest.entrypoint}`);
        }
    }

    return {
        valid: errors.length === 0,
        errors,
        manifest,
        packageJson,
    };
}

/**
 * Get normalized manifest with defaults applied
 */
export function getNormalizedManifest(manifest: ScrapelyManifest): Required<Pick<ScrapelyManifest, 'name' | 'version' | 'entrypoint' | 'maxMemory' | 'maxTimeout'>> & Omit<ScrapelyManifest, 'maxMemory' | 'maxTimeout'> {
    return {
        ...manifest,
        maxMemory: manifest.maxMemory ?? DEFAULT_MAX_MEMORY,
        maxTimeout: manifest.maxTimeout ?? DEFAULT_MAX_TIMEOUT,
    };
}

/**
 * CLI entry point for validation
 */
export async function runValidator(args: string[]): Promise<void> {
    const packageDir = args[0] || process.cwd();
    
    console.log(`[Scrapely] Validating package: ${packageDir}\n`);
    
    const result = validatePackage(packageDir);
    
    if (result.valid) {
        console.log('✓ Package is valid\n');
        
        if (result.manifest) {
            const normalized = getNormalizedManifest(result.manifest);
            console.log('Manifest:');
            console.log(`  Name: ${normalized.name}`);
            console.log(`  Version: ${normalized.version}`);
            console.log(`  Entrypoint: ${normalized.entrypoint}`);
            console.log(`  Max Memory: ${normalized.maxMemory} MB`);
            console.log(`  Max Timeout: ${normalized.maxTimeout} seconds`);
            if (normalized.description) {
                console.log(`  Description: ${normalized.description}`);
            }
            if (normalized.author) {
                console.log(`  Author: ${normalized.author}`);
            }
        }
        
        process.exit(0);
    } else {
        console.log('✗ Package validation failed\n');
        console.log('Errors:');
        for (const error of result.errors) {
            console.log(`  - ${error}`);
        }
        process.exit(1);
    }
}

// Run if called directly (CommonJS compatible)
if (require.main === module) {
    runValidator(process.argv.slice(2));
}

import crypto from 'crypto';
import { readFileSync } from 'fs';

// ─── Constants ────────────────────────────────────────────────────────────────

const RSA_PADDING = crypto.constants.RSA_PKCS1_OAEP_PADDING;
const RSA_OAEP_HASH = 'sha256';
const AES_ALGORITHM = 'aes-256-gcm';
const IV_LENGTH = 12;
const AUTH_TAG_LENGTH = 16;
const PBKDF2_ITERATIONS = 100_000;
const PBKDF2_HASH = 'sha256';
const SALT_LENGTH = 32;
const KEY_LENGTH = 32;
const UNSAFE_KEYS = new Set(['__proto__', 'constructor', 'prototype']);

// ─── Private Key Decryption ───────────────────────────────────────────────────

/**
 * Read the private key file from disk and decrypt it with the run passphrase.
 * The private key file is encrypted with AES-256-GCM using a key derived from the passphrase.
 * 
 * @param privateKeyFilePath - Path to the encrypted private key file (e.g., /run/secrets/private_key.pem)
 * @param passphrase - The run passphrase from APIFY_INPUT_SECRETS_PRIVATE_KEY_PASSPHRASE
 * @returns The decrypted private key as a PEM string
 */
export function decryptPrivateKeyFile(privateKeyFilePath: string, passphrase: string): string {
  const encryptedData = readFileSync(privateKeyFilePath);

  const salt = encryptedData.subarray(0, SALT_LENGTH);
  const iv = encryptedData.subarray(SALT_LENGTH, SALT_LENGTH + IV_LENGTH);
  const authTag = encryptedData.subarray(SALT_LENGTH + IV_LENGTH, SALT_LENGTH + IV_LENGTH + AUTH_TAG_LENGTH);
  const encrypted = encryptedData.subarray(SALT_LENGTH + IV_LENGTH + AUTH_TAG_LENGTH);

  // Derive key from passphrase
  const key = crypto.pbkdf2Sync(passphrase, salt, PBKDF2_ITERATIONS, KEY_LENGTH, PBKDF2_HASH);

  // Decrypt
  const decipher = crypto.createDecipheriv(AES_ALGORITHM, key, iv);
  decipher.setAuthTag(authTag);

  const decrypted = Buffer.concat([
    decipher.update(encrypted),
    decipher.final(),
  ]);

  return decrypted.toString('utf8');
}

// ─── Secret Value Decryption ──────────────────────────────────────────────────

/**
 * Decrypt a single secret value encrypted with RSA-OAEP.
 * 
 * @param encryptedBase64 - Base64-encoded encrypted data
 * @param privateKeyPem - The decrypted private key PEM
 * @returns The decrypted plaintext value
 */
export function decryptSecretValue(encryptedBase64: string, privateKeyPem: string): any {
  const encrypted = Buffer.from(encryptedBase64, 'base64');

  const decryptedData = crypto.privateDecrypt(
    {
      key: privateKeyPem,
      padding: RSA_PADDING,
      oaepHash: RSA_OAEP_HASH,
    },
    encrypted
  );

  const plaintext = decryptedData.toString('utf8');

  // Try to parse as JSON (for objects, arrays, numbers, booleans)
  try {
    return JSON.parse(plaintext);
  } catch {
    // Return as plain string
    return plaintext;
  }
}

/**
 * Decrypt all secret values from the encrypted input payload.
 * 
 * @param encryptedSecrets - Map of field name → base64 encrypted value
 * @param privateKeyPem - The decrypted private key PEM
 * @returns Map of field name → decrypted value
 */
export function decryptAllSecrets(
  encryptedSecrets: Record<string, string>,
  privateKeyPem: string
): Record<string, any> {
  const decrypted: Record<string, any> = {};

  for (const [fieldName, encryptedBase64] of Object.entries(encryptedSecrets)) {
    try {
      decrypted[fieldName] = decryptSecretValue(encryptedBase64, privateKeyPem);
    } catch (err) {
      console.warn(`[Scrapely] Failed to decrypt secret field "${fieldName}": ${(err as Error).message}`);
    }
  }

  return decrypted;
}

/**
 * Merge decrypted secret values into the original input object.
 * Supports nested paths like "credentials.apiKey" or "database.password".
 * 
 * @param input - The original input object (with secrets stripped/masked)
 * @param decryptedSecrets - Map of field name → decrypted value
 * @returns A new input object with secrets populated
 */
export function mergeDecryptedInput(
  input: Record<string, any>,
  decryptedSecrets: Record<string, any>
): Record<string, any> {
  const merged = { ...input };

  for (const [fieldName, value] of Object.entries(decryptedSecrets)) {
    // Never walk into or write onto an object's prototype chain.
    if (fieldName.split('.').some((part) => UNSAFE_KEYS.has(part))) continue;
    if (fieldName.includes('.')) {
      // Handle nested paths
      const parts = fieldName.split('.');
      let current = merged;
      for (let i = 0; i < parts.length - 1; i++) {
        if (!current[parts[i]] || typeof current[parts[i]] !== 'object') {
          current[parts[i]] = {};
        }
        current = current[parts[i]];
      }
      current[parts[parts.length - 1]] = value;
    } else {
      merged[fieldName] = value;
    }
  }

  return merged;
}

// ─── Full Decryption Flow ─────────────────────────────────────────────────────

/**
 * Full decryption flow: read private key from file, decrypt it with passphrase,
 * then decrypt all secrets and merge into input.
 * 
 * This is called from Actor.init() when APIFY_INPUT_SECRETS_PRIVATE_KEY_FILE is set.
 * 
 * @param input - Current actor input object
 * @returns The input object with decrypted secret values merged in
 */
export function decryptSecretInputs(
  input: Record<string, any>
): Record<string, any> {
  const privateKeyFile = process.env.APIFY_INPUT_SECRETS_PRIVATE_KEY_FILE;
  const passphrase = process.env.APIFY_INPUT_SECRETS_PRIVATE_KEY_PASSPHRASE;

  if (!privateKeyFile || !passphrase) {
    return input; // No secrets to decrypt
  }

  console.log('[Scrapely] Decrypting secret inputs...');

  try {
    // 1. Read and decrypt the private key file
    const privateKeyPem = decryptPrivateKeyFile(privateKeyFile, passphrase);
    console.log('[Scrapely] Private key decrypted successfully');

    // 2. Get the encrypted secrets from the input
    // The encrypted secrets are stored in a special __encrypted property
    const encryptedSecrets = input.__encrypted_secrets as Record<string, string> | undefined;
    
    if (!encryptedSecrets || Object.keys(encryptedSecrets).length === 0) {
      console.log('[Scrapely] No encrypted secrets found in input');
      return input;
    }

    // 3. Decrypt all secret values
    const decryptedSecrets = decryptAllSecrets(encryptedSecrets, privateKeyPem);
    console.log(`[Scrapely] Decrypted ${Object.keys(decryptedSecrets).length} secret field(s)`);

    // 4. Merge decrypted values into input and remove the __encrypted_secrets key
    const merged = mergeDecryptedInput(input, decryptedSecrets);
    delete merged.__encrypted_secrets;
    delete merged.__encrypted_secret_fields;

    return merged;
  } catch (err) {
    console.error(`[Scrapely] Failed to decrypt secret inputs: ${(err as Error).message}`);
    // Return input as-is - secrets won't be available but actor can still run
    return input;
  }
}
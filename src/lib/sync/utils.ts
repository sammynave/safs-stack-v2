/**
 * Utility functions for sync library
 */

import { nanoid } from "nanoid";

/**
 * Encode data to Buffer for transmission
 */
export function encode(data: unknown): Buffer | Uint8Array {
  const json = JSON.stringify(data);
  if (typeof Buffer !== "undefined") {
    return Buffer.from(json);
  }
  return new TextEncoder().encode(json);
}

/**
 * Decode Buffer to data
 */
export function decode(buffer: Buffer | Uint8Array | ArrayBuffer): unknown {
  let str: string;
  if (buffer instanceof ArrayBuffer) {
    str = new TextDecoder().decode(buffer);
  } else if (typeof Buffer !== "undefined" && Buffer.isBuffer(buffer)) {
    str = buffer.toString();
  } else {
    str = new TextDecoder().decode(buffer);
  }
  return JSON.parse(str);
}

/**
 * Convert hex string to bytes
 */
export function hexToBytes(hex: string): Uint8Array {
  const bytes = new Uint8Array(hex.length / 2);
  for (let i = 0; i < hex.length; i += 2) {
    bytes[i / 2] = parseInt(hex.substr(i, 2), 16);
  }
  return bytes;
}

/**
 * Convert bytes to hex string
 */
export function bytesToHex(bytes: Uint8Array): string {
  return Array.from(bytes)
    .map((b) => b.toString(16).padStart(2, "0"))
    .join("");
}

/**
 * Generate a unique site ID
 */
export function generateSiteId(): string {
  // Generate a nanoid and convert to hex-like format for consistency
  return nanoid(16);
}

/**
 * Serialize a Set to JSON (for BroadcastChannel)
 */
export function serializeSet<T>(set: Set<T>): T[] {
  return Array.from(set);
}

/**
 * Deserialize an array to Set
 */
export function deserializeSet<T>(arr: T[]): Set<T> {
  return new Set(arr);
}

/**
 * Convert primary key value to hex string
 * Handles various types (string, number, etc.)
 */
export function pkToHex(pk: unknown): string {
  const str = String(pk);
  const encoder = new TextEncoder();
  const bytes = encoder.encode(str);
  return bytesToHex(bytes);
}

/**
 * Convert hex string back to primary key value
 */
export function hexToPk(hex: string): string {
  const bytes = hexToBytes(hex);
  const decoder = new TextDecoder();
  return decoder.decode(bytes);
}

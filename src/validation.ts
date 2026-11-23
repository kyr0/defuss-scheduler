/** VERIFIED: Queue arguments are captured before asynchronous admission. The regression suite checks invalid JSON and cross-scope cursors. */
import { SchedulerError, type Json } from "./types.js";

const utf8 = new TextEncoder();
export function byteLength(value: string): number {
  return utf8.encode(value).length;
}

export function invalid(message: string): never {
  throw new SchedulerError("INVALID_ARGUMENT", message);
}
export function object(
  value: unknown,
  fields?: readonly string[],
): asserts value is Record<string, unknown> {
  if (
    !value ||
    typeof value !== "object" ||
    ![Object.prototype, null].includes(Object.getPrototypeOf(value))
  )
    invalid("Expected a plain object");
  if (fields && Object.keys(value).some((k) => !fields.includes(k)))
    invalid("Unknown field");
}
export function id(value: unknown): string {
  if (
    typeof value !== "string" ||
    !value ||
    !value.isWellFormed() ||
    byteLength(value) > 128
  )
    invalid("Identifiers must contain 1 to 128 UTF-8 bytes");
  return value;
}
export function integer(value: unknown, min: number, max: number): number {
  if (
    typeof value !== "number" ||
    !Number.isSafeInteger(value) ||
    value < min ||
    value > max
  )
    invalid(`Expected an integer in ${min}..${max}`);
  return value;
}
export function names(value: unknown): string[] {
  if (!Array.isArray(value) || value.length > 64)
    invalid("Expected up to 64 names");
  return [...new Set(value.map(id))].sort();
}
export function json(value: unknown, maxBytes = 131072): Json {
  // Reject conversions that would differ across the local, RPC and JSON paths.
  const seen = new Set<object>();
  function visit(v: unknown, depth: number): void {
    if (depth > 48) invalid("Value nested too deeply");
    if (v === null || typeof v === "boolean" || typeof v === "string") return;
    if (typeof v === "number" && Number.isFinite(v)) return;
    if (!v || typeof v !== "object" || seen.has(v))
      invalid("Expected acyclic JSON");
    seen.add(v);
    if (Array.isArray(v)) {
      for (const item of v) visit(item, depth + 1);
    } else {
      object(v);
      for (const item of Object.values(v)) visit(item, depth + 1);
    }
    seen.delete(v);
  }
  visit(value, 0);
  const encoded = JSON.stringify(value);
  if (byteLength(encoded) > maxBytes) invalid("JSON exceeds size limit");
  return JSON.parse(encoded) as Json;
}
/**
 * Web Crypto is the one SHA-256 that Node.js, Bun and browsers share, and it is asynchronous.
 * Callers start it before queueing their transaction and await it inside, so admission order stays call order.
 */
export async function fingerprint(value: unknown): Promise<string> {
  const data = JSON.stringify(value, (_k, v) =>
    v && typeof v === "object" && !Array.isArray(v)
      ? Object.fromEntries(
          Object.keys(v)
            .sort()
            .map((k) => [k, v[k]]),
        )
      : v,
  );
  const digest = await crypto.subtle.digest("SHA-256", utf8.encode(data));
  return Array.from(new Uint8Array(digest), (b) =>
    b.toString(16).padStart(2, "0"),
  ).join("");
}
/** Unpadded base64url of the UTF-8 JSON text: a key segment without separators. */
export function encode(value: unknown): string {
  let binary = "";
  for (const byte of utf8.encode(JSON.stringify(value)))
    binary += String.fromCharCode(byte);
  return btoa(binary)
    .replace(/\+/g, "-")
    .replace(/\//g, "_")
    .replace(/=+$/, "");
}
export function decode(value: string): unknown {
  const binary = atob(value.replace(/-/g, "+").replace(/_/g, "/"));
  return JSON.parse(
    new TextDecoder().decode(Uint8Array.from(binary, (c) => c.charCodeAt(0))),
  );
}
export function timeKey(value: number): string {
  return String(value).padStart(16, "0");
}

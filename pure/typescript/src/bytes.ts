export function bytesKey(bytes: Uint8Array): string {
  let key = "";
  for (let i = 0; i < bytes.byteLength; i++) {
    key += String.fromCharCode(bytes[i]!);
  }
  return key;
}

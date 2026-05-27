export const FLAG_FINAL = 0x00
export const FLAG_MORE = 0x01
export const FLAG_COMMAND = 0x02

export interface ZwsFrame {
  flag: number
  payload: Uint8Array
}

export function decodeZwsFrame(data: ArrayBuffer): ZwsFrame {
  const bytes = new Uint8Array(data)
  if (bytes.byteLength < 1) {
    throw new Error("ZWS frame too short")
  }

  const flag = bytes[0]!
  if (flag !== FLAG_FINAL && flag !== FLAG_MORE && flag !== FLAG_COMMAND) {
    throw new Error(`Invalid ZWS flag: 0x${flag.toString(16).padStart(2, "0")}`)
  }

  return {
    flag,
    payload: bytes.subarray(1),
  }
}

export function encodeZwsFrame(flag: number, payload: Uint8Array): Uint8Array<ArrayBuffer> {
  const frame = new Uint8Array(1 + payload.byteLength)
  frame[0] = flag
  frame.set(payload, 1)
  return frame
}

export function encodeDataFrames(msg: {
  parts: Uint8Array[]
}): Uint8Array<ArrayBuffer>[] {
  const frames: Uint8Array<ArrayBuffer>[] = []
  for (let i = 0; i < msg.parts.length; i++) {
    const isLast = i === msg.parts.length - 1
    frames.push(encodeZwsFrame(isLast ? FLAG_FINAL : FLAG_MORE, msg.parts[i]!))
  }
  return frames
}

export function encodeCommandFrame(payload: Uint8Array): Uint8Array<ArrayBuffer> {
  return encodeZwsFrame(FLAG_COMMAND, payload)
}

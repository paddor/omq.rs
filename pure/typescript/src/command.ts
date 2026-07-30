const encoder = new TextEncoder();
const decoder = new TextDecoder();

/** ZMQ socket type names as used in the ZMTP handshake. */
export type SocketTypeName =
  | "REQ"
  | "REP"
  | "PUB"
  | "SUB"
  | "XPUB"
  | "XSUB"
  | "PUSH"
  | "PULL"
  | "DEALER"
  | "ROUTER"
  | "PAIR"
  | "CLIENT"
  | "SERVER"
  | "RADIO"
  | "DISH"
  | "GATHER"
  | "SCATTER"
  | "PEER"
  | "CHANNEL";

/** Properties received in the peer's READY command during ZMTP handshake. */
export interface PeerProperties {
  /** The peer's socket type (e.g. "PUB", "ROUTER"). */
  socketType?: string;
  /** The peer's identity, if set. */
  identity?: Uint8Array;
  /** Any additional properties sent by the peer. */
  other: Map<string, Uint8Array>;
}

export function isSocketTypeName(value: string): value is SocketTypeName {
  return (
    value === "REQ" || value === "REP" ||
    value === "PUB" || value === "SUB" ||
    value === "XPUB" || value === "XSUB" ||
    value === "PUSH" || value === "PULL" ||
    value === "DEALER" || value === "ROUTER" ||
    value === "PAIR" ||
    value === "CLIENT" || value === "SERVER" ||
    value === "RADIO" || value === "DISH" ||
    value === "GATHER" || value === "SCATTER" ||
    value === "PEER" || value === "CHANNEL"
  );
}

/** ZMTP socket-type compatibility matrix (RFC 23 + 37 + 48 + 52). */
export function isCompatibleSocketType(
  ours: SocketTypeName,
  theirs: SocketTypeName,
): boolean {
  switch (ours) {
    case "PUB":
    case "XPUB":
      return theirs === "SUB" || theirs === "XSUB";
    case "SUB":
    case "XSUB":
      return theirs === "PUB" || theirs === "XPUB";
    case "PUSH":
      return theirs === "PULL";
    case "PULL":
      return theirs === "PUSH";
    case "REQ":
      return theirs === "REP" || theirs === "ROUTER" || theirs === "DEALER";
    case "REP":
      return theirs === "REQ" || theirs === "DEALER";
    case "DEALER":
      return theirs === "REQ" || theirs === "REP" ||
        theirs === "DEALER" || theirs === "ROUTER";
    case "ROUTER":
      return theirs === "REQ" || theirs === "DEALER" || theirs === "ROUTER";
    case "PAIR":
      return theirs === "PAIR";
    case "CLIENT":
      return theirs === "SERVER";
    case "SERVER":
      return theirs === "CLIENT";
    case "RADIO":
      return theirs === "DISH";
    case "DISH":
      return theirs === "RADIO";
    case "SCATTER":
      return theirs === "GATHER";
    case "GATHER":
      return theirs === "SCATTER";
    case "CHANNEL":
      return theirs === "CHANNEL";
    case "PEER":
      return theirs === "PEER";
  }
}

export function encodeReady(
  socketType: SocketTypeName,
  identity: Uint8Array,
): Uint8Array {
  const props: [string, Uint8Array][] = [
    ["Socket-Type", encoder.encode(socketType)],
  ];
  if (identity.byteLength > 0) {
    props.push(["Identity", identity]);
  }

  const name = encoder.encode("READY");
  let size = 1 + name.byteLength;
  for (const [key, value] of props) {
    size += 1 + encoder.encode(key).byteLength + 4 + value.byteLength;
  }

  const buf = new Uint8Array(size);
  const view = new DataView(buf.buffer);
  let offset = 0;

  buf[offset++] = name.byteLength;
  buf.set(name, offset);
  offset += name.byteLength;

  for (const [key, value] of props) {
    const keyBytes = encoder.encode(key);
    buf[offset++] = keyBytes.byteLength;
    buf.set(keyBytes, offset);
    offset += keyBytes.byteLength;
    view.setUint32(offset, value.byteLength, false);
    offset += 4;
    buf.set(value, offset);
    offset += value.byteLength;
  }

  return buf;
}

export function decodeCommand(
  data: Uint8Array,
): { name: string; body: Uint8Array } {
  if (data.byteLength < 1) throw new Error("Empty command");

  const nameLen = data[0]!;
  if (data.byteLength < 1 + nameLen) throw new Error("Command name truncated");

  const name = decoder.decode(data.subarray(1, 1 + nameLen));
  const body = data.subarray(1 + nameLen);
  return { name, body };
}

export function decodeReadyProperties(body: Uint8Array): PeerProperties {
  const view = new DataView(body.buffer, body.byteOffset, body.byteLength);
  const props: PeerProperties = { other: new Map() };
  let offset = 0;
  let sawSocketType = false;
  let sawIdentity = false;

  while (offset < body.byteLength) {
    if (offset + 1 > body.byteLength) break;
    const keyLen = body[offset]!;
    offset += 1;

    if (offset + keyLen > body.byteLength) {
      throw new Error("Property key truncated");
    }
    const key = decoder.decode(body.subarray(offset, offset + keyLen));
    offset += keyLen;

    if (offset + 4 > body.byteLength) {
      throw new Error("Property value length truncated");
    }
    const valueLen = view.getUint32(offset, false);
    offset += 4;

    if (offset + valueLen > body.byteLength) {
      throw new Error("Property value truncated");
    }
    const value = body.slice(offset, offset + valueLen);
    offset += valueLen;

    if (key === "Socket-Type") {
      if (sawSocketType) throw new Error("Duplicate Socket-Type property");
      sawSocketType = true;
      props.socketType = decoder.decode(value);
    } else if (key === "Identity") {
      if (sawIdentity) throw new Error("Duplicate Identity property");
      sawIdentity = true;
      props.identity = value;
    } else {
      props.other.set(key, value);
    }
  }

  return props;
}

export function encodeSubscribe(prefix: Uint8Array): Uint8Array {
  const name = encoder.encode("SUBSCRIBE");
  const buf = new Uint8Array(1 + name.byteLength + prefix.byteLength);
  buf[0] = name.byteLength;
  buf.set(name, 1);
  buf.set(prefix, 1 + name.byteLength);
  return buf;
}

export function encodeCancel(prefix: Uint8Array): Uint8Array {
  const name = encoder.encode("CANCEL");
  const buf = new Uint8Array(1 + name.byteLength + prefix.byteLength);
  buf[0] = name.byteLength;
  buf.set(name, 1);
  buf.set(prefix, 1 + name.byteLength);
  return buf;
}

export function encodeJoin(group: Uint8Array): Uint8Array {
  const name = encoder.encode("JOIN");
  const buf = new Uint8Array(1 + name.byteLength + group.byteLength);
  buf[0] = name.byteLength;
  buf.set(name, 1);
  buf.set(group, 1 + name.byteLength);
  return buf;
}

export function encodeLeave(group: Uint8Array): Uint8Array {
  const name = encoder.encode("LEAVE");
  const buf = new Uint8Array(1 + name.byteLength + group.byteLength);
  buf[0] = name.byteLength;
  buf.set(name, 1);
  buf.set(group, 1 + name.byteLength);
  return buf;
}

export function encodePong(context: Uint8Array): Uint8Array {
  if (context.byteLength > 16) throw new Error("PONG context too long");
  const name = encoder.encode("PONG");
  const buf = new Uint8Array(1 + name.byteLength + context.byteLength);
  buf[0] = name.byteLength;
  buf.set(name, 1);
  buf.set(context, 1 + name.byteLength);
  return buf;
}

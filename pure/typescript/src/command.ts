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

function encodeCommand(
  name: string,
  body: Uint8Array<ArrayBufferLike> = new Uint8Array(0),
): Uint8Array {
  const nameBytes = encoder.encode(name);
  if (nameBytes.byteLength > 255) throw new Error("Command name too long");

  const buf = new Uint8Array(1 + nameBytes.byteLength + body.byteLength);
  buf[0] = nameBytes.byteLength;
  buf.set(nameBytes, 1);
  buf.set(body, 1 + nameBytes.byteLength);
  return buf;
}

export function encodeReadyProperties(
  socketType: SocketTypeName,
  identity: Uint8Array,
): Uint8Array {
  const props: [string, Uint8Array<ArrayBufferLike>][] = [
    ["Socket-Type", encoder.encode(socketType)],
  ];
  if (identity.byteLength > 0) {
    props.push(["Identity", identity]);
  }

  let size = 0;
  for (const [key, value] of props) {
    size += 1 + encoder.encode(key).byteLength + 4 + value.byteLength;
  }

  const buf = new Uint8Array(size);
  const view = new DataView(buf.buffer);
  let offset = 0;

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

export function encodeReady(
  socketType: SocketTypeName,
  identity: Uint8Array,
): Uint8Array {
  return encodeCommand("READY", encodeReadyProperties(socketType, identity));
}

export function encodePlainHello(
  username: string,
  password: string,
): Uint8Array {
  const usernameBytes = encoder.encode(username);
  const passwordBytes = encoder.encode(password);
  if (usernameBytes.byteLength > 255) {
    throw new Error("PLAIN username exceeds 255 bytes");
  }
  if (passwordBytes.byteLength > 255) {
    throw new Error("PLAIN password exceeds 255 bytes");
  }

  const body = new Uint8Array(
    2 + usernameBytes.byteLength + passwordBytes.byteLength,
  );
  let offset = 0;
  body[offset++] = usernameBytes.byteLength;
  body.set(usernameBytes, offset);
  offset += usernameBytes.byteLength;
  body[offset++] = passwordBytes.byteLength;
  body.set(passwordBytes, offset);
  return encodeCommand("HELLO", body);
}

export function encodePlainInitiate(
  socketType: SocketTypeName,
  identity: Uint8Array,
): Uint8Array {
  return encodeCommand(
    "INITIATE",
    encodeReadyProperties(socketType, identity),
  );
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

export function decodeErrorReason(body: Uint8Array): string {
  if (body.byteLength < 1) throw new Error("ERROR body missing reason length");
  const reasonLen = body[0]!;
  if (body.byteLength < 1 + reasonLen) {
    throw new Error("ERROR reason truncated");
  }
  return decoder.decode(body.subarray(1, 1 + reasonLen));
}

export function encodeSubscribe(prefix: Uint8Array): Uint8Array {
  return encodeCommand("SUBSCRIBE", prefix);
}

export function encodeCancel(prefix: Uint8Array): Uint8Array {
  return encodeCommand("CANCEL", prefix);
}

export function encodeJoin(group: Uint8Array): Uint8Array {
  return encodeCommand("JOIN", group);
}

export function encodeLeave(group: Uint8Array): Uint8Array {
  return encodeCommand("LEAVE", group);
}

export function encodePong(context: Uint8Array): Uint8Array {
  if (context.byteLength > 16) throw new Error("PONG context too long");
  return encodeCommand("PONG", context);
}

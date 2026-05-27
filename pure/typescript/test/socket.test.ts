import { describe, it, expect, vi, beforeEach } from "vitest"
import { encodeReady } from "../src/command.ts"
import { encodeZwsFrame, encodeCommandFrame, FLAG_FINAL, FLAG_MORE, FLAG_COMMAND, decodeZwsFrame } from "../src/zws.ts"
import { decodeCommand } from "../src/command.ts"
import { Message } from "../src/message.ts"
import { Push } from "../src/push.ts"
import { Sub } from "../src/sub.ts"
import { Req } from "../src/req.ts"

class MockWebSocket {
  static CONNECTING = 0
  static OPEN = 1
  static CLOSING = 2
  static CLOSED = 3

  binaryType = "blob"
  protocol = "ZWS2.0/NULL"
  readyState = MockWebSocket.CONNECTING
  sentFrames: Uint8Array[] = []

  onopen: (() => void) | null = null
  onmessage: ((ev: { data: ArrayBuffer }) => void) | null = null
  onclose: (() => void) | null = null
  onerror: (() => void) | null = null

  constructor(
    public url: string,
    public protocols: string[],
  ) {}

  send(data: Uint8Array): void {
    this.sentFrames.push(new Uint8Array(data))
  }

  close(): void {
    this.readyState = MockWebSocket.CLOSED
    this.onclose?.()
  }

  simulateOpen(): void {
    this.readyState = MockWebSocket.OPEN
    this.onopen?.()
  }

  simulateMessage(data: Uint8Array): void {
    this.onmessage?.({ data: data.buffer.slice(data.byteOffset, data.byteOffset + data.byteLength) as ArrayBuffer })
  }
}

let createdSockets: MockWebSocket[] = []

beforeEach(() => {
  createdSockets = []
  // @ts-expect-error mock global WebSocket
  globalThis.WebSocket = class extends MockWebSocket {
    constructor(url: string, protocols: string[]) {
      super(url, protocols)
      createdSockets.push(this)
    }
  }
})

function makeReady(ws: MockWebSocket, serverType = "PULL"): void {
  ws.simulateOpen()
  const ready = encodeReady(serverType as "PUSH", new Uint8Array(0))
  ws.simulateMessage(encodeCommandFrame(ready))
}


describe("Push", () => {
  it("sends message after connection is ready", async () => {
    const push = new Push()
    push.connect("ws://localhost:8083")
    const ws = createdSockets[0]!

    const sendPromise = push.send(Message.from("COLOR 10:20 FF0000"))
    makeReady(ws, "PULL")

    await sendPromise
    // sentFrames[0] = READY, sentFrames[1] = data
    expect(ws.sentFrames.length).toBe(2)
    expect(ws.sentFrames[1]![0]).toBe(FLAG_FINAL)
    const payload = ws.sentFrames[1]!.subarray(1)
    expect(new TextDecoder().decode(payload)).toBe("COLOR 10:20 FF0000")
  })

  it("round-robins across multiple connections", async () => {
    const push = new Push()
    push.connect("ws://localhost:8083")
    push.connect("ws://localhost:9083")
    const ws1 = createdSockets[0]!
    const ws2 = createdSockets[1]!

    makeReady(ws1, "PULL")
    makeReady(ws2, "PULL")

    await push.send(Message.from("msg1"))
    await push.send(Message.from("msg2"))

    // Each WS got READY + 1 data frame
    // After round-robin, first send goes to ws1 (or ws2), second to the other
    const total = ws1.sentFrames.length + ws2.sentFrames.length
    expect(total).toBe(4) // 2 READYs + 2 data frames
  })

  it("queues send until connection ready", async () => {
    const push = new Push()
    push.connect("ws://localhost:8083")
    const ws = createdSockets[0]!

    const promise = push.send(Message.from("queued"))
    // Not ready yet, should only have 0 frames
    expect(ws.sentFrames.length).toBe(0)

    makeReady(ws, "PULL")
    await promise
    expect(ws.sentFrames.length).toBe(2) // READY + data
  })
})


describe("Sub", () => {
  it("sends SUBSCRIBE commands after handshake", () => {
    const sub = new Sub()
    sub.subscribe("canvas:")
    sub.subscribe("gol:")

    sub.connect("ws://localhost:8081")
    const ws = createdSockets[0]!
    makeReady(ws, "PUB")

    // sentFrames: [READY, SUBSCRIBE canvas:, SUBSCRIBE gol:]
    expect(ws.sentFrames.length).toBe(3)

    const sub1 = decodeCommand(ws.sentFrames[1]!.subarray(1))
    expect(sub1.name).toBe("SUBSCRIBE")
    expect(new TextDecoder().decode(sub1.body)).toBe("canvas:")

    const sub2 = decodeCommand(ws.sentFrames[2]!.subarray(1))
    expect(sub2.name).toBe("SUBSCRIBE")
    expect(new TextDecoder().decode(sub2.body)).toBe("gol:")
  })

  it("sends subscriptions to new connections too", () => {
    const sub = new Sub()
    sub.subscribe("feed:")
    sub.connect("ws://localhost:8081")
    makeReady(createdSockets[0]!, "PUB")

    sub.connect("ws://localhost:9081")
    makeReady(createdSockets[1]!, "PUB")

    // Second WS should also get SUBSCRIBE
    const frames = createdSockets[1]!.sentFrames
    expect(frames.length).toBe(2) // READY + SUBSCRIBE
    const cmd = decodeCommand(frames[1]!.subarray(1))
    expect(cmd.name).toBe("SUBSCRIBE")
    expect(new TextDecoder().decode(cmd.body)).toBe("feed:")
  })

  it("receives messages via recv()", async () => {
    const sub = new Sub()
    sub.subscribe("canvas:")
    sub.connect("ws://localhost:8081")
    const ws = createdSockets[0]!
    makeReady(ws, "PUB")

    const recvPromise = sub.recv()
    ws.simulateMessage(encodeZwsFrame(FLAG_FINAL, new TextEncoder().encode("hello")))

    const msg = await recvPromise
    expect(msg.string(0)).toBe("hello")
  })

  it("receives multipart messages", async () => {
    const sub = new Sub()
    sub.subscribe("")
    sub.connect("ws://localhost:8081")
    const ws = createdSockets[0]!
    makeReady(ws, "PUB")

    const recvPromise = sub.recv()

    ws.simulateMessage(encodeZwsFrame(FLAG_MORE, new TextEncoder().encode("topic")))
    ws.simulateMessage(encodeZwsFrame(FLAG_FINAL, new TextEncoder().encode("body")))

    const msg = await recvPromise
    expect(msg.parts.length).toBe(2)
    expect(msg.string(0)).toBe("topic")
    expect(msg.string(1)).toBe("body")
  })

  it("queues messages when no recv() is pending", async () => {
    const sub = new Sub()
    sub.subscribe("")
    sub.connect("ws://localhost:8081")
    const ws = createdSockets[0]!
    makeReady(ws, "PUB")

    ws.simulateMessage(encodeZwsFrame(FLAG_FINAL, new TextEncoder().encode("first")))
    ws.simulateMessage(encodeZwsFrame(FLAG_FINAL, new TextEncoder().encode("second")))

    const msg1 = await sub.recv()
    const msg2 = await sub.recv()
    expect(msg1.string(0)).toBe("first")
    expect(msg2.string(0)).toBe("second")
  })

  it("sends CANCEL on unsubscribe", () => {
    const sub = new Sub()
    sub.subscribe("canvas:")
    sub.connect("ws://localhost:8081")
    const ws = createdSockets[0]!
    makeReady(ws, "PUB")

    sub.unsubscribe("canvas:")

    const lastFrame = ws.sentFrames[ws.sentFrames.length - 1]!
    const cmd = decodeCommand(lastFrame.subarray(1))
    expect(cmd.name).toBe("CANCEL")
    expect(new TextDecoder().decode(cmd.body)).toBe("canvas:")
  })

  it("unsubscribe is idempotent", () => {
    const sub = new Sub()
    sub.connect("ws://localhost:8081")
    const ws = createdSockets[0]!
    makeReady(ws, "PUB")

    const before = ws.sentFrames.length
    sub.unsubscribe("nonexistent:")
    expect(ws.sentFrames.length).toBe(before)
  })
})


describe("Req", () => {
  it("sends with empty delimiter and receives reply", async () => {
    const req = new Req({ identity: "web:paddor" })
    req.connect("ws://localhost:8082")
    const ws = createdSockets[0]!
    makeReady(ws, "ROUTER")

    const sendPromise = req.send(Message.from("CANVAS"))

    // Sent frames: READY + empty delimiter + CANVAS
    expect(ws.sentFrames.length).toBe(3)
    expect(ws.sentFrames[1]![0]).toBe(FLAG_MORE) // empty delimiter
    expect(ws.sentFrames[1]!.subarray(1).byteLength).toBe(0) // empty body
    expect(ws.sentFrames[2]![0]).toBe(FLAG_FINAL) // CANVAS
    expect(new TextDecoder().decode(ws.sentFrames[2]!.subarray(1))).toBe("CANVAS")

    // Server replies: [empty delimiter, version, snapshot]
    ws.simulateMessage(encodeZwsFrame(FLAG_MORE, new Uint8Array(0))) // delimiter
    ws.simulateMessage(encodeZwsFrame(FLAG_MORE, new Uint8Array([0, 0, 0, 1]))) // version
    ws.simulateMessage(encodeZwsFrame(FLAG_FINAL, new TextEncoder().encode("snapshot data")))

    const reply = await sendPromise
    expect(reply.parts.length).toBe(2) // delimiter stripped
    expect(reply.parts[0]).toEqual(new Uint8Array([0, 0, 0, 1]))
    expect(reply.string(1)).toBe("snapshot data")
  })

  it("rejects double send (strict alternation)", async () => {
    const req = new Req()
    req.connect("ws://localhost:8082")
    const ws = createdSockets[0]!
    makeReady(ws, "ROUTER")

    req.send(Message.from("CANVAS")) // first send, pending reply

    await expect(req.send(Message.from("GOL"))).rejects.toThrow(
      "must receive a reply",
    )
  })

  it("allows send after receiving reply", async () => {
    const req = new Req()
    req.connect("ws://localhost:8082")
    const ws = createdSockets[0]!
    makeReady(ws, "ROUTER")

    const promise1 = req.send(Message.from("CANVAS"))
    ws.simulateMessage(encodeZwsFrame(FLAG_MORE, new Uint8Array(0)))
    ws.simulateMessage(encodeZwsFrame(FLAG_FINAL, new TextEncoder().encode("reply1")))
    await promise1

    // Should be able to send again
    const promise2 = req.send(Message.from("GOL"))
    ws.simulateMessage(encodeZwsFrame(FLAG_MORE, new Uint8Array(0)))
    ws.simulateMessage(encodeZwsFrame(FLAG_FINAL, new TextEncoder().encode("reply2")))
    const reply2 = await promise2
    expect(reply2.string(0)).toBe("reply2")
  })
})


describe("Socket connection management", () => {
  it("connect is idempotent for same URL", () => {
    const push = new Push()
    push.connect("ws://localhost:8083")
    push.connect("ws://localhost:8083")
    expect(push.connectionCount).toBe(1)
    expect(createdSockets.length).toBe(1)
  })

  it("disconnect removes connection", () => {
    const push = new Push()
    push.connect("ws://localhost:8083")
    expect(push.connectionCount).toBe(1)
    push.disconnect("ws://localhost:8083")
    expect(push.connectionCount).toBe(0)
  })

  it("close removes all connections", () => {
    const push = new Push()
    push.connect("ws://localhost:8083")
    push.connect("ws://localhost:9083")
    expect(push.connectionCount).toBe(2)
    push.close()
    expect(push.connectionCount).toBe(0)
  })

  it("tracks ready count", () => {
    const push = new Push()
    push.connect("ws://localhost:8083")
    push.connect("ws://localhost:9083")
    expect(push.readyCount).toBe(0)

    makeReady(createdSockets[0]!, "PULL")
    expect(push.readyCount).toBe(1)

    makeReady(createdSockets[1]!, "PULL")
    expect(push.readyCount).toBe(2)
  })

  it("readyCount decreases on disconnect", () => {
    const push = new Push()
    push.connect("ws://localhost:8083")
    push.connect("ws://localhost:9083")
    makeReady(createdSockets[0]!, "PULL")
    makeReady(createdSockets[1]!, "PULL")
    expect(push.readyCount).toBe(2)

    push.disconnect("ws://localhost:8083")
    expect(push.readyCount).toBe(1)
    expect(push.connectionCount).toBe(1)
  })
})


describe("Sub fair-queue across connections", () => {
  it("receives messages from multiple connections", async () => {
    const sub = new Sub()
    sub.subscribe("")
    sub.connect("ws://localhost:8081")
    sub.connect("ws://localhost:9081")
    const ws1 = createdSockets[0]!
    const ws2 = createdSockets[1]!

    makeReady(ws1, "PUB")
    makeReady(ws2, "PUB")

    ws1.simulateMessage(encodeZwsFrame(FLAG_FINAL, new TextEncoder().encode("from-ws1")))
    ws2.simulateMessage(encodeZwsFrame(FLAG_FINAL, new TextEncoder().encode("from-ws2")))

    const msg1 = await sub.recv()
    const msg2 = await sub.recv()

    const received = [msg1.string(0), msg2.string(0)].sort()
    expect(received).toEqual(["from-ws1", "from-ws2"])
  })
})


describe("Req edge cases", () => {
  it("waits for connection to be ready before first send", async () => {
    const req = new Req()
    req.connect("ws://localhost:8082")
    const ws = createdSockets[0]!

    const sendPromise = req.send(Message.from("CANVAS"))

    // Not ready yet
    expect(ws.sentFrames.length).toBe(0)

    makeReady(ws, "ROUTER")

    // Allow the send() microtask to flush (it awaits waitForReady)
    await new Promise((r) => setTimeout(r, 0))

    // Now the READY + data frames should be sent
    expect(ws.sentFrames.length).toBe(3) // READY + delimiter + CANVAS

    // Server replies
    ws.simulateMessage(encodeZwsFrame(FLAG_MORE, new Uint8Array(0)))
    ws.simulateMessage(encodeZwsFrame(FLAG_FINAL, new TextEncoder().encode("snapshot")))

    const reply = await sendPromise
    expect(reply.string(0)).toBe("snapshot")
  })

  it("multipart reply preserves all parts after delimiter", async () => {
    const req = new Req()
    req.connect("ws://localhost:8082")
    const ws = createdSockets[0]!
    makeReady(ws, "ROUTER")

    const promise = req.send(Message.from("CANVAS"))

    // Reply: [delimiter, version, payload1, payload2]
    ws.simulateMessage(encodeZwsFrame(FLAG_MORE, new Uint8Array(0)))
    ws.simulateMessage(encodeZwsFrame(FLAG_MORE, new Uint8Array([0, 0, 0, 42])))
    ws.simulateMessage(encodeZwsFrame(FLAG_MORE, new TextEncoder().encode("part1")))
    ws.simulateMessage(encodeZwsFrame(FLAG_FINAL, new TextEncoder().encode("part2")))

    const reply = await promise
    expect(reply.parts.length).toBe(3) // delimiter stripped
    expect(reply.parts[0]).toEqual(new Uint8Array([0, 0, 0, 42]))
    expect(reply.string(1)).toBe("part1")
    expect(reply.string(2)).toBe("part2")
  })

  it("ignores unsolicited replies", async () => {
    const req = new Req()
    req.connect("ws://localhost:8082")
    const ws = createdSockets[0]!
    makeReady(ws, "ROUTER")

    // Server sends a reply when nothing was asked
    ws.simulateMessage(encodeZwsFrame(FLAG_MORE, new Uint8Array(0)))
    ws.simulateMessage(encodeZwsFrame(FLAG_FINAL, new TextEncoder().encode("unsolicited")))

    // Should not throw. Now send a real request and it should work.
    const promise = req.send(Message.from("CANVAS"))
    ws.simulateMessage(encodeZwsFrame(FLAG_MORE, new Uint8Array(0)))
    ws.simulateMessage(encodeZwsFrame(FLAG_FINAL, new TextEncoder().encode("real reply")))

    const reply = await promise
    expect(reply.string(0)).toBe("real reply")
  })
})


describe("Push with identity", () => {
  it("sends identity in READY handshake", async () => {
    const push = new Push({ identity: "cli:bot" })
    push.connect("ws://localhost:8083")
    const ws = createdSockets[0]!
    makeReady(ws, "PULL")

    // READY was sent
    const readyFrame = ws.sentFrames[0]!
    expect(readyFrame[0]).toBe(FLAG_COMMAND)

    const cmd = decodeCommand(readyFrame.subarray(1))
    expect(cmd.name).toBe("READY")

    const { decodeReadyProperties } = await import("../src/command.ts")
    const props = decodeReadyProperties(cmd.body)
    expect(props.socketType).toBe("PUSH")
    expect(new TextDecoder().decode(props.identity)).toBe("cli:bot")
  })
})

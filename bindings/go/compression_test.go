package omq

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"strings"
	"testing"
	"time"
)

var zstdTestDict = mustHex("37a430ecbeaadd5c811120841042664644444444244902002114c418638c21841042" +
	"082184104208214444444444444444240900005110638c31c618630c21c418636666" +
	"864692040080000000c000000000010000")
var lz4TestDict = []byte("omq-quote-symbol-price-volume-json-shared-prefix")

func TestCompressionTransports(t *testing.T) {
	for _, transport := range []string{"lz4+tcp", "zstd+tcp"} {
		t.Run(transport, func(t *testing.T) {
			compressionRoundTrip(t, transport+"://127.0.0.1:*")
		})
	}
}

func TestCompressionStaticDict(t *testing.T) {
	tests := []struct {
		name      string
		endpoint  string
		dict      []byte
		threshold int
	}{
		{name: "lz4", endpoint: "lz4+tcp://127.0.0.1:*", dict: lz4TestDict, threshold: 32},
		{name: "zstd", endpoint: "zstd+tcp://127.0.0.1:*", dict: zstdTestDict, threshold: 32},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := openTestContext(t)
			defer closeContext(t, ctx)

			pull := newTestSocket(t, ctx, Pull)
			defer closeSocket(t, pull)
			push, err := ctx.Socket(Push,
				CompressionDict(test.dict),
				CompressionThreshold(test.threshold),
				CompressionLevel(1),
			)
			if err != nil {
				t.Fatal(err)
			}
			defer closeSocket(t, push)

			endpoint, err := pull.Bind(test.endpoint)
			if err != nil {
				t.Fatal(err)
			}
			if err := push.Connect(endpoint); err != nil {
				t.Fatal(err)
			}
			if _, err := push.WaitConnectedTimeout(1, 5*time.Second); err != nil {
				t.Fatal(err)
			}
			payload := compressionPayload(7, 512)
			if err := push.SendTimeout(Bytes(payload), time.Second); err != nil {
				t.Fatal(err)
			}
			msg, err := pull.RecvTimeout(time.Second)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(msg.Bytes(), payload) {
				t.Fatalf("payload mismatch")
			}
		})
	}
}

func TestCompressionAutoTrainAndMultipart(t *testing.T) {
	ctx := openTestContext(t)
	defer closeContext(t, ctx)

	pull, err := ctx.Socket(Pull, CompressionAutoTrain(true), CompressionDictCapacity(4096))
	if err != nil {
		t.Fatal(err)
	}
	defer closeSocket(t, pull)
	push, err := ctx.Socket(Push,
		CompressionAutoTrain(true),
		CompressionDictCapacity(4096),
		CompressionThreshold(32),
		CompressionLevel(1),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer closeSocket(t, push)

	endpoint, err := pull.Bind("lz4+tcp://127.0.0.1:*")
	if err != nil {
		t.Fatal(err)
	}
	if err := push.Connect(endpoint); err != nil {
		t.Fatal(err)
	}
	if _, err := push.WaitConnectedTimeout(1, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 140; i++ {
		if err := push.SendTimeout(Bytes(compressionPayload(i, 768)), time.Second); err != nil {
			t.Fatal(err)
		}
	}
	wantMulti := Multipart([]byte("meta"), []byte("payload"))
	if err := push.SendTimeout(wantMulti, time.Second); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 140; i++ {
		msg, err := pull.RecvTimeout(time.Second)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(msg.Bytes(), compressionPayload(i, 768)) {
			t.Fatalf("payload %d mismatch", i)
		}
	}
	gotMulti, err := pull.RecvTimeout(time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if gotMulti.Len() != 2 || string(gotMulti.Part(0)) != "meta" || string(gotMulti.Part(1)) != "payload" {
		t.Fatalf("multipart = %#v", gotMulti.Parts())
	}
}

func compressionRoundTrip(t *testing.T, endpoint string) {
	t.Helper()
	ctx := openTestContext(t)
	defer closeContext(t, ctx)

	pull, err := ctx.Socket(Pull, CompressionAutoTrain(true))
	if err != nil {
		t.Fatal(err)
	}
	defer closeSocket(t, pull)
	push, err := ctx.Socket(Push, CompressionAutoTrain(true))
	if err != nil {
		t.Fatal(err)
	}
	defer closeSocket(t, push)

	bound, err := pull.Bind(endpoint)
	if err != nil {
		t.Fatal(err)
	}
	if err := push.Connect(bound); err != nil {
		t.Fatal(err)
	}
	if _, err := push.WaitConnectedTimeout(1, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	if err := push.SendTimeout(String(`{"kind":"json","value":42}`), time.Second); err != nil {
		t.Fatal(err)
	}
	msg, err := pull.RecvTimeout(time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if got := msg.String(); got != `{"kind":"json","value":42}` {
		t.Fatalf("message = %q", got)
	}
}

func compressionPayload(seq, size int) []byte {
	head := fmt.Sprintf(`{"kind":"quote","symbol":"OMQ","seq":%d,"pad":"`, seq)
	tail := `"}`
	return []byte(head + strings.Repeat("A", size-len(head)-len(tail)) + tail)
}

func mustHex(input string) []byte {
	out, err := hex.DecodeString(input)
	if err != nil {
		panic(err)
	}
	return out
}

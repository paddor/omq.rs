#!/usr/bin/env python3
"""Measure OMQ Go, grpc-go, and pebbe/zmq4 over 2-process TCP loopback.

Adapted from bindings/pyomq/scripts/update_perf.py. Benchmark rows are
append-only in ~/.cache/omq.go/bindings.jsonl by default. The chart is
regenerated from the latest cached row per implementation, kind, and size.
"""

import argparse
import datetime as dt
import html
import json
import math
import os
import selectors
import shutil
import subprocess
import sys
import textwrap
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
REPO = ROOT.parents[1]
CACHE_DIR = Path(
    os.environ.get(
        "OMQ_GO_CACHE_DIR",
        Path(os.environ.get("XDG_CACHE_HOME", Path.home() / ".cache")) / "omq.go",
    )
)
JSONL = CACHE_DIR / "bindings.jsonl"
HARNESS_DIR = CACHE_DIR / "pushpull_tcp_peer"
HARNESS_BIN = HARNESS_DIR / "omq-go-bench-peer"
CHART_DIR = ROOT / "doc" / "charts"
CHART = CHART_DIR / "bindings.svg"

DEFAULT_SIZES = [16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768]
QUICK_SIZES = [16, 128, 1024, 4096, 32768]
LATENCY_MAX_SIZE = 4096
DEFAULT_IMPLS = ["omq-run-into", "zmq4", "grpc"]
DEFAULT_LATENCY_IMPLS = ["omq", "zmq4", "grpc"]
SUPPORTED_LATENCY_IMPLS = ["omq", "omq-run", "zmq4", "grpc"]
IMPL_LABELS = {
    "omq": "OMQ.go scalar",
    "omq-run": "OMQ.go Socket.Run",
    "omq-run-into": "OMQ.go hot path",
    "zmq4": "zmq4",
    "grpc": "gRPC (grpc-go v1.75.0)",
}
COLORS = {
    "omq": "#ef4444",
    "omq-run": "#fb923c",
    "omq-run-into": "#ef4444",
    "zmq4": "#60a5fa",
    "grpc": "#f472b6",
}

GO_PEER = r'''
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/encoding"
	omq "github.com/paddor/omq.rs/bindings/go"
	zmq4 "github.com/pebbe/zmq4"
)

const hwm = 1_000_000
const linger = 5 * time.Second
const timeout = 120 * time.Second
const maxTimeCheckMessages = 1024
const latencySampleCapacity = 16384
const grpcStreamMethod = "/bench.Blob/Stream"
const grpcSinkMethod = "/bench.Blob/Sink"
const grpcEchoMethod = "/bench.Blob/Echo"

type rawCodec struct{}

func (rawCodec) Name() string { return "raw" }
func (rawCodec) Marshal(value any) ([]byte, error) {
	switch value := value.(type) {
	case []byte:
		return value, nil
	case *[]byte:
		return *value, nil
	default:
		return nil, fmt.Errorf("raw codec: got %T", value)
	}
}
func (rawCodec) Unmarshal(data []byte, value any) error {
	output, ok := value.(*[]byte)
	if !ok {
		return fmt.Errorf("raw codec: got %T", value)
	}
	*output = append((*output)[:0], data...)
	return nil
}

type grpcBlobService struct {
	size     int
	duration time.Duration
	warmup   time.Duration
	done     chan struct{}
}
type grpcBlobServiceServer interface {
	stream(grpc.ServerStream) error
	sink(grpc.ServerStream) error
	echo(context.Context, []byte) ([]byte, error)
}

func (s *grpcBlobService) stream(stream grpc.ServerStream) error {
	payload := make([]byte, s.size)
	for {
		if err := stream.SendMsg(&payload); err != nil {
			return err
		}
	}
}
func (s *grpcBlobService) sink(stream grpc.ServerStream) error {
	deadline := time.Now().Add(s.warmup)
	for time.Now().Before(deadline) {
		var payload []byte
		if err := stream.RecvMsg(&payload); err != nil {
			return err
		}
	}
	started := time.Now()
	deadline = started.Add(s.duration)
	count := 0
	var payload []byte
	for {
		if err := stream.RecvMsg(&payload); err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		count++
		if count%timeCheckEvery(s.size) == 0 && !time.Now().Before(deadline) {
			break
		}
	}
	ended := time.Now()
	printThroughput("grpc", "", s.size, count, started, ended)
	close(s.done)
	return nil
}
func (s *grpcBlobService) echo(_ context.Context, input []byte) ([]byte, error) {
	return input, nil
}

var grpcBlobServiceDesc = grpc.ServiceDesc{
	ServiceName: "bench.Blob",
	HandlerType: (*grpcBlobServiceServer)(nil),
	Streams: []grpc.StreamDesc{{
		StreamName:    "Stream",
		ServerStreams: true,
		Handler: func(server any, stream grpc.ServerStream) error {
			return server.(grpcBlobServiceServer).stream(stream)
		},
	}, {
		StreamName:    "Sink",
		ClientStreams: true,
		Handler: func(server any, stream grpc.ServerStream) error {
			return server.(grpcBlobServiceServer).sink(stream)
		},
	}},
	Methods: []grpc.MethodDesc{{
		MethodName: "Echo",
		Handler: func(server any, ctx context.Context, decode func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
			var input []byte
			if err := decode(&input); err != nil {
				return nil, err
			}
			handler := func(ctx context.Context, request any) (any, error) {
				return server.(grpcBlobServiceServer).echo(ctx, *request.(*[]byte))
			}
			if interceptor == nil {
				return handler(ctx, &input)
			}
			return interceptor(ctx, &input, &grpc.UnaryServerInfo{Server: server, FullMethod: grpcEchoMethod}, handler)
		},
	}},
}

type result struct {
	Impl     string  `json:"impl"`
	Endpoint string  `json:"endpoint"`
	MsgSize  int     `json:"msg_size"`
	Messages int     `json:"messages"`
	Seconds  float64 `json:"seconds,omitempty"`
	MsgsS    float64 `json:"msgs_s,omitempty"`
	GBS      float64 `json:"gb_s,omitempty"`
	P50US    float64 `json:"p50_us,omitempty"`
	P99US    float64 `json:"p99_us,omitempty"`
}

func main() {
	if len(os.Args) != 8 {
		die("usage: omq-go-bench-peer <pushpull|reqrep> <omq|omq-run|omq-run-into|zmq4> <push|pull|req|rep> <endpoint> <size> <duration> <warmup>")
	}
	bench := os.Args[1]
	impl := os.Args[2]
	role := os.Args[3]
	endpoint := os.Args[4]
	size := parseInt(os.Args[5], "size")
	if size < 0 {
		die("invalid size")
	}

	switch bench {
	case "pushpull":
		duration := parseDurationSeconds(os.Args[6], "duration")
		warmup := parseDurationSeconds(os.Args[7], "warmup")
		if duration <= 0 || warmup < 0 {
			die("invalid duration/warmup")
		}
		switch role {
		case "pull":
			runPull(impl, endpoint, size, duration, warmup)
		case "push":
			runPush(impl, endpoint, size)
		default:
			die("bad pushpull role: " + role)
		}
	case "reqrep":
		duration := parseDurationSeconds(os.Args[6], "duration")
		warmup := parseDurationSeconds(os.Args[7], "warmup")
		if duration <= 0 || warmup < 0 {
			die("invalid duration/warmup")
		}
		switch role {
		case "rep":
			runRep(impl, endpoint, size)
		case "req":
			runReq(impl, endpoint, size, duration, warmup)
		default:
			die("bad reqrep role: " + role)
		}
	default:
		die("bad bench: " + bench)
	}
}

func parseInt(value string, name string) int {
	parsed, err := strconv.Atoi(value)
	if err != nil {
		die("invalid " + name + ": " + value)
	}
	return parsed
}

func parseDurationSeconds(value string, name string) time.Duration {
	parsed, err := strconv.ParseFloat(value, 64)
	if err != nil {
		die("invalid " + name + ": " + value)
	}
	return time.Duration(parsed * float64(time.Second))
}

func die(message string) {
	fmt.Fprintln(os.Stderr, message)
	os.Exit(1)
}

func ready(endpoint string) {
	fmt.Println("READY " + endpoint)
}

func printResult(row result) {
	data, err := json.Marshal(row)
	if err != nil {
		die(err.Error())
	}
	fmt.Printf("RESULT %s\n", data)
}

func makePayload(size int) []byte {
	payload := make([]byte, size)
	for i := range payload {
		payload[i] = byte(i)
	}
	return payload
}

func benchContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), timeout)
}

func runPull(impl string, endpoint string, size int, duration time.Duration, warmup time.Duration) {
	switch impl {
	case "omq":
		runOMQPull(endpoint, size, duration, warmup, false)
	case "omq-run-into":
		runOMQPull(endpoint, size, duration, warmup, true)
	case "zmq4":
		runZMQPull(endpoint, size, duration, warmup)
	case "grpc":
		runGRPCPull(endpoint, size, duration, warmup)
	default:
		die("bad impl: " + impl)
	}
}

func runPush(impl string, endpoint string, size int) {
	switch impl {
	case "omq":
		runOMQPush(endpoint, size, false)
	case "omq-run-into":
		runOMQPush(endpoint, size, true)
	case "zmq4":
		runZMQPush(endpoint, size)
	case "grpc":
		runGRPCPush(endpoint, size)
	default:
		die("bad impl: " + impl)
	}
}

func openOMQSocket(socketType omq.SocketType, sender bool) (*omq.Context, *omq.Socket) {
	ctx, err := omq.Open(omq.Config{IOThreads: 1})
	if err != nil {
		die(err.Error())
	}
	options := []omq.SocketOption{omq.Linger(linger)}
	if sender {
		options = append(options, omq.SendHWM(hwm))
	} else {
		options = append(options, omq.RecvHWM(hwm))
	}
	socket, err := ctx.Socket(socketType, options...)
	if err != nil {
		_ = ctx.Close()
		die(err.Error())
	}
	return ctx, socket
}

func runOMQPull(endpoint string, size int, duration time.Duration, warmup time.Duration, into bool) {
	ctx, pull := openOMQSocket(omq.Pull, false)
	defer ctx.Close()
	defer pull.Close(context.Background())
	if _, err := pull.Bind(endpoint); err != nil {
		die(err.Error())
	}
	ready(endpoint)

	runCtx, cancel := benchContext()
	defer cancel()
	if into {
		buffer := make([]byte, size)
		var started time.Time
		var ended time.Time
		var count int
		err := pull.Run(runCtx, func(socket *omq.BoundSocket) error {
			if warmup > 0 {
				if _, err := recvOMQIntoFor(socket, buffer, size, warmup); err != nil {
					return err
				}
			}
			started = time.Now()
			var err error
			count, err = recvOMQIntoFor(socket, buffer, size, duration)
			ended = time.Now()
			return err
		})
		if err != nil {
			die(err.Error())
		}
		printThroughput("omq-run-into", endpoint, size, count, started, ended)
		return
	}

	if warmup > 0 {
		recvOMQFor(pull, runCtx, size, warmup)
	}
	started := time.Now()
	count := recvOMQFor(pull, runCtx, size, duration)
	printThroughput("omq", endpoint, size, count, started, time.Now())
}

func runOMQPush(endpoint string, size int, run bool) {
	ctx, push := openOMQSocket(omq.Push, true)
	defer ctx.Close()
	defer push.Close(context.Background())
	if err := push.Connect(endpoint); err != nil {
		die(err.Error())
	}
	payload := makePayload(size)
	msg := omq.Bytes(payload)
	runCtx, cancel := benchContext()
	defer cancel()
	if run {
		err := push.Run(runCtx, func(socket *omq.BoundSocket) error {
			for {
				if err := socket.SendBlocking(msg); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			die(err.Error())
		}
		return
	}
	for {
		if err := push.Send(runCtx, msg); err != nil {
			die(err.Error())
		}
	}
}

func recvOMQFor(pull *omq.Socket, ctx context.Context, size int, duration time.Duration) int {
	count := 0
	checkEvery := timeCheckEvery(size)
	deadline := time.Now().Add(duration)
	for {
		msg, err := pull.Recv(ctx)
		if err != nil {
			die(err.Error())
		}
		if len(msg.Bytes()) != size {
			die(fmt.Sprintf("expected %d bytes, got %d", size, len(msg.Bytes())))
		}
		count++
		if count%checkEvery == 0 && !time.Now().Before(deadline) {
			return count
		}
	}
}

func recvOMQIntoFor(pull *omq.BoundSocket, buffer []byte, size int, duration time.Duration) (int, error) {
	count := 0
	checkEvery := timeCheckEvery(size)
	deadline := time.Now().Add(duration)
	for {
		n, err := pull.RecvIntoBlocking(buffer)
		if err != nil {
			return count, err
		}
		if n != size {
			return count, fmt.Errorf("expected %d bytes, got %d", size, n)
		}
		count++
		if count%checkEvery == 0 && !time.Now().Before(deadline) {
			return count, nil
		}
	}
}

func timeCheckEvery(size int) int {
	if size <= 0 {
		return maxTimeCheckMessages
	}
	n := (1024 * 1024) / size
	if n < 1 {
		return 1
	}
	if n > maxTimeCheckMessages {
		return maxTimeCheckMessages
	}
	return n
}

func openZMQSocket(socketType zmq4.Type, sender bool) (*zmq4.Context, *zmq4.Socket) {
	ctx, err := zmq4.NewContext()
	if err != nil {
		die(err.Error())
	}
	if err := ctx.SetIoThreads(1); err != nil {
		_ = ctx.Term()
		die(err.Error())
	}
	socket, err := ctx.NewSocket(socketType)
	if err != nil {
		_ = ctx.Term()
		die(err.Error())
	}
	if sender {
		if err := socket.SetSndhwm(hwm); err != nil {
			die(err.Error())
		}
	} else {
		if err := socket.SetRcvhwm(hwm); err != nil {
			die(err.Error())
		}
	}
	if err := socket.SetLinger(linger); err != nil {
		die(err.Error())
	}
	return ctx, socket
}

func runZMQPull(endpoint string, size int, duration time.Duration, warmup time.Duration) {
	ctx, pull := openZMQSocket(zmq4.PULL, false)
	defer ctx.Term()
	defer pull.Close()
	if err := pull.Bind(endpoint); err != nil {
		die(err.Error())
	}
	ready(endpoint)
	if warmup > 0 {
		recvZMQFor(pull, size, warmup)
	}
	started := time.Now()
	count := recvZMQFor(pull, size, duration)
	printThroughput("zmq4", endpoint, size, count, started, time.Now())
}

func runZMQPush(endpoint string, size int) {
	ctx, push := openZMQSocket(zmq4.PUSH, true)
	defer ctx.Term()
	defer push.Close()
	if err := push.Connect(endpoint); err != nil {
		die(err.Error())
	}
	payload := makePayload(size)
	for {
		if _, err := push.SendBytes(payload, 0); err != nil {
			die(err.Error())
		}
	}
}

func recvZMQFor(pull *zmq4.Socket, size int, duration time.Duration) int {
	count := 0
	checkEvery := timeCheckEvery(size)
	deadline := time.Now().Add(duration)
	for {
		msg, err := pull.RecvBytes(0)
		if err != nil {
			die(err.Error())
		}
		if len(msg) != size {
			die(fmt.Sprintf("expected %d bytes, got %d", size, len(msg)))
		}
		count++
		if count%checkEvery == 0 && !time.Now().Before(deadline) {
			return count
		}
	}
}

func printThroughput(impl string, endpoint string, size int, messages int, start time.Time, end time.Time) {
	seconds := end.Sub(start).Seconds()
	msgsS := float64(messages) / seconds
	printResult(result{
		Impl:     impl,
		Endpoint: endpoint,
		MsgSize:  size,
		Messages: messages,
		Seconds:  seconds,
		MsgsS:    msgsS,
		GBS:      msgsS * float64(size) / 1_000_000_000.0,
	})
}

func runRep(impl string, endpoint string, size int) {
	switch impl {
	case "omq":
		runOMQRep(endpoint)
	case "omq-run":
		runOMQRepRun(endpoint, size)
	case "zmq4":
		runZMQRep(endpoint)
	case "grpc":
		runGRPCRep(endpoint, size)
	default:
		die("latency unsupported for impl: " + impl)
	}
}

func runReq(impl string, endpoint string, size int, duration time.Duration, warmup time.Duration) {
	switch impl {
	case "omq":
		runOMQReq(endpoint, size, duration, warmup)
	case "omq-run":
		runOMQReqRun(endpoint, size, duration, warmup)
	case "zmq4":
		runZMQReq(endpoint, size, duration, warmup)
	case "grpc":
		runGRPCReq(endpoint, size, duration, warmup)
	default:
		die("latency unsupported for impl: " + impl)
	}
}

func runOMQRep(endpoint string) {
	ctx, rep := openOMQSocket(omq.Rep, false)
	defer ctx.Close()
	defer rep.Close(context.Background())
	if _, err := rep.Bind(endpoint); err != nil {
		die(err.Error())
	}
	ready(endpoint)
	runCtx, cancel := benchContext()
	defer cancel()
	for {
		msg, err := rep.Recv(runCtx)
		if err != nil {
			die(err.Error())
		}
		if err := rep.Send(runCtx, msg); err != nil {
			die(err.Error())
		}
	}
}

func runOMQRepRun(endpoint string, size int) {
	ctx, rep := openOMQSocket(omq.Rep, false)
	defer ctx.Close()
	defer rep.Close(context.Background())
	if _, err := rep.Bind(endpoint); err != nil {
		die(err.Error())
	}
	ready(endpoint)
	runCtx, cancel := benchContext()
	defer cancel()
	buffer := make([]byte, size)
	reply := omq.Bytes(makePayload(size))
	err := rep.Run(runCtx, func(socket *omq.BoundSocket) error {
		for {
			n, err := socket.RecvIntoBlocking(buffer)
			if err != nil {
				return err
			}
			if n != size {
				return fmt.Errorf("expected %d bytes, got %d", size, n)
			}
			if err := socket.SendBlocking(reply); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		die(err.Error())
	}
}

func runOMQReq(endpoint string, size int, duration time.Duration, warmup time.Duration) {
	ctx, req := openOMQSocket(omq.Req, true)
	defer ctx.Close()
	defer req.Close(context.Background())
	if err := req.Connect(endpoint); err != nil {
		die(err.Error())
	}
	payload := makePayload(size)
	msg := omq.Bytes(payload)
	runCtx, cancel := benchContext()
	defer cancel()
	warmupDeadline := time.Now().Add(warmup)
	for time.Now().Before(warmupDeadline) {
		omqRoundTrip(req, runCtx, msg, size)
	}
	deadline := time.Now().Add(duration)
	durations := make([]float64, 0, latencySampleCapacity)
	for {
		started := time.Now()
		omqRoundTrip(req, runCtx, msg, size)
		durations = append(durations, float64(time.Since(started).Nanoseconds())/1000.0)
		if !time.Now().Before(deadline) {
			break
		}
	}
	printLatency("omq", endpoint, size, durations)
}

func runOMQReqRun(endpoint string, size int, duration time.Duration, warmup time.Duration) {
	ctx, req := openOMQSocket(omq.Req, true)
	defer ctx.Close()
	defer req.Close(context.Background())
	if err := req.Connect(endpoint); err != nil {
		die(err.Error())
	}
	payload := makePayload(size)
	msg := omq.Bytes(payload)
	buffer := make([]byte, size)
	durations := make([]float64, 0, latencySampleCapacity)
	runCtx, cancel := benchContext()
	defer cancel()
	err := req.Run(runCtx, func(socket *omq.BoundSocket) error {
		warmupDeadline := time.Now().Add(warmup)
		for time.Now().Before(warmupDeadline) {
			if err := omqRoundTripRun(socket, msg, buffer, size); err != nil {
				return err
			}
		}
		deadline := time.Now().Add(duration)
		for {
			started := time.Now()
			if err := omqRoundTripRun(socket, msg, buffer, size); err != nil {
				return err
			}
			durations = append(durations, float64(time.Since(started).Nanoseconds())/1000.0)
			if !time.Now().Before(deadline) {
				break
			}
		}
		return nil
	})
	if err != nil {
		die(err.Error())
	}
	printLatency("omq-run", endpoint, size, durations)
}

func omqRoundTrip(req *omq.Socket, ctx context.Context, msg omq.Message, size int) {
	if err := req.Send(ctx, msg); err != nil {
		die(err.Error())
	}
	reply, err := req.Recv(ctx)
	if err != nil {
		die(err.Error())
	}
	if len(reply.Bytes()) != size {
		die(fmt.Sprintf("expected %d bytes, got %d", size, len(reply.Bytes())))
	}
}

func omqRoundTripRun(req *omq.BoundSocket, msg omq.Message, buffer []byte, size int) error {
	if err := req.SendBlocking(msg); err != nil {
		return err
	}
	n, err := req.RecvIntoBlocking(buffer)
	if err != nil {
		return err
	}
	if n != size {
		return fmt.Errorf("expected %d bytes, got %d", size, n)
	}
	return nil
}

func runZMQRep(endpoint string) {
	ctx, rep := openZMQSocket(zmq4.REP, false)
	defer ctx.Term()
	defer rep.Close()
	if err := rep.Bind(endpoint); err != nil {
		die(err.Error())
	}
	ready(endpoint)
	for {
		msg, err := rep.RecvBytes(0)
		if err != nil {
			die(err.Error())
		}
		if _, err := rep.SendBytes(msg, 0); err != nil {
			die(err.Error())
		}
	}
}

func runZMQReq(endpoint string, size int, duration time.Duration, warmup time.Duration) {
	ctx, req := openZMQSocket(zmq4.REQ, true)
	defer ctx.Term()
	defer req.Close()
	if err := req.Connect(endpoint); err != nil {
		die(err.Error())
	}
	payload := makePayload(size)
	warmupDeadline := time.Now().Add(warmup)
	for time.Now().Before(warmupDeadline) {
		zmqRoundTrip(req, payload, size)
	}
	deadline := time.Now().Add(duration)
	durations := make([]float64, 0, latencySampleCapacity)
	for {
		started := time.Now()
		zmqRoundTrip(req, payload, size)
		durations = append(durations, float64(time.Since(started).Nanoseconds())/1000.0)
		if !time.Now().Before(deadline) {
			break
		}
	}
	printLatency("zmq4", endpoint, size, durations)
}

func grpcAddr(endpoint string) string {
	return strings.TrimPrefix(endpoint, "tcp://")
}

func grpcServer(endpoint string, size int, duration, warmup time.Duration, done chan struct{}) (*grpc.Server, net.Listener) {
	encoding.RegisterCodec(rawCodec{})
	listener, err := net.Listen("tcp", grpcAddr(endpoint))
	if err != nil {
		die(err.Error())
	}
	server := grpc.NewServer(grpc.ForceServerCodec(rawCodec{}))
	server.RegisterService(&grpcBlobServiceDesc, &grpcBlobService{
		size: size, duration: duration, warmup: warmup, done: done,
	})
	return server, listener
}

func grpcClient(endpoint string) *grpc.ClientConn {
	conn, err := grpc.NewClient(
		grpcAddr(endpoint),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.ForceCodec(rawCodec{})),
	)
	if err != nil {
		die(err.Error())
	}
	return conn
}

func runGRPCPull(endpoint string, size int, duration, warmup time.Duration) {
	done := make(chan struct{})
	server, listener := grpcServer(endpoint, size, duration, warmup, done)
	ready(endpoint)
	go func() {
		if err := server.Serve(listener); err != nil {
			die(err.Error())
		}
	}()
	<-done
	server.Stop()
}

func runGRPCPush(endpoint string, size int) {
	conn := grpcClient(endpoint)
	defer conn.Close()
	stream, err := conn.NewStream(context.Background(), &grpc.StreamDesc{ClientStreams: true}, grpcSinkMethod)
	if err != nil {
		die(err.Error())
	}
	payload := makePayload(size)
	for {
		if err := stream.SendMsg(&payload); err != nil {
			return
		}
	}
}

func runGRPCRep(endpoint string, size int) {
	done := make(chan struct{})
	server, listener := grpcServer(endpoint, size, 0, 0, done)
	ready(endpoint)
	if err := server.Serve(listener); err != nil {
		die(err.Error())
	}
}

func runGRPCReq(endpoint string, size int, duration, warmup time.Duration) {
	conn := grpcClient(endpoint)
	defer conn.Close()
	payload := makePayload(size)
	warmupDeadline := time.Now().Add(warmup)
	var reply []byte
	for time.Now().Before(warmupDeadline) {
		if err := conn.Invoke(context.Background(), grpcEchoMethod, &payload, &reply); err != nil {
			die(err.Error())
		}
	}
	deadline := time.Now().Add(duration)
	durations := make([]float64, 0, latencySampleCapacity)
	for {
		started := time.Now()
		if err := conn.Invoke(context.Background(), grpcEchoMethod, &payload, &reply); err != nil {
			die(err.Error())
		}
		durations = append(durations, float64(time.Since(started).Nanoseconds())/1000.0)
		if !time.Now().Before(deadline) {
			break
		}
	}
	printLatency("grpc", endpoint, size, durations)
}

func zmqRoundTrip(req *zmq4.Socket, payload []byte, size int) {
	if _, err := req.SendBytes(payload, 0); err != nil {
		die(err.Error())
	}
	reply, err := req.RecvBytes(0)
	if err != nil {
		die(err.Error())
	}
	if len(reply) != size {
		die(fmt.Sprintf("expected %d bytes, got %d", size, len(reply)))
	}
}

func printLatency(impl string, endpoint string, size int, durations []float64) {
	sort.Float64s(durations)
	messages := len(durations)
	p50 := durations[len(durations)*50/100]
	p99 := durations[len(durations)*99/100]
	printResult(result{
		Impl:     impl,
		Endpoint: endpoint,
		MsgSize:  size,
		Messages: messages,
		P50US:    p50,
		P99US:    p99,
	})
}
'''


def parse_csv_ints(value):
    sizes = []
    for raw in value.split(","):
        raw = raw.strip().lower()
        if not raw:
            continue
        multiplier = 1
        if raw.endswith("kib"):
            multiplier = 1024
            raw = raw[:-3]
        elif raw.endswith("kb"):
            multiplier = 1000
            raw = raw[:-2]
        elif raw.endswith("k"):
            multiplier = 1024
            raw = raw[:-1]
        size = int(raw) * multiplier
        if size <= 0:
            raise argparse.ArgumentTypeError("sizes must be positive")
        sizes.append(size)
    if not sizes:
        raise argparse.ArgumentTypeError("at least one size is required")
    return sizes


def parse_csv_strings(value):
    return [part for part in value.split(",") if part]


def latency_sizes_from(sizes):
    return [size for size in sizes if size <= LATENCY_MAX_SIZE]


def run(cmd, cwd=ROOT, timeout=None, fail_on_warning=True):
    print("+ " + " ".join(str(part) for part in cmd), flush=True)
    result = subprocess.run(
        cmd,
        cwd=cwd,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        timeout=timeout,
        check=False,
    )
    if result.stdout:
        sys.stdout.write(result.stdout)
    if result.returncode != 0:
        raise SystemExit(result.returncode)
    if fail_on_warning and has_noise(result.stdout):
        raise RuntimeError("command printed warning/timeout")


def has_noise(text):
    lowered = (text or "").lower()
    return "warning" in lowered or "timeout" in lowered


def build_native(args):
    if args.no_build:
        return
    run(
        [
            "cargo",
            "build",
            "--release",
            "--features",
            "plain,curve,lz4,zstd",
            "--manifest-path",
            str(ROOT / "native" / "Cargo.toml"),
        ],
        cwd=REPO,
    )


def write_harness():
    HARNESS_DIR.mkdir(parents=True, exist_ok=True)
    (HARNESS_DIR / "go.mod").write_text(
        textwrap.dedent(
            f"""\
            module omq-go-perf

            go 1.25

            require (
            \tgithub.com/paddor/omq.rs/bindings/go v0.0.0
            \tgithub.com/pebbe/zmq4 v1.4.0
            \tgoogle.golang.org/grpc v1.75.0
            )

            replace github.com/paddor/omq.rs/bindings/go => {ROOT}
            """
        )
    )
    (HARNESS_DIR / "main.go").write_text(GO_PEER.lstrip())


def build_harness(args):
    write_harness()
    run(["go", "mod", "tidy"], cwd=HARNESS_DIR, fail_on_warning=False)
    if args.no_harness_build and HARNESS_BIN.exists():
        return
    run(["go", "build", "-o", str(HARNESS_BIN), "."], cwd=HARNESS_DIR)


def env_with_native_lib():
    env = os.environ.copy()
    release = ROOT / "native" / "target" / "release"
    debug = ROOT / "native" / "target" / "debug"
    key = "DYLD_LIBRARY_PATH" if sys.platform == "darwin" else "LD_LIBRARY_PATH"
    env[key] = os.pathsep.join(
        [str(release), str(debug), env[key]] if key in env else [str(release), str(debug)]
    )
    return env


def free_endpoint():
    import socket

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    finally:
        sock.close()
    return f"tcp://127.0.0.1:{port}"


def read_line_timeout(proc, seconds):
    selector = selectors.DefaultSelector()
    selector.register(proc.stdout, selectors.EVENT_READ)
    try:
        events = selector.select(seconds)
        if not events:
            return None
        return proc.stdout.readline()
    finally:
        selector.close()


def peer_cmd(bench, impl, role, endpoint, size, amount, warmup):
    return [
        str(HARNESS_BIN),
        bench,
        impl,
        role,
        endpoint,
        str(size),
        str(amount),
        str(warmup),
    ]


def parse_result(output):
    for line in output.splitlines():
        if line.startswith("RESULT "):
            return json.loads(line[len("RESULT ") :])
    raise RuntimeError("missing RESULT line:\n" + output)


def fail_on_noise(name, stdout, stderr):
    text = (stdout or "") + "\n" + (stderr or "")
    if has_noise(text):
        raise RuntimeError(f"{name} printed warning/timeout:\n{text}")


def kill(proc):
    if proc.poll() is not None:
        return proc.communicate(timeout=5)
    proc.kill()
    return proc.communicate(timeout=5)


def run_throughput_pair_once(impl, size, duration, warmup, timeout):
    endpoint = free_endpoint()
    env = env_with_native_lib()
    receiver = subprocess.Popen(
        peer_cmd("pushpull", impl, "pull", endpoint, size, f"{duration:.6f}", f"{warmup:.6f}"),
        cwd=ROOT,
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    sender = None
    try:
        ready = read_line_timeout(receiver, 10)
        if ready is None or not ready.startswith("READY "):
            out, err = receiver.communicate(timeout=1) if receiver.poll() is not None else ("", "")
            raise RuntimeError(f"receiver did not become ready:\n{ready or ''}{out}{err}")

        sender = subprocess.Popen(
            peer_cmd("pushpull", impl, "push", endpoint, size, f"{duration:.6f}", f"{warmup:.6f}"),
            cwd=ROOT,
            env=env,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        out, err = receiver.communicate(timeout=timeout)
        out = ready + out
        fail_on_noise("receiver", out, err)
        if receiver.returncode != 0:
            raise RuntimeError(f"receiver failed:\n{out}{err}")
        sender_out, sender_err = kill(sender)
        fail_on_noise("sender", sender_out, sender_err)
        return parse_result(out)
    except Exception:
        kill(receiver)
        if sender is not None:
            kill(sender)
        raise


def run_pair_once(bench, impl, receiver_role, sender_role, size, duration, warmup, timeout):
    endpoint = free_endpoint()
    env = env_with_native_lib()
    receiver = subprocess.Popen(
        peer_cmd(bench, impl, receiver_role, endpoint, size, duration, warmup),
        cwd=ROOT,
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        ready = read_line_timeout(receiver, 10)
        if ready is None or not ready.startswith("READY "):
            out, err = receiver.communicate(timeout=1) if receiver.poll() is not None else ("", "")
            raise RuntimeError(f"receiver did not become ready:\n{ready or ''}{out}{err}")

        sender = subprocess.run(
            peer_cmd(bench, impl, sender_role, endpoint, size, duration, warmup),
            cwd=ROOT,
            env=env,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=timeout,
            check=False,
        )
        fail_on_noise("sender", sender.stdout, sender.stderr)
        if sender.returncode != 0:
            raise RuntimeError(f"sender failed:\n{sender.stdout}{sender.stderr}")

        if bench == "reqrep":
            out, err = kill(receiver)
            fail_on_noise("receiver", ready + out, err)
            return parse_result(sender.stdout)
        out, err = receiver.communicate(timeout=timeout)
        out = ready + out
        fail_on_noise("receiver", out, err)
        if receiver.returncode != 0:
            raise RuntimeError(f"receiver failed:\n{out}{err}")
        return parse_result(out)
    except Exception:
        kill(receiver)
        raise


def run_throughput_cell(impl, size, args):
    runs = []
    total = args.warmup_rounds + args.rounds
    timeout = args.timeout + args.warmup_duration + args.duration
    for round_index in range(total):
        row = run_throughput_pair_once(
            impl,
            size,
            args.duration,
            args.warmup_duration,
            timeout,
        )
        if round_index >= args.warmup_rounds:
            runs.append(row)
        print(
            f"  {impl:12s} size={size:6d} round={round_index + 1}/{total} "
            f"{row['msgs_s']:12.0f} msg/s {row['gb_s']:7.3f} GB/s "
            f"n={row['messages']} t={row['seconds']:.3f}s",
            flush=True,
        )
    return sorted(runs, key=lambda row: row["msgs_s"])[len(runs) // 2]


def run_latency_cell(impl, size, args):
    runs = []
    total = args.warmup_rounds + args.rounds
    timeout = args.timeout + args.latency_warmup_duration + args.latency_duration
    for round_index in range(total):
        row = run_pair_once(
            "reqrep",
            impl,
            "rep",
            "req",
            size,
            f"{args.latency_duration:.6f}",
            f"{args.latency_warmup_duration:.6f}",
            timeout,
        )
        row["target_seconds"] = args.latency_duration
        row["warmup_seconds"] = args.latency_warmup_duration
        if round_index >= args.warmup_rounds:
            runs.append(row)
        print(
            f"  {impl:12s} size={size:6d} round={round_index + 1}/{total} "
            f"p50 {row['p50_us']:8.1f} μs p99 {row['p99_us']:8.1f} μs "
            f"n={row['messages']}",
            flush=True,
        )
    return sorted(runs, key=lambda row: row["p50_us"])[len(runs) // 2]


def append_jsonl(rows):
    JSONL.parent.mkdir(parents=True, exist_ok=True)
    with JSONL.open("a") as file:
        for row in rows:
            file.write(json.dumps(row, sort_keys=True) + "\n")


def load_jsonl():
    rows = []
    try:
        with JSONL.open() as file:
            for line in file:
                line = line.strip()
                if line:
                    rows.append(json.loads(line))
    except FileNotFoundError:
        pass
    return rows


def fmt_size(size):
    if size >= 1024:
        return f"{size // 1024} KiB"
    return f"{size} B"


def fmt_rate(rate):
    if rate >= 1_000_000:
        return f"{rate / 1_000_000:.2f} M/s"
    return f"{rate / 1_000:.0f} k/s"


def print_table(rows, sizes, latency_sizes, impls, latency_impls):
    by_key = {(row["kind"], row["msg_size"], row["impl"]): row for row in rows}
    print()
    if impls:
        print("PUSH/PULL TCP throughput")
        print("size    impl             msg/s      GB/s   vs zmq4")
        for size in sizes:
            base = by_key.get(("pushpull_tcp", size, "zmq4"))
            base_msgs = base["msgs_s"] if base else 0.0
            for impl in impls:
                row = by_key.get(("pushpull_tcp", size, impl))
                if row is None:
                    continue
                ratio = row["msgs_s"] / base_msgs if base_msgs else 0.0
                print(
                    f"{size:6d} {impl:12s} {row['msgs_s']:11.0f} "
                    f"{row['gb_s']:8.3f} {ratio:9.2f}x"
                )
            print()
    if latency_impls:
        print("REQ/REP TCP latency")
        print("size    impl             p50 μs    p99 μs   vs zmq4")
        for size in latency_sizes:
            base = by_key.get(("reqrep_tcp_latency", size, "zmq4"))
            base_p50 = base["p50_us"] if base else 0.0
            for impl in latency_impls:
                row = by_key.get(("reqrep_tcp_latency", size, impl))
                if row is None:
                    continue
                ratio = base_p50 / row["p50_us"] if row["p50_us"] else 0.0
                print(
                    f"{size:6d} {impl:12s} {row['p50_us']:9.1f} "
                    f"{row['p99_us']:9.1f} {ratio:9.2f}x"
                )
            print()


def latest_chart_data(sizes, latency_sizes, impls, latency_impls):
    latest = {}
    for row in load_jsonl():
        kind = row.get("kind")
        impl = row.get("impl")
        size = row.get("msg_size")
        key = (kind, impl, size)
        prev = latest.get(key)
        if prev is None or row.get("run_id", "") >= prev.get("run_id", ""):
            latest[key] = row

    throughput = {
        impl: [
            latest.get(("pushpull_tcp", impl, size), {}).get("msgs_s", 0.0)
            for size in sizes
        ]
        for impl in impls
    }
    latency = {
        impl: [
            latest.get(("reqrep_tcp_latency", impl, size), {}).get("p50_us", 0.0)
            for size in latency_sizes
        ]
        for impl in latency_impls
    }
    return {"throughput": throughput, "latency": latency}


def nice_ceil(value):
    if value <= 0:
        return 1
    exp = math.floor(math.log10(value))
    base = 10**exp
    for multiplier in [1, 2, 5, 10]:
        candidate = multiplier * base
        if candidate >= value:
            return candidate
    return 10 * base


def fmt_y_rate(value):
    if value >= 1_000_000:
        return f"{value / 1_000_000:g}M"
    if value >= 1_000:
        return f"{value / 1_000:g}k"
    return f"{value:g}"


def fmt_y_mbps(value):
    if value >= 1000:
        return f"{value / 1000:g} GB/s"
    if value >= 10:
        return f"{value:.0f} MB/s"
    return f"{value:.1f} MB/s"


def fmt_y_us(value):
    if value >= 1000:
        return f"{value / 1000:g} ms"
    return f"{value:g} μs"


def read_chart_hw():
    config = {}
    path = REPO / ".chart_hw"
    try:
        with path.open() as file:
            for line in file:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                key, sep, value = line.partition("=")
                if sep:
                    config[key.strip()] = value.strip()
    except OSError:
        pass
    return config


def detect_hardware():
    config = read_chart_hw()
    label = os.environ.get("OMQ_HW_LABEL") or config.get("label")
    if label:
        return label
    prefix = os.environ.get("OMQ_HW_PREFIX") or config.get("prefix")
    postfix = os.environ.get("OMQ_HW_POSTFIX") or config.get("postfix")
    if prefix and postfix:
        return f"{prefix}, {postfix}"
    if prefix:
        return prefix
    if postfix:
        return postfix
    return None


def chart_selection(args, sizes):
    if args.latency_only or args.throughput_only:
        return (
            DEFAULT_SIZES,
            latency_sizes_from(DEFAULT_SIZES),
            DEFAULT_IMPLS,
            DEFAULT_LATENCY_IMPLS,
        )
    return sizes, latency_sizes_from(sizes), args.impls, args.latency_impls


def svg_line(points, color, dashed=False):
    dash = ' stroke-dasharray="6,4"' if dashed else ""
    return (
        f'  <polyline points="{points}" fill="none" stroke="{color}"'
        f' stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"{dash}/>'
    )


def gen_chart(data, path, sizes, latency_sizes, impls, latency_impls):
    hw_label = detect_hardware()
    hw_offset = 14 if hw_label else 0
    svg_w = 850
    svg_h = 810 + hw_offset
    x_left, x_right = 60, 790
    plot_w = x_right - x_left
    top_left, top_mid, top_right = 60, 395, 790
    top_right_left = 455
    t1_top = 95 + hw_offset
    t1_bot = 430 + hw_offset
    t1_h = t1_bot - t1_top
    t2_top = t1_bot + 105
    t2_bot = t2_top + 200
    t2_h = t2_bot - t2_top
    mid_x = (x_left + x_right) / 2
    small_sizes = [size for size in sizes if size <= 1024]
    large_sizes = [size for size in sizes if size >= 256]
    small_indices = [sizes.index(size) for size in small_sizes]
    large_indices = [sizes.index(size) for size in large_sizes]
    small_xs = [
        top_left + i * (top_mid - top_left) / max(len(small_sizes) - 1, 1)
        for i in range(len(small_sizes))
    ]
    large_xs = [
        top_right_left + i * (top_right - top_right_left) / max(len(large_sizes) - 1, 1)
        for i in range(len(large_sizes))
    ]
    lat_xs = [
        x_left + i * plot_w / max(len(latency_sizes) - 1, 1)
        for i in range(len(latency_sizes))
    ]

    small_rates = [
        rate
        for values in data["throughput"].values()
        for index in small_indices
        for rate in [values[index]]
    ]
    gbs_values = [
        rate * sizes[index] / 1_000_000_000.0
        for values in data["throughput"].values()
        for index in large_indices
        for rate in [values[index]]
    ]
    msg_max = max(5_000_000, nice_ceil(max(small_rates or [0]) * 1.05))
    gbs_max = max(1, math.ceil(max(gbs_values or [0])))
    lat_max = 200

    def y_msg(value):
        return t1_bot - (value / msg_max) * t1_h

    def y_gbs(value):
        return t1_bot - (value / gbs_max) * t1_h

    def y_lat(value):
        return t2_bot - (min(value, lat_max) / lat_max) * t2_h

    lines = [
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {svg_w} {svg_h}"'
        f' font-family="system-ui, -apple-system, sans-serif">',
        f'  <rect width="{svg_w}" height="{svg_h}" fill="#000000"/>',
        f'  <text x="{mid_x}" y="{t1_top - 65}" text-anchor="middle" fill="#f9fafb"'
        f' font-size="13" font-weight="700">'
        f"PUSH/PULL throughput: 2-process, TCP loopback (higher is better)</text>",
    ]
    if hw_label:
        lines.append(
            f'  <text x="{mid_x}" y="{t1_top - 51}" text-anchor="middle"'
            f' fill="#9ca3af" font-size="10">{html.escape(hw_label)}</text>'
        )

    panels = (
        (
            top_left,
            top_mid,
            small_xs,
            [msg_max * i / 10 for i in range(1, 11)],
            msg_max,
            fmt_y_rate,
            top_left - 8,
        ),
        (
            top_right_left,
            top_right,
            large_xs,
            [i / 2 for i in range(1, int(gbs_max * 2) + 1)],
            gbs_max,
            lambda value: f"{value:g} GB/s",
            top_right + 8,
        ),
    )
    for panel_left, panel_right, panel_xs, ticks, panel_max, formatter, label_x in panels:
        for tick in ticks:
            yy = t1_bot - (tick / panel_max) * t1_h
            lines.append(
                f'  <line x1="{panel_left}" y1="{yy:.1f}" x2="{panel_right}"'
                f' y2="{yy:.1f}" stroke="#374151" stroke-width="1"/>'
            )
            anchor = "end" if label_x < panel_left else "start"
            lines.append(
                f'  <text x="{label_x}" y="{yy:.1f}" text-anchor="{anchor}"'
                f' dominant-baseline="middle" fill="#e5e7eb" font-size="10">'
                f"{formatter(tick)}</text>"
            )
        for x in panel_xs:
            lines.append(
                f'  <line x1="{x:.1f}" y1="{t1_top}" x2="{x:.1f}" y2="{t1_bot}"'
                f' stroke="#374151" stroke-width="1"/>'
            )
        if panel_left == top_left:
            lines.append(
                f'  <line x1="{panel_left}" y1="{t1_top}" x2="{panel_left}"'
                f' y2="{t1_bot}" stroke="#9ca3af" stroke-width="1.5"/>'
            )
        if panel_right == top_right:
            lines.append(
                f'  <line x1="{panel_right}" y1="{t1_top}" x2="{panel_right}"'
                f' y2="{t1_bot}" stroke="#9ca3af" stroke-width="1.5"/>'
            )
        lines.append(
            f'  <line x1="{panel_left}" y1="{t1_bot}" x2="{panel_right}"'
            f' y2="{t1_bot}" stroke="#9ca3af" stroke-width="1.5"/>'
        )

    lines.append(
        f'  <text x="{(top_left + top_mid) / 2:.1f}" y="{t1_top - 17}"'
        f' text-anchor="middle" fill="#f9fafb" font-size="12" font-weight="700">'
        f"small messages</text>"
    )
    lines.append(
        f'  <text x="{(top_right_left + top_right) / 2:.1f}" y="{t1_top - 17}"'
        f' text-anchor="middle" fill="#f9fafb" font-size="12" font-weight="700">'
        f"medium/large messages</text>"
    )

    for impl in impls:
        values = data["throughput"].get(impl, [])
        points = " ".join(
            f"{small_xs[j]:.1f},{y_msg(values[index]):.1f}"
            for j, index in enumerate(small_indices)
        )
        lines.append(svg_line(points, COLORS[impl], dashed=True))
    for impl in impls:
        values = data["throughput"].get(impl, [])
        gbs = [values[index] * sizes[index] / 1_000_000_000.0 for index in large_indices]
        points = " ".join(f"{large_xs[i]:.1f},{y_gbs(value):.1f}" for i, value in enumerate(gbs))
        lines.append(svg_line(points, COLORS[impl]))
        for i, value in enumerate(gbs):
            lines.append(
                f'  <circle cx="{large_xs[i]:.1f}" cy="{y_gbs(value):.1f}" r="3"'
                f' fill="{COLORS[impl]}" stroke="#000000" stroke-width="1"/>'
            )

    for i, size in enumerate(small_sizes):
        lines.append(
            f'  <text x="{small_xs[i]:.1f}" y="{t1_bot + 14}" text-anchor="middle"'
            f' fill="#e5e7eb" font-size="8.5">{fmt_size(size)}</text>'
        )
    for i, size in enumerate(large_sizes):
        lines.append(
            f'  <text x="{large_xs[i]:.1f}" y="{t1_bot + 14}" text-anchor="middle"'
            f' fill="#e5e7eb" font-size="8.5">{fmt_size(size)}</text>'
        )

    lines.append(
        f'  <text x="{mid_x:.1f}" y="{t1_bot + 32}" text-anchor="middle"'
        f' fill="#9ca3af" font-size="9">'
        f"dashed = message rate · solid = bandwidth</text>"
    )

    lines.append(
        f'  <text x="{mid_x}" y="{t2_top - 17}" text-anchor="middle" fill="#f9fafb"'
        f' font-size="13" font-weight="700">'
        f"REQ/REP latency: 2-process, TCP loopback, p50 μs (lower is better)</text>"
    )

    for i in range(1, 11):
        value = lat_max * i / 10
        yy = y_lat(value)
        lines.append(
            f'  <line x1="{x_left}" y1="{yy:.1f}" x2="{x_right}" y2="{yy:.1f}"'
            f' stroke="#374151" stroke-width="1"/>'
        )
        lines.append(
            f'  <text x="{x_left - 8}" y="{yy:.1f}" text-anchor="end"'
            f' dominant-baseline="middle" fill="#e5e7eb" font-size="10">'
            f"{fmt_y_us(value)}</text>"
        )
    for x in lat_xs:
        lines.append(
            f'  <line x1="{x:.1f}" y1="{t2_top}" x2="{x:.1f}" y2="{t2_bot}"'
            f' stroke="#374151" stroke-width="1"/>'
        )
    lines.extend(
        [
            f'  <line x1="{x_left}" y1="{t2_top}" x2="{x_left}" y2="{t2_bot}"'
            f' stroke="#9ca3af" stroke-width="1.5"/>',
            f'  <line x1="{x_left}" y1="{t2_bot}" x2="{x_right}" y2="{t2_bot}"'
            f' stroke="#9ca3af" stroke-width="1.5"/>',
        ]
    )
    for impl in latency_impls:
        values = data["latency"].get(impl, [])
        points = " ".join(
            f"{lat_xs[i]:.1f},{y_lat(value):.1f}" for i, value in enumerate(values)
        )
        lines.append(svg_line(points, COLORS[impl]))
        for i, value in enumerate(values):
            lines.append(
                f'  <circle cx="{lat_xs[i]:.1f}" cy="{y_lat(value):.1f}" r="3"'
                f' fill="{COLORS[impl]}" stroke="#000000" stroke-width="1"/>'
            )
    for i, size in enumerate(latency_sizes):
        lines.append(
            f'  <text x="{lat_xs[i]:.1f}" y="{t2_bot + 14}" text-anchor="middle"'
            f' fill="#e5e7eb" font-size="8.5">{fmt_size(size)}</text>'
        )

    add_legend(lines, [(IMPL_LABELS[impl], COLORS[impl]) for impl in latency_impls], mid_x, t2_bot + 40)
    lines.append("</svg>")

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n")
    print(f"wrote {path}")


def add_legend(lines, legend_items, mid_x, leg_y):
    item_w = 170
    total_w = len(legend_items) * item_w
    start_x = mid_x - total_w / 2
    for index, (label, color) in enumerate(legend_items):
        lx = start_x + index * item_w
        lines.append(
            f'  <line x1="{lx:.0f}" y1="{leg_y}" x2="{lx + 14:.0f}" y2="{leg_y}"'
            f' stroke="{color}" stroke-width="2.5"/>'
        )
        lines.append(f'  <circle cx="{lx + 7:.0f}" cy="{leg_y}" r="2.5" fill="{color}"/>')
        lines.append(
            f'  <text x="{lx + 20:.0f}" y="{leg_y + 4}" fill="#e5e7eb"'
            f' font-size="11" font-weight="500">{html.escape(label)}</text>'
        )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--quick", action="store_true")
    parser.add_argument("--sizes", type=parse_csv_ints)
    parser.add_argument("--impls", type=parse_csv_strings, default=DEFAULT_IMPLS)
    parser.add_argument("--latency-impls", type=parse_csv_strings, default=DEFAULT_LATENCY_IMPLS)
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument("--warmup-rounds", type=int, default=0)
    parser.add_argument("--duration", type=float, default=2.5)
    parser.add_argument("--warmup-duration", type=float, default=0.5)
    parser.add_argument("--latency-duration", type=float, default=1.5)
    parser.add_argument("--latency-warmup-duration", type=float)
    parser.add_argument("--timeout", type=float, default=120.0)
    parser.add_argument("--no-build", action="store_true")
    parser.add_argument("--no-harness-build", action="store_true")
    parser.add_argument("--no-save", action="store_true")
    parser.add_argument("--no-chart", action="store_true")
    parser.add_argument("--chart-only", action="store_true")
    parser.add_argument("--throughput-only", action="store_true")
    parser.add_argument("--latency-only", action="store_true")
    args = parser.parse_args()

    if args.throughput_only and args.latency_only:
        parser.error("--throughput-only and --latency-only are mutually exclusive")

    sizes = args.sizes or (QUICK_SIZES if args.quick else DEFAULT_SIZES)
    if args.quick:
        args.rounds = min(args.rounds, 1)
        args.warmup_rounds = 0
        args.duration = min(args.duration, 0.5)
        args.latency_duration = min(args.latency_duration, 0.5)
        args.warmup_duration = min(args.warmup_duration, 0.1)
    if args.latency_warmup_duration is None:
        args.latency_warmup_duration = args.warmup_duration
    if args.rounds < 1:
        parser.error("--rounds must be at least 1")
    if args.warmup_rounds < 0:
        parser.error("--warmup-rounds cannot be negative")
    if args.duration <= 0:
        parser.error("--duration must be positive")
    if args.warmup_duration < 0:
        parser.error("--warmup-duration cannot be negative")
    if args.latency_duration <= 0:
        parser.error("--latency-duration must be positive")
    if args.latency_warmup_duration < 0:
        parser.error("--latency-warmup-duration cannot be negative")
    for impl in args.impls:
        if impl not in IMPL_LABELS:
            parser.error(f"unknown throughput impl: {impl}")
    for impl in args.latency_impls:
        if impl not in SUPPORTED_LATENCY_IMPLS:
            parser.error(f"unknown latency impl: {impl}")
    latency_sizes = latency_sizes_from(sizes)

    if args.chart_only:
        data = latest_chart_data(sizes, latency_sizes, args.impls, args.latency_impls)
        gen_chart(data, CHART, sizes, latency_sizes, args.impls, args.latency_impls)
        return

    build_native(args)
    build_harness(args)

    run_id = dt.datetime.now(dt.UTC).strftime("%Y%m%dT%H%M%SZ")
    rows = []
    print(f"run_id={run_id}")
    if not args.latency_only:
        print("PUSH/PULL TCP throughput")
        for size in sizes:
            for impl in args.impls:
                row = run_throughput_cell(impl, size, args)
                row["run_id"] = run_id
                row["kind"] = "pushpull_tcp"
                row["transport"] = "tcp"
                rows.append(row)
    if not args.throughput_only:
        print("REQ/REP TCP latency")
        for size in latency_sizes:
            for impl in args.latency_impls:
                row = run_latency_cell(impl, size, args)
                row["run_id"] = run_id
                row["kind"] = "reqrep_tcp_latency"
                row["transport"] = "tcp"
                rows.append(row)

    if not args.no_save:
        append_jsonl(rows)
        print(f"appended {len(rows)} rows to {JSONL}")
    print_table(
        rows,
        sizes,
        latency_sizes,
        [] if args.latency_only else args.impls,
        [] if args.throughput_only else args.latency_impls,
    )

    if not args.no_chart and not args.no_save:
        chart_sizes, chart_latency_sizes, chart_impls, chart_latency_impls = chart_selection(
            args, sizes
        )
        data = latest_chart_data(
            chart_sizes, chart_latency_sizes, chart_impls, chart_latency_impls
        )
        gen_chart(
            data,
            CHART,
            chart_sizes,
            chart_latency_sizes,
            chart_impls,
            chart_latency_impls,
        )


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
    except (RuntimeError, subprocess.TimeoutExpired, OSError, FileNotFoundError) as exc:
        if shutil.which("go") is None:
            print("go not found", file=sys.stderr)
        print(f"error: {exc}", file=sys.stderr)
        sys.exit(1)

<?php
// Drop-in checks for the PECL zmq extension running on libomq_zmq.
// Run through scripts/test-php.sh, which stages libzmq.so.5 -> libomq_zmq.

declare(strict_types=1);

$failures = 0;

function check(bool $ok, string $what): void
{
    if (!$ok) {
        throw new RuntimeException("check failed: $what");
    }
}

function socket(ZMQContext $ctx, int $type, array $opts = []): ZMQSocket
{
    $s = $ctx->getSocket($type);
    $s->setSockOpt(ZMQ::SOCKOPT_LINGER, 0);
    $s->setSockOpt(ZMQ::SOCKOPT_RCVTIMEO, 2000);
    $s->setSockOpt(ZMQ::SOCKOPT_SNDTIMEO, 2000);
    foreach ($opts as $opt => $value) {
        $s->setSockOpt($opt, $value);
    }
    return $s;
}

function bind_tcp(ZMQSocket $s, string $scheme = 'tcp'): string
{
    $s->bind("$scheme://127.0.0.1:*");
    $endpoint = $s->getSockOpt(ZMQ::SOCKOPT_LAST_ENDPOINT);
    check(str_starts_with($endpoint, "$scheme://127.0.0.1:"), "last endpoint $endpoint");
    return $endpoint;
}

function pair(ZMQContext $ctx, int $server_type, int $client_type, string $endpoint): array
{
    $server = socket($ctx, $server_type);
    if (str_ends_with($endpoint, ':*')) {
        $server->bind($endpoint);
        $endpoint = $server->getSockOpt(ZMQ::SOCKOPT_LAST_ENDPOINT);
    } else {
        $server->bind($endpoint);
    }
    $client = socket($ctx, $client_type);
    $client->connect($endpoint);
    return [$server, $client];
}

function wait_readable(ZMQSocket $s, int $timeout_ms = 2000): void
{
    $poll = new ZMQPoll();
    $poll->add($s, ZMQ::POLL_IN);
    $readable = $writable = [];
    check($poll->poll($readable, $writable, $timeout_ms) === 1, 'socket readable');
}

$tests = [];

$tests['reports the libzmq ABI version'] = function (ZMQContext $ctx): void {
    check(version_compare(ZMQ::LIBZMQ_VERSION, '4.3.0', '>='), 'version ' . ZMQ::LIBZMQ_VERSION);
};

$tests['push/pull over tcp, ipc, and inproc'] = function (ZMQContext $ctx): void {
    $ipc = 'ipc://' . sys_get_temp_dir() . '/omq-php-' . getmypid() . '.sock';
    foreach (['tcp://127.0.0.1:*', $ipc, 'inproc://omq-php-pushpull'] as $endpoint) {
        [$pull, $push] = pair($ctx, ZMQ::SOCKET_PULL, ZMQ::SOCKET_PUSH, $endpoint);
        $push->send('hello');
        check($pull->recv() === 'hello', "push/pull on $endpoint");
    }
};

$tests['OMQ transports through endpoint strings'] = function (ZMQContext $ctx): void {
    foreach (['lz4+tcp', 'zstd+tcp', 'ws'] as $scheme) {
        [$pull, $push] = pair($ctx, ZMQ::SOCKET_PULL, ZMQ::SOCKET_PUSH, "$scheme://127.0.0.1:*");
        $body = str_repeat('omq', 2000);
        $push->send($body);
        check($pull->recv() === $body, "push/pull on $scheme");
    }
};

$tests['req/rep round trip'] = function (ZMQContext $ctx): void {
    [$rep, $req] = pair($ctx, ZMQ::SOCKET_REP, ZMQ::SOCKET_REQ, 'tcp://127.0.0.1:*');
    $req->send('ping');
    check($rep->recv() === 'ping', 'rep receives');
    $rep->send('pong');
    check($req->recv() === 'pong', 'req receives');
};

$tests['pub/sub filters by prefix'] = function (ZMQContext $ctx): void {
    [$pub, $sub] = pair($ctx, ZMQ::SOCKET_PUB, ZMQ::SOCKET_SUB, 'tcp://127.0.0.1:*');
    $sub->setSockOpt(ZMQ::SOCKOPT_SUBSCRIBE, 'a.');
    $deadline = microtime(true) + 2.0;
    $got = false;
    while (!$got && microtime(true) < $deadline) {
        $pub->send('b.skip');
        $pub->send('a.keep');
        $got = $sub->recv(ZMQ::MODE_DONTWAIT);
        if ($got === false) {
            usleep(10_000);
        }
    }
    check($got === 'a.keep', 'subscriber gets matching topic only');
};

$tests['dealer/router multipart with identity'] = function (ZMQContext $ctx): void {
    $router = socket($ctx, ZMQ::SOCKET_ROUTER);
    $endpoint = bind_tcp($router);
    $dealer = socket($ctx, ZMQ::SOCKET_DEALER, [ZMQ::SOCKOPT_IDENTITY => 'peer-1']);
    $dealer->connect($endpoint);
    $dealer->sendMulti(['part-1', 'part-2']);
    $frames = $router->recvMulti();
    check($frames === ['peer-1', 'part-1', 'part-2'], 'router frames ' . json_encode($frames));
    $router->sendMulti(['peer-1', 'reply']);
    check($dealer->recvMulti() === ['reply'], 'dealer reply');
};

$tests['nonblocking recv and poll'] = function (ZMQContext $ctx): void {
    [$pull, $push] = pair($ctx, ZMQ::SOCKET_PULL, ZMQ::SOCKET_PUSH, 'tcp://127.0.0.1:*');
    check($pull->recv(ZMQ::MODE_DONTWAIT) === false, 'empty nonblocking recv');
    $poll = new ZMQPoll();
    $poll->add($pull, ZMQ::POLL_IN);
    $readable = $writable = [];
    check($poll->poll($readable, $writable, 50) === 0, 'poll timeout');
    $push->send('x');
    wait_readable($pull);
    check($pull->recv(ZMQ::MODE_DONTWAIT) === 'x', 'recv after poll');
};

$tests['event loop fd and events'] = function (ZMQContext $ctx): void {
    [$pull, $push] = pair($ctx, ZMQ::SOCKET_PULL, ZMQ::SOCKET_PUSH, 'tcp://127.0.0.1:*');
    $fd = $pull->getSockOpt(ZMQ::SOCKOPT_FD);
    check(is_resource($fd), 'ZMQ_FD stream');
    $push->send('wake');
    // ZMQ_FD is edge-triggered: wait on it, then trust ZMQ_EVENTS.
    $deadline = microtime(true) + 2.0;
    while (($pull->getSockOpt(ZMQ::SOCKOPT_EVENTS) & ZMQ::POLL_IN) === 0) {
        check(microtime(true) < $deadline, 'ZMQ_EVENTS has POLLIN');
        $read = [$fd];
        $write = $except = null;
        stream_select($read, $write, $except, 0, 100_000);
    }
    check($pull->recv(ZMQ::MODE_DONTWAIT) === 'wake', 'recv after fd wake');
};

$tests['unbind and disconnect'] = function (ZMQContext $ctx): void {
    [$pull, $push] = pair($ctx, ZMQ::SOCKET_PULL, ZMQ::SOCKET_PUSH, 'tcp://127.0.0.1:*');
    $endpoint = $pull->getSockOpt(ZMQ::SOCKOPT_LAST_ENDPOINT);
    $push->disconnect($endpoint);
    $pull->unbind($endpoint);
    // The port is free again once the listener is gone.
    $other = socket($ctx, ZMQ::SOCKET_PULL);
    $deadline = microtime(true) + 2.0;
    while (true) {
        try {
            $other->bind($endpoint);
            break;
        } catch (ZMQSocketException $e) {
            check(microtime(true) < $deadline, "rebind after unbind: {$e->getMessage()}");
            usleep(10_000);
        }
    }
};

$tests['device forwards until idle callback stops it'] = function (ZMQContext $ctx): void {
    [$frontend, $client] = pair($ctx, ZMQ::SOCKET_PULL, ZMQ::SOCKET_PUSH, 'inproc://omq-php-front');
    [$backend, $worker] = pair($ctx, ZMQ::SOCKET_PUSH, ZMQ::SOCKET_PULL, 'inproc://omq-php-back');
    $client->send('job');
    $device = new ZMQDevice($frontend, $backend);
    $device->setIdleCallback(fn() => false, 50, null);
    $device->run();
    check($worker->recv() === 'job', 'forwarded message');
};

$tests['curve handshake'] = function (ZMQContext $ctx): void {
    $server_keys = ZMQ::curveKeyPair();
    $client_keys = ZMQ::curveKeyPair();
    $server = socket($ctx, ZMQ::SOCKET_PULL, [
        ZMQ::SOCKOPT_CURVE_SERVER => 1,
        ZMQ::SOCKOPT_CURVE_SECRETKEY => $server_keys['secret_key'],
    ]);
    $endpoint = bind_tcp($server);
    $client = socket($ctx, ZMQ::SOCKET_PUSH, [
        ZMQ::SOCKOPT_CURVE_SERVERKEY => $server_keys['public_key'],
        ZMQ::SOCKOPT_CURVE_PUBLICKEY => $client_keys['public_key'],
        ZMQ::SOCKOPT_CURVE_SECRETKEY => $client_keys['secret_key'],
    ]);
    $client->connect($endpoint);
    $client->send('secret');
    check($server->recv() === 'secret', 'curve message');
};

$tests['persistent context and socket'] = function (ZMQContext $ctx): void {
    $persistent = new ZMQContext(1, true);
    check($persistent->isPersistent(), 'persistent context');
    $a = $persistent->getSocket(ZMQ::SOCKET_PUSH, 'omq-php-persistent');
    $b = $persistent->getSocket(ZMQ::SOCKET_PUSH, 'omq-php-persistent');
    check($a->isPersistent() && $a->getPersistentId() === 'omq-php-persistent', 'persistent socket');
    $a->setSockOpt(ZMQ::SOCKOPT_LINGER, 0);
    check($b->getSockOpt(ZMQ::SOCKOPT_LINGER) === 0, 'same native socket');
};

$tests['socket monitor events'] = function (ZMQContext $ctx): void {
    $pull = socket($ctx, ZMQ::SOCKET_PULL);
    $pull->monitor('inproc://omq-php-monitor', ZMQ::EVENT_LISTENING | ZMQ::EVENT_ACCEPTED);
    // recvEvent() reads from a PAIR connected to the monitor endpoint.
    $monitor = socket($ctx, ZMQ::SOCKET_PAIR);
    $monitor->connect('inproc://omq-php-monitor');
    $endpoint = bind_tcp($pull);
    $event = $monitor->recvEvent();
    check($event['event'] === ZMQ::EVENT_LISTENING, 'listening event ' . json_encode($event));
    check($event['address'] === $endpoint, 'event address');
    $push = socket($ctx, ZMQ::SOCKET_PUSH);
    $push->connect($endpoint);
    $event = $monitor->recvEvent();
    check($event['event'] === ZMQ::EVENT_ACCEPTED, 'accepted event ' . json_encode($event));
};

foreach ($tests as $name => $test) {
    $ctx = new ZMQContext(1);
    try {
        $test($ctx);
        echo "ok   $name\n";
    } catch (Throwable $e) {
        $failures++;
        echo "FAIL $name: " . get_class($e) . ': ' . $e->getMessage() . "\n";
    }
}

if ($failures > 0) {
    echo "$failures failed\n";
    exit(1);
}
echo count($tests) . " passed\n";

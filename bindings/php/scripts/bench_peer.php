<?php
// Benchmark peer for scripts/update_perf.rb. The library under test is chosen
// by the dynamic loader (libzmq.so.5 or the staged libomq_zmq alias).
//
// usage: php bench_peer.php <impl> <pushpull|reqrep> <role> <endpoint> <size> <duration> <warmup>

declare(strict_types=1);

[, $impl, $pattern, $role, $endpoint, $size, $duration, $warmup] = $argv;
$size = (int) $size;
$duration = (float) $duration;
$warmup = (float) $warmup;
$payload = str_repeat('x', $size);

function now(): float
{
    return hrtime(true) / 1e9;
}

$type = match ("$pattern/$role") {
    'pushpull/pull' => ZMQ::SOCKET_PULL,
    'pushpull/push' => ZMQ::SOCKET_PUSH,
    'reqrep/rep' => ZMQ::SOCKET_REP,
    'reqrep/req' => ZMQ::SOCKET_REQ,
};
$ctx = new ZMQContext(1);
$socket = $ctx->getSocket($type);
$socket->setSockOpt(ZMQ::SOCKOPT_LINGER, 0);
if ($role === 'pull' || $role === 'rep') {
    $socket->bind($endpoint);
    echo 'ENDPOINT ', $socket->getSockOpt(ZMQ::SOCKOPT_LAST_ENDPOINT), "\n";
} else {
    $socket->connect($endpoint);
}

$result = ['impl' => $impl, 'pattern' => $pattern, 'size' => $size, 'libzmq_version' => ZMQ::LIBZMQ_VERSION];

switch ($role) {
    case 'push':
        while (true) {
            $socket->send($payload);
        }
    case 'rep':
        while (true) {
            $socket->send($socket->recv());
        }
    case 'pull':
        $deadline = now() + $warmup;
        do {
            for ($i = 0; $i < 64; $i++) {
                $socket->recv();
            }
        } while (now() < $deadline);
        $count = 0;
        $started = now();
        $deadline = $started + $duration;
        do {
            for ($i = 0; $i < 64; $i++) {
                if (strlen($socket->recv()) !== $size) {
                    fwrite(STDERR, "bad message size\n");
                    exit(1);
                }
            }
            $count += 64;
        } while (now() < $deadline);
        $elapsed = now() - $started;
        $result += ['count' => $count, 'elapsed' => $elapsed, 'msgs_s' => $count / $elapsed];
        break;
    case 'req':
        $deadline = now() + $warmup;
        do {
            $socket->send($payload);
            $socket->recv();
        } while (now() < $deadline);
        $samples = [];
        $deadline = now() + $duration;
        do {
            $t0 = hrtime(true);
            $socket->send($payload);
            $socket->recv();
            $t1 = hrtime(true);
            $samples[] = ($t1 - $t0) / 1000.0;
        } while ($t1 / 1e9 < $deadline);
        sort($samples);
        $n = count($samples);
        $result += [
            'count' => $n,
            'p50_us' => $samples[intdiv($n, 2)],
            'p99_us' => $samples[min($n - 1, intdiv($n * 99, 100))],
        ];
        break;
}

echo 'RESULT ', json_encode($result), "\n";

<?php
// Lifecycle checks for the native extension: forked children get their own
// context, persistent sockets keep their native socket and endpoints, and
// device callbacks may change the device that runs them.

declare(strict_types=1);

function check(bool $ok, string $what): void
{
    if (!$ok) {
        fwrite(STDERR, "FAIL $what\n");
        exit(1);
    }
    echo "ok   $what\n";
}

function pair(ZMQContext $ctx, string $endpoint): array
{
    $pull = $ctx->getSocket(ZMQ::SOCKET_PULL);
    $pull->setSockOpt(ZMQ::SOCKOPT_RCVTIMEO, 2000);
    $pull->bind($endpoint);
    $push = $ctx->getSocket(ZMQ::SOCKET_PUSH);
    $push->setSockOpt(ZMQ::SOCKOPT_LINGER, 1000);
    $push->connect($pull->getSockOpt(ZMQ::SOCKOPT_LAST_ENDPOINT));
    return [$pull, $push];
}

// The parent uses the default persistent context before forking, as a PHP-FPM
// master would if a preload script touched ZMQ.
$parent = new ZMQContext();
[$pull, $push] = pair($parent, 'inproc://lifecycle-parent');
$push->send('before fork');
check($pull->recv() === 'before fork', 'parent context works before fork');

// The child records its last step, so a failure says where it stopped.
$progress = tempnam(sys_get_temp_dir(), 'omq-fork-');
$pid = pcntl_fork();
if ($pid === 0) {
    $step = static fn (string $what) => file_put_contents($progress, $what);
    // The child's persistent context must be a fresh one with live IO threads.
    $step('creating a context');
    $child = new ZMQContext();
    $step('creating sockets');
    [$childPull, $childPush] = pair($child, 'tcp://127.0.0.1:*');
    $step('sending');
    $childPush->send('in child');
    $step('receiving');
    $received = $childPull->recv() === 'in child';
    $step('shutting down');
    exit($received ? 0 : 3);
}

pcntl_waitpid($pid, $status);
$childOk = pcntl_wifexited($status) && pcntl_wexitstatus($status) === 0;
$how = match (true) {
    pcntl_wifexited($status) => 'exit ' . pcntl_wexitstatus($status),
    pcntl_wifsignaled($status) => 'signal ' . pcntl_wtermsig($status),
    default => "status $status",
};
$stopped = file_get_contents($progress);
unlink($progress);
check($childOk, 'forked child gets a working context' . ($childOk ? '' : " ($how while $stopped)"));

$push->send('after fork');
check($pull->recv() === 'after fork', 'parent context works after the child exits');

// A persistent socket keeps its native socket and endpoints across lookups,
// as it does across PHP-FPM requests.
$first = $parent->getSocket(ZMQ::SOCKET_PULL, 'lifecycle-persistent', function (ZMQSocket $socket) {
    $socket->bind('inproc://lifecycle-persistent');
});
$again = $parent->getSocket(ZMQ::SOCKET_PULL, 'lifecycle-persistent', function () {
    check(false, 'on_new_socket must not run for an existing persistent socket');
});
check($again->getEndpoints()['bind'] === ['inproc://lifecycle-persistent'], 'persistent socket keeps its endpoints');

$sender = $parent->getSocket(ZMQ::SOCKET_PUSH);
$sender->connect('inproc://lifecycle-persistent');
$sender->send('persistent');
$again->setSockOpt(ZMQ::SOCKOPT_RCVTIMEO, 2000);
check($first->recv() === 'persistent', 'both handles share one native socket');

// A device callback may replace itself; the replacement runs on its own
// timeout.
$device = new ZMQDevice($parent->getSocket(ZMQ::SOCKET_SUB), $parent->getSocket(ZMQ::SOCKET_PUB));
$calls = 0;
$replaced = 0.0;
$elapsed = 0.0;
$device->setTimerCallback(function () use ($device, &$calls, &$replaced, &$elapsed) {
    $calls++;
    $replaced = microtime(true);
    $device->setTimerCallback(function () use ($replaced, &$elapsed) {
        $elapsed = (microtime(true) - $replaced) * 1000;
        return false;
    }, 60, null);
    return true;
}, 10, null);
$device->run();
check($calls === 1 && $elapsed >= 55, 'a device callback can replace itself');

// The deprecated two-argument form raises a notice; an error handler may use
// the device meanwhile.
set_error_handler(function () use ($device) {
    $device->setTimerTimeout($device->getTimerTimeout() + 1);
    return true;
});
$device->setTimerCallback(fn() => false, 'user data');
restore_error_handler();
check($device->getTimerTimeout() === 60, 'a notice handler may use the device');

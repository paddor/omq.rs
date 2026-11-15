<?php
// Lifecycle checks for the native extension: forked children get their own
// context, and persistent sockets keep their native socket and endpoints.

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

$pid = pcntl_fork();
if ($pid === 0) {
    // The child's persistent context must be a fresh one with live IO threads.
    $child = new ZMQContext();
    [$childPull, $childPush] = pair($child, 'tcp://127.0.0.1:*');
    $childPush->send('in child');
    exit($childPull->recv() === 'in child' ? 0 : 3);
}

pcntl_waitpid($pid, $status);
check(pcntl_wifexited($status) && pcntl_wexitstatus($status) === 0, 'forked child gets a working context');

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

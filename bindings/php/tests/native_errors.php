<?php
// Misuse raises the same exception classes as ext-zmq.

declare(strict_types=1);

function raises(string $class, callable $call, string $what): void
{
    try {
        $call();
    } catch (Throwable $e) {
        if ($e instanceof $class) {
            echo "ok   $what\n";
            return;
        }
        fwrite(STDERR, "FAIL $what: " . get_class($e) . ": {$e->getMessage()}\n");
        exit(1);
    }
    fwrite(STDERR, "FAIL $what: nothing raised\n");
    exit(1);
}

$ctx = new ZMQContext();
$socket = $ctx->getSocket(ZMQ::SOCKET_PUSH);

raises(Error::class, fn() => clone $socket, 'sockets cannot be cloned');
raises(Error::class, fn() => clone $ctx, 'contexts cannot be cloned');
raises(TypeError::class, fn() => $socket->send([1, 2]), 'sending an array is a TypeError');
raises(ZMQSocketException::class, fn() => $ctx->getSocket(999), 'an unknown socket type is rejected');
raises(ZMQSocketException::class, fn() => $socket->getSockOpt(12345), 'an unknown option is rejected');
raises(ZMQPollException::class, fn() => (new ZMQPoll())->add('nope', ZMQ::POLL_IN), 'polling a string is rejected');
raises(ZMQPollException::class, function () {
    $readable = $writable = [];
    (new ZMQPoll())->poll($readable, $writable, 0);
}, 'polling an empty set is rejected');

$e = new ZMQSocketException('custom', 7);
raises(ZMQException::class, function () use ($e) {
    throw $e;
}, 'exceptions are constructible and extend ZMQException');
echo $e->getCode() === 7 ? "ok   exception keeps its code\n" : exit(1);

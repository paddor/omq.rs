<?php
// OMQ extension options that take strings or bytes reach omq-libzmq.

declare(strict_types=1);

const OMQ_WS_ALLOWED_ORIGINS = 1009;

$ctx = new ZMQContext();
$socket = $ctx->getSocket(ZMQ::SOCKET_PULL);
$origins = "http://localhost:5173\nhttp://127.0.0.1:5173";
$socket->setSockOpt(OMQ_WS_ALLOWED_ORIGINS, $origins);
if ($socket->getSockOpt(OMQ_WS_ALLOWED_ORIGINS) !== $origins) {
    fwrite(STDERR, "FAIL WS origins did not round-trip\n");
    exit(1);
}
echo "ok   OMQ_WS_ALLOWED_ORIGINS takes a string\n";

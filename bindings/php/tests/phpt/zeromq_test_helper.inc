<?php

define('ZEROMQ_TEST_DSN', 'inproc://php-test');
define('ZEROMQ_TEST_DSN2', 'inproc://php-test-second');

function create_server($persistent_id = null) 
{
    $context = new ZMQContext();
    $server  = $context->getSocket(ZMQ::SOCKET_REP, $persistent_id)
                       ->bind(ZEROMQ_TEST_DSN);
    return $server;
}

function create_client($persistent_id = null) 
{
    $context = new ZMQContext();
    $client  = $context->getSocket(ZMQ::SOCKET_REQ, $persistent_id)
                       ->connect(ZEROMQ_TEST_DSN);

    return $client;
}

function bind_callback(ZMQSocket $socket, $persistent_id = null)
{
	static $port = 5566;

	$socket->bind(ZEROMQ_TEST_DSN . "-{$port}");
	$port++;
}
/* 1580 bytes covering every byte value, in place of upstream's rose.jpg. */
function binary_fixture()
{
	$data = '';
	for ($i = 0; $i < 1580; $i++) {
		$data .= chr(($i * 7 + 13) & 0xFF);
	}
	return $data;
}

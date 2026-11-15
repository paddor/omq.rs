<?php
// PHP-FPM check for the native extension, run by scripts/test-php.sh:
//   php fpm_check.php <php-fpm binary> <extension path>
// Two static workers serve requests that send through a persistent PUSH
// socket. Each worker must create its socket once and reuse it, every
// message must arrive, and FPM must stop cleanly.

declare(strict_types=1);

const REQUESTS = 6;
const WORKERS = 2;

[, $fpmBinary, $extension] = $argv;

function fail(string $what): never
{
    fwrite(STDERR, "FAIL $what\n");
    exit(1);
}

function free_port(): int
{
    $server = stream_socket_server('tcp://127.0.0.1:0');
    $port = (int) substr(strrchr(stream_socket_get_name($server, false), ':'), 1);
    fclose($server);
    return $port;
}

/** One FastCGI GET of `$script`; returns the response body. */
function fastcgi_get(int $port, string $script): string
{
    $record = fn(int $type, string $body) => pack('CCnnCC', 1, $type, 1, strlen($body), 0, 0) . $body;
    $params = '';
    foreach (['SCRIPT_FILENAME' => $script, 'REQUEST_METHOD' => 'GET', 'GATEWAY_INTERFACE' => 'CGI/1.1'] as $key => $value) {
        $params .= pack('CC', strlen($key), strlen($value)) . $key . $value;
    }

    $socket = stream_socket_client("tcp://127.0.0.1:$port", $errno, $error, 5) ?: fail("connect to FPM: $error");
    stream_set_timeout($socket, 5);
    fwrite($socket, $record(1, pack('nC', 1, 0) . str_repeat("\0", 5)) . $record(4, $params) . $record(4, '') . $record(5, ''));
    $out = '';
    while (strlen($header = (string) fread($socket, 8)) === 8) {
        ['type' => $type, 'len' => $len, 'pad' => $pad] = unpack('Cver/Ctype/nid/nlen/Cpad/Cres', $header);
        $body = $len > 0 ? stream_get_contents($socket, $len) : '';
        if ($pad > 0) {
            stream_get_contents($socket, $pad);
        }
        if ($type === 6) {
            $out .= $body;
        }
        if ($type === 3) {
            break;
        }
    }
    fclose($socket);
    return explode("\r\n\r\n", $out, 2)[1] ?? $out;
}

$work = sys_get_temp_dir() . '/omq-php-fpm-' . getmypid();
@mkdir($work);
$pull = (new ZMQContext())->getSocket(ZMQ::SOCKET_PULL);
$pull->setSockOpt(ZMQ::SOCKOPT_RCVTIMEO, 5000);
$pull->bind('tcp://127.0.0.1:*');
$collector = $pull->getSockOpt(ZMQ::SOCKOPT_LAST_ENDPOINT);

file_put_contents("$work/app.php", <<<PHP
<?php
\$new = false;
\$push = (new ZMQContext())->getSocket(ZMQ::SOCKET_PUSH, 'fpm-push', function (ZMQSocket \$socket) use (&\$new) {
    \$new = true;
    \$socket->connect('$collector');
});
\$push->send(getmypid() . ' ' . (\$new ? 'new' : 'reused'));
echo 'ok';
PHP);

$port = free_port();
file_put_contents("$work/fpm.conf", "[global]\npid = $work/fpm.pid\nerror_log = $work/fpm.log\ndaemonize = no\n"
    . "[www]\nlisten = 127.0.0.1:$port\npm = static\npm.max_children = " . WORKERS . "\n");
$fpm = proc_open([$fpmBinary, '-n', '-d', "extension=$extension", '-y', "$work/fpm.conf", '-F'], [], $pipes);
$fpm ?: fail('start php-fpm');
for ($wait = 0; @stream_socket_client("tcp://127.0.0.1:$port") === false; $wait++) {
    $wait < 100 ?: fail('php-fpm did not listen');
    usleep(50_000);
}

for ($i = 0; $i < REQUESTS; $i++) {
    fastcgi_get($port, "$work/app.php") === 'ok' ?: fail("request $i");
}

$seen = [];
for ($i = 0; $i < REQUESTS; $i++) {
    $message = $pull->recv() ?: fail("message $i did not arrive");
    [$pid, $state] = explode(' ', $message);
    $seen[$pid][] = $state;
}
foreach ($seen as $pid => $states) {
    $states[0] === 'new' && !in_array('new', array_slice($states, 1), true)
        ?: fail("worker $pid created its persistent socket more than once: " . implode(',', $states));
}
echo 'ok   ' . count($seen) . " FPM workers each created their persistent socket once\n";

// SIGQUIT (3) stops FPM gracefully; proc_terminate needs no posix extension.
proc_terminate($fpm, 3);
for ($wait = 0; proc_get_status($fpm)['running']; $wait++) {
    if ($wait >= 100) {
        proc_terminate($fpm, 9);
        fail('php-fpm did not stop on SIGQUIT');
    }
    usleep(50_000);
}
echo "ok   FPM stopped cleanly\n";
array_map('unlink', glob("$work/*"));
rmdir($work);

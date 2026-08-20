import sys

import zmq


mode = sys.argv[1]
endpoint = sys.argv[2] if len(sys.argv) > 2 else None
context = zmq.Context()

if mode == "pull":
    socket = context.socket(zmq.PULL)
    port = socket.bind_to_random_port("tcp://127.0.0.1")
    print(f"tcp://127.0.0.1:{port}", flush=True)
    print(socket.recv_multipart()[0].decode(), flush=True)
    socket.close(linger=0)
elif mode == "push":
    socket = context.socket(zmq.PUSH)
    socket.connect(endpoint)
    socket.send_multipart([b"from", b"pyzmq"])
    socket.close(linger=1_000)
elif mode == "curve_push":
    server_public = sys.argv[3].encode()
    client_public, client_secret = zmq.curve_keypair()
    socket = context.socket(zmq.PUSH)
    socket.curve_serverkey = server_public
    socket.curve_publickey = client_public
    socket.curve_secretkey = client_secret
    socket.connect(endpoint)
    socket.send(b"from-pyzmq-curve")
    socket.close(linger=1_000)
elif mode == "curve_req":
    server_public = sys.argv[3].encode()
    client_public, client_secret = zmq.curve_keypair()
    socket = context.socket(zmq.REQ)
    socket.curve_serverkey = server_public
    socket.curve_publickey = client_public
    socket.curve_secretkey = client_secret
    socket.connect(endpoint)
    socket.send(b"ping")
    socket.rcvtimeo = 5_000
    print(socket.recv().decode(), flush=True)
    socket.close(linger=0)
elif mode == "curve_pull":
    from zmq.auth import CURVE_ALLOW_ANY
    from zmq.auth.thread import ThreadAuthenticator

    authenticator = ThreadAuthenticator(context)
    authenticator.start()
    authenticator.configure_curve(domain="*", location=CURVE_ALLOW_ANY)
    server_public, server_secret = zmq.curve_keypair()
    socket = context.socket(zmq.PULL)
    socket.curve_server = True
    socket.curve_publickey = server_public
    socket.curve_secretkey = server_secret
    port = socket.bind_to_random_port("tcp://127.0.0.1")
    print(f"tcp://127.0.0.1:{port}", flush=True)
    print(server_public.decode(), flush=True)
    print(socket.recv().decode(), flush=True)
    socket.close(linger=0)
    authenticator.stop()
elif mode == "curve_rep":
    from zmq.auth import CURVE_ALLOW_ANY
    from zmq.auth.thread import ThreadAuthenticator

    authenticator = ThreadAuthenticator(context)
    authenticator.start()
    authenticator.configure_curve(domain="*", location=CURVE_ALLOW_ANY)
    server_public, server_secret = zmq.curve_keypair()
    socket = context.socket(zmq.REP)
    socket.curve_server = True
    socket.curve_publickey = server_public
    socket.curve_secretkey = server_secret
    port = socket.bind_to_random_port("tcp://127.0.0.1")
    print(f"tcp://127.0.0.1:{port}", flush=True)
    print(server_public.decode(), flush=True)
    socket.rcvtimeo = 5_000
    print(socket.recv().decode(), flush=True)
    socket.send(b"pong")
    socket.close(linger=0)
    authenticator.stop()
else:
    raise SystemExit(f"unknown mode: {mode}")

context.term()

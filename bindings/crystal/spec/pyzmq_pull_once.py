#!/usr/bin/env python3
import pathlib
import sys
import zmq


endpoint_file = pathlib.Path(sys.argv[1])
payload_file = pathlib.Path(sys.argv[2])

ctx = zmq.Context()
pull = ctx.socket(zmq.PULL)
pull.linger = 0
pull.bind("tcp://127.0.0.1:*")
endpoint = pull.getsockopt_string(zmq.LAST_ENDPOINT)
endpoint_file.write_text(endpoint)
payload = pull.recv()
payload_file.write_bytes(payload)
pull.close()
ctx.term()

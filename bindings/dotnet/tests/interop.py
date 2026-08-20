"""External pyzmq <-> OMQ.Net interop tests.

Run after building the peer:
  LD_LIBRARY_PATH=target/release python3 bindings/dotnet/tests/interop.py
"""
import os, socket, subprocess
import zmq
import zmq.auth.thread

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.."))
PROJECT = os.path.join(ROOT, "bindings/dotnet/tests/Omq.Net.Interop.csproj")
ENV = {**os.environ, "LD_LIBRARY_PATH": f"{ROOT}/target/release:{os.environ.get('LD_LIBRARY_PATH','')}"}

def endpoint():
    s=socket.socket(); s.bind(("127.0.0.1",0)); p=s.getsockname()[1]; s.close(); return f"tcp://127.0.0.1:{p}"

def py_server(ep, security):
    ctx=zmq.Context(); s=ctx.socket(zmq.REP); s.linger=0; s.rcvtimeo=10000; s.sndtimeo=10000; auth=None
    if security == "curve":
        pub, sec = zmq.curve_keypair(); s.curve_server=True; s.curve_secretkey=sec; s.curve_publickey=pub
    if security == "plain":
        auth=zmq.auth.thread.ThreadAuthenticator(ctx); auth.start(); auth.allow("127.0.0.1")
        auth.configure_plain("global", {"interop": "secret"}); s.zap_domain=b"global"; s.plain_server=True
    s.bind(ep); return ctx,s,(pub if security == "curve" else None),(sec if security == "curve" else None),auth

def run(security="none"):
    ep=endpoint(); ctx,s,pub,sec,auth=py_server(ep,security)
    if security == "curve":
        client_pub, client_sec=zmq.curve_keypair()
        args=["req",ep,security,pub.decode(),client_pub.decode(),client_sec.decode()]
    else: args=["req",ep,security,"-"]
    p=subprocess.Popen(["dotnet","run","--no-build","--project",PROJECT,"--",*args],cwd=ROOT,env=ENV,text=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
    try:
        try:
            req=s.recv_multipart()
        except zmq.Again:
            out,err=p.communicate(timeout=2)
            raise AssertionError(f"{security} peer produced no request: rc={p.returncode} stdout={out!r} stderr={err!r}")
        assert req==[b"interop",b"hello"],req
        s.send_multipart([b"interop",b"world"])
        out,err=p.communicate(timeout=10)
        assert p.returncode==0,(out,err)
    finally:
        if p.poll() is None: p.kill()
        s.close()
    if auth is not None: auth.stop()
    ctx.term()

def run_reverse(security="none"):
    ep=endpoint(); ctx=zmq.Context(); s=ctx.socket(zmq.REQ); s.linger=0; s.rcvtimeo=10000; s.sndtimeo=10000
    if security == "curve":
        server_pub,server_sec=zmq.curve_keypair(); client_pub,client_sec=zmq.curve_keypair()
        args=["rep",ep,security,server_pub.decode(),server_sec.decode()]
        s.curve_publickey=client_pub; s.curve_secretkey=client_sec; s.curve_serverkey=server_pub
    elif security == "plain":
        args=["rep",ep,security,"-"]; s.plain_username=b"interop"; s.plain_password=b"secret"
    else: args=["rep",ep,security,"-"]
    p=subprocess.Popen(["dotnet","run","--no-build","--project",PROJECT,"--",*args],cwd=ROOT,env=ENV,text=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
    try:
        s.connect(ep); s.send_multipart([b"interop",b"hello"]); reply=s.recv_multipart()
        assert reply==[b"interop",b"world"],reply
        out,err=p.communicate(timeout=10); assert p.returncode==0,(out,err)
    finally:
        if p.poll() is None: p.kill()
        s.close(); ctx.term()

for mode in ("none","curve","plain"):
    run(mode); print(f"pyzmq {mode} interop PASS")
    run_reverse(mode); print(f"OMQ.Net {mode} interop PASS")

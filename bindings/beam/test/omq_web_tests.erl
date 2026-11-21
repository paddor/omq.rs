-module(omq_web_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("public_key/include/public_key.hrl").

-define(ORIGIN, <<"http://localhost:5173">>).

web_features_test() ->
    ?assertEqual(true, omq:has(ws)),
    ?assertEqual(true, omq:has(wss)),
    ?assertEqual(true, omq:has(quic)),
    ?assertEqual(true, omq:has(webtransport)).

ws_round_trip_test() ->
    with_socket_pair(pull, push, fun(Pull, Push) ->
        {ok, Endpoint} = omq:bind(Pull, <<"ws://127.0.0.1:0/omq">>),
        ?assertMatch(<<"ws://127.0.0.1:", _/binary>>, Endpoint),
        ok = omq:connect(Push, Endpoint),
        ok = omq:send(Push, <<"over ws">>),
        ?assertEqual({ok, <<"over ws">>}, omq:recv(Pull, 5000))
    end).

ws_allowed_origins_admit_only_listed_browsers_test() ->
    with_socket(pub, fun(Pub) ->
        ok = omq:setsockopt(Pub, omq_ws_allowed_origins, ?ORIGIN),
        {ok, Endpoint} = omq:bind(Pub, <<"ws://127.0.0.1:0/omq">>),
        Port = endpoint_port(Endpoint),
        ?assertMatch(<<"HTTP/1.1 101", _/binary>>, upgrade(Port, ?ORIGIN)),
        ?assertNotMatch(<<"HTTP/1.1 101", _/binary>>, upgrade(Port, <<"http://evil.example">>))
    end).

ws_option_round_trip_test() ->
    with_socket(pub, fun(Sock) ->
        ?assertEqual({ok, <<>>}, omq:getsockopt(Sock, omq_ws_allowed_origins)),
        Origins = <<"https://app.example.com\nhttp://localhost:5173">>,
        ok = omq:setsockopt(Sock, omq_ws_allowed_origins, Origins),
        ?assertEqual({ok, Origins}, omq:getsockopt(Sock, omq:omq_ws_allowed_origins())),
        ?assertMatch(
            {error, badarg, _},
            omq:setsockopt(Sock, omq_ws_allowed_origins, <<"https://*.example.com">>)
        ),
        ?assertEqual({ok, Origins}, omq:getsockopt(Sock, omq_ws_allowed_origins)),
        ok = omq:setsockopt(Sock, omq_ws_allowed_origins, <<>>),
        ?assertEqual({ok, <<>>}, omq:getsockopt(Sock, omq_ws_allowed_origins)),
        ok = omq:setsockopt(Sock, wss_cert_pem, <<"cert">>),
        ok = omq:setsockopt(Sock, wss_key_pem, <<"key">>),
        ok = omq:setsockopt(Sock, wss_trust_pem, <<"trust">>),
        ok = omq:setsockopt(Sock, wss_hostname, <<"broker.example.com">>),
        ?assertEqual({ok, 1}, omq:getsockopt(Sock, wss_trust_system)),
        ok = omq:setsockopt(Sock, wss_trust_system, false),
        ?assertEqual({ok, <<"cert">>}, omq:getsockopt(Sock, wss_cert_pem)),
        ?assertEqual({ok, <<"key">>}, omq:getsockopt(Sock, wss_key_pem)),
        ?assertEqual({ok, <<"trust">>}, omq:getsockopt(Sock, wss_trust_pem)),
        ?assertEqual({ok, <<"broker.example.com">>}, omq:getsockopt(Sock, wss_hostname)),
        ?assertEqual({ok, 0}, omq:getsockopt(Sock, wss_trust_system))
    end).

quic_option_round_trip_test() ->
    with_socket(pub, fun(Sock) ->
        ok = omq:setsockopt(Sock, omq_quic_cert_pem, <<"cert">>),
        ok = omq:setsockopt(Sock, omq_quic_key_pem, <<"key">>),
        ok = omq:setsockopt(Sock, omq_quic_trust_pem, <<"trust">>),
        ok = omq:setsockopt(Sock, omq_quic_server_name, <<"broker.example.com">>),
        ?assertEqual({ok, 1}, omq:getsockopt(Sock, omq_quic_trust_system)),
        ok = omq:setsockopt(Sock, omq_quic_trust_system, 0),
        ?assertEqual({ok, <<"cert">>}, omq:getsockopt(Sock, omq_quic_cert_pem)),
        ?assertEqual({ok, <<"key">>}, omq:getsockopt(Sock, omq_quic_key_pem)),
        ?assertEqual({ok, <<"trust">>}, omq:getsockopt(Sock, omq_quic_trust_pem)),
        ?assertEqual({ok, <<"broker.example.com">>}, omq:getsockopt(Sock, omq_quic_server_name)),
        ?assertEqual({ok, 0}, omq:getsockopt(Sock, omq_quic_trust_system)),
        ok = omq:setsockopt(Sock, omq_quic_server_name, <<>>),
        ?assertEqual({ok, <<>>}, omq:getsockopt(Sock, omq_quic_server_name)),
        ok = omq:setsockopt(Sock, omq_quic_allowed_origins, ?ORIGIN),
        ?assertEqual({ok, ?ORIGIN}, omq:getsockopt(Sock, omq_quic_allowed_origins)),
        ?assertMatch(
            {error, badarg, _},
            omq:setsockopt(Sock, omq_quic_allowed_origins, <<"null">>)
        ),
        ?assertEqual({ok, <<"/omq">>}, omq:getsockopt(Sock, omq_quic_webtransport_path)),
        ok = omq:setsockopt(Sock, omq_quic_webtransport_path, <<"/feed">>),
        ?assertEqual({ok, <<"/feed">>}, omq:getsockopt(Sock, omq_quic_webtransport_path)),
        ?assertMatch(
            {error, badarg, _},
            omq:setsockopt(Sock, omq_quic_webtransport_path, <<"feed">>)
        )
    end).

web_options_are_fixed_after_bind_test() ->
    with_socket(pub, fun(Sock) ->
        {ok, _} = omq:bind(Sock, <<"ws://127.0.0.1:0/omq">>),
        ?assertMatch({error, badarg, _}, omq:setsockopt(Sock, omq_ws_allowed_origins, ?ORIGIN)),
        ?assertMatch({error, badarg, _}, omq:setsockopt(Sock, omq_quic_cert_pem, <<"cert">>))
    end).

quic_round_trip_test() ->
    #{cert := Cert, key := Key, trust := Trust} = test_certificate(),
    with_socket_pair(pull, push, fun(Pull, Push) ->
        ok = omq:setsockopt(Pull, omq_quic_cert_pem, Cert),
        ok = omq:setsockopt(Pull, omq_quic_key_pem, Key),
        ok = omq:setsockopt(Push, omq_quic_trust_pem, Trust),
        ok = omq:setsockopt(Push, omq_quic_trust_system, false),
        ok = omq:setsockopt(Push, omq_quic_server_name, <<"localhost">>),
        {ok, Endpoint} = omq:bind(Pull, <<"quic://127.0.0.1:0">>),
        ok = omq:connect(Push, Endpoint),
        ok = omq:send(Push, <<"over quic">>),
        ?assertEqual({ok, <<"over quic">>}, omq:recv(Pull, 5000))
    end).

with_socket(Type, Fun) ->
    {ok, Ctx} = omq:context(),
    {ok, Sock} = omq:socket(Ctx, Type),
    try
        Fun(Sock)
    after
        ok = omq:close(Sock),
        ok = omq:term(Ctx)
    end.

with_socket_pair(TypeA, TypeB, Fun) ->
    {ok, Ctx} = omq:context(),
    {ok, A} = omq:socket(Ctx, TypeA),
    {ok, B} = omq:socket(Ctx, TypeB),
    try
        Fun(A, B)
    after
        ok = omq:close(B),
        ok = omq:close(A),
        ok = omq:term(Ctx)
    end.

endpoint_port(Endpoint) ->
    {match, [Port]} = re:run(Endpoint, <<":([0-9]+)/">>, [{capture, all_but_first, binary}]),
    binary_to_integer(Port).

%% Sends a browser-style WebSocket upgrade and returns the response head, or
%% <<>> when the listener closes without one.
upgrade(Port, Origin) ->
    {ok, Socket} = gen_tcp:connect({127, 0, 0, 1}, Port, [binary, {active, false}]),
    Request = [
        <<"GET /omq HTTP/1.1\r\n">>,
        <<"Host: 127.0.0.1:">>, integer_to_binary(Port), <<"\r\n">>,
        <<"Upgrade: websocket\r\n">>,
        <<"Connection: Upgrade\r\n">>,
        <<"Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n">>,
        <<"Sec-WebSocket-Version: 13\r\n">>,
        <<"Sec-WebSocket-Protocol: ZWS2.0\r\n">>,
        <<"Origin: ">>, Origin, <<"\r\n\r\n">>
    ],
    ok = gen_tcp:send(Socket, Request),
    Response =
        case gen_tcp:recv(Socket, 0, 5000) of
            {ok, Data} -> Data;
            {error, closed} -> <<>>
        end,
    ok = gen_tcp:close(Socket),
    Response.

%% A server certificate for localhost and its CA, all PEM.
test_certificate() ->
    San = #'Extension'{
        extnID = ?'id-ce-subjectAltName',
        extnValue = [{dNSName, "localhost"}],
        critical = false
    },
    Ec = [{key, {namedCurve, ed25519}}],
    #{server_config := Server, client_config := Client} = public_key:pkix_test_data(#{
        server_chain => #{root => Ec, intermediates => [], peer => [{extensions, [San]} | Ec]},
        client_chain => #{root => Ec, intermediates => [], peer => Ec}
    }),
    {cert, Cert} = lists:keyfind(cert, 1, Server),
    {key, {KeyType, Key}} = lists:keyfind(key, 1, Server),
    {cacerts, CaCerts} = lists:keyfind(cacerts, 1, Client),
    #{
        cert => public_key:pem_encode([{'Certificate', Cert, not_encrypted}]),
        key => public_key:pem_encode([
            public_key:pem_entry_encode('PrivateKeyInfo', public_key:der_decode(KeyType, Key))
        ]),
        trust => public_key:pem_encode([{'Certificate', Ca, not_encrypted} || Ca <- CaCerts])
    }.

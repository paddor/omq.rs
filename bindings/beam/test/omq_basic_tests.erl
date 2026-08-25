-module(omq_basic_tests).

-include_lib("eunit/include/eunit.hrl").

endpoint(Name) ->
    Id = integer_to_binary(erlang:unique_integer([positive, monotonic])),
    <<"inproc://", Name/binary, "-", Id/binary>>.

context_socket_type_test() ->
    {ok, Ctx} = omq:context(),
    {ok, Sock} = omq:socket(Ctx, req),
    ?assertEqual({ok, req}, omq:socket_type(Sock)),
    ?assertEqual(ok, omq:close(Sock)),
    ?assertEqual(ok, omq:term(Ctx)).

metadata_and_destroy_alias_test() ->
    {ok, <<"tokio">>} = omq:backend_name(),
    {ok, Version} = omq:version(),
    ?assert(is_binary(Version)),
    ?assert(byte_size(Version) > 0),
    ?assertEqual(32, omq:pollpri()),
    ?assertEqual(2, omq:forwarder()),
    ?assertEqual(3, omq:queue()),
    ?assertEqual(1, omq:streamer()),
    ?assertEqual(0, omq:null()),
    ?assertEqual(1, omq:plain()),
    ?assertEqual(2, omq:curve()),
    {ok, Ctx} = omq:context(),
    ?assertEqual(ok, omq:destroy(Ctx)),
    ?assertEqual(true, omq:context_closed(Ctx)).

socket_id_and_closed_test() ->
    {ok, Ctx} = omq:context(),
    {ok, A} = omq:socket(Ctx, pair),
    {ok, B} = omq:socket(Ctx, pair),
    ?assertEqual(false, omq:closed(A)),
    {ok, AId} = omq:socket_id(A),
    {ok, BId} = omq:socket_id(B),
    ?assert(is_integer(AId)),
    ?assert(is_integer(BId)),
    ?assertNotEqual(AId, BId),
    ok = omq:close(A),
    ?assertEqual(true, omq:closed(A)),
    ?assertEqual(ok, omq:close(A)),
    ?assertEqual(false, omq:closed(B)),
    ok = omq:close(B),
    ok = omq:term(Ctx).

context_share_key_roundtrip_test() ->
    {ok, Ctx} = omq:context(),
    ?assertEqual(false, omq:context_closed(Ctx)),
    {ok, ShareKey} = omq:context_share_key(Ctx),
    ?assertEqual({ok, ShareKey}, omq:share_key(Ctx)),
    ?assert(is_integer(ShareKey)),
    {ok, Shared} = omq:context_from_share_key(ShareKey),
    {ok, SharedAlias} = omq:from_share_key(ShareKey),
    ok = omq:term(SharedAlias),
    Endpoint = endpoint(<<"beam-share-key">>),
    {ok, Pull} = omq:socket(Shared, pull),
    {ok, Push} = omq:socket(Ctx, push),
    {ok, Endpoint} = omq:bind(Pull, Endpoint),
    ok = omq:connect(Push, Endpoint),
    ok = omq:send(Push, <<"shared">>),
    ?assertEqual({ok, <<"shared">>}, omq:recv(Pull, 1000)),
    ok = omq:term(Shared),
    ?assertEqual(true, omq:context_closed(Shared)),
    ?assertEqual(false, omq:context_closed(Ctx)),
    ok = omq:close(Push),
    ok = omq:close(Pull),
    ok = omq:term(Ctx),
    ?assertEqual(true, omq:context_closed(Ctx)).

context_from_share_key_observes_owner_term_test() ->
    {ok, Ctx} = omq:context(),
    {ok, ShareKey} = omq:context_share_key(Ctx),
    {ok, Shared} = omq:context_from_share_key(ShareKey),
    ok = omq:term(Ctx),
    ?assertEqual(true, omq:context_closed(Ctx)),
    ?assertEqual(true, omq:context_closed(Shared)),
    ?assertMatch({error, closed, _}, omq:context_from_share_key(ShareKey)).

last_endpoint_and_random_port_test() ->
    {ok, Ctx} = omq:context(),
    {ok, Pull} = omq:socket(Ctx, pull),
    ?assertEqual({ok, <<>>}, omq:getsockopt(Pull, last_endpoint)),
    {ok, Bound} = omq:bind(Pull, <<"tcp://127.0.0.1:0">>),
    ?assertEqual({ok, Bound}, omq:getsockopt(Pull, last_endpoint)),
    ok = omq:close(Pull),
    {ok, Other} = omq:socket(Ctx, pull),
    {ok, Port} = omq:bind_to_random_port(Other, <<"tcp://127.0.0.1">>, 49152, 65535),
    ?assert(is_integer(Port)),
    ?assert(Port >= 49152),
    ?assert(Port =< 65535),
    {ok, RandomEndpoint} = omq:getsockopt(Other, last_endpoint),
    {_, RandomPort} = tcp_host_port(RandomEndpoint),
    ?assertEqual(Port, RandomPort),
    ok = omq:close(Other),
    ?assertEqual(ok, omq:term(Ctx)).

all_socket_types_create_test() ->
    Types = [
        pair, pub, sub, req, rep, dealer, router, pull, push, xpub, xsub,
        stream, server, client, radio, dish, gather, scatter, peer, channel
    ],
    {ok, Ctx} = omq:context(),
    lists:foreach(fun(Type) ->
        {ok, Sock} = omq:socket(Ctx, Type),
        ?assertEqual({ok, Type}, omq:socket_type(Sock)),
        ?assertEqual(ok, omq:close(Sock))
    end, Types),
    ?assertEqual(ok, omq:term(Ctx)).

pair_inproc_roundtrip_test() ->
    {ok, Ctx} = omq:context(),
    Endpoint = endpoint(<<"beam-basic-pair">>),
    {ok, A} = omq:socket(Ctx, pair),
    {ok, B} = omq:socket(Ctx, pair),
    {ok, Endpoint} = omq:bind(A, Endpoint),
    ok = omq:connect(B, Endpoint),
    ok = omq:send(B, <<"ping">>),
    ?assertEqual({ok, <<"ping">>}, omq:recv(A, 1000)),
    ok = omq:close(A),
    ok = omq:close(B),
    ok = omq:term(Ctx).

push_pull_multipart_test() ->
    {ok, Ctx} = omq:context(),
    Endpoint = endpoint(<<"beam-basic-push-pull">>),
    {ok, Pull} = omq:socket(Ctx, pull),
    {ok, Push} = omq:socket(Ctx, push),
    {ok, Endpoint} = omq:bind(Pull, Endpoint),
    ok = omq:connect(Push, Endpoint),
    ok = omq:send_multipart(Push, [<<"a">>, <<"b">>]),
    ?assertEqual({ok, [<<"a">>, <<"b">>]}, omq:recv_multipart(Pull, 1000)),
    ok = omq:close(Pull),
    ok = omq:close(Push),
    ok = omq:term(Ctx).

sndmore_flag_aggregates_test() ->
    {ok, Ctx} = omq:context(),
    Endpoint = endpoint(<<"beam-sndmore">>),
    {ok, Pull} = omq:socket(Ctx, pull),
    {ok, Push} = omq:socket(Ctx, push),
    {ok, Endpoint} = omq:bind(Pull, Endpoint),
    ok = omq:connect(Push, Endpoint),
    ok = omq:send(Push, <<"a">>, omq:sndmore()),
    ok = omq:send(Push, <<"b">>, [sndmore]),
    ok = omq:send(Push, <<"c">>, [dontwait]),
    ?assertEqual({ok, [<<"a">>, <<"b">>, <<"c">>]}, omq:recv_multipart(Pull, 1000)),
    ok = omq:close(Push),
    ok = omq:close(Pull),
    ok = omq:term(Ctx).

send_recv_string_test() ->
    {ok, Ctx} = omq:context(),
    Endpoint = endpoint(<<"beam-string">>),
    {ok, Pull} = omq:socket(Ctx, pull),
    {ok, Push} = omq:socket(Ctx, push),
    {ok, Endpoint} = omq:bind(Pull, Endpoint),
    ok = omq:connect(Push, Endpoint),
    ok = omq:send_string(Push, <<"hello">>),
    ?assertEqual({ok, <<"hello">>}, omq:recv_string(Pull, 1000)),
    ok = omq:send_string(Push, <<"hello">>),
    eventually(fun() -> omq:try_recv_string(Pull) end, {ok, <<"hello">>}, 100),
    ok = omq:close(Push),
    ok = omq:close(Pull),
    ok = omq:term(Ctx).

send_recv_string_encoding_test() ->
    {ok, Ctx} = omq:context(),
    Endpoint = endpoint(<<"beam-string-encoding">>),
    {ok, Pull} = omq:socket(Ctx, pull),
    {ok, Push} = omq:socket(Ctx, push),
    {ok, Endpoint} = omq:bind(Pull, Endpoint),
    ok = omq:connect(Push, Endpoint),
    ok = omq:send_string(Push, <<"hello">>, utf16),
    ?assertEqual({ok, <<"hello">>}, omq:recv_string(Pull, 1000, utf16)),
    ok = omq:close(Push),
    ok = omq:close(Pull),
    ok = omq:term(Ctx).

recv_frame_rcvmore_test() ->
    {ok, Ctx} = omq:context(),
    Endpoint = endpoint(<<"beam-rcvmore">>),
    {ok, Pull} = omq:socket(Ctx, pull),
    {ok, Push} = omq:socket(Ctx, push),
    {ok, Endpoint} = omq:bind(Pull, Endpoint),
    ok = omq:connect(Push, Endpoint),
    ok = omq:send_multipart(Push, [<<"x">>, <<"y">>, <<"z">>]),
    ?assertEqual({ok, <<"x">>}, omq:recv_frame(Pull, 1000)),
    ?assertEqual({ok, 1}, omq:getsockopt(Pull, rcvmore)),
    ?assertEqual({ok, <<"y">>}, omq:recv_frame(Pull, 1000)),
    ?assertEqual({ok, 1}, omq:getsockopt(Pull, omq:rcvmore())),
    ?assertEqual({ok, <<"z">>}, omq:recv_frame(Pull, 1000)),
    ?assertEqual({ok, 0}, omq:getsockopt(Pull, rcvmore)),
    ok = omq:close(Push),
    ok = omq:close(Pull),
    ok = omq:term(Ctx).

req_rep_roundtrip_test() ->
    {ok, Ctx} = omq:context(),
    Endpoint = endpoint(<<"beam-basic-req-rep">>),
    {ok, Rep} = omq:socket(Ctx, rep),
    {ok, Req} = omq:socket(Ctx, req),
    {ok, Endpoint} = omq:bind(Rep, Endpoint),
    ok = omq:connect(Req, Endpoint),
    ok = omq:send(Req, <<"request">>),
    ?assertEqual({ok, <<"request">>}, omq:recv(Rep, 1000)),
    ok = omq:send(Rep, <<"reply">>),
    ?assertEqual({ok, <<"reply">>}, omq:recv(Req, 1000)),
    ok = omq:close(Req),
    ok = omq:close(Rep),
    ok = omq:term(Ctx).

pub_sub_prefix_test() ->
    {ok, Ctx} = omq:context(),
    Endpoint = endpoint(<<"beam-basic-pub-sub">>),
    {ok, Pub} = omq:socket(Ctx, pub),
    {ok, Sub} = omq:socket(Ctx, sub),
    ok = omq:subscribe(Sub, <<"topic">>),
    {ok, Endpoint} = omq:bind(Pub, Endpoint),
    ok = omq:connect(Sub, Endpoint),
    {ok, _} = omq:wait_connected(Pub, 1, 1000),
    {ok, _} = omq:wait_subscribed(Pub, 1, 1000),
    ok = omq:send(Pub, <<"other drop">>),
    ok = omq:send(Pub, <<"topic keep">>),
    ?assertEqual({ok, <<"topic keep">>}, omq:recv(Sub, 1000)),
    ok = omq:close(Sub),
    ok = omq:close(Pub),
    ok = omq:term(Ctx).

xpub_xsub_raw_subscription_test() ->
    {ok, Ctx} = omq:context(),
    Endpoint = endpoint(<<"beam-basic-xpub-xsub">>),
    {ok, XPub} = omq:socket(Ctx, xpub),
    {ok, XSub} = omq:socket(Ctx, xsub),
    {ok, Endpoint} = omq:bind(XPub, Endpoint),
    ok = omq:connect(XSub, Endpoint),
    ok = omq:send(XSub, <<1, "topic">>),
    ?assertEqual({ok, <<1, "topic">>}, omq:recv(XPub, 1000)),
    ok = omq:send(XPub, <<"topic raw">>),
    ?assertEqual({ok, <<"topic raw">>}, omq:recv(XSub, 1000)),
    ok = omq:close(XSub),
    ok = omq:close(XPub),
    ok = omq:term(Ctx).

radio_dish_group_test() ->
    {ok, Ctx} = omq:context(),
    Endpoint = endpoint(<<"beam-basic-radio-dish">>),
    {ok, Radio} = omq:socket(Ctx, radio),
    {ok, Dish} = omq:socket(Ctx, dish),
    ok = omq:join(Dish, <<"g">>),
    {ok, Endpoint} = omq:bind(Radio, Endpoint),
    ok = omq:connect(Dish, Endpoint),
    {ok, _} = omq:wait_connected(Radio, 1, 1000),
    ok = omq:send_group(Radio, <<"x">>, <<"drop">>),
    ok = omq:send_group(Radio, <<"g">>, <<"keep">>),
    ?assertEqual({ok, #{parts => [<<"g">>, <<"keep">>], routing_id => undefined}}, omq:recv(Dish, 1000)),
    ok = omq:close(Dish),
    ok = omq:close(Radio),
    ok = omq:term(Ctx).

radio_dish_udp_group_test() ->
    {ok, Ctx} = omq:context(),
    Endpoint = udp_endpoint(),
    {ok, Radio} = omq:socket(Ctx, radio),
    {ok, Dish} = omq:socket(Ctx, dish),
    {ok, Endpoint} = omq:bind(Dish, Endpoint),
    ok = omq:join(Dish, <<"weather">>),
    ok = omq:connect(Radio, Endpoint),
    timer:sleep(50),
    ok = omq:send_group(Radio, <<"news">>, <<"ignored">>),
    ok = omq:send_group(Radio, <<"weather">>, <<"sunny">>),
    ?assertEqual(
        {ok, #{parts => [<<"weather">>, <<"sunny">>], routing_id => undefined}},
        omq:recv(Dish, 1000)
    ),
    ok = omq:leave(Dish, <<"weather">>),
    ok = omq:send_group(Radio, <<"weather">>, <<"ignored">>),
    ?assertMatch({error, timeout, _}, omq:recv(Dish, 100)),
    ok = omq:close(Dish),
    ok = omq:close(Radio),
    ok = omq:term(Ctx).

client_server_routing_id_test() ->
    {ok, Ctx} = omq:context(),
    Endpoint = endpoint(<<"beam-basic-client-server">>),
    {ok, Server} = omq:socket(Ctx, server),
    {ok, Client} = omq:socket(Ctx, client),
    {ok, Endpoint} = omq:bind(Server, Endpoint),
    ok = omq:connect(Client, Endpoint),
    ok = omq:send(Client, <<"hello">>),
    {ok, #{data := <<"hello">>, routing_id := RoutingId}} = omq:recv(Server, 1000),
    ok = omq:send(Server, <<"world">>, [{routing_id, RoutingId}]),
    ?assertEqual({ok, <<"world">>}, omq:recv(Client, 1000)),
    ok = omq:close(Client),
    ok = omq:close(Server),
    ok = omq:term(Ctx).

peer_routing_id_test() ->
    {ok, Ctx} = omq:context(),
    Endpoint = endpoint(<<"beam-basic-peer">>),
    {ok, A} = omq:socket(Ctx, peer),
    {ok, B} = omq:socket(Ctx, peer),
    ok = omq:setsockopt(A, identity, <<"peer-a">>),
    ok = omq:setsockopt(B, identity, <<"peer-b">>),
    {ok, Endpoint} = omq:bind(A, Endpoint),
    ok = omq:connect(B, Endpoint),
    ok = omq:send_multipart(B, [<<"peer-a">>, <<"ping">>]),
    ?assertEqual({ok, [<<"peer-b">>, <<"ping">>]}, omq:recv_multipart(A, 1000)),
    ok = omq:send_multipart(A, [<<"peer-b">>, <<"pong">>]),
    ?assertEqual({ok, [<<"peer-a">>, <<"pong">>]}, omq:recv_multipart(B, 1000)),
    ok = omq:close(B),
    ok = omq:close(A),
    ok = omq:term(Ctx).

dealer_router_identity_test() ->
    {ok, Ctx} = omq:context(),
    Endpoint = endpoint(<<"beam-basic-dealer-router">>),
    {ok, Router} = omq:socket(Ctx, router),
    {ok, Dealer} = omq:socket(Ctx, dealer),
    ok = omq:setsockopt(Dealer, identity, <<"dealer-a">>),
    {ok, Endpoint} = omq:bind(Router, Endpoint),
    ok = omq:connect(Dealer, Endpoint),
    ok = omq:send(Dealer, <<"hello">>),
    ?assertEqual({ok, [<<"dealer-a">>, <<"hello">>]}, omq:recv_multipart(Router, 1000)),
    ok = omq:send_multipart(Router, [<<"dealer-a">>, <<"world">>]),
    ?assertEqual({ok, <<"world">>}, omq:recv(Dealer, 1000)),
    ok = omq:close(Dealer),
    ok = omq:close(Router),
    ok = omq:term(Ctx).

scatter_gather_roundtrip_test() ->
    {ok, Ctx} = omq:context(),
    Endpoint = endpoint(<<"beam-basic-scatter-gather">>),
    {ok, Gather} = omq:socket(Ctx, gather),
    {ok, Scatter} = omq:socket(Ctx, scatter),
    {ok, Endpoint} = omq:bind(Gather, Endpoint),
    ok = omq:connect(Scatter, Endpoint),
    ok = omq:send(Scatter, <<"work">>),
    ?assertEqual({ok, <<"work">>}, omq:recv(Gather, 1000)),
    ?assertMatch({error, protocol, _}, omq:send_multipart(Scatter, [<<"a">>, <<"b">>])),
    ok = omq:close(Scatter),
    ok = omq:close(Gather),
    ok = omq:term(Ctx).

channel_roundtrip_test() ->
    {ok, Ctx} = omq:context(),
    Endpoint = endpoint(<<"beam-basic-channel">>),
    {ok, A} = omq:socket(Ctx, channel),
    {ok, B} = omq:socket(Ctx, channel),
    {ok, Endpoint} = omq:bind(A, Endpoint),
    ok = omq:connect(B, Endpoint),
    ok = omq:send(B, <<"one">>),
    ?assertEqual({ok, <<"one">>}, omq:recv(A, 1000)),
    ?assertMatch({error, protocol, _}, omq:send_multipart(B, [<<"a">>, <<"b">>])),
    ok = omq:close(A),
    ok = omq:close(B),
    ok = omq:term(Ctx).

stream_raw_tcp_test() ->
    {ok, Ctx} = omq:context(),
    {ok, Stream} = omq:socket(Ctx, stream),
    {ok, Bound} = omq:bind(Stream, <<"tcp://127.0.0.1:0">>),
    {Host, Port} = tcp_host_port(Bound),
    {ok, Tcp} = gen_tcp:connect(Host, Port, [binary, {active, false}, {packet, raw}]),
    {ok, [Identity, <<>>]} = omq:recv_multipart(Stream, 1000),
    ok = gen_tcp:send(Tcp, <<"raw">>),
    {ok, [Identity, <<"raw">>]} = omq:recv_multipart(Stream, 1000),
    ok = omq:send_multipart(Stream, [Identity, <<"echo">>]),
    {ok, <<"echo">>} = gen_tcp:recv(Tcp, 4, 1000),
    ok = gen_tcp:close(Tcp),
    ok = omq:close(Stream),
    ok = omq:term(Ctx).

recv_timeout_option_test() ->
    {ok, Ctx} = omq:context(),
    {ok, Pull} = omq:socket(Ctx, pull),
    ok = omq:setsockopt(Pull, rcvtimeo, 5),
    ?assertMatch({error, timeout, _}, omq:recv(Pull)),
    ?assertEqual({ok, 5}, omq:getsockopt(Pull, rcvtimeo)),
    ok = omq:close(Pull),
    ok = omq:term(Ctx).

try_recv_nonblocking_test() ->
    {ok, Ctx} = omq:context(),
    Endpoint = endpoint(<<"beam-try-recv">>),
    {ok, Pull} = omq:socket(Ctx, pull),
    {ok, Push} = omq:socket(Ctx, push),
    {ok, Endpoint} = omq:bind(Pull, Endpoint),
    ok = omq:connect(Push, Endpoint),
    ?assertMatch({error, would_block, _}, omq:try_recv(Pull)),
    ok = omq:send(Push, <<"ready">>),
    eventually(fun() -> omq:try_recv(Pull) end, {ok, <<"ready">>}, 100),
    ok = omq:send_multipart(Push, [<<"a">>, <<"b">>]),
    eventually(fun() -> omq:try_recv_multipart(Pull) end, {ok, [<<"a">>, <<"b">>]}, 100),
    ok = omq:close(Push),
    ok = omq:close(Pull),
    ok = omq:term(Ctx).

sndtimeo_full_queue_test() ->
    {ok, Ctx} = omq:context(),
    {ok, Push} = omq:socket(Ctx, push),
    ok = omq:setsockopt(Push, sndhwm, 1),
    ok = omq:setsockopt(Push, sndtimeo, 50),
    {ok, _Endpoint} = omq:bind(Push, <<"tcp://127.0.0.1:0">>),
    Result = fill_until_timeout(Push, 1000),
    ?assertMatch({error, timeout, _}, Result),
    ok = omq:close(Push),
    ok = omq:term(Ctx).

huge_rcvtimeo_receives_late_message_test() ->
    {ok, Ctx} = omq:context(),
    Endpoint = endpoint(<<"beam-huge-timeout">>),
    {ok, Pull} = omq:socket(Ctx, pull),
    {ok, Push} = omq:socket(Ctx, push),
    {ok, Endpoint} = omq:bind(Pull, Endpoint),
    ok = omq:connect(Push, Endpoint),
    ok = omq:setsockopt(Pull, rcvtimeo, 9223372036854775807),
    Parent = self(),
    spawn(fun() ->
        timer:sleep(20),
        Parent ! omq:send(Push, <<"late">>)
    end),
    ?assertEqual({ok, <<"late">>}, omq:recv(Pull)),
    receive ok -> ok after 1000 -> ?assert(false) end,
    ok = omq:close(Push),
    ok = omq:close(Pull),
    ok = omq:term(Ctx).

poll_and_select_test() ->
    {ok, Ctx} = omq:context(),
    Endpoint = endpoint(<<"beam-poll">>),
    {ok, Pull} = omq:socket(Ctx, pull),
    {ok, Push} = omq:socket(Ctx, push),
    {ok, Endpoint} = omq:bind(Pull, Endpoint),
    ok = omq:connect(Push, Endpoint),
    ?assertEqual({ok, []}, omq:poll([{Pull, omq:pollin()}], 5)),
    ok = omq:send(Push, <<"ready">>),
    {ok, Ready} = omq:poll([{Pull, omq:pollin()}, {Push, omq:pollout()}], 1000),
    ?assert(lists:member({Pull, omq:pollin()}, Ready)),
    ?assert(lists:member({Push, omq:pollout()}, Ready)),
    ?assertEqual({ok, <<"ready">>}, omq:recv(Pull, 1000)),
    ok = omq:send(Push, <<"select">>),
    {ok, [Pull], [Push], []} = omq:select([Pull], [Push], [], 1000),
    ?assertEqual({ok, <<"select">>}, omq:recv(Pull, 1000)),
    ok = omq:close(Push),
    ok = omq:close(Pull),
    ok = omq:term(Ctx).

monitor_and_connections_test() ->
    {ok, Ctx} = omq:context(),
    {ok, Pull} = omq:socket(Ctx, pull),
    {ok, Push} = omq:socket(Ctx, push),
    {ok, Monitor} = omq:monitor(Pull),
    ?assertMatch({error, would_block, _}, omq:monitor_try_recv(Monitor)),
    {ok, Bound} = omq:bind(Pull, <<"tcp://127.0.0.1:0">>),
    #{event := listening, endpoint := Bound} =
        eventually_monitor_event(Monitor, listening, 100),
    ok = omq:connect(Push, Bound),
    {ok, 1} = omq:wait_connected(Pull, 1, 1000),
    #{event := handshake_succeeded, connection_id := ConnectionId} =
        eventually_monitor_event(Monitor, handshake_succeeded, 100),
    {ok, Connections} = omq:connections(Pull),
    ?assertEqual(1, length(Connections)),
    [Info] = Connections,
    ?assertEqual(ConnectionId, maps:get(connection_id, Info)),
    ?assertEqual(Bound, maps:get(endpoint, Info)),
    ?assert(is_binary(maps:get(identity, Info))),
    {ok, SameInfo} = omq:connection_info(Pull, ConnectionId),
    ?assertEqual(ConnectionId, maps:get(connection_id, SameInfo)),
    ?assertEqual({ok, undefined}, omq:connection_info(Pull, ConnectionId + 1)),
    ok = omq:close(Push),
    _ = eventually_monitor_event(Monitor, disconnected, 100),
    ok = omq:close(Pull),
    _ = eventually_monitor_event(Monitor, closed, 100),
    ok = omq:term(Ctx).

proxy_req_rep_capture_test() ->
    {ok, Ctx} = omq:context(),
    FrontendEp = endpoint(<<"beam-proxy-frontend">>),
    BackendEp = endpoint(<<"beam-proxy-backend">>),
    CaptureEp = endpoint(<<"beam-proxy-capture">>),
    {ok, Frontend} = omq:socket(Ctx, router),
    {ok, Backend} = omq:socket(Ctx, dealer),
    {ok, Client} = omq:socket(Ctx, req),
    {ok, Worker} = omq:socket(Ctx, rep),
    {ok, CapturePush} = omq:socket(Ctx, push),
    {ok, CapturePull} = omq:socket(Ctx, pull),
    {ok, FrontendEp} = omq:bind(Frontend, FrontendEp),
    {ok, BackendEp} = omq:bind(Backend, BackendEp),
    {ok, CaptureEp} = omq:bind(CapturePull, CaptureEp),
    ok = omq:connect(CapturePush, CaptureEp),
    Proxy = spawn(fun() -> omq:proxy(Frontend, Backend, CapturePush) end),
    ok = omq:connect(Client, FrontendEp),
    ok = omq:connect(Worker, BackendEp),
    ok = omq:send(Client, <<"ping">>),
    ?assertEqual({ok, <<"ping">>}, omq:recv(Worker, 1000)),
    ok = omq:send(Worker, <<"pong">>),
    ?assertEqual({ok, <<"pong">>}, omq:recv(Client, 1000)),
    {ok, CapturedRequest} = omq:recv_multipart(CapturePull, 1000),
    ?assert(lists:member(<<"ping">>, CapturedRequest)),
    {ok, CapturedReply} = omq:recv_multipart(CapturePull, 1000),
    ?assert(lists:member(<<"pong">>, CapturedReply)),
    exit(Proxy, kill),
    ok = omq:close(CapturePull),
    ok = omq:close(CapturePush),
    ok = omq:close(Worker),
    ok = omq:close(Client),
    ok = omq:close(Backend),
    ok = omq:close(Frontend),
    ok = omq:term(Ctx).

proxy_steerable_pause_resume_test() ->
    {ok, Ctx} = omq:context(),
    FrontendEp = endpoint(<<"beam-steer-frontend">>),
    BackendEp = endpoint(<<"beam-steer-backend">>),
    ControlEp = endpoint(<<"beam-steer-control">>),
    {ok, Frontend} = omq:socket(Ctx, router),
    {ok, Backend} = omq:socket(Ctx, dealer),
    {ok, Client} = omq:socket(Ctx, req),
    {ok, Worker} = omq:socket(Ctx, rep),
    {ok, ControlIn} = omq:socket(Ctx, pair),
    {ok, ControlOut} = omq:socket(Ctx, pair),
    {ok, FrontendEp} = omq:bind(Frontend, FrontendEp),
    {ok, BackendEp} = omq:bind(Backend, BackendEp),
    {ok, ControlEp} = omq:bind(ControlIn, ControlEp),
    ok = omq:connect(ControlOut, ControlEp),
    Parent = self(),
    Proxy = spawn(fun() ->
        Parent ! {proxy_done, omq:proxy_steerable(Frontend, Backend, undefined, ControlIn)}
    end),
    ok = omq:connect(Client, FrontendEp),
    ok = omq:connect(Worker, BackendEp),
    ok = omq:send(ControlOut, <<"PAUSE">>),
    timer:sleep(50),
    ok = omq:send(Client, <<"ping">>),
    ?assertMatch({error, timeout, _}, omq:recv(Worker, 50)),
    ok = omq:send(ControlOut, <<"RESUME">>),
    ?assertEqual({ok, <<"ping">>}, omq:recv(Worker, 1000)),
    ok = omq:send(Worker, <<"pong">>),
    ?assertEqual({ok, <<"pong">>}, omq:recv(Client, 1000)),
    ok = omq:send(ControlOut, <<"TERMINATE">>),
    receive
        {proxy_done, ok} -> ok
    after 1000 ->
        exit(Proxy, kill),
        ?assert(false)
    end,
    ok = omq:close(ControlOut),
    ok = omq:close(ControlIn),
    ok = omq:close(Worker),
    ok = omq:close(Client),
    ok = omq:close(Backend),
    ok = omq:close(Frontend),
    ok = omq:term(Ctx).

connect_before_bind_tcp_test() ->
    {ok, Ctx} = omq:context(),
    Endpoint = unbound_tcp_endpoint(),
    {ok, Push} = omq:socket(Ctx, push),
    {ok, Pull} = omq:socket(Ctx, pull),
    ok = omq:connect(Push, Endpoint),
    timer:sleep(25),
    {ok, Endpoint} = omq:bind(Pull, Endpoint),
    ok = omq:send(Push, <<"late">>),
    ?assertEqual({ok, <<"late">>}, omq:recv(Pull, 1000)),
    ok = omq:close(Push),
    ok = omq:close(Pull),
    ok = omq:term(Ctx).

connect_before_bind_matrix_test() ->
    lists:foreach(fun cbb_push_pull/1, cbb_endpoints(<<"push-pull">>)),
    lists:foreach(fun cbb_req_rep/1, cbb_endpoints(<<"req-rep">>)),
    lists:foreach(fun cbb_pair/1, cbb_endpoints(<<"pair">>)).

setsockopt_subscribe_alias_test() ->
    {ok, Ctx} = omq:context(),
    Endpoint = endpoint(<<"beam-subscribe-alias">>),
    {ok, Pub} = omq:socket(Ctx, pub),
    {ok, Sub} = omq:socket(Ctx, sub),
    ok = omq:setsockopt(Sub, omq:subscribe_opt(), <<"a/">>),
    {ok, Endpoint} = omq:bind(Pub, Endpoint),
    ok = omq:connect(Sub, Endpoint),
    {ok, _} = omq:wait_connected(Pub, 1, 1000),
    {ok, _} = omq:wait_subscribed(Pub, 1, 1000),
    ok = omq:send(Pub, <<"b/drop">>),
    ok = omq:send(Pub, <<"a/take">>),
    ?assertEqual({ok, <<"a/take">>}, omq:recv(Sub, 1000)),
    ok = omq:setsockopt(Sub, unsubscribe_opt, <<"a/">>),
    ok = omq:close(Sub),
    ok = omq:close(Pub),
    ok = omq:term(Ctx).

router_mandatory_no_route_test() ->
    {ok, Ctx} = omq:context(),
    {ok, Router} = omq:socket(Ctx, router),
    ok = omq:setsockopt(Router, router_mandatory, true),
    ?assertEqual({ok, 1}, omq:getsockopt(Router, router_mandatory)),
    ?assertMatch({error, no_route, _}, omq:send_multipart(Router, [<<"missing">>, <<"body">>])),
    ok = omq:close(Router),
    ok = omq:term(Ctx).

pyzmq_push_pull_interop_tcp_test() ->
    with_pyzmq(fun() ->
        {ok, Ctx} = omq:context(),
        {ok, Pull} = omq:socket(Ctx, pull),
        {ok, Endpoint} = omq:bind(Pull, <<"tcp://127.0.0.1:0">>),
        Port = python_peer("push", Endpoint),
        ?assertEqual({ok, <<"from-pyzmq">>}, omq:recv(Pull, 2000)),
        wait_python(Port),
        ok = omq:close(Pull),
        ok = omq:term(Ctx)
    end).

pyzmq_pull_push_interop_tcp_test() ->
    with_pyzmq(fun() ->
        {ok, Ctx} = omq:context(),
        {ok, Push} = omq:socket(Ctx, push),
        {ok, Endpoint} = omq:bind(Push, <<"tcp://127.0.0.1:0">>),
        Port = python_peer("pull", Endpoint),
        wait_python_ready(Port),
        ok = omq:send(Push, <<"from-omq">>),
        wait_python(Port),
        ok = omq:close(Push),
        ok = omq:term(Ctx)
    end).

pyzmq_req_rep_interop_tcp_test() ->
    with_pyzmq(fun() ->
        {ok, Ctx} = omq:context(),
        {ok, Rep} = omq:socket(Ctx, rep),
        {ok, Endpoint} = omq:bind(Rep, <<"tcp://127.0.0.1:0">>),
        Port = python_peer("req", Endpoint),
        ?assertEqual({ok, <<"ping">>}, omq:recv(Rep, 2000)),
        ok = omq:send(Rep, <<"pong">>),
        wait_python(Port),
        ok = omq:close(Rep),
        ok = omq:term(Ctx)
    end).

pyzmq_rep_req_interop_tcp_test() ->
    with_pyzmq(fun() ->
        {ok, Ctx} = omq:context(),
        {ok, Req} = omq:socket(Ctx, req),
        {ok, Endpoint} = omq:bind(Req, <<"tcp://127.0.0.1:0">>),
        Port = python_peer("rep", Endpoint),
        wait_python_ready(Port),
        ok = omq:send(Req, <<"ping">>),
        ?assertEqual({ok, <<"pong">>}, omq:recv(Req, 2000)),
        wait_python(Port),
        ok = omq:close(Req),
        ok = omq:term(Ctx)
    end).

pyzmq_dealer_router_interop_tcp_test() ->
    with_pyzmq(fun() ->
        {ok, Ctx} = omq:context(),
        {ok, Router} = omq:socket(Ctx, router),
        {ok, Endpoint} = omq:bind(Router, <<"tcp://127.0.0.1:0">>),
        Port = python_peer("dealer", Endpoint),
        ?assertEqual({ok, [<<"D">>, <<"hi">>]}, omq:recv_multipart(Router, 2000)),
        ok = omq:send_multipart(Router, [<<"D">>, <<"back">>]),
        wait_python(Port),
        ok = omq:close(Router),
        ok = omq:term(Ctx)
    end).

pyzmq_pub_sub_interop_tcp_test() ->
    with_pyzmq(fun() ->
        {ok, Ctx} = omq:context(),
        {ok, Pub} = omq:socket(Ctx, pub),
        {ok, Endpoint} = omq:bind(Pub, <<"tcp://127.0.0.1:0">>),
        Port = python_peer("sub", Endpoint),
        wait_python_ready(Port),
        {ok, _} = omq:wait_connected(Pub, 1, 2000),
        {ok, _} = omq:wait_subscribed(Pub, 1, 2000),
        ok = omq:send(Pub, <<"topic from-omq">>),
        wait_python(Port),
        ok = omq:close(Pub),
        ok = omq:term(Ctx)
    end).

pyzmq_sub_pub_interop_tcp_test() ->
    with_pyzmq(fun() ->
        {ok, Ctx} = omq:context(),
        {ok, Sub} = omq:socket(Ctx, sub),
        ok = omq:subscribe(Sub, <<"topic">>),
        {ok, Endpoint} = omq:bind(Sub, <<"tcp://127.0.0.1:0">>),
        Port = python_peer("pub", Endpoint),
        wait_python_ready(Port),
        ?assertEqual({ok, <<"topic from-pyzmq">>}, omq:recv(Sub, 3000)),
        wait_python(Port),
        ok = omq:close(Sub),
        ok = omq:term(Ctx)
    end).

pyzmq_pair_interop_tcp_test() ->
    with_pyzmq(fun() ->
        {ok, Ctx} = omq:context(),
        {ok, Pair} = omq:socket(Ctx, pair),
        {ok, Endpoint} = omq:bind(Pair, <<"tcp://127.0.0.1:0">>),
        Port = python_peer("pair", Endpoint),
        ?assertEqual({ok, <<"from-pyzmq">>}, omq:recv(Pair, 2000)),
        ok = omq:send(Pair, <<"from-omq">>),
        wait_python(Port),
        ok = omq:close(Pair),
        ok = omq:term(Ctx)
    end).

has_feature_test() ->
    ?assertEqual(true, omq:has(ipc)),
    ?assertEqual(true, omq:has(<<"INPROC">>)),
    ?assertEqual(true, omq:has("tcp")),
    ?assertEqual(false, omq:has(pgm)),
    ?assert(is_boolean(omq:has(curve))),
    ?assert(is_boolean(omq:has(plain))),
    ?assertEqual(true, omq:has(zstd)),
    ?assertEqual(false, omq:has(gssapi)).

curve_key_helpers_test() ->
    with_feature(curve, fun() ->
        {ok, Public, Secret} = omq:curve_keypair(),
        ?assertEqual(40, byte_size(Public)),
        ?assertEqual(40, byte_size(Secret)),
        ?assertEqual({ok, Public}, omq:curve_public(Secret)),
        ?assertMatch({error, badarg, _}, omq:curve_public(<<"not-valid-z85-key">>))
    end).

plain_push_pull_tcp_test() ->
    with_feature(plain, fun() ->
        {ok, Ctx} = omq:context(),
        {ok, Pull} = omq:socket(Ctx, pull),
        {ok, Push} = omq:socket(Ctx, push),
        ok = omq:setsockopt(Pull, plain_server, true),
        ok = omq:setsockopt(Push, plain_username, <<"alice">>),
        ok = omq:setsockopt(Push, plain_password, <<"secret">>),
        {ok, Endpoint} = omq:bind(Pull, <<"tcp://127.0.0.1:0">>),
        ok = omq:connect(Push, Endpoint),
        ok = omq:send(Push, <<"hello over plain">>),
        ?assertEqual({ok, <<"hello over plain">>}, omq:recv(Pull, 5000)),
        ok = omq:close(Push),
        ok = omq:close(Pull),
        ok = omq:term(Ctx)
    end).

curve_push_pull_tcp_test() ->
    with_feature(curve, fun() ->
        {ok, Ctx} = omq:context(),
        {ok, Pull} = omq:socket(Ctx, pull),
        {ok, Push} = omq:socket(Ctx, push),
        set_curve_server_client(Pull, Push),
        {ok, Endpoint} = omq:bind(Pull, <<"tcp://127.0.0.1:0">>),
        ok = omq:connect(Push, Endpoint),
        ok = omq:send(Push, <<"hello over curve">>),
        ?assertEqual({ok, <<"hello over curve">>}, omq:recv(Pull, 5000)),
        ok = omq:close(Push),
        ok = omq:close(Pull),
        ok = omq:term(Ctx)
    end).

lz4_push_pull_tcp_test() ->
    with_feature(lz4, fun() ->
        compression_push_pull(<<"lz4+tcp://127.0.0.1:0">>, <<"hello over lz4">>)
    end).

zstd_push_pull_tcp_test() ->
    with_feature(zstd, fun() ->
        {ok, Ctx} = omq:context(),
        {ok, Pull} = omq:socket(Ctx, pull),
        {ok, Push} = omq:socket(Ctx, push),
        ok = omq:setsockopt(Push, omq_compression_level, 1),
        {ok, Endpoint} = omq:bind(Pull, <<"zstd+tcp://127.0.0.1:0">>),
        ok = omq:connect(Push, Endpoint),
        Msg = payload(4096),
        ok = omq:send(Push, Msg),
        ?assertEqual({ok, Msg}, omq:recv(Pull, 5000)),
        ok = omq:close(Push),
        ok = omq:close(Pull),
        ok = omq:term(Ctx)
    end).

options_before_materialize_test() ->
    {ok, Ctx} = omq:context(),
    {ok, Sock} = omq:socket(Ctx, dealer),
    ok = omq:set(Sock, identity, <<"beam-id">>),
    ?assertEqual({ok, <<"beam-id">>}, omq:get(Sock, identity)),
    ok = omq:setsockopt(Sock, sndhwm, 42),
    ?assertEqual({ok, 42}, omq:getsockopt(Sock, sndhwm)),
    ok = omq:setsockopt(Sock, rcvhwm, 43),
    ?assertEqual({ok, 43}, omq:getsockopt(Sock, rcvhwm)),
    ok = omq:setsockopt(Sock, sndbuf, 8192),
    ?assertEqual({ok, 8192}, omq:getsockopt(Sock, sndbuf)),
    ok = omq:setsockopt(Sock, rcvbuf, 4096),
    ?assertEqual({ok, 4096}, omq:getsockopt(Sock, rcvbuf)),
    ok = omq:setsockopt(Sock, tcp_keepalive_idle, 11),
    ok = omq:setsockopt(Sock, tcp_keepalive_intvl, 12),
    ok = omq:setsockopt(Sock, tcp_keepalive_cnt, 13),
    ?assertEqual({ok, 11}, omq:getsockopt(Sock, tcp_keepalive_idle)),
    ?assertEqual({ok, 12}, omq:getsockopt(Sock, tcp_keepalive_intvl)),
    ?assertEqual({ok, 13}, omq:getsockopt(Sock, tcp_keepalive_cnt)),
    ok = omq:setsockopt(Sock, plain_server, true),
    ok = omq:setsockopt(Sock, plain_username, <<"user">>),
    ok = omq:setsockopt(Sock, plain_password, <<"pass">>),
    ?assertEqual({ok, 1}, omq:getsockopt(Sock, plain_server)),
    ?assertEqual({ok, <<"user">>}, omq:getsockopt(Sock, plain_username)),
    ?assertEqual({ok, <<"pass">>}, omq:getsockopt(Sock, plain_password)),
    ok = omq:setsockopt(Sock, curve_server, true),
    ok = omq:setsockopt(Sock, curve_publickey, <<"pub">>),
    ok = omq:setsockopt(Sock, curve_secretkey, <<"sec">>),
    ok = omq:setsockopt(Sock, curve_serverkey, <<"srv">>),
    ?assertEqual({ok, 1}, omq:getsockopt(Sock, curve_server)),
    ?assertEqual({ok, <<"pub">>}, omq:getsockopt(Sock, curve_publickey)),
    ?assertEqual({ok, <<"sec">>}, omq:getsockopt(Sock, curve_secretkey)),
    ?assertEqual({ok, <<"srv">>}, omq:getsockopt(Sock, curve_serverkey)),
    ok = omq:setsockopt(Sock, immediate, true),
    ?assertEqual({ok, 0}, omq:getsockopt(Sock, immediate)),
    NoopOptions = [
        xpub_verbose, probe_router, req_correlate, req_relaxed, router_handover,
        zap_domain, rate, connect_timeout, recovery_ivl, ipv6, ipv4only,
        tcp_accept_filter, tcp_maxrt, multicast_hops
    ],
    lists:foreach(fun(Option) ->
        ok = omq:setsockopt(Sock, Option, 0)
    end, NoopOptions),
    ?assertEqual({ok, <<>>}, omq:getsockopt(Sock, last_endpoint)),
    ok = omq:close(Sock),
    ok = omq:term(Ctx).

option_parity_rejections_test() ->
    {ok, Ctx} = omq:context(),
    {ok, Sock} = omq:socket(Ctx, push),
    ok = omq:setsockopt(Sock, sndhwm, 64),
    ok = omq:setsockopt(Sock, rcvhwm, 32),
    ?assertMatch({error, badarg, _}, omq:setsockopt(Sock, sndhwm, -1)),
    ?assertMatch({error, badarg, _}, omq:setsockopt(Sock, rcvhwm, -1)),
    ?assertEqual({ok, 64}, omq:getsockopt(Sock, sndhwm)),
    ?assertEqual({ok, 32}, omq:getsockopt(Sock, rcvhwm)),
    ok = omq:setsockopt(Sock, hwm, 128),
    ?assertEqual({ok, 128}, omq:getsockopt(Sock, hwm)),
    ?assertEqual({ok, 128}, omq:getsockopt(Sock, sndhwm)),
    ?assertEqual({ok, 128}, omq:getsockopt(Sock, rcvhwm)),
    ok = omq:setsockopt(Sock, omq:hwm(), 256),
    ?assertEqual({ok, 256}, omq:getsockopt(Sock, omq:hwm())),
    ?assertEqual({ok, 256}, omq:getsockopt(Sock, sndhwm)),
    ?assertEqual({ok, 256}, omq:getsockopt(Sock, rcvhwm)),
    ok = omq:set_hwm(Sock, 512),
    ?assertEqual({ok, 512}, omq:get_hwm(Sock)),
    ?assertEqual({ok, 512}, omq:getsockopt(Sock, sndhwm)),
    ?assertEqual({ok, 512}, omq:getsockopt(Sock, rcvhwm)),
    ok = omq:setsockopt_string(Sock, identity, <<"string-id">>),
    ?assertEqual({ok, <<"string-id">>}, omq:getsockopt_string(Sock, identity)),
    ?assertMatch({error, badarg, _}, omq:setsockopt(Sock, type, omq:pull())),
    ?assertMatch({error, badarg, _}, omq:setsockopt(Sock, affinity, 1)),
    ?assertMatch({error, badarg, _}, omq:setsockopt(Sock, backlog, 1)),
    ?assertEqual({ok, 0}, omq:getsockopt(Sock, omq_compression_level)),
    ok = omq:setsockopt(Sock, omq_on_mute, 1),
    ?assertEqual({ok, 1}, omq:getsockopt(Sock, omq_on_mute)),
    ok = omq:setsockopt(Sock, omq_on_mute, 2),
    ?assertEqual({ok, 2}, omq:getsockopt(Sock, omq_on_mute)),
    ok = omq:setsockopt(Sock, omq_on_mute, omq:omq_on_mute_block()),
    ?assertEqual({ok, 0}, omq:getsockopt(Sock, omq_on_mute)),
    ?assertEqual(1, omq:omq_on_mute_drop_newest()),
    ?assertEqual(2, omq:omq_on_mute_drop_oldest()),
    ?assertMatch({error, badarg, _}, omq:setsockopt(Sock, omq_on_mute, 99)),
    ok = omq:setsockopt(Sock, omq_compression_level, 1),
    ?assertEqual({ok, 1}, omq:getsockopt(Sock, omq_compression_level)),
    ?assertMatch({error, badarg, _}, omq:setsockopt(Sock, omq_compression_level, -9)),
    ?assertMatch({error, badarg, _}, omq:setsockopt(Sock, omq_compression_level, 5)),
    Dict = <<"my-dict-bytes-1234">>,
    ?assertEqual({ok, <<>>}, omq:getsockopt(Sock, omq_compression_dict)),
    ok = omq:setsockopt(Sock, omq_compression_dict, Dict),
    ?assertEqual({ok, Dict}, omq:getsockopt(Sock, omq_compression_dict)),
    ok = omq:setsockopt(Sock, omq_compression_dict, <<>>),
    ?assertEqual({ok, <<>>}, omq:getsockopt(Sock, omq_compression_dict)),
    ?assertMatch({error, badarg, _}, omq:setsockopt(Sock, omq_compression_dict, binary:copy(<<0>>, 8193))),
    ok = omq:setsockopt(Sock, omq_compression_auto_train, 1),
    ?assertEqual({ok, 1}, omq:getsockopt(Sock, omq_compression_auto_train)),
    ok = omq:setsockopt(Sock, omq_compression_auto_train, 0),
    ?assertEqual({ok, 0}, omq:getsockopt(Sock, omq_compression_auto_train)),
    ok = omq:close(Sock),
    ok = omq:term(Ctx).

unbound_tcp_endpoint() ->
    {ok, Socket} = gen_tcp:listen(0, [binary, {active, false}, {ip, {127, 0, 0, 1}}]),
    {ok, Port} = inet:port(Socket),
    ok = gen_tcp:close(Socket),
    iolist_to_binary(io_lib:format("tcp://127.0.0.1:~B", [Port])).

udp_endpoint() ->
    {ok, Socket} = gen_udp:open(0, [binary, {ip, {127, 0, 0, 1}}]),
    {ok, Port} = inet:port(Socket),
    ok = gen_udp:close(Socket),
    iolist_to_binary(io_lib:format("udp://127.0.0.1:~B", [Port])).

ipc_endpoint(Name) ->
    Id = integer_to_binary(erlang:unique_integer([positive, monotonic])),
    Path = iolist_to_binary(["/tmp/omq-beam-", Name, "-", Id, ".sock"]),
    _ = file:delete(Path),
    <<"ipc://", Path/binary>>.

cbb_endpoints(Name) ->
    [endpoint(<<"beam-cbb-", Name/binary>>), ipc_endpoint(Name), unbound_tcp_endpoint()].

cbb_push_pull(Endpoint) ->
    {ok, Ctx} = omq:context(),
    {ok, Push} = omq:socket(Ctx, push),
    {ok, Pull} = omq:socket(Ctx, pull),
    ok = omq:connect(Push, Endpoint),
    timer:sleep(50),
    {ok, Endpoint} = omq:bind(Pull, Endpoint),
    ok = omq:send(Push, <<"late">>),
    ?assertEqual({ok, <<"late">>}, omq:recv(Pull, 1000)),
    ok = omq:close(Push),
    ok = omq:close(Pull),
    ok = omq:term(Ctx),
    cleanup_ipc_endpoint(Endpoint).

cbb_req_rep(Endpoint) ->
    {ok, Ctx} = omq:context(),
    {ok, Req} = omq:socket(Ctx, req),
    {ok, Rep} = omq:socket(Ctx, rep),
    ok = omq:connect(Req, Endpoint),
    timer:sleep(50),
    {ok, Endpoint} = omq:bind(Rep, Endpoint),
    ok = omq:send(Req, <<"q">>),
    ?assertEqual({ok, <<"q">>}, omq:recv(Rep, 1000)),
    ok = omq:send(Rep, <<"a">>),
    ?assertEqual({ok, <<"a">>}, omq:recv(Req, 1000)),
    ok = omq:close(Req),
    ok = omq:close(Rep),
    ok = omq:term(Ctx),
    cleanup_ipc_endpoint(Endpoint).

cbb_pair(Endpoint) ->
    {ok, Ctx} = omq:context(),
    {ok, A} = omq:socket(Ctx, pair),
    {ok, B} = omq:socket(Ctx, pair),
    ok = omq:connect(A, Endpoint),
    timer:sleep(50),
    {ok, Endpoint} = omq:bind(B, Endpoint),
    ok = omq:send(A, <<"from-a">>),
    ?assertEqual({ok, <<"from-a">>}, omq:recv(B, 1000)),
    ok = omq:send(B, <<"from-b">>),
    ?assertEqual({ok, <<"from-b">>}, omq:recv(A, 1000)),
    ok = omq:close(A),
    ok = omq:close(B),
    ok = omq:term(Ctx),
    cleanup_ipc_endpoint(Endpoint).

cleanup_ipc_endpoint(<<"ipc://", Path/binary>>) ->
    _ = file:delete(Path),
    ok;
cleanup_ipc_endpoint(_Endpoint) ->
    ok.

fill_until_timeout(_Push, 0) ->
    ok;
fill_until_timeout(Push, Attempts) ->
    case omq:send(Push, <<"x">>) of
        ok -> fill_until_timeout(Push, Attempts - 1);
        Error -> Error
    end.

tcp_host_port(Endpoint) ->
    <<"tcp://", Rest/binary>> = Endpoint,
    [HostBin, PortBin] = binary:split(Rest, <<":">>, [global, trim_all]),
    {binary_to_list(HostBin), binary_to_integer(PortBin)}.

eventually(Fun, Expected, Attempts) ->
    case Fun() of
        Expected -> ok;
        _Other when Attempts > 0 ->
            timer:sleep(10),
            eventually(Fun, Expected, Attempts - 1);
        Other ->
            ?assertEqual(Expected, Other)
    end.

eventually_monitor_event(Monitor, Kind, Attempts) ->
    case omq:monitor_recv(Monitor, 100) of
        {ok, #{event := Kind} = Event} ->
            Event;
        {ok, _Other} when Attempts > 0 ->
            eventually_monitor_event(Monitor, Kind, Attempts - 1);
        Other ->
            ?assertMatch({ok, #{event := Kind}}, Other)
    end.

with_feature(Feature, Fun) ->
    case omq:has(Feature) of
        true -> Fun();
        false -> ok
    end.

set_curve_server_client(Server, Client) ->
    {ok, ServerPublic, ServerSecret} = omq:curve_keypair(),
    {ok, ClientPublic, ClientSecret} = omq:curve_keypair(),
    ok = omq:setsockopt(Server, curve_server, true),
    ok = omq:setsockopt(Server, curve_publickey, ServerPublic),
    ok = omq:setsockopt(Server, curve_secretkey, ServerSecret),
    ok = omq:setsockopt(Client, curve_serverkey, ServerPublic),
    ok = omq:setsockopt(Client, curve_publickey, ClientPublic),
    ok = omq:setsockopt(Client, curve_secretkey, ClientSecret).

compression_push_pull(Endpoint, Msg) ->
    {ok, Ctx} = omq:context(),
    {ok, Pull} = omq:socket(Ctx, pull),
    {ok, Push} = omq:socket(Ctx, push),
    {ok, Bound} = omq:bind(Pull, Endpoint),
    ok = omq:connect(Push, Bound),
    ok = omq:send(Push, Msg),
    ?assertEqual({ok, Msg}, omq:recv(Pull, 5000)),
    ok = omq:close(Push),
    ok = omq:close(Pull),
    ok = omq:term(Ctx).

payload(Size) ->
    Prefix = <<"{\"kind\":\"quote\",\"symbol\":\"OMQ\",\"pad\":\"">>,
    Suffix = <<"\"}">>,
    Padding = binary:copy(<<"A">>, Size - byte_size(Prefix) - byte_size(Suffix)),
    <<Prefix/binary, Padding/binary, Suffix/binary>>.

with_pyzmq(Fun) ->
    case pyzmq_available() of
        true -> Fun();
        false -> ok
    end.

pyzmq_available() ->
    Python = os:find_executable("python3"),
    Python =/= false andalso
        os:cmd(Python ++ " -c 'import zmq' >/dev/null 2>&1; echo $?") =:= "0\n".

python_peer(Mode, Endpoint) ->
    Python = os:find_executable("python3"),
    ?assertNotEqual(false, Python),
    open_port({spawn_executable, Python}, [
        {args, ["-u", "-c", python_peer_code(), Mode, binary_to_list(Endpoint)]},
        exit_status,
        use_stdio,
        stderr_to_stdout,
        {line, 4096}
    ]).

wait_python_ready(Port) ->
    receive
        {Port, {data, {eol, "READY"}}} -> ok;
        {Port, {data, {eol, Line}}} -> wait_python_ready_line(Port, Line);
        {Port, {exit_status, Status}} -> ?assertEqual(0, Status)
    after 5000 ->
        ?assert(false)
    end.

wait_python_ready_line(Port, _Line) ->
    wait_python_ready(Port).

wait_python(Port) ->
    receive
        {Port, {data, {eol, _Line}}} -> wait_python(Port);
        {Port, {exit_status, 0}} -> ok;
        {Port, {exit_status, Status}} -> ?assertEqual(0, Status)
    after 5000 ->
        ?assert(false)
    end.

python_peer_code() ->
    "import sys, time, zmq\n"
    "mode, ep = sys.argv[1], sys.argv[2]\n"
    "ctx = zmq.Context()\n"
    "def sock(t):\n"
    "    s = ctx.socket(t)\n"
    "    s.setsockopt(zmq.LINGER, 0)\n"
    "    return s\n"
    "try:\n"
    "    if mode == 'push':\n"
    "        s = sock(zmq.PUSH); s.connect(ep); time.sleep(0.05); s.send(b'from-pyzmq'); s.close()\n"
    "    elif mode == 'pull':\n"
    "        s = sock(zmq.PULL); s.connect(ep); print('READY', flush=True); assert s.recv() == b'from-omq'; s.close()\n"
    "    elif mode == 'req':\n"
    "        s = sock(zmq.REQ); s.connect(ep); s.send(b'ping'); assert s.recv() == b'pong'; s.close()\n"
    "    elif mode == 'rep':\n"
    "        s = sock(zmq.REP); s.connect(ep); print('READY', flush=True); assert s.recv() == b'ping'; s.send(b'pong'); s.close()\n"
    "    elif mode == 'dealer':\n"
    "        s = sock(zmq.DEALER); s.setsockopt(zmq.IDENTITY, b'D'); s.connect(ep); s.send(b'hi'); assert s.recv() == b'back'; s.close()\n"
    "    elif mode == 'sub':\n"
    "        s = sock(zmq.SUB); s.setsockopt(zmq.SUBSCRIBE, b'topic'); s.connect(ep); print('READY', flush=True); assert s.recv() == b'topic from-omq'; s.close()\n"
    "    elif mode == 'pub':\n"
    "        s = sock(zmq.PUB); s.connect(ep); print('READY', flush=True); time.sleep(0.3); s.send(b'topic from-pyzmq'); s.close()\n"
    "    elif mode == 'pair':\n"
    "        s = sock(zmq.PAIR); s.connect(ep); s.send(b'from-pyzmq'); assert s.recv() == b'from-omq'; s.close()\n"
    "    else:\n"
    "        raise SystemExit('bad mode: ' + mode)\n"
    "finally:\n"
    "    ctx.term()\n".

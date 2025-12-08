-module(client_server_SUITE).

-include("http2.hrl").

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-compile([export_all]).

all() ->
    [
     {group, default_handler},
     {group, peer_handler},
     {group, double_body_handler},
     {group, echo_handler},
     {group, client}
    ].

groups() -> [{default_handler,  [complex_request,
                                 upgrade_tcp_connection,
                                 basic_push,
                                 connect_timeout]},
             {peer_handler, [get_peer_in_handler]},
             {double_body_handler, [send_body_opts]},
             {echo_handler, [echo_body,
                             large_body]},
             {client, [extra_data_on_closed_stream]}
            ].

init_per_suite(Config) ->
    Config.

init_per_group(default_handler, Config) ->
    %% We'll start up a chatterbox server once, with this data_dir.
    NewConfig = [{www_root, data_dir},{initial_window_size,99999999}|Config],
    chatterbox_test_buddy:start(NewConfig);
init_per_group(double_body_handler, Config) ->
    NewConfig = [{stream_callback_mod, double_body_handler},
                 {initial_window_size,99999999}|Config],
    chatterbox_test_buddy:start(NewConfig),
    Config;
init_per_group(peer_handler, Config) ->
    NewConfig = [{stream_callback_mod, peer_test_handler},
                 {initial_window_size,99999999}|Config],
    chatterbox_test_buddy:start(NewConfig);
init_per_group(echo_handler, Config) ->
    NewConfig = [{stream_callback_mod, echo_handler}|Config],
    chatterbox_test_buddy:start(NewConfig);
init_per_group(client, Config) ->
    ProtocolCallbackOpts = [{defer_data_until_rst_stream, true}],
    NewConfig = [{protocol_callback_opts, ProtocolCallbackOpts},
                 {protocol_callback_mod, http2s}|Config],
    chatterbox_test_buddy:start(NewConfig);
init_per_group(_, Config) -> Config.

init_per_testcase(_, Config) ->
    Config.

end_per_group(_, Config) ->
    chatterbox_test_buddy:stop(Config),
    ok.

end_per_suite(_Config) ->
    ok.

complex_request(_Config) ->
    application:set_env(chatterbox, client_initial_window_size, 99999999),
    {ok, Client} = chatterbox_h2_client:start_link(),
    RequestHeaders =
        [
         {<<":method">>, <<"GET">>},
         {<<":path">>, <<"/index.html">>},
         {<<":scheme">>, <<"https">>},
         {<<":authority">>, <<"localhost:8080">>},
         {<<"accept">>, <<"*/*">>},
         {<<"accept-encoding">>, <<"gzip, deflate">>},
         {<<"user-agent">>, <<"chattercli/0.0.1 :D">>}
        ],
    {ok, {ResponseHeaders, ResponseBody, _Trailers}} = chatterbox_h2_client:sync_request(Client, RequestHeaders, <<>>),

    ct:pal("Response Headers: ~p", [ResponseHeaders]),
    ct:pal("Response Body: ~p", [ResponseBody]),

    ok.

upgrade_tcp_connection(_Config) ->
    %% TODO Why don't the options of keyfile/certfile/cacertfile work here
    %% but instead we have to turn off verification?
    {ok, Client} = chatterbox_h2_client:start_ssl_upgrade_link("localhost", 8081, <<>>, [{verify, verify_none}]),

    RequestHeaders =
        [
         {<<":method">>, <<"GET">>},
         {<<":path">>, <<"/index.html">>},
         {<<":scheme">>, <<"https">>},
         {<<":authority">>, <<"localhost:8080">>},
         {<<"accept">>, <<"*/*">>},
         {<<"accept-encoding">>, <<"gzip, deflate">>},
         {<<"user-agent">>, <<"chattercli/0.0.1 :D">>}
        ],
    {ok, {ResponseHeaders, ResponseBody, _Trailers}} = chatterbox_h2_client:sync_request(Client, RequestHeaders, <<>>),
    ct:pal("Response Headers: ~p", [ResponseHeaders]),
    ct:pal("Response Body: ~p", [ResponseBody]),
    ok.


basic_push(_Config) ->
    {ok, Client} = chatterbox_h2_client:start_link(),
    RequestHeaders =
        [
         {<<":method">>, <<"GET">>},
         {<<":path">>, <<"/index.html">>},
         {<<":scheme">>, <<"https">>},
         {<<":authority">>, <<"localhost:8080">>},
         {<<"accept">>, <<"*/*">>},
         {<<"accept-encoding">>, <<"gzip, deflate">>},
         {<<"user-agent">>, <<"chattercli/0.0.1 :D">>}
        ],
    {ok, {ResponseHeaders, _ResponseBody, _Trailers}} = chatterbox_h2_client:sync_request(Client, RequestHeaders, <<>>),

    ct:pal("Response Headers: ~p", [ResponseHeaders]),
    %ct:pal("Response Body: ~p", [ResponseBody]),

    %% Give it time to deliver pushes
    %% We'll know we're done when we're notified of all the streams ending.
    wait_for_n_notifications(12),

    timer:sleep(1000),

    Streams = Client,
    ct:pal("Streams ~p", [Streams]),
    ?assertEqual(0, (chatterbox_h2_stream_set:my_active_count(Streams))),
    %?assertEqual(0, (chatterbox_h2_stream_set:their_active_count(Streams))),

    MyActiveStreams = chatterbox_h2_stream_set:my_active_streams(Streams),
    ct:pal("my active ~p", [MyActiveStreams]),
    ?assertEqual(0, (length(MyActiveStreams))), %% This closed stream should be GC'ed

    TheirActiveStreams = chatterbox_h2_stream_set:their_active_streams(Streams),
    ct:pal("my active ~p", [TheirActiveStreams]),
    ?assertEqual(12, (length(TheirActiveStreams))),

    [?assertEqual(closed, (chatterbox_h2_stream_set:type(S))) || S <- TheirActiveStreams],
    ok.

wait_for_n_notifications(0) ->
    ok;
wait_for_n_notifications(N) ->
    ct:pal("test waiting for END_STREAM on ~p", [self()]),
    receive
        {'END_STREAM', _} ->
            ct:pal("got END_STREAM ~p", [N]),
            wait_for_n_notifications(N-1);
        _ ->
            wait_for_n_notifications(N)
    after
        2000 ->
            ok
    end.

connect_timeout(_Config) ->
    {ok, Port} = application:get_env(chatterbox, port),
    ?assertMatch({error, {shutdown, timeout}},
                 chatterbox_h2_client:start(http, "localhost", Port, [], #{connect_timeout => 0})),
    ok.

get_peer_in_handler(_Config) ->
    {ok, Client} = chatterbox_h2_client:start_link(),
    RequestHeaders =
        [
         {<<":method">>, <<"GET">>},
         {<<":path">>, <<"/index.html">>},
         {<<":scheme">>, <<"https">>},
         {<<":authority">>, <<"localhost:8080">>},
         {<<"accept">>, <<"*/*">>},
         {<<"accept-encoding">>, <<"gzip, deflate">>},
         {<<"user-agent">>, <<"chattercli/0.0.1 :D">>}
        ],


    {ok, {ResponseHeaders, ResponseBody, _Trailers}} = chatterbox_h2_client:sync_request(Client, RequestHeaders, <<>>),
    ct:pal("Response Headers: ~p", [ResponseHeaders]),
    ct:pal("Response Body: ~p", [ResponseBody]),
    ok.

send_body_opts(_Config) ->
    {ok, Client} = chatterbox_h2_client:start_link(),
    RequestHeaders =
        [
         {<<":method">>, <<"GET">>},
         {<<":path">>, <<"/index.html">>},
         {<<":scheme">>, <<"https">>},
         {<<":authority">>, <<"localhost:8080">>},
         {<<"accept">>, <<"*/*">>},
         {<<"accept-encoding">>, <<"gzip, deflate">>},
         {<<"user-agent">>, <<"chattercli/0.0.1 :D">>}
        ],

    ExpectedResponseBody = <<"BodyPart1\nBodyPart2">>,

    {ok, {ResponseHeaders, ResponseBody, _Trailers}} = chatterbox_h2_client:sync_request(Client, RequestHeaders, <<>>),
    ct:pal("Response Headers: ~p", [ResponseHeaders]),
    ct:pal("Response Body: ~p", [ResponseBody]),
    ?assertEqual(ExpectedResponseBody, (iolist_to_binary(ResponseBody))),
    ok.

echo_body(_Config) ->
    {ok, Client} = http2c:start_link(),
    RequestHeaders =
    [
      {<<":method">>, <<"POST">>},
      {<<":path">>, <<"/">>},
      {<<":scheme">>, <<"https">>},
      {<<":authority">>, <<"localhost:8080">>},
      {<<"accept">>, <<"*/*">>},
      {<<"accept-encoding">>, <<"gzip, deflate">>},
      {<<"user-agent">>, <<"chattercli/0.0.1 :D">>}
    ],

    {ok, {HeadersBin, _EncodeContext}} = hpack:encode(RequestHeaders, hpack:new_context()),

    HeaderFrame = {#frame_header{
                      length=byte_size(HeadersBin),
                      type=?HEADERS,
                      flags=?FLAG_END_HEADERS,
                      stream_id=3
                     },
                   chatterbox_h2_frame_headers:new(HeadersBin)
                  },

    http2c:send_unaltered_frames(Client, [HeaderFrame]),

    Body = crypto:strong_rand_bytes(128),
    BodyFrames = chatterbox_h2_frame_data:to_frames(3, Body, #settings{max_frame_size=64}),
    http2c:send_unaltered_frames(Client, BodyFrames),

    timer:sleep(300),
    Frames = http2c:get_frames(Client, 3),
    DataFrames = lists:filter(fun({#frame_header{type=?DATA}, _}) -> true;
                                 (_) -> false end, Frames),
    ResponseData = lists:map(fun({_, DataP}) ->
                                     chatterbox_h2_frame_data:data(DataP)
                             end, DataFrames),
    io:format("Body: ~p, response: ~p~n", [Body, ResponseData]),
    ?assertEqual(Body, (iolist_to_binary(ResponseData))),
    ok.

large_body(_Config) ->
    {ok, Client} = http2c:start_link(),
    RequestHeaders =
    [
      {<<":method">>, <<"POST">>},
      {<<":path">>, <<"/">>},
      {<<":scheme">>, <<"https">>},
      {<<":authority">>, <<"localhost:8080">>},
      {<<"accept">>, <<"*/*">>},
      {<<"accept-encoding">>, <<"gzip, deflate">>},
      {<<"user-agent">>, <<"chattercli/0.0.1 :D">>}
    ],

    {ok, {HeadersBin, _EncodeContext}} = hpack:encode(RequestHeaders, hpack:new_context()),

    HeaderFrame = {#frame_header{
                      length=byte_size(HeadersBin),
                      type=?HEADERS,
                      flags=?FLAG_END_HEADERS,
                      stream_id=3
                     },
                   chatterbox_h2_frame_headers:new(HeadersBin)
                  },

    http2c:send_unaltered_frames(Client, [HeaderFrame]),

    Body = crypto:strong_rand_bytes(32828),
    BodyFrames = chatterbox_h2_frame_data:to_frames(3, Body, #settings{max_frame_size=16384}),
    http2c:send_unaltered_frames(Client, BodyFrames),

    timer:sleep(300),
    Frames = http2c:get_frames(Client, 3),
    DataFrames = lists:filter(fun({#frame_header{type=?DATA}, _}) -> true;
                                 (_) -> false end, Frames),
    ResponseData = lists:map(fun({_, DataP}) ->
                                     chatterbox_h2_frame_data:data(DataP)
                             end, DataFrames),
    io:format("response: ~p~n", [ResponseData]),
    ?assertEqual(size(Body), iolist_size(ResponseData)),
    ok.

extra_data_on_closed_stream(_Config) ->
    {ok, StreamSet} = chatterbox_h2_client:start_link(),
    RequestHeaders =
        [
         {<<":method">>, <<"GET">>},
         {<<":path">>, <<"/index.html">>},
         {<<":scheme">>, <<"https">>},
         {<<":authority">>, <<"localhost:8080">>},
         {<<"accept">>, <<"*/*">>},
         {<<"accept-encoding">>, <<"gzip, deflate">>},
         {<<"user-agent">>, <<"chattercli/0.0.1 :D">>}
        ],
    {ok, StreamId} = chatterbox_h2_client:send_request(StreamSet, RequestHeaders, <<"body">>),
    Stream = chatterbox_h2_stream_set:get(StreamId, StreamSet),
    Pid = chatterbox_h2_stream_set:stream_pid(Stream),
    chatterbox_h2_stream:rst_stream(Pid, ?CANCEL),
    receive
        {'END_STREAM', StreamId} ->
            ok
    after 5000 ->
            ct:fail(timeout)
    end,
    %% Wait for the http2s to receive the rst_stream, upon which it
    %% should send its deferred data, so that the h2_connection client
    %% will receive data on a closed stream.  We should still be able
    %% to send new requests on the connection.
    %% Chek that the h2_connection is still alive.
    H2ConnectionPid = chatterbox_h2_stream_set:connection(StreamSet),
    MRef = monitor(process, H2ConnectionPid),
    {ok, StreamId2} = chatterbox_h2_client:send_request(StreamSet, RequestHeaders, <<>>),
    receive
        {'DOWN', MRef, _, _, Reason} ->
            error({connection_terminated, Reason});
        {'END_STREAM', StreamId2} ->
            {ok, {_ResponseHeaders, _ResponseBody, _Trailers}} =
                chatterbox_h2_client:get_response(StreamSet, StreamId2)
    end,
    ok.

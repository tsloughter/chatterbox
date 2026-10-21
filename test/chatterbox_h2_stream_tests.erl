-module(chatterbox_h2_stream_tests).

-include_lib("eunit/include/eunit.hrl").
-include("http2.hrl").

-export([init/3, handle_info/2]).

stream_id_test() ->
    with_stream(fun(Stream, _Peer) ->
        ?assertEqual(17, chatterbox_h2_stream:stream_id(Stream))
    end).

connection_test() ->
    with_stream(fun(Stream, _Peer) ->
        ?assertEqual(self(), chatterbox_h2_stream:connection(Stream))
    end).

from_callback_test() ->
    with_stream(fun(Stream, _Peer) ->
        receive
            {from_callback, Stream, StreamId, Connection} ->
                ?assertEqual(17, StreamId),
                ?assertEqual(self(), Connection)
        after 1000 ->
            error(timeout)
        end
    end).

half_closed_local_call_test() ->
    with_stream(fun(Stream, _Peer) ->
        to_half_closed_local(Stream),
        ?assertEqual(17, chatterbox_h2_stream:stream_id(Stream)),
        ?assertEqual(self(), chatterbox_h2_stream:connection(Stream))
    end).

half_closed_local_rst_stream_code_test() ->
    with_stream(fun(Stream, Peer) ->
        to_half_closed_local(Stream),
        Ref = monitor(process, Stream),
        chatterbox_h2_stream:rst_stream(Stream, ?CANCEL),
        ?assertEqual({17, ?CANCEL}, recv_rst_stream(Peer)),
        assert_stopped(Ref, Stream)
    end).

half_closed_local_window_update_test() ->
    with_stream(fun(Stream, Peer) ->
        to_half_closed_local(Stream),
        assert_window_update(Stream, Peer),
        ?assertMatch({half_closed_local, _}, sys:get_state(Stream))
    end).

half_closed_local_info_test() ->
    with_stream(fun(Stream, Peer) ->
        to_half_closed_local(Stream),
        Stream ! ping,
        assert_handle_info(Stream, ping),
        ?assertMatch({half_closed_local, _}, sys:get_state(Stream)),
        assert_no_frame(Peer)
    end).

half_closed_remote_rst_stream_code_test() ->
    with_stream(fun(Stream, Peer) ->
        to_half_closed_remote(Stream),
        Ref = monitor(process, Stream),
        chatterbox_h2_stream:rst_stream(Stream, ?CANCEL),
        ?assertEqual({17, ?CANCEL}, recv_rst_stream(Peer)),
        assert_stopped(Ref, Stream)
    end).

half_closed_remote_window_update_test() ->
    with_stream(fun(Stream, Peer) ->
        to_half_closed_remote(Stream),
        assert_window_update(Stream, Peer),
        ?assertMatch({half_closed_remote, _}, sys:get_state(Stream))
    end).

closed_call_test() ->
    with_closed_stream(fun(Stream) ->
        ?assertEqual(17, chatterbox_h2_stream:stream_id(Stream))
    end).

closed_rst_stream_test() ->
    with_closed_stream(fun(Stream) ->
        chatterbox_h2_stream:rst_stream(Stream, ?CANCEL)
    end).

closed_window_update_test() ->
    with_closed_stream(fun(Stream) ->
        chatterbox_h2_stream:send_event(Stream, {send_window_update, 100})
    end).

closed_connection_window_update_test() ->
    with_closed_stream(fun(Stream) ->
        chatterbox_h2_stream:send_event(Stream, {send_connection_window_update, 200}),
        assert_connection_window_update(200)
    end).

closed_info_test() ->
    with_closed_stream(fun(Stream) ->
        Stream ! ping,
        assert_handle_info(Stream, ping)
    end).

connection_window_update_survives_close_test() ->
    with_stream(fun(Stream, Peer) ->
        to_half_closed_local(Stream),
        Ref = monitor(process, Stream),
        ok = sys:suspend(Stream),
        Stream ! {send_connection_window_update, 200},
        chatterbox_h2_stream:rst_stream(Stream, ?CANCEL),
        ok = sys:resume(Stream),
        assert_connection_window_update(200),
        ?assertEqual({17, ?CANCEL}, recv_rst_stream(Peer)),
        assert_stopped(Ref, Stream)
    end).

outside_stream_process_test() ->
    ?assertError(not_a_stream_process, chatterbox_h2_stream:stream_id()),
    ?assertError(not_a_stream_process, chatterbox_h2_stream:connection()).

to_half_closed_local(Stream) ->
    chatterbox_h2_stream:send_event(Stream, {recv_pp, []}),
    chatterbox_h2_stream:send_event(Stream, {recv_h, []}),
    ?assertMatch({half_closed_local, _}, sys:get_state(Stream)).

to_half_closed_remote(Stream) ->
    chatterbox_h2_stream:send_event(Stream, {send_h, []}),
    chatterbox_h2_stream:send_event(Stream, recv_es),
    ?assertMatch({half_closed_remote, _}, sys:get_state(Stream)).

%% A content-length mismatch leaves the stream in closed without the
%% zero timeout that would otherwise stop it right away.
to_closed(Stream, Peer) ->
    chatterbox_h2_stream:send_event(Stream, {send_h, [{<<"content-length">>, <<"5">>}]}),
    chatterbox_h2_stream:send_event(Stream, recv_es),
    ?assertEqual({17, ?PROTOCOL_ERROR}, recv_rst_stream(Peer)),
    ?assertMatch({closed, _}, sys:get_state(Stream)).

with_closed_stream(Test) ->
    with_stream(fun(Stream, Peer) ->
        to_closed(Stream, Peer),
        Ref = monitor(process, Stream),
        Test(Stream),
        assert_stopped(Ref, Stream),
        assert_no_frame(Peer)
    end).

assert_connection_window_update(Size) ->
    receive
        {'$gen_cast', {send_window_update, Size}} -> ok
    after 1000 ->
        error(no_connection_window_update)
    end.

assert_window_update(Stream, Peer) ->
    chatterbox_h2_stream:send_event(Stream, {send_window_update, 100}),
    ?assertMatch({ok, <<4:24, ?WINDOW_UPDATE:8, _Flags:8, 0:1, 17:31, 0:1, 100:31>>},
                 gen_tcp:recv(Peer, 13, 1000)),
    assert_no_frame(Peer).

assert_no_frame(Peer) ->
    ?assertEqual({error, timeout}, gen_tcp:recv(Peer, 0, 100)).

assert_handle_info(Stream, Event) ->
    receive
        {handle_info, Stream, Event} -> ok
    after 1000 ->
        error(no_handle_info)
    end.

assert_stopped(Ref, Stream) ->
    receive
        {'DOWN', Ref, process, Stream, normal} -> ok
    after 1000 ->
        error(stream_not_stopped)
    end.

recv_rst_stream(Peer) ->
    {ok, <<4:24, ?RST_STREAM:8, _Flags:8, 0:1, StreamId:31, Code:32>>} =
        gen_tcp:recv(Peer, 13, 1000),
    {StreamId, Code}.

with_stream(Test) ->
    {ok, Listen} = gen_tcp:listen(0, [binary, {active, false}]),
    {ok, Port} = inet:port(Listen),
    {ok, Client} = gen_tcp:connect("localhost", Port, [binary, {active, false}]),
    {ok, Peer} = gen_tcp:accept(Listen, 1000),
    Socket = {gen_tcp, Client},
    Streams = chatterbox_h2_stream_set:new(client, Socket, ?MODULE, [], false),
    {ok, Stream} = chatterbox_h2_stream:start_link(17, Streams, self(), ?MODULE, [self()]),
    try
        Test(Stream, Peer)
    after
        unlink(Stream),
        exit(Stream, kill),
        [gen_tcp:close(S) || S <- [Peer, Client, Listen]],
        flush(Stream)
    end.

flush(Stream) ->
    receive
        {from_callback, Stream, _, _} -> flush(Stream);
        {handle_info, Stream, _} -> flush(Stream);
        {'$gen_cast', _} -> flush(Stream)
    after 0 ->
        ok
    end.

init(_Streams, _StreamId, [_Socket, TestPid]) ->
    TestPid ! {from_callback, self(), chatterbox_h2_stream:stream_id(),
               chatterbox_h2_stream:connection()},
    {ok, TestPid}.

handle_info({send_connection_window_update, Size}, TestPid) ->
    chatterbox_h2_stream:send_connection_window_update(Size),
    TestPid;
handle_info(Event, TestPid) ->
    TestPid ! {handle_info, self(), Event},
    TestPid.

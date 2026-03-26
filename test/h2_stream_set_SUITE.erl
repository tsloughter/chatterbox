-module(h2_stream_set_SUITE).

-include("http2.hrl").

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-compile([export_all, nowarn_export_all]).

all() ->
    [
     {group, x}
    ].

groups() -> [{x, [send_window_size_after_settings_in_handshake,
                  scheduling_when_out_of_window]}
            ].

init_per_suite(Config) ->
    Config.

init_per_group(_, Config) ->
    Config.

init_per_testcase(_, Config) ->
    Config.

end_per_testcase(_, Config) ->
    Config.

end_per_group(_, _Config) ->
    ok.

end_per_suite(_Config) ->
    ok.

send_window_size_after_settings_in_handshake(_Config) ->
    {ok, #{server_addr := Addr, server_port := Port}=ServerInfo} =
        start_server(),
    {ok, #{sock := _CSock, streams := Streams}=Conn} =
        connect_to_server(Addr, Port),
    ok = handshake_as_if_exchanged_settings(65535, Conn),
    ct:pal("handshaked, streams=~n  ~p~n", [extract_stream_set(Streams)]),
    timer:sleep(1000),
    close_connection(Conn, ServerInfo),
    ok.

extract_stream_set(StreamSet) ->
  #{stream_set => StreamSet,
    counters => maps:from_list(
                  [{{I, K}, atomics:get(element(3, StreamSet), I)}
                   || {K, I} <- [{recv_window_size, 1},
                                 {send_window_size, 2},
                                 {my_next_available_stream_id, 3},
                                 {their_next_available_stream_id, 4},
                                 {my_active_count, 5},
                                 {their_active_count, 6},
                                 {last_send_all_we_can_stream_ID, 7}]]),
    table => ets:tab2list(element(6, StreamSet))}.

scheduling_when_out_of_window() ->
    [{timetrap, {seconds, 15}}].

scheduling_when_out_of_window(_Config) ->
    {ok, #{server_addr := Addr, server_port := Port}=ServerInfo} =
        start_server(),
    ct:pal("Addr,Port=~p,~p", [Addr, Port]),
    {ok, #{sock := _CSock, streams := Streams}=Conn} =
        connect_to_server(Addr, Port),
    ok = handshake_as_if_exchanged_settings(65535, Conn),
    ct:pal("handshaked: Stream:~n  ~p", [extract_stream_set(Streams)]),

    %% Trigger window update, as if we'd gotten it from the from server
    %% h2_stream_set:increment_socket_send_window(10, Streams)
    simulate_window_update(10, 0, Streams, ServerInfo),

    %% This is an infinite stream
    {_StreamPid1, StreamId1, _} = chatterbox_h2_stream_set:new_stream(
                                   next, self(), undefined, [], Streams),
    ok = send_headers_and_data_from_client(Conn, StreamId1,
                                           <<"hello world 12345678">>),

    %% Inidcate that there's room to send 5 more bytes in addition to
    %% the 10 bytes above, but the entire text to send is 20 bytes, so
    %% there should still be more data to send on this stream:
    simulate_window_update(5, 0, Streams, ServerInfo),

    %% Now, before all data has been sent on stream 1, do start some more
    %% streams each with 20 more bytes of data to send

    AllStreamIds = [1, 3, 5, 7, 9, 11], % client stream ids are odd-numbered
    MoreStreamIds = tl(AllStreamIds), % send on the rest of the streams:
    lists:foreach(
        fun(_) ->
            {_StreamPidN, StreamIdN, _} =
                    chatterbox_h2_stream_set:new_stream(next, self(), undefined, [],
                                             Streams),
            ok = send_headers_and_data_from_client(Conn, StreamIdN,
                                                   <<"hello world 12345678">>),
            simulate_window_update(20, 0, Streams, ServerInfo)
        end, MoreStreamIds),

    %% The server has received data for all streams 1..11
    %%
    %% If everything is good, the first stream's data should have made
    %% it over to the server side by now:
    %% initially 10 bytes, then 5more bytes, then the rest as the window size
    %% was increased when each new stream sent more data.
    %%
    %% However, without the fix, stream 1 will get starved for sending,
    %% while all of subsequently started streams' data will make it through.
    Events = get_await_data_for_streams(ServerInfo, AllStreamIds, []),
    show_event_data(Events),
    ?assertEqual(20, iolist_size(get_event_data_payloads(StreamId1, Events))),
    close_connection(Conn, ServerInfo),
    ok.

get_await_data_for_streams(ServerInfo, StreamIdsToAwait, Events) ->
    MoreEvents = get_clear_events(ServerInfo),
    AllEvents = Events ++ MoreEvents,
    StreamIdsReceived =
        [StreamId || #{type := data,
                       stream_id := StreamId} <- AllEvents],
    case sets:is_equal(sets:from_list(StreamIdsToAwait),
                       sets:from_list(StreamIdsReceived)) of
        true ->
            AllEvents;
        false ->
            %% We asked the server too early, some data could be in transit
            %% over TCP to the server still. Ask again soon:
            timer:sleep(10),
            get_await_data_for_streams(ServerInfo, StreamIdsToAwait, AllEvents)
    end.

show_event_data(Events) ->
    StreamIds = lists:usort([StreamId || #{stream_id := StreamId} <- Events]),
    Rows = [begin
                Bytes = get_event_data_payloads(StreamId, Events),
                io_lib:format("  ~3w     ~3p: ~p~n",
                              [StreamId, iolist_size(Bytes), Bytes])
            end
            || StreamId <- StreamIds],
    ct:pal("  Stream  Bytes received server side~n"
           "  ----------------------------------~n"
           "~s~n"
           "  ----------------------------------~n"
           " Events received on the server side: ~p~n",
           [Rows, Events]).

get_event_data_payloads(StreamId, Events) ->
    [Data || #{type := data,
               stream_id := SId, payload := {data, Data}} <- Events,
             SId == StreamId].

connect_to_server(Addr, Port) ->
    {ok, Sock} = gen_tcp:connect(Addr, Port,
                                 [binary, {packet, 0}, {active, false}]),
    CSocket = {gen_tcp, Sock},
    Streams = chatterbox_h2_stream_set:new(client, CSocket, undefined, [], false),
    {ok, #{sock => CSocket,
           streams => Streams}}.

handshake_as_if_exchanged_settings(ServierSideSendWindowSize,
                                   #{streams := Streams}) ->
    %% Simulate a handshake exchange of settings
    %% We're a client in this scenario, so we'll first get
    %% settings from the server side.
    %% Simulate that the server side sends us (only) its initial window size.

    %% For comparison, printouts in h2_connection when initiating a client
    %% connection to a http2 implemented in go.
    %%
    %% First we receive settings from the go server, which was created with:
    %%        s := grpc.NewServer(
    %%                grpc.InitialConnWindowSize(10*1024*1024),
    %%                grpc.InitialWindowSize(10*1024*1024),
    %%        )
    %% And we get:
    %%   PS={settings,4096,1,unlimited,65535,16384,unlimited}
    %%   Payload={settings,[{<<5>>,16384},{<<4>>,10485760}]}
    %%     OldIWS=65535
    %%     HTS=4096
    %%     => {settings,4096,1,unlimited,10485760,16384,unlimited}
    %%        Delta=10420225
    %%
    %% Then we get an ack for our own sent settings.
    %% We had initially set this chatterbox application env variable:
    %%   client_initial_window_size = 1024 * 1024 * 10
    %% and this happens:
    %%   Settings ACK:
    %%     NewSettings (queued to get applied):
    %%       {settings,4096,1,unlimited,65535,16384,unlimited}
    %%     get_self_settings_locked returned:
    %%       {settings,4096,1,unlimited,10485760,16384,unlimited}
    %%     Delta = 10420225
    %%     Max Concurrent = unlimited

    #settings{initial_window_size=PeerOldIWS} = PeerSettings0 =
        chatterbox_h2_stream_set:get_peer_settings_locked(Streams),
    ServerDelta = ServierSideSendWindowSize - PeerOldIWS,
    PeerSettings1 = PeerSettings0#settings{
                      initial_window_size = ServierSideSendWindowSize},
    chatterbox_h2_stream_set:update_peer_settings(Streams, PeerSettings1),
    chatterbox_h2_stream_set:update_all_send_windows(ServerDelta, Streams),

    %% Now simulate that we've sent our settings and gotten an ACK,
    %% so we must now apply our sent settings.
    #settings{initial_window_size=OurSentInitialWindowSize} = OurSettings0 =
        chatterbox:settings(client),
    ?assert(is_integer(OurSentInitialWindowSize), OurSettings0),
    #settings{initial_window_size=OurOldIWS} =
        chatterbox_h2_stream_set:get_self_settings_locked(Streams),
    OurDelta = OurSentInitialWindowSize - OurOldIWS,
    OurSettings1 = OurSettings0#settings{initial_window_size = OurSentInitialWindowSize},
    chatterbox_h2_stream_set:update_self_settings(Streams, OurSettings1),
    chatterbox_h2_stream_set:update_all_recv_windows(OurDelta, Streams),
    ok.

start_server() ->
    Testcase = self(),
    proc_lib:start(erlang, apply, [fun() -> init_tcp_server(Testcase) end, []]).

simulate_window_update(N, 0=_StreamId, Streams, ServerInfo) ->
    chatterbox_h2_stream_set:increment_socket_send_window(N, Streams),
    save_event(ServerInfo, {socket_window_update, N}),
    chatterbox_h2_stream_set:send_all_we_can(Streams);
simulate_window_update(N, StreamId, Streams, ServerInfo) ->
    save_event(ServerInfo, {stream_window_update, N, StreamId}),
    chatterbox_h2_stream_set:send_what_we_can(
      StreamId,
      fun(Stream) ->
              chatterbox_h2_stream_set:increment_send_window_size(N, Stream)
      end, Streams).

get_clear_events(#{server_pid := Pid}) ->
    do_request(Pid, get_clear_events).

save_event(#{server_pid := Pid}, Event) ->
    do_request(Pid, {save_event, Event}).

send_headers_and_data_from_client(Conn, StreamId, Body) ->
    ok = send_headers(Conn, StreamId),
    ok = send_body(Conn, StreamId, Body).

send_headers(#{sock := Sock, streams := Streams}, StreamId) ->
    Headers =
        [
         {<<":method">>, <<"GET">>},
         {<<":path">>, <<"/index.html">>},
         {<<":scheme">>, <<"https">>},
         {<<":authority">>, <<"localhost:8080">>},
         {<<"accept">>, <<"*/*">>},
         {<<"accept-encoding">>, <<"gzip, deflate">>},
         {<<"user-agent">>, <<"chattercli/0.0.1 :D">>}
        ],
    Stream = chatterbox_h2_stream_set:get(StreamId, Streams),
    {_SelfSettings, PeerSettings} = chatterbox_h2_stream_set:get_settings(Streams),
    {Lock, EncodeContext} = chatterbox_h2_stream_set:get_encode_context(Streams, Headers),
    Complete = true,
    {FramesToSend, NewContext} =
        chatterbox_h2_frame_headers:to_frames(
          chatterbox_h2_stream_set:stream_id(Stream),
          Headers,
          EncodeContext,
          PeerSettings#settings.max_frame_size,
          Complete
         ),
    chatterbox_sock:send(Sock, [chatterbox_h2_frame:to_binary(Frame) || Frame <- FramesToSend]),
    chatterbox_h2_stream_set:release_encode_context(Streams, {Lock, NewContext}),
    case chatterbox_h2_stream_set:pid(Stream) of
        undefined ->
            %% TODO: Should this be some kind of error?
            ok;
        Pid ->
            chatterbox_h2_stream:send_event(Pid, {send_h, Headers})
    end,
    ok.

send_body(#{streams := Streams}, StreamId, Body) ->
    BodyComplete = true,
    chatterbox_h2_stream_set:send_what_we_can(
      StreamId,
      fun(Stream) ->
              OldBody = chatterbox_h2_stream_set:queued_data(Stream),
              NewBody = case is_binary(OldBody) of
                            true -> <<OldBody/binary, Body/binary>>;
                            false -> Body
                        end,
              chatterbox_h2_stream_set:update_data_queue(NewBody, BodyComplete, Stream)
      end, Streams),
    ok.



init_tcp_server(Testcase) ->
    TcMRef = monitor(process, Testcase),
    {ok, LSock} = gen_tcp:listen(0, [binary, {packet, 0}, {active, false}]),
    {ok, {_Addr, Port}} = inet:sockname(LSock), % Addr will be {0,0,0,0}
    proc_lib:init_ack({ok, #{listen_socket => LSock,
                             server_pid => self(),
                             server_addr => {127, 0, 0, 1},
                             server_port => Port}}),
    {ok, Sock} = gen_tcp:accept(LSock),
    ct:pal("tcp_server: Client connected, Sock=~p", [Sock]),
    loop_tcp_server(#{listen_socket => LSock,
                      socket => {gen_tcp, Sock},
                      decoder => hpack:new_context(),
                      testcase => Testcase,
                      testcase_mref => TcMRef,
                      events => []}).

loop_tcp_server(#{socket := Sock, listen_socket := LSock,
                  testcase_mref := TcMRef, events := Events}=State) ->
    case chatterbox_h2_frame:read(Sock, 50) of
        {error, closed} ->
            ct:pal("tcp_server: Socket remotely closed,~n"
                   " events= ~p~n",
                   [lists:reverse(Events)]),
            gen_tcp:close(LSock),
            ok;
        {error, Reason} when Reason /= timeout ->
            ct:pal("tcp_server: Failure to read from socket ~p: ~p,~n"
                   " events= ~p~n",
                   [Sock, Reason, lists:reverse(Events)]),
            gen_tcp:close(LSock),
            error({read_error,Reason});
        {#frame_header{type=Type, stream_id=StreamId} = _Header, Payload} ->
            ct:pal("tcp_server: Got H,P=~p,~p~n", [_Header, Payload]),
            Event = #{type => pretty_type(Type),
                      stream_id => StreamId,
                      payload => Payload},
            ?FUNCTION_NAME(State#{events := [Event | Events]});
        {error, timeout} ->
            receive
                {request, From, Req} ->
                    case Req of
                        get_clear_events ->
                            do_reply(From, lists:reverse(Events)),
                            ?FUNCTION_NAME(State#{events := []});
                        {save_event, Event} ->
                            do_reply(From, ok),
                            ?FUNCTION_NAME(State#{events := [Event | Events]});
                        stop ->
                            ct:pal("tcp_server: terminating on request~n"),
                            gen_tcp:close(LSock),
                            sock_close(Sock),
                            do_reply(From, ok);
                        X ->
                            ct:pal("tcp_server: Unexpected request, "
                                   "terminating:~n  ~p",
                                   [X]),
                            error({tcp_server_received_unexpected_request, X})
                    end;
                {'DOWN', TcMRef, _, _, _} ->
                    ct:pal("tcp_server: test case terminated,~n"
                           " events = ~p~n",
                           [lists:reverse(Events)]),
                    gen_tcp:close(LSock),
                    sock_close(Sock),
                    ok
            after 0 -> ?FUNCTION_NAME(State)
            end;
        X ->
            ct:pal("tcp_server: read from ~p returned unexpected value (terminating):~n"
                   "  ~p~n"
                   "  events = ~p~n",
                   [Sock, X, lists:reverse(Events)]),
            gen_tcp:close(LSock),
            sock_close(Sock),
            error({unexpected_read_value, X})
    end.

pretty_type(?DATA) -> data;
pretty_type(?HEADERS) -> headers;
pretty_type(?PRIORITY) -> priority;
pretty_type(?RST_STREAM) -> rst_stream;
pretty_type(?SETTINGS) -> settings;
pretty_type(?PUSH_PROMISE) -> push_promise;
pretty_type(?PING) -> ping;
pretty_type(?GOAWAY) -> goaway;
pretty_type(?WINDOW_UPDATE) -> window_update;
pretty_type(?CONTINUATION) -> continuation;
pretty_type(Unexpected) -> Unexpected.

close_connection(#{sock := CSock}, #{server_pid := ServerPid}) ->
    ct:pal("close_connection: stopping server~n"),
    try do_request(ServerPid, stop)
    catch _:_ -> ok
    end,
    ct:pal("close_connection: closing socket~n"),
    sock_close(CSock),
    ok.

sock_close({gen_tcp, Sock}) ->
    gen_tcp:close(Sock).

do_request(Pid, Req) ->
    MRef = monitor(process, Pid),
    Pid ! {request, {MRef, self()}, Req},
    receive
        {reply, MRef, Reply} ->
            demonitor(MRef, [flush]),
            Reply;
        {'DOWN', MRef, _, _, Reason} ->
            error({terminated, Reason, Req})
    end.

do_reply({Ref, Pid}, Reply) ->
    Pid ! {reply, Ref, Reply}.

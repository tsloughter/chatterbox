-module(http2s).
%%-behaviour(ranch_protocol).
-include("../include/http2.hrl").

-export([is_stream/1]).

%% Ranch callbacks
-export([start_link/3]).
-export([init/3]).

start_link(Ref, Transport, Opts) ->
    Pid = proc_lib:spawn_link(?MODULE, init, [Ref, Transport, Opts]),
    {ok, Pid}.

is_stream(StreamId) ->
    request({is_stream, StreamId}).

init(Ref, Transport, Opts) ->
    {ok, Socket} = ranch:handshake(Ref),
    register(?MODULE, self()),
    Sock = {transport(Transport), Socket},
    ct:pal("New incoming connection to the server."),
    {ok, ?PREFACE} = chatterbox_sock:recv(Sock, byte_size(?PREFACE), 5000),
    GarbageOnEnd = false,
    Streams = chatterbox_h2_stream_set:new(server, Sock, undefined, [], GarbageOnEnd),
    {CurrentSettings, _PeerSettings} = chatterbox_h2_stream_set:get_settings(Streams),
    SettingsToSend = chatterbox:settings(server),
    Bin = chatterbox_h2_frame_settings:send(CurrentSettings, SettingsToSend),
    ok = chatterbox_sock:send(Sock, Bin),

    DoDefer = proplists:get_bool(defer_data_until_rst_stream, Opts),
    NoEndStream = proplists:get_bool(no_end_stream, Opts),
    State0 = #{no_end_stream => NoEndStream,
               defer_until_rst_stream => DoDefer,
               deferred_to_rst_stream => undefined,
               streams => []},
    receive_loop(Sock, hpack:new_context(), State0).

transport(ranch_ssl) ->
    ssl;
transport(ranch_tcp) ->
    gen_tcp;
transport(tcp) ->
    gen_tcp;
transport(gen_tcp) ->
    gen_tcp;
transport(ssl) ->
    ssl;
transport(_Other) ->
    error(unknown_protocol).

receive_loop(Socket, Decoder,
             #{no_end_stream := NoEndStream,
               defer_until_rst_stream := DoDefer,
               deferred_to_rst_stream := DeferredData,
               streams := Streams}=State) ->
    case chatterbox_h2_frame:read(Socket, 10) of
        Frame={#frame_header{stream_id = StreamId}=Header, _Payload} ->
            Streams1 = lists:uniq([StreamId | Streams]),
            State1 = State#{streams := Streams1},
            case Header#frame_header.type of
                ?SETTINGS ->
                    if ?NOT_FLAG((Header#frame_header.flags), ?FLAG_ACK) ->
                            ok = chatterbox_sock:send(Socket, chatterbox_h2_frame_settings:ack());
                       true ->
                            ok
                    end,
                    receive_loop(Socket, Decoder, State1);
                ?HEADERS ->
                    HeadersBin = chatterbox_h2_frame_headers:from_frames([Frame]),
                    {ok, {_Headers, NewDecoder}} = hpack:decode(HeadersBin, Decoder),
                    receive_loop(Socket, NewDecoder, State1);
                ?DATA ->
                    Response = <<"response">>,
                    Flags = case NoEndStream of
                                true -> 0;
                                false -> ?FLAG_END_STREAM
                            end,
                    ToSend = {#frame_header{stream_id=StreamId, type=?DATA,
                                            length=iolist_size(Response),
                                            flags=Flags},
                              chatterbox_h2_frame_data:new(Response)},
                    if DoDefer ->
                            State2 = State1#{defer_until_rst_stream => false,
                                             deferred_to_rst_stream => ToSend},
                            receive_loop(Socket, Decoder, State2);
                       NoEndStream ->
                            ok = chatterbox_sock:send(Socket, chatterbox_h2_frame:to_binary(ToSend)),
                            receive_loop(Socket, Decoder, State1);
                       not DoDefer ->
                            ok = chatterbox_sock:send(Socket, chatterbox_h2_frame:to_binary(ToSend)),
                            Streams2 = Streams1 -- [StreamId],
                            State2 = State1#{streams := Streams2},
                            receive_loop(Socket, Decoder, State2)
                    end;
                ?RST_STREAM ->
                    if DeferredData == undefined ->
                            ok;
                       true ->
                            ok = chatterbox_sock:send(Socket, chatterbox_h2_frame:to_binary(DeferredData))
                    end,
                    Streams2 = Streams1 -- [StreamId],
                    State2 = State1#{streams := Streams2,
                                     deferred_to_rst_stream := undefined},
                    receive_loop(Socket, Decoder, State2);
                ?WINDOW_UPDATE ->
                    receive_loop(Socket, Decoder, State1);
                Other ->
                    ct:pal("~p: got frame of type ~p~n frame: ~p",
                           [?MODULE, Other, Frame]),
                    receive_loop(Socket, Decoder, State1)
            end;
        {error, closed} ->
            terminate_receive_loop;
        {error, timeout} ->
            receive
                {request, From, Req} ->
                    case Req of
                        {is_stream, StreamId} ->
                            reply(From, lists:member(StreamId, Streams))
                    end
            after 0 ->
                    ok
            end,
            receive_loop(Socket, Decoder, State)
    end.

request(Req) ->
    case whereis(?MODULE) of
        P when is_pid(P) ->
            MRef = monitor(process, P),
            From = {MRef, self()},
            P ! {request, From, Req},
            receive
                {resp, MRef, Resp} ->
                    demonitor(MRef, [flush]),
                    Resp;
                {'DOWN', MRef, _, _, Reason} ->
                    error({terminated, Reason})
            end;
        undefined ->
            error({noproc, ?MODULE})
    end.

reply({Ref, Pid}, Resp) ->
    Pid ! {resp, Ref, Resp}.

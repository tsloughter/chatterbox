-module(http2s).
%%-behaviour(ranch_protocol).
-include("../include/http2.hrl").

%% Ranch callbacks
-export([start_link/3]).
-export([init/3]).

start_link(Ref, Transport, Opts) ->
    Pid = proc_lib:spawn_link(?MODULE, init, [Ref, Transport, Opts]),
    {ok, Pid}.

init(Ref, Transport, Opts) ->
    {ok, Socket} = ranch:handshake(Ref),
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
    State0 = #{defer_until_rst_stream => DoDefer,
               deferred_to_rst_stream => undefined},
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
                #{defer_until_rst_stream := DoDefer,
                  deferred_to_rst_stream := DeferredData}=State) ->
    maybe
        Frame={#frame_header{}=Header, _Payload} ?= chatterbox_h2_frame:read(Socket, infinity),
        case Header#frame_header.type of
            ?SETTINGS ->
                if ?NOT_FLAG((Header#frame_header.flags), ?FLAG_ACK) ->
                        ok = chatterbox_sock:send(Socket, chatterbox_h2_frame_settings:ack());
                   true ->
                        ok
                end,
                receive_loop(Socket, Decoder, State);
            ?HEADERS ->
                HeadersBin = chatterbox_h2_frame_headers:from_frames([Frame]),
                {ok, {_Headers, NewDecoder}} = hpack:decode(HeadersBin, Decoder),
                receive_loop(Socket, NewDecoder, State);
            ?DATA ->
                StreamId = Header#frame_header.stream_id,
                Response = <<"response">>,
                ToSend = {#frame_header{stream_id=StreamId, type=?DATA,
                                        length=iolist_size(Response),
                                        flags=?FLAG_END_STREAM},
                          chatterbox_h2_frame_data:new(Response)},
                if DoDefer ->
                        State1 = State#{defer_until_rst_stream => false,
                                        deferred_to_rst_stream => ToSend},
                        receive_loop(Socket, Decoder, State1);
                   not DoDefer ->
                        ok = chatterbox_sock:send(Socket, chatterbox_h2_frame:to_binary(ToSend)),
                        receive_loop(Socket, Decoder, State)
                end;
            ?RST_STREAM ->
                if DeferredData == undefined ->
                        ok;
                   true ->
                        ok = chatterbox_sock:send(Socket, chatterbox_h2_frame:to_binary(DeferredData))
                end,
                State1 = State#{deferred_to_rst_stream := undefined},
                receive_loop(Socket, Decoder, State1);
            ?WINDOW_UPDATE ->
                receive_loop(Socket, Decoder, State);
            Other ->
                ct:pal("~p: got frame of type ~p~n frame: ~p",
                       [?MODULE, Other, Frame]),
                receive_loop(Socket, Decoder, State)
        end
    else
        {error, closed} -> terminate_receive_loop
    end.

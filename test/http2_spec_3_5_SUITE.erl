-module(http2_spec_3_5_SUITE).

-include("http2.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-compile([export_all]).

all() ->
    [
     sends_invalid_connection_preface,
     sends_incomplete_connection_preface
    ].

init_per_suite(Config) ->
    application:ensure_started(crypto),
    chatterbox_test_buddy:start(Config).

end_per_suite(Config) ->
    chatterbox_test_buddy:stop(Config),
    ok.

%% Inspired by https://github.com/summerwind/h2spec/blob/master/3_5.go
sends_invalid_connection_preface(Config) ->
    %% Preface correct except for last character
    send_invalid_connection_preface(<<"PRI * HTTP/2.0\r\n\r\nSM\r\n\rQ">>, Config),
    %% Preface incorrect at first character
    send_invalid_connection_preface(<<"QRI * HTTP/2.0\r\n\r\nSM\r\n\r\n">>, Config),
    %% Just plain wrong
    send_invalid_connection_preface(<<"INVALID CONNECTION PREFACE\r\n\r\n">>, Config),
    ok.

send_invalid_connection_preface(Preface, _Config) ->
    {ok, Port} = application:get_env(chatterbox, port),
    ClientOptions = [
               binary,
               {packet, raw},
               {active, false}
              ],
    {ok, SSLOptions} = application:get_env(chatterbox, ssl_options),
    Options =  ClientOptions ++ SSLOptions ++ [{client_preferred_next_protocols, {client, [<<"h2">>]}}],

    {ok, Socket} = ssl:connect("localhost", Port, Options),

    ssl:send(Socket, Preface),

    %% Server should reject the preface and close the socket. Drain
    %% until we observe the close — on slow CI the close hasn't yet
    %% propagated by the time we'd otherwise call ssl:send.
    ok = wait_for_close(Socket),
    %% First post-close send can spuriously succeed (TCP is full-duplex —
    %% the FIN we observed only closed the server→client direction; the
    %% kernel only learns our write side is dead once an RST comes back).
    ok = wait_for_send_error(Socket),
    {error, _} = ssl:connection_information(Socket),
    ok.

wait_for_close(Socket) ->
    case ssl:recv(Socket, 0, 5000) of
        {error, _} -> ok;
        {ok, _}    -> wait_for_close(Socket)
    end.

wait_for_send_error(Socket) ->
    wait_for_send_error(Socket, 50).

wait_for_send_error(_Socket, 0) ->
    {error, send_kept_succeeding};
wait_for_send_error(Socket, N) ->
    case ssl:send(Socket, <<"x">>) of
        {error, _} -> ok;
        ok ->
            timer:sleep(100),
            wait_for_send_error(Socket, N - 1)
    end.

sends_incomplete_connection_preface(_Config) ->
    {ok, Port} = application:get_env(chatterbox, port),
    ClientOptions = [
               binary,
               {packet, raw},
               {active, false}
              ],
    {ok, SSLOptions} = application:get_env(chatterbox, ssl_options),
    Options =  ClientOptions ++ SSLOptions ++ [{client_preferred_next_protocols, {client, [<<"h2">>]}}],

    {ok, Socket} = ssl:connect("localhost", Port, Options),

    ssl:send(Socket, <<"PRI * HTTP/2.0">>),

    ssl:recv(Socket, 0, 1000),

    {ok, _ConnectionInfo} = ssl:connection_information(Socket),

    %% There's a 5 second timeout before the socket will be closed.
    ok = wait_for_close(Socket),
    %% First post-close send can spuriously succeed (TCP is full-duplex —
    %% the FIN we observed only closed the server→client direction; the
    %% kernel only learns our write side is dead once an RST comes back).
    ok = wait_for_send_error(Socket),
    {error, _} = ssl:connection_information(Socket),
    ok.

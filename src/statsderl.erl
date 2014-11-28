-module(statsderl).

%% public
-export([
    decrement/3,
    gauge/3,
    gauge_decr/2,
    gauge_incr/2,
    increment/3,
    pool_size/0,
    server_name/1,
    start_link/1,
    timing/3,
    timing_fun/3,
    timing_now/3
]).

-behaviour(gen_server).
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-define(POOL_SIZE, 2).
-define(SERVER, ?MODULE).

-record(state, {
    hostname,
    port,
    socket,
    basekey
}).

%% public
decrement(Key, Value, SampleRate) ->
    maybe_send(decrement, Key, Value, SampleRate).

gauge(Key, Value, SampleRate) ->
    maybe_send(gauge, Key, Value, SampleRate).

gauge_decr(Key, Value) ->
    maybe_send(gauge_decr, Key, Value, 1).

gauge_incr(Key, Value) ->
    maybe_send(gauge_incr, Key, Value, 1).

increment(Key, Value, SampleRate) ->
    maybe_send(increment, Key, Value, SampleRate).

pool_size() ->
    ?POOL_SIZE.

server_name(N) ->
    list_to_atom("statsderl_" ++ integer_to_list(N)).

start_link(Name) ->
    gen_server:start_link({local, Name}, ?MODULE, [], []).

timing(Key, Value, SampleRate) ->
    maybe_send(timing, Key, Value, SampleRate).

timing_fun(Key, Fun, SampleRate) ->
    Timestamp = os:timestamp(),
    Result = Fun(),
    timing_now(Key, Timestamp, SampleRate),
    Result.

timing_now(Key, Timestamp, SampleRate) ->
    timing(Key, now_diff_ms(Timestamp), SampleRate).

%% gen_server callbacks
init(_Args) ->
    {ok, Hostname} = application:get_env(statsderl, hostname),
    {ok, Port} = application:get_env(statsderl, port),
    BaseKey = get_base_key(application:get_env(statsderl, base_key)),
    {ok, Socket} = gen_udp:open(0, [{active, true}]),

    {ok, #state {
        hostname = lookup_hostname(Hostname),
        port = Port,
        basekey = BaseKey,
        socket = Socket
    }}.

handle_call(_Request, _From, State) ->
    {noreply, ok, State}.

handle_cast({send, Packet}, #state {
        hostname = HostName,
        port = Port,
        socket = Socket,
        basekey = BaseKey} = State) ->

    Message = [BaseKey, Packet],
    try gen_udp:send(Socket, HostName, Port, Message) of
        ok -> ok;
        {error, Reason} ->
          error_logger:info_msg("** UDP ** send failed ~p~n", [Reason])
    catch
        E1:E2 -> 
          error_logger:info_msg("** UDP ** send failed ~p~n", [{E1, E2}])
    end,
    {noreply, State};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(Info, State) ->
    error_logger:info_msg("** UDP ** received ~p~n", [Info]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% private
get_base_key({ok, hostname}) ->
    {ok, Hostname} = inet:gethostname(),
    [Hostname, $.];
get_base_key({ok, sname}) ->
    Name = atom_to_list(node()),
    SName = string:sub_word(Name, 1, $@),
    [SName, $.];
get_base_key({ok, name}) ->
    Name = atom_to_list(node()),
    Value = re:replace(Name, "@", ".", [global, {return, list}]),
    [Value, $.];
get_base_key({ok, Key}) ->
    [Key, $.];
get_base_key(undefined) ->
    <<"">>.

format_sample_rate(SampleRate) ->
    [<<"|@">>, io_lib:format("~.3f", [SampleRate])].

generate_packet(decrement, Key, Value, SampleRate) when SampleRate >= 1 ->
    [Key, <<":-">>, Value, <<"|c">>];
generate_packet(decrement, Key, Value, SampleRate) ->
    [Key, <<":-">>, Value, <<"|c">>, format_sample_rate(SampleRate)];
generate_packet(increment, Key, Value, SampleRate) when SampleRate >= 1 ->
    [Key, <<":">>, Value, <<"|c">>];
generate_packet(increment, Key, Value, SampleRate) ->
    [Key, <<":">>, Value, <<"|c">>, format_sample_rate(SampleRate)];
generate_packet(gauge, Key, Value, _SampleRate) ->
    [Key, <<":">>, Value, <<"|g">>];
generate_packet(gauge_decr, Key, Value, _SampleRate) ->
    [Key, <<":-">>, Value, <<"|g">>];
generate_packet(gauge_incr, Key, Value, _SampleRate) ->
    [Key, <<":+">>, Value, <<"|g">>];
generate_packet(timing, Key, Value, _SampleRate) ->
    [Key, <<":">>, Value, <<"|ms">>].

lookup_hostname(Address) when is_tuple(Address) ->
    Address;
lookup_hostname(Hostname) ->
    case inet:gethostbyname(Hostname) of
        {ok, {_, _, _, _, _, [Address | _]}} ->
            Address;
        _Else ->
            {127, 0, 0, 1}
    end.

maybe_seed() ->
    case get(random_seed) of
        undefined ->
            random:seed(os:timestamp());
        {X, X, X} ->
            random:seed(os:timestamp());
        _ ->
            ok
    end.

maybe_send(Method, Key, Value, 1) ->
    send(Method, Key, Value, 1);
maybe_send(Method, Key, Value, 1.0) ->
    send(Method, Key, Value, 1.0);
maybe_send(Method, Key, Value, SampleRate) ->
    maybe_seed(),
    case random:uniform() =< SampleRate of
        true  ->
            send(Method, Key, Value, SampleRate);
        false ->
            ok
    end.

now_diff_ms(Timestamp) ->
    timer:now_diff(os:timestamp(), Timestamp) div 1000.

random_server() ->
    Random = erlang:phash2({os:timestamp(), self()}, ?POOL_SIZE) + 1,
    server_name(Random).

send(Method, Key, Value, SampleRate) when is_integer(Value) ->
    BinValue = list_to_binary(integer_to_list(Value)),
    Packet = generate_packet(Method, Key, BinValue, SampleRate),
    gen_server:cast(random_server(), {send, Packet});
send(Method, Key, Value, SampleRate) when is_float(Value) ->
    BinValue = list_to_binary(io_lib:format("~.2f", [Value])),
    Packet = generate_packet(Method, Key, BinValue, SampleRate),
    gen_server:cast(random_server(), {send, Packet}).

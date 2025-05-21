-module(fast_disk_log_writer).
-include("fast_disk_log.hrl").

-export([
    start_link/4
]).

-behaviour(metal).

-export([
    handle_msg/2,
    init/3
]).

-record(state, {
    fd,
    logger,
    name,
    timer_delay,
    timer_ref,
    write_count = 0
}).

-type state() :: #state{}.

%% public
-spec handle_msg(term(), state()) -> {ok, state()}.

handle_msg(auto_close, #state {
        write_count = 0,
        logger = Logger
    } = State) ->

    spawn(fun () -> fast_disk_log:close(Logger) end),
    {ok, State};
handle_msg(auto_close, #state {timer_delay = TimerDelay} = State) ->
    {ok, State#state {
        timer_ref = new_timer(TimerDelay, auto_close),
        write_count = 0
    }};
handle_msg({close, PoolSize, Pid}, #state {
        fd = Fd,
        name = Name
    }) ->
    Buffer = lists:reverse(close_wait(PoolSize)),
    case file:write(Fd, Buffer) of
        ok -> ok;
        {error, Reason} ->
            ?ERROR_MSG("failed to write: ~p~n", [Reason])
    end,
    case file:sync(Fd) of
        ok -> ok;
        {error, Reason2} ->
            ?ERROR_MSG("failed to sync: ~p~n", [Reason2])
    end,
    case file:close(Fd) of
        ok -> ok;
        {error, Reason3} ->
            ?ERROR_MSG("failed to close: ~p~n", [Reason3])
    end,
    Pid ! {fast_disk_log, {closed, Name}},
    exit(normal);
handle_msg({write, Buffer}, #state {
        fd = Fd,
        write_count = WriteCount
    } = State) ->

    case file:write(Fd, Buffer) of
        ok ->
            {ok, State#state {
                write_count = WriteCount + 1
            }};
        {error, Reason} ->
            ?ERROR_MSG("failed to write: ~p~n", [Reason]),
            {ok, State}
    end.

-spec init(atom(), pid(), {name(), filename(), open_options()}) -> {ok, state()} | {stop, term()}.

init(Name, Parent, {Logger, Filename, Opts}) ->
    case file:open(Filename, [append, raw]) of
        {ok, Fd} ->
            State = #state {
                name = Name,
                fd = Fd,
                logger = Logger
            },

            case ?LOOKUP(auto_close, Opts, ?DEFAULT_AUTO_CLOSE) of
                true ->
                    AutoCloseDelay = ?ENV(max_delay, ?DEFAULT_MAX_DELAY) * 2,
                    {ok, State#state {
                        timer_delay = AutoCloseDelay,
                        timer_ref = new_timer(AutoCloseDelay, auto_close)
                    }};
                false ->
                    {ok, State}
            end;
        {error, Reason} ->
            ?ERROR_MSG("failed to open file: ~p ~p~n", [Reason, Filename]),
            {stop, Reason}
    end.

-spec start_link(atom(), name(), filename(), open_options()) -> {ok, pid()}.

start_link(Name, Logger, Filename, Opts) ->
    metal:start_link(?MODULE, Name, {Logger, Filename, Opts}).

%% private
close_wait(0) ->
    [];
close_wait(N) ->
    receive
        {write, Buffer} ->
            [Buffer | close_wait(N - 1)]
    after ?CLOSE_TIMEOUT ->
        []
    end.

new_timer(Delay, Msg) ->
    erlang:send_after(Delay, self(), Msg).

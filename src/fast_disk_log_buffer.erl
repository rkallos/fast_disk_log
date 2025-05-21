-module(fast_disk_log_buffer).
-include("fast_disk_log.hrl").

-export([
    start_link/2
]).

-behaviour(metal).

-export([
    init/3,
    handle_msg/2
]).

-record(state, {
    buffer = [],
    buffer_size = 0,
    name,
    max_buffer_size,
    max_delay,
    timer_ref,
    writer
}).

-type state() :: #state{}.

%% public
-spec init(pid(), atom(), atom()) -> no_return().

init(Name, _Parent, Writer) ->
    MaxBufferSize = ?ENV(max_size, ?DEFAULT_MAX_SIZE),
    MaxDelay = ?ENV(max_delay, ?DEFAULT_MAX_DELAY),

    {ok, #state {
        name = Name,
        max_buffer_size = MaxBufferSize,
        max_delay = MaxDelay,
        timer_ref = new_timer(MaxDelay),
        writer = Writer
    }}.

-spec start_link(atom(), atom()) -> {ok, pid()}.

start_link(Name, Writer) ->
    metal:start_link(?MODULE, Name, Writer).

-spec handle_msg(term(), state()) -> {ok, state()}.

handle_msg(close, #state {
        buffer = Buffer,
        timer_ref = TimerRef,
        writer = Writer
    }) ->

    Writer ! {write, lists:reverse(Buffer)},
    erlang:cancel_timer(TimerRef),
    exit(normal);
handle_msg(sync, #state {
        buffer = Buffer,
        writer = Writer
    } = State) ->

    write(Writer, Buffer),
    reset_buffer(State);
handle_msg(timeout, #state {
        buffer = Buffer,
        writer = Writer
    } = State) ->

    write(Writer, Buffer),
    reset_buffer(State);
handle_msg({log, Bin}, #state {
        buffer = Buffer,
        buffer_size = BufferSize,
        max_buffer_size = MaxBufferSize,
        writer = Writer
    } = State) ->

    NewBuffer = [Bin | Buffer],
    case BufferSize + size(Bin) of
        X when X >= MaxBufferSize ->
            write(Writer, NewBuffer),
            reset_buffer(State);
        NewBufferSize ->
            {ok, State#state {
                buffer = NewBuffer,
                buffer_size = NewBufferSize
            }}
    end.

%% private

new_timer(Time) ->
    erlang:send_after(Time, self(), timeout).

reset_buffer(#state {
        max_delay = MaxDelay,
        timer_ref = TimerRef
    } = State) ->

    erlang:cancel_timer(TimerRef),
    {ok, State#state {
        buffer = [],
        buffer_size = 0,
        timer_ref = new_timer(MaxDelay)
    }}.

write(_Writer, []) ->
    ok;
write(Writer, Buffer) ->
    Writer ! {write, lists:reverse(Buffer)}.

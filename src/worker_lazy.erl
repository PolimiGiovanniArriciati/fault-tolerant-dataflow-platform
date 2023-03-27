-module(worker_lazy).
-export([start/0, start/1, start/2, ping/2]).
-importlib([functions]).

start() ->
    start("localhost", 8080).

start(Port) ->
    start("localhost", Port).

start(Host, Port) ->
    case gen_tcp:connect(Host, Port, [binary, {packet, 0}, {active, false}, {buffer, 16384}, {send_timeout, 10000}, {send_timeout_close, true}], 10000) of
        {ok, Sock} ->
            io:format("Worker connected to coordinator~n"),
            ok = gen_tcp:send(Sock, term_to_binary(join)),
            _ = spawn_link(?MODULE, ping, [Sock, 3000]),
            worker_routine(Sock);
        {error, Error} ->
            io:format("Worker could not connect to coordinator ~w~n", [Error])
    end.
    
% Worker routine, up to now just a stub to handle messaging with the coordinator
worker_routine(Sock) ->
    case gen_tcp:recv(Sock, 0) of
        {ok, Msg} ->
            {job, {CallerPid, Counter, {Operation, Function, Args}, Data}} = binary_to_term(Msg),
            io:format("Worker received job: ~w~n", [Operation]),
            io:format("Worker received job: ~p~n", [[Function, Args, Data]]),
            Result = erlang:apply(functions, Operation, [Function, Args, Data]),
            timer:sleep(2000), % Simulate a long running task
            case gen_tcp:send(Sock, term_to_binary({result, CallerPid, Counter, Result})) of
                ok ->
                    worker_routine(Sock);
                {error, Error} ->
                    io:fwrite("Error: ~w,~n...shutting down the worker", [Error]),
                    halt();
                Error ->
                    io:format("Unexpected message: ~w~n", [Error]),
                    halt()
            end;
        {error, closed} ->
            io:fwrite("Connection closed,~n...shutting down the worker"),
            halt();
        {error, Error} ->
            io:fwrite("Error: ~w,~n...shutting down the worker", [Error]),
            halt();
        Error ->
            io:format("Unexpected message: ~w~n", [Error]),
            halt()
    end,
    worker_routine(Sock).

ping(Sock, Interval) ->
    timer:sleep(Interval),
    case gen_tcp:send(Sock, term_to_binary(ping)) of
        ok ->
            ping(Sock, Interval);
        {error, Error} ->
            io:fwrite("Error: ~w,~n...shutting down the worker", [Error]),
            halt();
        Unknown ->
            io:fwrite("unknown message ~w ~n", [Unknown])
    end.
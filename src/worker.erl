-module(worker).
-export([start/0, start/1, start/2]).
-importlib([functions]).

start() ->
    start("localhost", 8080).

start(Port) ->
    start("localhost", Port).

start(Host, Port) ->
    case gen_tcp:connect(Host, Port, [binary, {packet, 0}, {active, false}, {buffer, 16384}]) of
        {ok, Sock} ->
            io:format("Worker connected to coordinator~n"),
            ok = gen_tcp:send(Sock, term_to_binary(join)),
            worker_routine(Sock);
        {error, Error} ->
            io:format("Worker could not connect to coordinator ~w~n", [Error])
    end.
    
% Worker routine, up to now just a stub to handle messaging with the coordinator
worker_routine(Sock) ->
    case gen_tcp:recv(Sock, 0) of
        {ok, Msg} ->
            {job, {CallerPid, Counter, {Operation, Function, Args}, Data}} = binary_to_term(Msg),
            io:format("Worker received job: ~w~n", [[Operation, Function, Args, Counter, Data]]),
            Result = erlang:apply(functions, Operation, [Function, Args, Data]),
            case gen_tcp:send(Sock, term_to_binary({result, CallerPid, Counter, Result})) of
                ok ->
                    worker_routine(Sock);
                {error, Error} ->
                    io:fwrite("Error: ~w,~n...shutting down the worker", [Error]),
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
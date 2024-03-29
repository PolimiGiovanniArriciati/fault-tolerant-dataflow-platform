-module(worker).
-export([start/0, start/1, start/2, ping/2, try_execute_job/2]).
-importlib([functions]).

start() ->
    start("localhost", 8080).

start(Port) ->
    start("localhost", Port).

start(Host, Port) ->
    case gen_tcp:connect(Host, Port, [binary, {packet, 0}, {active, false}, {buffer, 4096}]) of
        {ok, Sock} ->
            io:format("Worker connected to coordinator~n"),
            ok = gen_tcp:send(Sock, term_to_binary(join)),
            _ = spawn_link(?MODULE, ping, [Sock, 3000]),
            worker_routine(Sock);
        {error, Error} ->
            io:format("Worker could not connect to coordinator ~w~n", [Error])
    end.
    
worker_routine(Sock) ->
    case gen_tcp:recv(Sock, 0) of
        {ok, Msg} ->
            try_execute_job(Sock, Msg),
            worker_routine(Sock);
        {error, closed} ->
            io:fwrite("Connection closed,~n...shutting down the worker"),
            halt();
        {error, Error} ->
            io:fwrite("Error: ~w,~n...shutting down the worker", [Error]),
            halt();
        Error ->
            io:format("Unexpected message: ~w~n", [Error]),
            halt()
    end.

ping(Sock, Interval) ->
    timer:sleep(Interval),
    case gen_tcp:send(Sock, term_to_binary(ping)) of
        ok ->
            ping(Sock, Interval);
        {error, Error} ->
            io:fwrite("Error: ~w,~n...shutting down the worker", [Error]),
            halt()
    end.

try_execute_job(Sock, Msg) ->
    try
        {job, {CallerPid, Counter, {Operation, Function, Args}, Data}} = binary_to_term(Msg),
        io:format("Worker received job: ~w~n", [[Operation, Function, Args, Counter, Data]]),
        Result = erlang:apply(functions, Operation, [Function, Args, Data]),
        case gen_tcp:send(Sock, term_to_binary({result, CallerPid, Counter, Result})) of
            {error, Error} ->
                io:fwrite("Error: ~w,~n...shutting down the worker", [Error]);
            _ -> ok         
        end
    catch
        error:X ->
            io:format("Worker could not execute job: ~w~n", [X]),
            io:format("Worker could not parse message: ~w~n", [Msg]),
            case gen_tcp:send(Sock, term_to_binary(worker_resend)) of
                {error, Error1} ->
                    io:fwrite("Error: ~w,~n...shutting down the worker", [Error1]);
                _ -> ok
            end;
        exit:X ->
            io:format("Worker could not execute job: ~w~n", [X]),
            io:format("Worker could not parse message: ~w~n", [Msg]),
            case gen_tcp:send(Sock, term_to_binary(worker_resend)) of
                {error, Error2} ->
                    io:fwrite("Error: ~w,~n...shutting down the worker", [Error2]);
                _ -> ok
            end
    end.
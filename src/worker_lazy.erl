-module(worker_lazy).
-export([start/0, start/1, start/2]).
-importlib([functions, worker]).

start() ->
    start("localhost", 8080).

start(Port) ->
    start("localhost", Port).

start(Host, Port) ->
    case gen_tcp:connect(Host, Port, [binary, {packet, 0}, {active, false}, {buffer, 4096}, {send_timeout, 10000}, {send_timeout_close, true}], 10000) of
        {ok, Sock} ->
            io:format("Worker connected to coordinator~n"),
            ok = gen_tcp:send(Sock, term_to_binary(join)),
            _ = spawn_link(worker, ping, [Sock, 3000]),
            worker_routine(Sock);
        {error, Error} ->
            io:format("Worker could not connect to coordinator ~w~n", [Error])
    end.
    
worker_routine(Sock) ->
    case gen_tcp:recv(Sock, 0) of
        {ok, Msg} ->
            timer:sleep(1000),
            worker:try_execute_job(Sock, Msg),
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
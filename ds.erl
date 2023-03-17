-module('ds').
-export([worker/0, worker/2, coordinator_start/0, coordinator_start/1, accept_connection/2, coordinator_listener/2, coordinator/2, dispach_work/2]).

% Starts coordinator with default harcoded port 8080
coordinator_start() ->
    coordinator_start(8080).

% Starts an accept socket with the given port 
coordinator_start(Port) ->
    {Outcome, AcceptSock} = gen_tcp:listen(Port, [binary, {packet, 0}, {active, false}]),
    if 
        Outcome == error -> 
            io:format("Unexpected error launching the coordinator "),
            io:format(AcceptSock);
        true -> 
            io:format("AcceptSocket generated, ready to listen for new workers~n"),
            % accepts incoming connections from the workers
            Pid = spawn(?MODULE, coordinator, [[], []]),
            accept_connection(Pid, AcceptSock)
            %spawn(?MODULE, accept_connection, [self(), AcceptSock])
    end.
    %coordinator([], []).

% Coordinator has parameter R : ready workers (a list) and B: busy
coordinator(R, B) ->
    io:format("Coordinator waiting for workers"),
    receive
        {S, join} -> 
            io:format("accepted a join "),
            L = [S | R]
    after 1500 ->
        L = R,
        io:format("Ready processes: "),
        io:fwrite("~w~n ",[length(L)]),
        if 
            length(L) == 0 ->
                coordinator(R, B), 
                halt();
            true -> 
                io:format("Coordinator - TODO")
            end
    end,
    Control = io:fread("~nCoordinator ready, do you want to start executing the tasks in the input file? [y/n]~n", "~s"),
    if 
        Control == 'y' ->
            %dispach_work(L, B);
            Pid = spawn(?MODULE, dispach_work, [L, B]),
            coordinator_loop(Pid),
            halt();
        true -> 
            coordinator(L, B), 
            halt()
    end.

coordinator_loop(Scheduler) ->
    receive 
        {_, join} ->
            io:format("Coord_loop - TODO")          
end,
coordinator_loop(Scheduler).

dispach_work(R, B) ->
    if  % Case no busy jobs
        B == [] ->
        io:format("Dispatcher - TODO");
            % TODO dispach the work and read the csv file 
        % Case at least one worker is on
        true ->
            receive 
                {Sock, result, R} ->
                    io:format(R),
                    io:format(Sock);
                    % TODO
                {Sock, error} ->
                    io:format(Sock);
                    % TODO
                {Sock, join} ->
                    io:format(Sock)
                    % TODO
            end
    end.
   

% Accept connection accepts requests from other sockets.
accept_connection(CoordPid, AcceptSock) ->
    {Outcome, Sock} = gen_tcp:accept(AcceptSock),
    if 
        Outcome == error -> 
            io:format("~nUnexpected error during socket accept~n"),
            io:format(Sock);
        true ->
            io:format("Coordinator accepted a new connection~n"),
            % Starts the specific worker Pid that waits to receive messages from the worker
            %coordinator_listener(CoordPid, Sock)
            spawn(?MODULE, coordinator_listener, [CoordPid, Sock])
    end,
    accept_connection(CoordPid, AcceptSock).
    

% Coordinator listener manages the communication with the worker
% Waits to receive messages from the host and 
coordinator_listener(CoordinatorPid, Sock) ->
    case gen_tcp:recv(Sock, 0) of
        {ok, <<"join">>} ->
            CoordinatorPid ! {Sock, join};
        {error, <<"closed">>} ->
            CoordinatorPid ! {Sock, closed}
    end,
    coordinator_listener(CoordinatorPid, Sock).

% Worker uses worker function with default host, ip
worker() ->
    worker("127.0.0.1", 8080).

% Worker starts a tcp connection to the coordinator
worker(Host, Port) ->
    {Outcome, Sock} = gen_tcp:connect(Host, Port, [binary, {packet, 0}, {active, false}]),
    if 
        Outcome == error -> 
            io:format("Unexpected error during socket accept~n"),
            io:format(Sock);
        Outcome == ok ->
            ok = gen_tcp:send(Sock, "join"),
            worker_routine(Sock)
    end.
    
worker_routine(Sock) ->
    case gen_tcp:recv(Sock, 0) of
        % TODO - the worker will receive messages and then do computation
        % Receives the commands, then compute and sends results, then wait for new task
        {ok, <<"">>} ->
            io:format("");
        {error, R} ->
            io:format("An error has occured, shutting down the worker ~n"),
            exit(R)
    end,
    worker_routine(Sock).

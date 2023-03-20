-module('ds').
-export([c/0, c1/0, w/0, w1/0, worker/0, worker/2, coordinator_start/0, coordinator_start/1, accept_connection/2, coordinator_listener/2, coordinator/1, dispatch_work/1, get_result/3]).

c() ->
    coordinator_start(8080).

c1() ->
    coordinator_start(8081).

w() ->
    worker().

w1() -> 
    worker("127.0.0.1", 8081).

% Starts coordinator with default harcoded port 8080
coordinator_start() ->
    coordinator_start(8080).

% Starts an accept socket with the given port 
coordinator_start(Port) ->
    case gen_tcp:listen(Port, [binary, {packet, 0}, {active, false}]) of
        {error, ErrorMessage} -> 
            io:format("Unexpected error launching the coordinator "),
            io:format(ErrorMessage);
        {ok, AcceptSock} -> 
            io:format("AcceptSocket generated, ready to listen for new workers~n"),
            % accepts incoming connections from the workers
            Pid = spawn(?MODULE, coordinator, [[]]),
            accept_connection(Pid, AcceptSock)
    end.

% Coordinator has parameter R : ready workers (a list) and B: busy
coordinator(ReadyW) ->
    io:format("Coordinator waiting for workers"),
    receive
        % Receives the socket of a new joining worker
        {NewWSock, join} -> 
            io:format("accepted a join ~n"),
            NewReadyW = [NewWSock | ReadyW]
    % After 2500 ms of no new joins, checks the number of workers
    after 2500 ->
        NewReadyW = ReadyW,
        io:format("~nReady processes: "),
        io:fwrite("~w~n",[length(NewReadyW)]),
        if 
            length(NewReadyW) == length(ReadyW) ->
                coordinator(ReadyW), 
                halt();
            true -> 
                io:format("Coordinator - TODO")
            end
    end,
    {ok, Control} = io:fread("Coordinator ready, do you want to start executing the tasks in the input file? [y/n]", "~s"),
    io:format(Control),    
    if 
        Control == ["y"] ->
            io:format("~n Starting the normal executuion ~n"),
            Pid = spawn(?MODULE, dispatch_work, [NewReadyW]),
            coordinator_loop(Pid),
            halt();
        true ->
            coordinator(NewReadyW),
            halt()
    end.

coordinator_loop(Scheduler) ->
    receive
        {Socket, join} ->
            Scheduler ! {Socket, join};
        {Socket, error} ->
            Scheduler ! {Socket, error}
end,
coordinator_loop(Scheduler).


% Send work takes the list of ready processes, the function to apply
% and sends to all the ready processes, a lists of inputs (Input)
% to be processed
% While in execution, it changes the mapping between busy workers (sockets) 
% and the submitted input to that node

% TODO - in this case is not taken into consideration the case of
% non ready processes for the data. During the scheduling, also, it might fail
% the last ready W, leaving no nodes R to take the input...
% In this case, send_work what should do?
% Notify the sending process was unsuccessful and give a non send input to be
% reschedule when the receive routine gets the result back?
send_work(_, _ ,[], BusyWMap) ->
    io:format("Send work empty"),
    BusyWMap;

send_work([ReadyW | RWList], Function, [Input | InList], BusyWMap) ->
    io:format("Send work full"),
    SendOutcome = gen_tcp:send(ReadyW, {work, Function, Input}),
    case SendOutcome of 
        ok -> 
            NewBWMap = send_work(RWList, Function, InList, BusyWMap),
            NewBWMap = maps:put(ReadyW,Input,BusyWMap);
        % In this case an error has occured
        % TODO: it is possible to use a function to reunite the inputs
        %       and divide them again to respect of the others Ready workers
        true -> 
            NewBWMap = send_work(RWList, Function, [Input | InList], BusyWMap)
    end,
    NewBWMap.

%dispatch_work(ReadyWorker, []) ->
%%    io:format("Dispatcher - TODO"),
 %   % TODO the parameter will depend on the function and input data
%    BusyWMap = send_work(ReadyWorker, [], [], #{}),
%    dispatch_work(ReadyWorker, BusyWMap);
%    % TODO dispach the work and read the csv file 

% Case at least one worker is on
dispatch_work(ReadyWorker) ->
    EmptyMap = #{},
    BusyWMap = send_work(ReadyWorker, [], [], EmptyMap),
    [NewReadyW, ResultMap] = get_result([], BusyWMap, EmptyMap),
    io:format([ResultMap]),
    dispatch_work(NewReadyW). 
    

get_result(ReadyW, #{}, ResultMap) ->
    [ReadyW, ResultMap];

get_result(ReadyW, BusyWMap, ResultsMap) ->
    receive 
        {Sock, result, Result} ->
            io:format(Result),
            io:format(Sock),
            NewResultMap = ResultsMap#{Sock => Result},
            NewBusyMap = maps:remove(Sock, BusyWMap),
            NewReadyW = [Sock | ReadyW];
            % TODO
        {Sock, error} ->
            NewBusyMap = maps:remove(Sock, BusyWMap),
            NewResultMap = ResultsMap,
            io:format(Sock),
            
            %TODO - reschedule the job
            NewReadyW = ReadyW
            %BusyWUpToNow = maps:remove(Sock, BusyWMap),
            %send_work(ReadyW, )
            ;
            % TODO
        {Sock, join} ->
            NewResultMap = ResultsMap,
            NewBusyMap = BusyWMap,
            NewReadyW = [Sock, ReadyW],
            io:format(Sock);
        _ ->
            io:write("Unrecognized message"),
            NewResultMap = ResultsMap,
            NewReadyW = ReadyW,
            NewBusyMap = BusyWMap
    end,
    get_result(NewReadyW, NewBusyMap, NewResultMap).
            % TODO

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
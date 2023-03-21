-module('ds').
-export([c/0, c1/0, w/0, w1/0, c2w/0, c1w/0, worker/0, worker/2, coordinator_start/0, coordinator_start/1, accept_connection/2, coordinator_listener/2, coordinator/1, dispatch_work/1, get_result/3]).
-importlib([pr]).

c() ->
    coordinator_start(8080).

c1() ->
    coordinator_start(8081).

w() ->
    worker().

w1() -> 
    worker("127.0.0.1", 8081).

c1w() ->
    spawn(?MODULE, coordinator_start, [8080]),
    spawn(?MODULE, worker, []).
c2w() ->
    spawn(?MODULE, coordinator_start, [8080]),
    spawn(?MODULE, worker, []),
    spawn(?MODULE, worker, []).


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
    % After 5000 ms of no new joins, prints the number of workers
    after 5000 ->
        NewReadyW = ReadyW,
        io:format("~nReady processes: "),
        io:fwrite("~w~n",[length(NewReadyW)])
    end,
    if
        % In case the ready processes are 0 or did not increased from the last request to start, waits for new 
        length(NewReadyW) == length(ReadyW) ->
            io:format("~nNo new workers have join~n"),
            coordinator(NewReadyW);
        true -> 
                {ok, Control} = io:fread("Coordinator ready, do you want to start executing the tasks in the input file? [y/n]", "~s"),
                io:format(Control),    
                if 
                    Control == ["y"] ->
                        io:format("~n Starting the normal executuion ~n"),
                        dispatch_work(NewReadyW);
                    true ->
                        coordinator(NewReadyW)
            end
    end.

% Send work takes the list of ready processes, the function to apply
% and sends to all the ready processes, a lists of inputs (Input)
% to be processed
% While in execution, it changes the mapping between busy workers (sockets) 
% and the submitted input to that node

% TODO - in this case is not taken into consideration the case of
% non ready processes for the data. During the scheduling, also, it might fail
% the last ready Worker,
% leaving no nodes ready to take the input...
% In this case, send_work what should do?
% Notify the sending process was unsuccessful and give a non send input to be
% reschedule when the receive routine gets the result back?
send_work(_, _ ,[], BusyWMap) ->
    io:format("Send work empty"),
    BusyWMap;


send_work([ReadyW | RWList], Function, [Input | InList], BusyWMap) ->
    io:format("Send work full~n"),
    io:fwrite("Input: ~w~n", [Input|InList]),
    io:fwrite("Ready processes: ~w~n", [ReadyW | RWList]),
    case gen_tcp:send(ReadyW, term_to_binary({work, Function, Input})) of 
        ok -> 
            NewBWMap = send_work(RWList, Function, InList, BusyWMap),
            NewBWMap = maps:put(ReadyW, Input, BusyWMap);
        % In this case an error has occured
        % TODO: it is possible to use a function to reunite the inputs
        %       and divide them again to respect of the others Ready workers
        _ ->
            NewBWMap = send_work(RWList, Function, [Input | InList], BusyWMap)
    end,
    NewBWMap.

% Divide job splits the map into cardinality |ReadyWorker| lists of tuples
% Final case, one worker left returns InputList
divide_jobs([_ | []], InputList, _) ->
    [InputList];
% Case more than one worker, create sublists
divide_jobs([_ | ListReadyW], InputList, Size) ->
        [[lists:sublist(InputList, 0, Size-1)] | divide_jobs(ListReadyW, lists:sublist(InputList, Size), Size)].

% Sends the work to the ready workers and waits for the results
dispatch_work(ReadyWorker) ->
    EmptyMap = #{},
    {ok, Op, Fun, _, InputList} = file_processing:get_input(),
    BusyWMap = send_work(ReadyWorker, {Op, Fun}, divide_jobs(ReadyWorker, InputList, ceil(length(InputList)/length(ReadyWorker))), EmptyMap),
    [NewReadyW, ResultMap] = get_result([], BusyWMap, EmptyMap, #{}),
    maps:values(ResultMap),
    io:format(maps:to_list(ResultMap)),
    dispatch_work(NewReadyW).
    
% Get results, given the ReadyWorkers, BusyWorkerMap and the actual ResultMap, 
% Returns in a list the updated ReadyWorkers and the updated ResultMap

% Case no more result to wait
get_result(ReadyW, #{}, ResultMap) ->
    [ReadyW, ResultMap].

% Case no Ready workers (waits to receive at least a worker)
get_result([], #{}, _, _) ->
    receive 
        {Sock, join} ->
            [[Sock], #{}]
    end;

% Normal case
get_result(ReadyW, BusyWMap, ResultsMap, SockOrderMap) ->
    receive 
        {Sock, result, Result} ->
            io:format(Result),
            io:format(Sock),
            #{Sock := Index} = SockOrderMap,
            NewResultMap = ResultsMap#{Index => Result},
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
        {error, _} ->
            CoordinatorPid ! {Sock, closed}
    end,
    coordinator_listener(CoordinatorPid, Sock).

% Worker uses worker function with default host, ip
worker() ->
    worker("127.0.0.1", 8080).

% Worker starts a tcp connection to the coordinator
% Worker function is for now just a stub to test the coordinator
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
    
% Worker routine, up to now just a stub to handle messaging with the coordinator
worker_routine(Sock) ->
    case gen_tcp:recv(Sock, 0) of
        % TODO - the worker will receive messages and then do computation
        % Receives the commands, then compute and sends results, then wait for new task
        {ok, EncodedMsg} ->
            {Type, {Operation, Function}, Input} = binary_to_term(EncodedMsg),
            io:fwrite("~w", [{Type, {Operation, Function}, Input}]);
        {error, R} ->
            io:format("An error has occured, shutting down the worker ~n"),
            exit(R)
    end,
    worker_routine(Sock).
-module('ds').
-export([c/0, c1/0, w/0, w1/0, c2w/0, c1w2/0, c1w/0, worker/0, worker/2, coordinator_start/0, coordinator_start/1, accept_connection/2, coordinator_listener/2, coordinator/1, dispatch_work/1, get_result/3]).
-importlib([pr]).

% DEBUG FUNCTIONS

% Starts a coordinator listening on port 8080
c() ->
    coordinator_start(8080).

% Starts a coordinator listening on port 8081
c1() ->
    coordinator_start(8081).

% Starts a worker connecting to "127.0.0.1", 8080
w() ->
    worker().

% Starts a worker connecting to "127.0.0.1", 8081
w1() -> 
    worker("127.0.0.1", 8081).

% Starts a coordinator and a worker, ip: "127.0.0.1" port: 8080
c1w() ->
    spawn(?MODULE, coordinator_start, [8080]),
    spawn(?MODULE, worker, []).

% Starts a coordinator and two workers, ip: "127.0.0.1" port: 8080
c2w() ->
    spawn(?MODULE, coordinator_start, [8080]),
    spawn(?MODULE, worker, []),
    spawn(?MODULE, worker, []).

% Starts a coordinator and two workers, ip: "127.0.0.1" port: 8081
c1w2() ->
    spawn(?MODULE, coordinator_start, [8081]),
    spawn(?MODULE, worker, ["127.0.0.1", 8081]),
    spawn(?MODULE, worker, ["127.0.0.1", 8081]).

% MAIN PROGRAM 

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
                        io:format("~n Starting the normal execution ~n"),
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
    io:format("Send work empty ~n~n"),
    BusyWMap;


send_work([ReadyW | RWList], Function, [Input | InList], BusyWMap) ->
    io:format("Send work full~n"),
    io:fwrite("Input: ~w~n", [Input]),
    io:fwrite("Remaining input: ~w~n~n", [InList]),

    io:fwrite("Ready process: ~w~n",  [ReadyW]),
    case gen_tcp:send(ReadyW, term_to_binary({work, Function, Input})) of 
        ok -> 
            io:fwrite("Send successfully message~n"),
            ReturnedBWMap = send_work(RWList, Function, InList, BusyWMap),
            NewBWMap = maps:put(ReadyW, Input, ReturnedBWMap);
        % In this case an error has occured
        % TODO: it is possible to use a function to reunite the inputs
        %       and divide them again to respect of the others Ready workers
        Msg ->
            io:fwrite("An error has occured sending the message ~w~n", Msg),
            NewBWMap = send_work(RWList, Function, [Input | InList], BusyWMap)
    end,
    NewBWMap.

% Divide job splits the map into cardinality |ReadyWorker| lists of tuples
% Final case, one worker left returns InputList
divide_jobs([_ | []], InputList, _) ->
    [InputList];
% Case more than one worker, create sublists
divide_jobs([_ | ListReadyW], InputList, Size) ->
        [lists:sublist(InputList, 1, Size) | divide_jobs(ListReadyW, lists:sublist(InputList, Size , 2*Size+1), Size)].

% Sends the work to the ready workers and waits for the results
dispatch_work(ReadyWorker) ->
    EmptyMap = #{},
    % Gets the input from file_processing
    {ok, Op, Fun, _, InputList} = file_processing:get_input(),
    io:fwrite("Length input lists: ~w~n",[lists:flatlength(InputList)]),
    io:fwrite("Length worker lists: ~w~n",[lists:flatlength(ReadyWorker)]),
    io:fwrite("Size per sublist: ~w~n",[ceil(lists:flatlength(InputList)/lists:flatlength(ReadyWorker))]),
    % Sends the work, with the input list subdivided by divide_jobs
    BusyWMap = send_work(ReadyWorker, {Op, Fun}, divide_jobs(ReadyWorker, InputList, ceil(lists:flatlength(InputList)/lists:flatlength(ReadyWorker))), EmptyMap),
    % Receives the ready workers for new work and the results from the get_result function 
    io:fwrite("Waiting for results, busy map: ~w~n", [maps:to_list(BusyWMap)]),
    [NewReadyW, ResultMap] = get_result([], BusyWMap, EmptyMap, #{}),
    maps:values(ResultMap),
    io:format(maps:to_list(ResultMap)),
    dispatch_work(NewReadyW).
    
% Get results, given the ReadyWorkers, BusyWorkerMap and the actual ResultMap, 
% Returns in a list the updated ReadyWorkers and the updated ResultMap

% Case no more result to wait
get_result(ReadyW, {}, ResultMap) ->
    [ReadyW, ResultMap].

% Case no Ready workers (waits to receive at least a worker)
get_result([], {}, Result, _) ->
    io:format("Waiting for new ready workers to join, since no work are scheduled and no ready workers are available ~n"),
    receive 
        {Sock, join} ->
            [[Sock], #{}]
    end,
    Result;

% Normal case
get_result(ReadyW, BusyWMap, ResultsMap, SockOrderMap) ->
    receive 
        {Sock, result, Result} ->
            io:fwrite("New results have arrived ~w ", [Result]),
            io:fwrite("from socket ~w~n", [Sock]),
            #{Sock := Index} = SockOrderMap,
            NewResultMap = ResultsMap#{Index => Result},
            NewBusyMap = maps:remove(Sock, BusyWMap),
            NewReadyW = [Sock | ReadyW];
            % TODO
        {Sock, error} ->
            io:fwrite("Error from socket ~w~n", [Sock]),
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
            io:fwrite("New process has join ~w~n", [Sock]),
            NewResultMap = ResultsMap,
            NewBusyMap = BusyWMap,
            NewReadyW = [Sock, ReadyW],
            io:format(Sock);
        _ ->
            io:fwrite("Unrecognized message~n"),
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
        {ok, Msg} ->
            case binary_to_term(Msg) of 
                join -> 
                    CoordinatorPid ! {Sock, join};
                {result, Result} ->
                    CoordinatorPid ! {Sock, result, Result}
            end;
        {error, _} ->
            CoordinatorPid ! {Sock, error}
            
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
            ok = gen_tcp:send(Sock, term_to_binary(join)),
            worker_routine(Sock)
    end.
    
% Worker routine, up to now just a stub to handle messaging with the coordinator
worker_routine(Sock) ->
    case gen_tcp:recv(Sock, 0) of
        % TODO - the worker will receive messages and then do computation
        % Receives the commands, then compute and sends results, then wait for new task
        {ok, EncodedMsg} ->
            {Type, {Operation, Function}, Input} = binary_to_term(EncodedMsg),
            io:fwrite("Received: ~w~n", [{Type, {Operation, Function}, Input}]),
            % DUMMY WORKER - just sends back the input it has received
            io:fwrite("Sending the results ~w~n", [Input]),
            ok = gen_tcp:send(Sock, term_to_binary({result, Input}));
        {error, _} ->
            io:format("An error has occured, shutting down the worker ~n"),
            halt()
    end,
    worker_routine(Sock).
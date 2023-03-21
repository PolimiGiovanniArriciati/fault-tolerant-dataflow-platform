-module('coordinator').
-export([start/0, start/1, accept_connection/2, coordinator_listener/2, coordinator/1, dispatch_work/1, get_result/3]).
-importlib([file_processing, partition]).

start() ->
    start(8080).

% Starts an accept socket with the given port 
start(Port) ->
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

% Sends the work to the ready workers and waits for the results
dispatch_work(Workers) ->
    % Gets the input from file_processing
    {ok, Op, Fun, Args, InputList} = file_processing:get_input(),
    Work_force = lists:flatlength(Workers),
    Inputs = partition:partition(InputList, Work_force),
    % TODO: add a Blocknumber to the input to be able to identify the block
    % here should loop over the inputs and send the work to the workers
    BusyWMap = send_work(Workers, {Op, Fun, Args},  Inputs, #{}),
    % Receives the ready workers for new work and the results from the get_result function 
    io:fwrite("Waiting for results, busy map: ~w~n", [maps:to_list(BusyWMap)]),
    [NewReadyW, ResultMap] = get_result([], BusyWMap, #{}, #{}),
    maps:values(ResultMap),
    io:format(maps:to_list(ResultMap)),
    dispatch_work(NewReadyW).

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
send_work([], _ ,[], BusyWMap) ->
    io:format("Send work empty ~n~n"),
    BusyWMap;

send_work([], _, Input, BusyWMap) ->
    {error, {no_ready_workers, Input}, BusyWMap};

send_work([ReadyW | RWList], Function, [Input | InList], BusyWMap) ->
    case gen_tcp:send(ReadyW, term_to_binary({work, Function, Input})) of 
        ok -> 
            io:fwrite("Send successfully message~n"),
            send_work(RWList, Function, InList, maps:put(ReadyW, Input, BusyWMap));
        {error, Error} ->
            io:fwrite("An error has occured sending the message: ~w~n", Error),
            send_work(RWList, Function, [Input | InList], BusyWMap)
    end.
    
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
        %FIXME: Sock is ignored and lost
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

% Accept connection accepts requests from other sockets.
accept_connection(CoordPid, AcceptSock) ->
    case gen_tcp:accept(AcceptSock) of
        {error, Error} ->
            io:format("~nUnexpected error during socket accept~n"),
            io:format(Error);
        {ok, Sock} ->
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
                % TODO: maybe differenciate the cases of join and result, with different actors
                join -> 
                    io:format("New worker has joined~n"),
                    CoordinatorPid ! {Sock, join};
                {result, Result} ->
                    io:format("Result received ~w~n", [Result]),
                    CoordinatorPid ! {Sock, result, Result}
            end,
            coordinator_listener(CoordinatorPid, Sock);
        {error, Error} ->
            io:format("Error in coordinator listener ~w~n", [Error]),
            CoordinatorPid ! {Sock, error}
    end.
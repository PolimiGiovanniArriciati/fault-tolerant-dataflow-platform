-module('coordinator').
-export([start/0, start/1]).
%Functions for internal use not called directly
-export([dispatch_work/4, get_results/3, accept_connection/2, socket_listener/2, jobs_queue/3, coordinator/1]).
-importlib([file_processing, partition]).
-define(NAME, string:chomp(io:get_line("Input file to process: "))).
-define(LOG(STRING), io:format("_LOG_ " ++ STRING)).
-define(LOG(STRING, ARGS), io:format("_LOG_ " ++ STRING, ARGS)).

start() ->
    start(8080).

% Starts an accept socket with the given port
start(Port) ->
    case gen_tcp:listen(Port, [binary, {packet, 0}, {active, false}, {buffer, 16384}]) of
        {ok, AcceptSock} -> 
            ?LOG("AcceptSocket generated, ready to listen for new workers on port ~p~n", [Port]),
            CoordinatorPid = spawn(?MODULE, coordinator, [[]]),
            accept_connection(CoordinatorPid, AcceptSock);
        {error, eaddrinuse} ->
            ?LOG("Port ~p already in use trying next one~n", [Port]),
            start(Port + 1);
        {error, ErrorMessage} -> 
            ?LOG("Unexpected error launching the coordinator: ~p~n", [ErrorMessage])
    end.

accept_connection(CoordinatorPid, AcceptSock) ->
    case gen_tcp:accept(AcceptSock) of
        {ok, Sock} ->
            ?LOG("Coordinator accepted a new connection~n"),
            spawn(?MODULE, socket_listener, [CoordinatorPid, Sock]);
        {error, Error} ->
            ?LOG("Unexpected error during socket accept!! ~w~n", Error)
    end,
    accept_connection(CoordinatorPid, AcceptSock).

% Coordinator has parameter R : ready workers (a list) and B: busy
coordinator(Workers) ->
    ?LOG("Coordinator waiting for workers"),
    receive
        % Receives the socket of a new joining worker
        {join, Worker} -> 
            Workers1 = [Worker | Workers]
    after 3000 ->
            ?LOG("no workers joined yet~n"),
            Workers1 = Workers
    end,
    if  Workers1 == [] ->
            ?LOG("No workers available, waiting for new workers to join~n"),
            coordinator(Workers1);
        true ->
            case io:fread("Coordinator ready, do you want to start executing the tasks in the input file? [y/n] [e:exit]", "~s") of
                {ok, ["y"]} ->
                    Workers2 = start_work(Workers1);
                {ok, ["n"]} -> Workers2 = Workers1;
                {ok, ["e"]} ->
                    lists:foreach(fun(Worker) -> gen_tcp:close(Worker) end, Workers1),
                    io:format("Program ended~n"),
                    Workers2 = Workers1,
                    halt();
                {ok, _} ->
                    ?LOG("Invalid input, please type 'y' or 'n'~n"),
                    Workers2 = Workers1
            end,
            coordinator(Workers2)
    end.

start_work(Workers) ->
    OpFile = get_in("Input operations to execute: "),
    DataFile = get_in("Input file to process: "),
    start_work(Workers, OpFile, DataFile).
    
start_work(Workers, OpFile, DataFile) ->
    {ok, _, Npartitions, Ops} = file_processing:get_operations(OpFile),
    {ok, DataFileName, InputList} = file_processing:get_data(DataFile),
    Inputs = partition:partition(InputList, Npartitions),
    ?LOG("Input: ~p~n", [Inputs]),
    CollectorPid = spawn(?MODULE, get_results, [self(), #{}, length(Inputs)]),
    [spawn(?MODULE, dispatch_work, [Ops, Data, self(), CollectorPid]) || Data <- lists:zip(lists:seq(1, length(Inputs)), Inputs)],
    jobs_queue(Workers, [], DataFileName).

-spec dispatch_work(Ops, Input, CoordinatorPid, CollectorPid) -> ok when
    Ops :: [{Op, Function, integer()}],
    Input :: {PartitionNumber, Data},
    CoordinatorPid :: pid(),
    CollectorPid :: pid(),
    Op :: map | reduce | changeKey,
    Function :: atom(),
    PartitionNumber :: integer(),
    Data :: [{integer(), integer()}].

dispatch_work([], Output, _, CollectorPid) ->
    CollectorPid ! {end_dataflow_partition, Output};

dispatch_work([{reduce, Function, Arg} | Ops], {_, Data}, CoordinatorPid, CollectorPid) ->
    ?LOG("Dispatching work to the queue~p~n", [reduce]),
    CollectorPid ! {reduce_prep, Data, self()},
    receive
        {reduce, [], _} -> not_enough_keys;
        {reduce, ResultToReduce, PartitionNumber} ->
            CoordinatorPid ! {job, {self(), {reduce, Function, Arg}, ResultToReduce}},
            receive_work({reduce, Function, Arg}, Ops, {PartitionNumber, Data}, CoordinatorPid, CollectorPid)
    end;

dispatch_work([Op | Ops], {PartitionNumber, Data}, CoordinatorPid, CollectorPid) ->
    ?LOG("Dispatching work to the queue~s~n", [io_lib:format("~p", [Op])]),
    CoordinatorPid ! {job, {self(), Op, Data}},
    receive_work(Op, Ops, {PartitionNumber, Data}, CoordinatorPid, CollectorPid).

receive_work(Op, Ops, {PartitionNumber, Data}, CoordinatorPid, CollectorPid) ->
    receive
        {result, Result} ->
            ?LOG("Received result from worker: ~p~n", [Result]),
            dispatch_work(Ops, {PartitionNumber, Result}, CoordinatorPid, CollectorPid);
        {error, Error} ->
            ?LOG("Error in coordinator listener ~w~n", [Error]),
            CoordinatorPid ! {job, {self(), Op, Data}},
            receive_work(Op, Ops, {PartitionNumber, Data}, CoordinatorPid, CollectorPid)
    after 3000 ->
        self() ! {error, "Timeout in coordinator listener"},
        CoordinatorPid ! {job, {self(), Op, Data}},
        receive_work(Op, Ops, {PartitionNumber, Data}, CoordinatorPid, CollectorPid)
    end.

-spec jobs_queue(Workers, DispatchersJobs, FileName) -> Workers when
    Workers :: [pid()],
    DispatchersJobs :: [{pid(), Op, Data}],
    FileName :: string(),
    Op :: {map | reduce | changeKey, atom(), integer()},
    Data :: [{integer(), integer()}].

jobs_queue([Worker | Workers], [Job | Jobs], FileName) ->
        gen_tcp:send(Worker, term_to_binary({job, Job})),
        jobs_queue(Workers, Jobs, FileName);

jobs_queue(Workers, DispatchersJobs, FileName) ->
    ?LOG("Waiting for a job to be done~n"),
    ?LOG("Workers, Jobs: ~p~n", [[Workers, DispatchersJobs]]),
    receive
        {job, Job1} ->
            case lists:member(Job1, DispatchersJobs) of
                false -> jobs_queue(Workers, DispatchersJobs ++ [Job1], FileName); %appends the job, if not already in the queue, maybe efficented with the use of a set?
                true -> jobs_queue(Workers, DispatchersJobs, FileName) end; 
        {join, NewWorker} ->
            jobs_queue([NewWorker | Workers], DispatchersJobs, FileName);
        {done_work, Output} ->
            file_processing:save_data(FileName, Output),
            Workers;
        {error, CrushedWorker, Error} ->
            ?LOG("Error in coordinator listener ~w~n", [Error]),
            Workers1 = lists:delete(CrushedWorker, Workers),
            jobs_queue(Workers1, DispatchersJobs, FileName)
    end.

-spec get_results(CoordinatorPid, OutputMap, N) -> ok when
    CoordinatorPid :: pid(),
    OutputMap :: #{integer() => ListKV},
    ListKV :: [{integer(), integer()}],
    N :: integer().

get_results(CoordinatorPid, OutputMap, 0) ->
    ?LOG("All results received, sending to the coordinator~n"),
    Output = lists:flatten(maps:values(OutputMap)),
    %?LOG("Output: ~p~n", [Output]),
    CoordinatorPid ! {done_work, Output};

get_results(CoordinatorPid, OutputMap, NPartitions) ->
    receive
        {end_dataflow_partition, {PartitionNumber, Output}} ->
            ?LOG("Received result from partition ~w~n", [PartitionNumber]),
            ?LOG("OutputMap: ~w~n", [OutputMap]),
            OutputMap1 = maps:put(PartitionNumber, Output, OutputMap),
            get_results(CoordinatorPid, OutputMap1, NPartitions-1);
        {reduce_prep, Data, DispatcherId} ->
            NewNPartitions = prepare_reduce_input([DispatcherId], Data, NPartitions, 1),
            ?LOG("Number of partitions: ~p became ~p ~n", [NPartitions, NewNPartitions]),
            if NPartitions =/= NewNPartitions -> ?LOG("Changed number of partitions: ~p became ~p ~n", [NPartitions, NewNPartitions]); true -> ok end,
            get_results(CoordinatorPid, #{}, NewNPartitions)
    end.

-spec prepare_reduce_input(DispatchersIds, Data, NPartitions, NReceived) -> NPartitions when
    DispatchersIds :: [pid()],
    Data :: [{integer(), integer()}],
    NPartitions :: integer(),
    NReceived :: integer().

prepare_reduce_input(DispatcherIds, Datas, NPartitions, NReceived) when NPartitions =/= NReceived ->
    receive
        {reduce_prep, Data, DispatcherId} ->
            prepare_reduce_input([DispatcherId | DispatcherIds], Data ++ Datas, NPartitions, NReceived + 1)
    end;

prepare_reduce_input(DispatchersIds, Data, NPartitions, NReceived) when NPartitions == NReceived -> 
    % Reduce all the partitions into a list {Key, ListOfValues} and then re-partition it
    % It's needed because the keys may be distributed in more partition
    MapReduce = lists:foldl(
                fun({Key, Value}, Acc) ->
                    case maps:is_key(Key, Acc) of
                        true ->
                            maps:update_with(Key, fun(V) -> [Value | V] end, Acc);
                        false ->
                            maps:put(Key, [Value], Acc)
                    end
                end,
                #{},
                Data),
    ListReduce = maps:to_list(MapReduce),
    ?LOG("ListReduce: ~w~n", [ListReduce]),
    NKeys = erlang:length(ListReduce),
    if NKeys < NPartitions ->
        NewPartitionedList = partition:partition(ListReduce, NKeys) ++ lists:duplicate(NPartitions - NKeys, []),
        NewNPartitions = NKeys;
    true ->
        NewPartitionedList = partition:partition(ListReduce, NPartitions),
        NewNPartitions = NPartitions
    end,
    % Sends each reduced partition to a dispatcher ({{PartitionNumber, Data}, Dispatcher})
    PartitionDataDispatcherList = lists:zip3(lists:seq(1, NPartitions), NewPartitionedList, DispatchersIds),
    ?LOG("PartitionDataDispatcher: ~w~n", [PartitionDataDispatcherList]),
    [DispatcherId ! {reduce, DataToSend, PartitionNumber} || {PartitionNumber, DataToSend, DispatcherId} <- PartitionDataDispatcherList],
    % The number of partitions can change if the number of keys is less than the number of partitions
    NewNPartitions.

% Manages the communication with the worker
% A process for each worker 
% waits to receive messages and passes them
% to the dispatcher
socket_listener(CoordinatorPid, Sock) ->
    case gen_tcp:recv(Sock, 0) of
        {ok, Msg} ->
            case binary_to_term(Msg) of 
                join -> 
                    ?LOG("New worker has joined~n"),
                    CoordinatorPid ! {join, Sock};
                {result, DispatcherId, Result} ->
                    ?LOG("Received result for dispatcher ~w result: ~w~n", [DispatcherId, Result]),
                    DispatcherId ! {result, Result},
                    CoordinatorPid ! {join, Sock};
                    % Updates the coordinator about the worker that has finished the job
                Error ->
                    ?LOG("Error in socket listener, unexpected message:~n~w~n", [Error]),
                    CoordinatorPid ! {error, Sock, Error}
            end,
            socket_listener(CoordinatorPid, Sock);
        {error, Error} ->
            ?LOG("Error in coordinator listener ~w~n", [Error]),
            ?LOG("Closing the socket~n"),
            CoordinatorPid ! {error, Sock, Error};
        % Timeout may happen!
        Else ->
            ?LOG("Error in coordinator listener ~w~n", [Else]),
            ?LOG("Closing the socket~n"),
            CoordinatorPid ! {error, Sock, Else}
    end.

get_in(String) ->
    {ok, In} = io:fread(String, "~s"),
    In.
-module('coordinator').
-export([start/0, start/1]).
%Functions for internal use not called directly
-export([dispatch_work/5, get_results/3, accept_connection/2, socket_listener/2, jobs_queue/4, coordinator/1]).
-importlib([file_processing, partition]).
-define(NAME, string:chomp(io:get_line("Input file to process: "))).
-define(LOG(STRING), io:format("__LOG__ " ++ STRING)).
-define(LOG(STRING, ARGS), io:format("__LOG__ function: ~p; line: ~p " ++ STRING, [?FUNCTION_NAME, ?LINE] ++ ARGS)).
-define(DEBUG(STRING, ARGS), ok).
                            %io:format("_DEBUG_ function: ~p; line: ~p " ++ STRING, [?FUNCTION_NAME, ?LINE] ++ ARGS)).

start() ->
    start(8080).

% Starts an accept socket with the given port
start(Port) ->
    case gen_tcp:listen(Port, [binary, {packet, 0}, {active, false}, {buffer, 16384}]) of
        {ok, AcceptSock} -> 
            ?LOG("AcceptSocket generated, ready to listen for new workers on port ~p~n", [Port]),
            spawn(?MODULE, accept_connection, [self(), AcceptSock]),
            coordinator([]);
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
            timer:sleep(1000),
            ?LOG("Unexpected error during socket accept!! ~w~n", [Error])
    end,
    accept_connection(CoordinatorPid, AcceptSock).

% Coordinator has parameter R : ready workers (a list) and B: busy
coordinator(Workers) ->
    ?DEBUG("Coordinator waiting for workers ~p~n", [Workers]),
    receive
        {join, Worker} -> 
            Workers1 = [Worker | Workers];
        {start_work, OpFile, DataFile} ->
            Workers1 = start_work(Workers, OpFile, DataFile)
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
    [spawn(?MODULE, dispatch_work, [Ops, Data, self(), CollectorPid, 0]) || Data <- lists:zip(lists:seq(1, length(Inputs)), Inputs)],
    jobs_queue(Workers, [], DataFileName, #{}).

-spec dispatch_work(Ops, Input, CoordinatorPid, CollectorPid, Counter) -> ok when
    Ops :: [{Op, Function, integer()}],
    Input :: {PartitionNumber, Data},
    CoordinatorPid :: pid(),
    CollectorPid :: pid(),
    Op :: map | reduce | changeKey,
    Function :: atom(),
    PartitionNumber :: integer(),
    Data :: [{integer(), integer()}],
    Counter :: integer().

dispatch_work([], Output, _, CollectorPid, _) ->
    CollectorPid ! {end_dataflow_partition, Output};

dispatch_work([{reduce, Function, Arg} | Ops], {_, Data}, CoordinatorPid, CollectorPid, OperationCounter) ->
    ?LOG("Dispatching work to the queue: ~p~n", [reduce]),
    CollectorPid ! {reduce_prep, Data, self()},
    receive
        {reduce, [], _} -> not_enough_keys;
        {reduce, ResultToReduce, PartitionNumber} ->
            CoordinatorPid ! {job, {self(), OperationCounter, {reduce, Function, Arg}, ResultToReduce}},
            receive_work({reduce, Function, Arg}, Ops, {PartitionNumber, Data}, CoordinatorPid, CollectorPid, OperationCounter);
        OtherMessage ->
            ?LOG("RECEIVE_WORK OtherMessage 1 - Unexpected message in coordinator listener ~w~n", [OtherMessage])
    end;

dispatch_work([Op | Ops], {PartitionNumber, Data}, CoordinatorPid, CollectorPid, OperationCounter) ->
    ?LOG("Dispatching work to the queue~s~n", [io_lib:format("~p", [Op])]),
    CoordinatorPid ! {job, {self(), OperationCounter, Op, Data}},
    receive_work(Op, Ops, {PartitionNumber, Data}, CoordinatorPid, CollectorPid, OperationCounter).

receive_work(Op, Ops, {PartitionNumber, Data}, CoordinatorPid, CollectorPid, OperationCounter) ->
    receive
        {result, ResOperationCounter, Result} when OperationCounter == ResOperationCounter->
            ?DEBUG("RECEIVE_WORK {result, Result}:~w~n", [Result]),
            dispatch_work(Ops, {PartitionNumber, Result}, CoordinatorPid, CollectorPid, OperationCounter+1);
        {error, Error} ->
            ?DEBUG("RECEIVE_WORK {error, Error} Error in coordinator listener ~w~n", [Error]),
            CoordinatorPid ! {job, {self(), OperationCounter, Op, Data}}, % Send with high priority this task to remaining workers
            receive_work(Op, Ops, {PartitionNumber, Data}, CoordinatorPid, CollectorPid, OperationCounter);
        OtherMsg ->
            ?LOG("RECEIVE_WORK Other. Unexpected message in coordinator listener ~w~n", [OtherMsg]),
            receive_work(Op, Ops, {PartitionNumber, Data}, CoordinatorPid, CollectorPid, OperationCounter)
    end.

-spec jobs_queue(Workers, DispatchersJobs, FileName, BusyMap) -> ok when
    BusyMap :: #{gen_tcp:socket() => {pid(), Op, Data}},
    Workers :: [pid()],
    DispatchersJobs :: [{pid(), Op, Data}],
    FileName :: string(),
    Op :: {map | reduce | changeKey, atom(), integer()},
    Data :: [{integer(), integer()}].

jobs_queue([Worker | Workers], [Job | Jobs], FileName, BusyMap) ->
    case gen_tcp:send(Worker, term_to_binary({job, Job})) of
        ok ->
            ?LOG("Sent job to worker: ~p~n", [{job, Job}]),
            jobs_queue(Workers, Jobs, FileName, BusyMap#{Worker => Job});
        {error, enotconn} ->
            ?LOG("Error removed ~w from workers because of error enotconn~n", [Worker]),
            jobs_queue(Workers, [Job | Jobs], FileName, BusyMap);
        Other ->
            ?LOG("JOBS QUEUE 1 -Unexpected message waiting in jobs_queue ~w~n", [Other]),
            jobs_queue(Workers, [Job | Jobs], FileName, BusyMap)
    end;

jobs_queue(Workers, DispatchersJobs, FileName, BusyMap) ->
    receive
        {job, Job1} ->
            ?LOG("Received job from coordinator: ~p~n", [Job1]),
            case not lists:member(Job1, DispatchersJobs) andalso not lists:member(Job1, maps:values(BusyMap)) of
                true -> jobs_queue(Workers, DispatchersJobs ++ [Job1], FileName, BusyMap);
                false -> jobs_queue(Workers, DispatchersJobs, FileName, BusyMap) end;
        {join, NewWorker} ->
            jobs_queue([NewWorker | Workers], DispatchersJobs, FileName, maps:remove(NewWorker, BusyMap));
        {done_work, Output} ->
            file_processing:save_data(FileName, Output),
            Workers;
        {worker_resend, Worker} ->
            case maps:find(Worker, BusyMap) of
                {ok, OldJob}  ->
                    ?LOG("Reschedule old job ~w for redispatch work: ~n", [OldJob]),
                    jobs_queue([Worker | Workers], [OldJob] ++ DispatchersJobs, FileName, maps:remove(Worker, BusyMap));
                _ ->
                    jobs_queue([Worker | Workers], DispatchersJobs, FileName, BusyMap)
            end;
        {error, CrushedWorker, Error} ->
            ?LOG("JOBS QUEUE - Error in coordinator listener ~w~n", [Error]),
            Workers1 = lists:delete(CrushedWorker, Workers),
            case maps:find(CrushedWorker, BusyMap) of
                {ok, OldJob}  ->
                    ?LOG("Reschedule old job ~w for redispatch work: ~n", [OldJob]),
                    jobs_queue(Workers1, [OldJob] ++ DispatchersJobs, FileName, maps:remove(CrushedWorker, BusyMap));
                _ ->
                    jobs_queue(Workers1, DispatchersJobs, FileName, BusyMap)
            end
    end.

-spec get_results(CoordinatorPid, OutputMap, N) -> ok when
    CoordinatorPid :: pid(),
    OutputMap :: #{integer() => ListKV},
    ListKV :: [{integer(), integer()}],
    N :: integer().

get_results(CoordinatorPid, OutputMap, 0) ->
    ?LOG("All results received, sending to the coordinator~n"),
    Output = lists:flatten(maps:values(OutputMap)),
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
            if NPartitions =/= NewNPartitions -> ?DEBUG("Changed number of partitions: ~p became ~p ~n", [NPartitions, NewNPartitions]); true -> ok end,
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
    % It's needed because the keys may be distributed across more partition
    ?LOG("Ready to transform input ~w for reduce~n", [Data]),
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
    NewNPartitions = erlang:min(NKeys, NPartitions),
    NewPartitionedList = partition:partition(ListReduce, NPartitions) ++
        if  NKeys < NPartitions -> lists:duplicate(NPartitions - NKeys, []);
            true -> [] end,
    % Sends each reduced partition to a dispatcher ({PartitionNumber, Data, Dispatcher})
    PartitionDataDispatcherList = lists:zip3(lists:seq(1, NPartitions), NewPartitionedList, DispatchersIds),
    lists:foreach(fun({PartitionNumber, DataToSend, DispatcherId}) -> DispatcherId ! {reduce, DataToSend, PartitionNumber} end, PartitionDataDispatcherList),
    NewNPartitions.

% Manages the communication with the worker
% A process for each worker 
% waits to receive messages and passes them
% to the dispatcher
socket_listener(CoordinatorPid, Sock) ->
    case gen_tcp:recv(Sock, 0, 5000) of
        {ok, Msg} ->
            case binary_to_term(Msg) of 
                join -> 
                    ?LOG("New worker has joined~n"),
                    CoordinatorPid ! {join, Sock};
                ping -> ?DEBUG("Ping received from socket ~p~n", [Sock]);
                {result, DispatcherId, Counter, Result} ->
                    ?DEBUG("Received result for dispatcher ~w with counter: ~w and result: ~w~n", [DispatcherId, Counter, Result]),
                    CoordinatorPid ! {join, Sock},
                    DispatcherId   ! {result, Counter, Result};
                worker_resend ->
                    ?DEBUG("Received job resend from socket ~p~n", [Sock]),
                    CoordinatorPid ! {worker_resend, Sock};
                Error ->
                    ?DEBUG("Error in socket listener, unexpected message: ~w~n", [Error]),
                    CoordinatorPid ! {error, Sock, Error},
                    halt()
                end,
            socket_listener(CoordinatorPid, Sock);
        Else ->
            ?LOG("Socket listener: Error in coordinator listener ~w~n", [Else]),
            ?LOG("Closing the socket~n"),
            CoordinatorPid ! {error, Sock, Else}
    end.

get_in(String) ->
    {ok, In} = io:fread(String, "~s"),
    In.
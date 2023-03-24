-module('coordinator').
-export([start/0, start/1]).
%Functions for internal use not called directly
-export([dispatch_work/3, get_results/3, accept_connection/2, socket_listener/2, jobs_queue/4, coordinator/2]).
-importlib([file_processing, partition]).
-define(NAME, string:chomp(io:get_line("Input file to process: "))).
-define(LOG(STRING), io:format("" ++ STRING ++ "~n")).
-define(LOG(STRING, ARGS), io:format("" ++ STRING ++ "~n", ARGS)).

start() ->
    start(8080).

% Starts an accept socket with the given port
start(Port) ->
    case gen_tcp:listen(Port, [binary, {packet, 0}, {active, false}]) of
        {ok, AcceptSock} -> 
            ?LOG("AcceptSocket generated, ready to listen for new workers on port ~p~n", [Port]),
            CoordinatorPid = spawn(?MODULE, coordinator, [[],[]]),
            accept_connection(CoordinatorPid, AcceptSock);
        {error, eaddrinuse} ->
            ?LOG("Port ~p already in use trying next one~n", [Port]),
            start(Port + 1);
        {error, ErrorMessage} -> 
            ?LOG("Unexpected error launching the coordinator: ~p~n", [ErrorMessage])
    end.

accept_connection(Coordinator_id, AcceptSock) ->
    case gen_tcp:accept(AcceptSock) of
        {ok, Sock} ->
            ?LOG("Coordinator accepted a new connection~n"),
            spawn(?MODULE, socket_listener, [Coordinator_id, Sock]),
            accept_connection(Coordinator_id, AcceptSock);
        {error, Error} ->
            ?LOG("Unexpected error during socket accept!! ~w~n", Error),
            accept_connection(Coordinator_id, AcceptSock)
    end.

% Coordinator has parameter R : ready workers (a list) and B: busy
coordinator(Workers, JobsInProgress) ->
    ?LOG("Coordinator waiting for workers"),
    receive
        % Receives the socket of a new joining worker
        {join, Worker} -> 
            Workers1 = [Worker | Workers]
    after 3000 ->
            ?LOG("no workers joined yet~n"),
            Workers1 = Workers
    end,
    if Workers1 == [] ->
            ?LOG("No workers available, waiting for new workers to join~n"),
            coordinator(Workers1, JobsInProgress);
        true ->
            case io:fread("Coordinator ready, do you want to start executing the tasks in the input file? [y/n]", "~s") of
                {ok, ["y"]} ->
                    start_work(Workers1);
                {ok, ["n"]} ->
                    coordinator(Workers1, JobsInProgress);
                {ok, _} ->
                    ?LOG("Invalid input, please type 'y' or 'n'~n")
            end,
            coordinator(Workers1, [])
    end.

start_work(Workers) ->
    Operations = get_in("Input operations to execute: "),
    {Outcome1, Npartitions, Ops} = file_processing:get_operations("in/" ++ Operations),
    FileName =get_in("Input file to process: "),
    {Outcome2, InputList}        = file_processing:get_data("in/"++FileName),
    if  {Outcome1, Outcome2} =:= {ok, ok} ->
            Inputs = partition:partition(InputList, Npartitions),
            [spawn(?MODULE, dispatch_work, [Ops, Data, self()]) || Data <- lists:zip(lists:seq(1, Npartitions), Inputs)],
            ResultCollectorPid = spawn(?MODULE, get_results, [self(), #{}, Npartitions]),
            jobs_queue(Workers, [], ResultCollectorPid, FileName);
        true ->
            io:fwrite("An error has occured with the input and operation files, try again ~n"),
            start_work(Workers)
    end.

-spec dispatch_work(Ops, Data, CoordinatorPid) -> ok when
    Ops :: [{Op, Function, integer()}],
    Data :: {PartitionNumber, Data},
    CoordinatorPid :: pid(),
    Op :: map | reduce | changeKey,
    Function :: atom(),
    PartitionNumber :: integer(),
    Data :: term().

dispatch_work([], Output, CoordinatorPid) ->
    CoordinatorPid ! {endDataflowPartition, Output};

dispatch_work([{reduce, Function, Arg} | Ops], {_, Data}, QueuePid) ->
    ?LOG("~nDispatching work to the queue~n~p", [reduce]),
    ?LOG("~nData: ~w~n", [Data]),
    QueuePid ! {reduce_prep, Data, self()},
    receive
        {reduce, []} -> not_enough_keys;
        {reduce, ResultToReduce, PartitionNumber} ->
            QueuePid ! {job, {self(), {reduce, Function, Arg}, ResultToReduce}},
            receive_work({reduce, Function, Arg}, Ops, {PartitionNumber, Data}, QueuePid)
    end;

dispatch_work([Op | Ops], {PartitionNumber, Data}, QueuePid) ->
    ?LOG("Dispatching work to the queue~n~s", [io_lib:format("~p", [Op])]),
    QueuePid ! {job, {self(), Op, Data}},
    receive_work(Op, Ops, {PartitionNumber, Data}, QueuePid).

receive_work(Op, Ops, {PartitionNumber, Data}, QueuePid) ->
    receive
        {result, Result} ->
            dispatch_work(Ops, {PartitionNumber, Result}, QueuePid);
        {error, Error} ->
            ?LOG("Error in coordinator listener ~w~n", [Error]),
            dispatch_work([Op | Ops], {PartitionNumber, Data}, QueuePid)
    after 3000 ->
        self() ! {error, "Timeout in coordinator listener"}
    end.

jobs_queue(Workers, DispatchersJobs, ResultCollectorPid, FileName)
    when Workers =/= [] andalso DispatchersJobs =/= [] ->
        [Job | Jobs] = DispatchersJobs,
        [Worker | Ls] = Workers,
        gen_tcp:send(Worker, term_to_binary({job, Job})),
        jobs_queue(Ls, Jobs, ResultCollectorPid, FileName);

jobs_queue(Workers, DispatchersJobs, ResultCollectorPid, FileName) ->
    receive
        {job, Job1} ->
            jobs_queue(Workers, DispatchersJobs ++ [Job1], ResultCollectorPid, FileName); %appends the job
        {join, NewWorker} ->
            jobs_queue([NewWorker | Workers], DispatchersJobs, ResultCollectorPid, FileName);
        {reduce_prep, Data, Pid} ->
            ResultCollectorPid ! {reduce_prep, Data, Pid},
            jobs_queue(Workers, DispatchersJobs, ResultCollectorPid, FileName);
        {endDataflowPartition, Output} ->
            ResultCollectorPid ! {endDataflowPartition, Output},
            jobs_queue(Workers, DispatchersJobs, ResultCollectorPid, FileName);
        {done_work, Output} ->
            %FIXME: file name 
            file_processing:save_data("data", Output);
        {error, CrushedWorker, Error} ->
            ?LOG("Error in coordinator listener ~w~n", [Error]),
            Workers1 = lists:delete(CrushedWorker, Workers),
            jobs_queue(Workers1, DispatchersJobs, ResultCollectorPid, FileName)
    end.

-spec get_results(CoordinatorPid, OutputMap, N) -> ok when
    CoordinatorPid :: pid(),
    OutputMap :: #{integer() => ListKV},
    ListKV :: [{integer(), integer()}],
    N :: integer().

get_results(CoordinatorPid, OutputMap, 0) ->
    Output = lists:flatmap(fun({_, V}) -> V end, maps:to_list(OutputMap)),
    ?LOG("All results received, sending to the coordinator~n"),
    ?LOG("Output: ~w~n", [Output]),
    CoordinatorPid ! {done_work, Output};

get_results(CoordinatorPid, OutputMap, N) ->
    receive
        {endDataflowPartition, {PartitionNumber, Output}} ->
            OutputMap1 = maps:put(PartitionNumber, Output, OutputMap),
            ?LOG("Received result from partition ~w~n", [PartitionNumber]),
            ?LOG("OutputMap: ~w~n", [OutputMap1]),
            get_results(CoordinatorPid, OutputMap1, N-1);
        {reduce_prep, {_, Data}, DispatcherId} ->
            N1 = prepare_reduce_input([DispatcherId], Data, N, 1),
            if N =/= N1 -> ?LOG("Changed number of partitions: ~p became ~p ~n", [N, N1]) end,
            get_results(CoordinatorPid, #{}, N1)
    end.

-spec prepare_reduce_input(DispatchersIds, Data, NPartition, NReceived) -> NPartition when
    DispatchersIds :: [pid()],
    Data :: [{integer(), integer()}],
    NPartition :: integer(),
    NReceived :: integer().

prepare_reduce_input(DispatcherIds, Datas, NPartition, NReceived) when NPartition =/= NReceived ->
    receive
        {reduce_prep, Data, DispatcherId} ->
            prepare_reduce_input([DispatcherId | DispatcherIds], Data ++ Datas, NPartition, NReceived + 1)
    end;

prepare_reduce_input(DispatchersIds, Data, NPartition, NReceived) when NPartition == NReceived -> 
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
    NKey = erlang:length(ListReduce),
    if
        NKey < NPartition ->
            NewPartitionedList = partition:partition(ListReduce, NKey) ++ lists:duplicate(NPartition - NKey, []);
        true ->
            NewPartitionedList = partition:partition(ListReduce, NPartition)
    end,
    % Sends each reduced partition to a dispatcher ({{Npartition, Data}, Dispatcher})
    PartitionDataDispatcherList = lists:zip3(lists:seq(1, NPartition), NewPartitionedList, DispatchersIds),
    ?LOG("PartitionDataDispatcher: ~w~n", [PartitionDataDispatcherList]),
    [DispatcherId ! {reduce, DataToSend, PartitionNumber} || {PartitionNumber, DataToSend, DispatcherId} <- PartitionDataDispatcherList],
    % The number of partitions can change if the number of keys is less than the number of partitions
    case NKey < NPartition of true -> NKey; false -> NPartition end.

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
            CoordinatorPid ! {error, Sock, Error}
    end.

get_in(String) ->
    {ok, In} = io:fread(String, "~s"),
    In.
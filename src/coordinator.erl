-module('coordinator').
-export([start/0, start/1]).
%Functions for internal use not called directly
-export([dispatch_work/3, get_results/3, dispatch_work/1, accept_connection/3, socket_listener/2, jobs_queue/3, coordinator/2]).
-importlib([file_processing, partition]).
-define(NAME, string:chomp(io:get_line("Input file to process: "))).

start() ->
    start(8080).

% Starts an accept socket with the given port
start(Port) ->
    case gen_tcp:listen(Port, [binary, {packet, 0}, {active, false}]) of
        {ok, AcceptSock} -> 
            io:format("AcceptSocket generated, ready to listen for new workers~n"),
            CoordinatorPid = spawn(?MODULE, coordinator, [[],[]]),
            accept_connection(CoordinatorPid, AcceptSock, []);
        {error, ErrorMessage} -> 
            io:format("Unexpected error launching the coordinator~n"),
            io:format(ErrorMessage)
    end.

accept_connection(Coordinator_id, AcceptSock, Workers) ->
    case gen_tcp:accept(AcceptSock) of
        {ok, Sock} ->
            io:format("Coordinator accepted a new connection~n"),
            % Starts the specific worker Pid that waits to receive messages from the worker
            Worker = spawn(?MODULE, socket_listener, [Coordinator_id, Sock]),
            accept_connection(Coordinator_id, AcceptSock, Workers ++ [Worker]);
        {error, Error} ->
            io:format("!!Unexpected error during socket accept!! ~w~n", Error),
            accept_connection(Coordinator_id, AcceptSock, Workers)
    end.

% Coordinator has parameter R : ready workers (a list) and B: busy
coordinator(Workers, JobsInProgress) ->
    io:format("Coordinator waiting for workers"),
    receive
        % Receives the socket of a new joining worker
        {join, Worker} -> 
            Workers1 = [Worker | Workers]
    after 3000 ->
            io:format("... no workers joined yet~n"),
            Workers1 = Workers
    end,
    if Workers1 == [] ->
            io:format("No workers available, waiting for new workers to join~n"),
            coordinator(Workers1, JobsInProgress);
        true ->
            case io:fread("Coordinator ready, do you want to start executing the tasks in the input file? [y/n]", "~s") of
                {ok, ["y"]} ->
                    dispatch_work(Workers1);
                    % Job_ = spawn(?MODULE, dispatch_work, [Workers1]),
                    % Job_ ! {workers, Workers1},
                {ok, ["n"]} ->
                    coordinator(Workers1, JobsInProgress)
            end,
            coordinator(Workers1, [])
    end.

dispatch_work(Workers) ->
    {ok, Npartitions, Ops} = file_processing:get_operations("../in/op"), %FIXME:?NAME),
    {ok, InputList}        = file_processing:get_data("../in/data"),     %FIXME:?NAME),
    Inputs = partition:partition(InputList, Npartitions),
    [spawn(?MODULE, dispatch_work, [Ops, Data, self()]) || Data <- lists:zip(lists:seq(1, Npartitions), Inputs)],
    % TODO: collector for reduce operations
    ResultCollectorPid = spawn(?MODULE, get_results, [self(), #{}, Npartitions]),
    jobs_queue(Workers, [], ResultCollectorPid).

dispatch_work([], Output, CoordinatorPid) ->
    CoordinatorPid ! {done, Output};

dispatch_work([{Op, Function, Arg} | Ops], {PartitionNumber, Data}, QueuePid)
    when Op =:= reduce ->
    io:format("__LOG__ Dispatching work to the queue~n~s", [io_lib:format("~p", [Op])]),
    % NO! QueuePid ! {job, {self(), {Op, Function, Arg}, Data}},
    QueuePid ! {reduce_prep, self(), Data},
    receive
        {reduce, Op, []} -> ok;
        {reduce, Op, ResultToReduce} ->
            QueuePid ! {job, {self(), {Op, Function, Arg}, ResultToReduce}},
            receive
                {result, ResultToReduce} ->
                    QueuePid ! {reduce, Op, ResultToReduce};
                {error, Error} ->
                    io:format("Error in coordinator listener ~w~n", [Error]),
                    dispatch_work([{Op, Function, Arg} | Ops], {PartitionNumber, Data}, QueuePid)
            after 3000 ->
                self() ! {error, "Timeout in coordinator listener"}
            end
    end;

dispatch_work([Op | Ops], {PartitionNumber, Data}, QueuePid) ->
    io:format("__LOG__ Dispatching work to the queue~n~s", [io_lib:format("~p", [Op])]),
    QueuePid ! {job, {self(), Op, Data}},
    receive
        {result, Result} ->
            dispatch_work(Ops, {PartitionNumber, Result}, QueuePid);
        {error, Error} ->
            io:format("Error in coordinator listener ~w~n", [Error]),
            dispatch_work([Op | Ops], {PartitionNumber, Data}, QueuePid)
    after 3000 ->
        self() ! {error, "Timeout in coordinator listener"}
    end.

get_results(CoordinatorPid, OutputMap, 0) ->
    Output = lists:flatmap(fun({_, V}) -> V end, maps:to_list(OutputMap)),
    io:format("__LOG__ All results received, sending to the coordinator~n"),
    io:format("__LOG__ Output: ~w~n", [Output]),
    CoordinatorPid ! {done, Output};

get_results(CoordinatorPid, OutputMap, N) ->
    receive
        {result, {PartitionNumber, Output}} ->
            OutputMap1 = maps:put(PartitionNumber, Output, OutputMap),
            io:format("__LOG__ Received result from partition ~w~n", [PartitionNumber]),
            io:format("__LOG__ OutputMap: ~w~n", [OutputMap1]),
            get_results(CoordinatorPid, OutputMap1, N-1)
    end.

jobs_queue(Workers, DispatcherJobs, ResultCollectorPid)
    when Workers =/= [] andalso DispatcherJobs =/= [] ->
        [Job | Jobs] = DispatcherJobs,
        [Worker | Ls] = Workers,
        gen_tcp:send(Worker, term_to_binary({job, Job})),
        jobs_queue(Ls, Jobs, ResultCollectorPid);

jobs_queue(Workers, DispatcherJobs, ResultCollectorPid) ->
    receive
        {job, Job1} ->
            jobs_queue(Workers, DispatcherJobs ++ [Job1], ResultCollectorPid); %appends the job
        {join, NewWorker} ->
            jobs_queue([NewWorker | Workers], DispatcherJobs, ResultCollectorPid);
        % This message come from a single dispatcher. 
        % Each one will send this, when they arrive to dispatch the reduce!
        % Data is from the single dispatcher and is the first get by the get_reduce_input
        {reduce_prep, Data, Pid} ->
            % Get the lists of tuples {Key, Value} from all Dispatchers' partitions
            NewFlatList = get_reduce_input(Data, lists:flat_lenght(DispatcherJobs) - 1),
            % Reduce all the partitions into a list {Key, ListOfValues}
            MapReduce = lists:foldl(fun({K, V}, Acc) ->
                            case maps:find(K,Acc) of
                                {ok, List} ->
                                    maps:put(K, [V | List]);
                                error ->
                                    maps:put(K, [V])
                            end
                        end,
                        #{}, % Map is more efficient, Turing, 1950 CIRCA.
                        NewFlatList),
            NKey = lists:flat_length(MapReduce),
            NPartition = lists:flat_length(DispatcherJobs),
            if 
                % case less key than partitions, the remaining partitions are empty
                NKey < NPartition ->
                    NewPartitionedList = partition:partition(MapReduce, NKey) ++ lists:duplicate([], NPartition - NKey);
                true ->
                    NewPartitionedList = partition:partition(MapReduce, NKey)
            end,
        % Sends each reduced partition to a dispatcher ({{Npartition, Data}, Dispatcher})
            lists:foreach(fun(Tuple) -> 
                {DataToSend, DispatcherId} = Tuple, 
                DispatcherId ! {reduce, DataToSend} end, lists:zip(lists:zip(lists:seq(1, NPartition), NewPartitionedList), DispatcherJobs)),
            jobs_queue(Workers, DispatcherJobs, ResultCollectorPid);
        {reduce_done, Ops, Data} ->
            % TODO
            ok;
            %Input = partition:reduce(Data, ?N),
            %[spawn(?MODULE, dispatch_work, [Ops, In, self()]) || In <- Input];
        {done, Output} ->
            ResultCollectorPid ! {result, Output};
        {result, Output} ->
            file_processing:save_data("out/data", Output);
        {error, CrushedWorker, Error} ->
            io:format("Error in coordinator listener ~w~n", [Error]),
            Workers1 = lists:delete(CrushedWorker, Workers),
            jobs_queue(Workers1, DispatcherJobs, ResultCollectorPid);
        Error ->
            io:format("Error in coordinator listener, unexpected message:~n~w~n", [Error]),
            jobs_queue(Workers, DispatcherJobs, ResultCollectorPid)
    end.

% This function just waits to have all the partitions from the dispatcher
% until it has all the input partitions for the reduce
-spec get_reduce_input(Data, DispatcherJobs) -> List when 
    Data::[A],
    A::int,
    DispatcherJobs::[B],
    B::process_id,
    List::[C],
    C::integer.

% Get reduced in case has no other dispatcher to wait, just return the new data
get_reduce_input(Data, 0) ->
    Data;

% Still there are dispatch workers to get the input partition for the reduce
get_reduce_input(Data, NumDispatchWorkers) ->
    receive 
        {reduce_prep, NewData, _} ->
            Data ++ get_reduce_input(NewData, NumDispatchWorkers - 1)
    end.

% Coordinator listener manages the communication with the worker
% Waits to receive messages from the host and 
socket_listener(CoordinatorPid, Sock) ->
    case gen_tcp:recv(Sock, 0) of
        {ok, Msg} ->
            case binary_to_term(Msg) of 
                join -> 
                    io:format("New worker has joined~n"),
                    CoordinatorPid ! {join, Sock};
                {result, DispatcherId, Result} ->
                    DispatcherId ! {result, Result},
                    CoordinatorPid ! {join, Sock};
                    % Updates the coordinator about the worker that has finished the job
                Error ->
                    io:format("Error in socket listener, unexpected message:~n~w~n", [Error]),
                    CoordinatorPid ! {error, Sock, Error}
            end,
            socket_listener(CoordinatorPid, Sock);
        {error, Error} ->
            io:format("Error in coordinator listener ~w~n", [Error]),
            io:format("Closing the socket~n"),
            CoordinatorPid ! {error, Sock, Error}
    end.
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

% Sends the work to the ready workers and waits for the results
dispatch_work(Workers) ->
    {ok, Npartitions, Ops} = file_processing:get_operations("in/op"), %FIXME?NAME),
    {ok, InputList}        = file_processing:get_data("in/data"),     %?NAME),
    Inputs = partition:partition(InputList, Npartitions),
    [spawn(?MODULE, dispatch_work, [Ops, In, self()]) || In <- lists:zip(lists:seq(1, Npartitions), Inputs)],
    % TODO: collector for reduce operations
    ResultCollectorPid = spawn(?MODULE, get_results, [self(), #{}, Npartitions]),
    jobs_queue(Workers, [], ResultCollectorPid).

dispatch_work([], Out, CoordinatorPid) ->
    CoordinatorPid ! {done, Out};

dispatch_work([Op | Ops], In, QueuePid) ->
    io:format("Dispatching work to the queue~n~s", [io_lib:format("~p", [Op])]),
    case Op of
        {reduce, _} -> ok;
            %TODO
        _ ->
        QueuePid ! {job, {self(), Op, In}},
        receive
            {result, Result} ->
                dispatch_work(Ops, Result, QueuePid);
            {error, Error} ->
                io:format("Error in coordinator listener ~w~n", [Error]),
                dispatch_work([Op | Ops], In, QueuePid)
        after 3000 ->
            self() ! {error, "Timeout in coordinator listener"}
        end
    end.

get_results(CoordinatorPid, OutMap, 0) ->
    Output = lists:flatmap(fun({_, V}) -> V end, maps:to_list(OutMap)),
    CoordinatorPid ! {done, Output};

get_results(CoordinatorPid, OutMap, N) ->
    receive
        {result, {OutId, Output}} ->
            get_results(CoordinatorPid, maps:put(OutId, Output, OutMap), N-1)
    end.

% Coordinator manages the workers queue, assigning jobs to the workers
jobs_queue([Worker | Ls], DispatcherJobs, ResultCollectorPid) ->
    if
        Worker =/= [] andalso DispatcherJobs =/= [] ->
            [Job | Jobs] = DispatcherJobs,
            gen_tcp:send(Worker, term_to_binary({job, Job})),
            jobs_queue(Ls, Jobs, ResultCollectorPid);
        true ->
            receive
                {job, Job1} ->
                    jobs_queue([Worker | Ls], DispatcherJobs ++ [Job1], ResultCollectorPid);
                {join, NewWorker} ->
                    jobs_queue([NewWorker, Worker | Ls], DispatcherJobs, ResultCollectorPid);
                {done, NewWorker} ->
                    jobs_queue([NewWorker, Worker | Ls], DispatcherJobs, ResultCollectorPid);
                {result, Output} ->
                    file_processing:save_data("out/data", Output);
                {error, CrushedWorker, Error} ->
                    io:format("Error in coordinator listener ~w~n", [Error]),
                    Workers = lists:delete(CrushedWorker, [Worker | Ls]),
                    jobs_queue(Workers, DispatcherJobs, ResultCollectorPid);
                Error ->
                    io:format("Error in coordinator listener, unexpected message:~n~w~n", [Error]),
                    jobs_queue([Worker | Ls], DispatcherJobs, ResultCollectorPid)
            end
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
                    CoordinatorPid ! {done, Sock};
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
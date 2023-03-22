-module(partition).
-export([partition/2]).

partition(L, N) ->
    M = length(L),
    Chunk = M div N,
    End = M - Chunk*(N-1),
    parthelp(L, N, 1, Chunk, End, []).

parthelp(L, 1, P, _, E, Res) ->
    PartitionedList = Res ++ [lists:sublist(L, P, E)],
    map:from_list(lists:zip(lists:seq(1, length(PartitionedList)), PartitionedList));


parthelp(L, N, P, C, E, Res) ->
    R = lists:sublist(L, P, C),
    parthelp(L, N-1, P+C, C, E, Res ++ [R]).
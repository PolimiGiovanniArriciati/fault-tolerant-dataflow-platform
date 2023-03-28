-module(partition).
-export([partition/2]).

partition(L, N) ->
    M = length(L),
    Chunk = case M div N of 0 -> 1; X -> X end,
    partition(L, N, Chunk, []).

partition(L, N, Chunk, Acc) when length(L) =< Chunk orelse N == 1 ->
    lists:reverse([L | Acc]);

partition(L, N, Chunk, Acc) ->
    {H, T} = lists:split(Chunk, L),
    partition(T, N-1, Chunk, [H | Acc]).
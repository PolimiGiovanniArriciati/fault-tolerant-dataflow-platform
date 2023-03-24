-module(partition).
-export([partition/2]).

partition(L, N) ->
    M = length(L),
    Chunk = M div N,
    End = M - Chunk*(N-1),
    if  Chunk =:= 0 ->
            lists:sublist(L, 1, End);
        true ->
            parthelp(L, N, 1, Chunk, End, [])
    end.

parthelp(L, 1, P, _, E, Res) ->
    Res ++ [lists:sublist(L, P, E)];

parthelp(L, N, P, C, E, Res) ->
    R = lists:sublist(L, P, C),
    parthelp(L, N-1, P+C, C, E, Res ++ [R]).
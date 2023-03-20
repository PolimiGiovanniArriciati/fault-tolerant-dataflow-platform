-module(functions).
-export([map/3]).
-compile([{nowarn_unused_function}]).

map(Op, Args, List) ->
	lists:map(fun({K, V}) ->
								{K,
								 case Args of
									 [] -> apply(?MODULE, Op, [V]);
									 _ -> apply(?MODULE, Op, [V | Args]) end}
						end,
						List).

changeKey(Op, Args, List) ->
	lists:map(fun({K, V}) ->
								{apply(?MODULE, Op, [Args, K]),
								 V}
						end,
						List).

reduce(Op, _, List) ->
	lists:foldl(fun({K, V}, Acc) -> apply(?MODULE, Op, [K, V, Acc]) end, [], List).

add(X, Y) ->
		X + Y.

subtract(X, Y) ->
		X - Y.

multiply(X, Y) ->
		X * Y.

divide(X, Y) ->
		X / Y.

power(X, Y) ->
	math:pow(X, Y).

sqrt(X) ->
	math:sqrt(X).

sum(X, Y, Acc) ->
		Acc + X + Y.

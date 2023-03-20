-module(functions).
-export([map/3]).
-compile([{nowarn_unused_function}]).

map(Op, Args, List) ->
	lists:map(fun({K, V}) -> apply(?MODULE, Op, [Args, V]) end, List).

changeKey(Op, Args, List) ->
	lists:map(fun({K, V}) -> apply(?MODULE, Op, [Args, K]) end, List).

reduce(Op, _, List) ->
	lists:foldl(fun({K, V}, Acc) -> apply(?MODULE, Op, [K, V, Acc]) end, [], List).

add(X, Y) ->
		X + Y.

sub(X, Y) ->
		X - Y.

mult(X, Y) ->
		X * Y.

div(X, Y) ->
		X / Y.

pow(X, Y) ->
	math:pow(X, Y).

sqrt(X) ->
	math:sqrt(X).

sum(X, Y, Acc) ->
		Acc + X + Y.

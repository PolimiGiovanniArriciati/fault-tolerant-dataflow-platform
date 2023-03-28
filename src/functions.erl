-module(functions).
-export([map/3, changeKey/3, reduce/3, add/2, subtract/2, multiply/2, divide/2, power/2, root/2]).
-compile({no_warn_unused_function}).

-spec map(Fun, Arg, ListKV) -> ListKV when
	Fun ::  fun(({Key, Value})->{Key, Value}),
	Arg :: Int,
	ListKV :: list({Key, Value}),
	Key :: Int,
	Value :: Int.

map(Fun, Arg, List) ->
	lists:map(fun({K, V}) ->
					{K, apply(?MODULE, Fun, [V, Arg])}
				end,
				List).

-spec changeKey(Fun, Arg, ListKV) -> ListKV when
	Fun ::  fun((Int, Int) -> Int),
	Arg :: Int,
	ListKV :: list({Key, Value}),
	Key :: Int,
	Value :: Int.

changeKey(Fun, Arg, List) ->
	lists:map(fun({K, V}) ->
					{apply(?MODULE, Fun, [K, Arg]), V}
				end,
				List).

-spec reduce(Fun, Arg, ListKVs) -> ListKV when
	Fun ::  fun((Int, Int) -> Int),
	Arg :: Int,
	ListKV :: list({Key, Value}),
	ListKVs :: list({Key, list(Value)}),
	Key :: Int,
	Value :: integer().

reduce(_, _, []) ->
	[];

reduce(Fun, Arg, [X | Xs]) ->
	reduce(Fun, Arg, X) ++ reduce(Fun, Arg, Xs);

reduce(Fun, Arg, {Key, Values}) ->
	[{Key,
	 lists:foldl(fun(V, Acc) -> apply(?MODULE, Fun, [V, Acc]) end,
				 Arg,
				 Values)}].
	
add(X, Y) ->
		X + Y.

subtract(X, Y) ->
		X - Y.

multiply(X, Y) ->
		X * Y.

divide(X, Y) ->
		X div Y.

power(X, Y) ->
	math:pow(X, Y).

root(X, Y) ->
	math:pow(X, 1 / Y).
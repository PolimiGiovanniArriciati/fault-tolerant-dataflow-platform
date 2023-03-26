-module(functions).
-export([map/3, changeKey/3, reduce/3, add/2, subtract/2, multiply/2, divide/2, power/2, sqrt/1, sum/3, prime_factor_decomposition/1]).
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
	 lists:foldl(fun(X, Acc) -> apply(?MODULE, Fun, [X, Acc]) end,
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

sqrt(X) ->
	math:sqrt(X).

sum(X, Y, Acc) ->
		Acc + X + Y.

prime_factor_decomposition(X) when X > 2 ->
		{_, Factors} = factor(X, 2, []),
		Factors;

prime_factor_decomposition(X) ->
		[X].

factor(X, N, Acc) ->
	case X rem N of
		0 ->
			factor(X div N, N, [N | Acc]);
		_ ->
			if
				N * N < X ->
					factor(X, N + 1, Acc);
				true -> % is prime
					{ok, lists:reverse([X | Acc])}
			end
	end.

-module(functions).
-export([map/3, changeKey/3, reduce/3, add/2, subtract/2, multiply/2, divide/2, power/2, sqrt/1, sum/3, prime_factor_decomposition/1]).
-compile({no_warn_unused_function}).

map(Op, Args, List) ->
	lists:map(fun({K, V}) ->
								{K,
								 case Args of
									[] ->  	    apply(?MODULE, Op, [V]);
									[X] ->      apply(?MODULE, Op, [V , X]);
									[X | Xs] -> apply(?MODULE, Op, [V , X]),
												apply(?MODULE, map, [Op, Xs, [{K, V}]])
												end}
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

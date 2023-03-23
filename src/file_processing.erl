-module(file_processing). %processing
-export([get_operations/1, get_data/1, save_data/2]). 
-define(SPLIT(X), string:split(string:trim(X), " ", all)).

get_operations(File_name) ->
	case file:open(File_name, [read, raw]) of
		{ok, File} ->
			read_op(File);
		{error, Reason} ->
			io:format("Error: ~p~n", [Reason]),
			test
	end.

read_op(File) ->
	Part = get_part(file:read_line(File)),
	{ok, Part,
		read_op(File, [], file:read_line(File))}.

read_op(File, Ops, {ok, Line}) ->
	{Op, Fun, Args} = split_operations(Line),
	read_op(File, [{Op, Fun, Args} | Ops], file:read_line(File));

read_op(File, Ops, eof) ->
	file:close(File),
	lists:reverse(Ops).

split_operations(Operation) ->
	[Op, Fun | Args] = ?SPLIT(Operation),
	{erlang:list_to_atom(Op),
	 erlang:list_to_atom(Fun),
	 lists:map(fun(X)-> apply(erlang, list_to_integer, [X]) end, Args)}.

get_data(File_name) ->
	case file:open(File_name, [read, raw]) of
		{ok, File} ->
			read_data(File, file:read_line(File), []);
		{error, Reason} ->
			io:format("Error: ~p~n", [Reason]),
			test
	end.

read_data(File, {ok, Line}, Acc) ->
	[Key, Value] = ?SPLIT(Line),
	{Key_, _} = string:to_integer(Key),
	{Value_, _} = string:to_integer(Value),
	read_data(File, file:read_line(File), [{Key_, Value_} | Acc]);

read_data(File, eof, Acc) ->
	file:close(File),
	{ok, lists:reverse(Acc)}.

% Split the operations to be performed, the functions and eventually the arguments
% data is in form of [{Key, Value}, ...]
save_data(File_name, Data) ->
	Serialized_data =
	  lists:foldl(fun({K, V}, Acc) -> Acc ++ io_lib:format("~p ~p~n", [K, V])
	  end, "", Data),
	file:write_file("out/"++File_name, Serialized_data).

get_part({ok, Line}) ->
	{_, String_Int} = string:take(string:trim(Line), lists:seq($0,$9), false, trailing),
	{Int, _} = string:to_integer(String_Int),
	Int.
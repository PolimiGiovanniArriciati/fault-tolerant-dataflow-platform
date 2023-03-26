-module(file_processing). %processing
-export([get_operations/1, get_data/1, save_data/2]). 
-define(SPLIT(X), string:split(string:trim(X), " ", all)).

get_operations(FileName) ->
	case file:open("in/"++FileName, [read, raw]) of
		{ok, File} ->
			read_op(File, FileName);
		{error, Reason} ->
			io:format("Error opening the operation file - reason: ~p~n", [Reason]),
			get_operations(get_in("Input file containing operations to execute: "))
	end.

read_op(File, FileName) ->
	Part = get_part(file:read_line(File)),
	{ok, FileName, Part,
		read_op(File, [], file:read_line(File))}.

read_op(File, Ops, {ok, Line}) ->
	{Op, Fun, Arg} = split_operations(Line),
	read_op(File, [{Op, Fun, Arg} | Ops], file:read_line(File));

read_op(File, Ops, eof) ->
	file:close(File),
	lists:reverse(Ops).

split_operations(Operation) ->
	[Op, Fun | Arg] = ?SPLIT(Operation),
	{erlang:list_to_atom(Op),
	 erlang:list_to_atom(Fun),
	 case string:to_integer(Arg) of
		{N, _} -> N;
		_ -> 0 end}. 

get_data(FileName) ->
	case file:open("in/"++FileName, [read, raw]) of
		{ok, File} ->
			read_data(File, FileName, file:read_line(File), []);
		{error, Reason} ->
			io:format("Error opening the data file - reason: ~p~n", [Reason]),
			{error, Reason}
	end.

read_data(File, FileName, {ok, Line}, Acc) ->
	[Key, Value] = ?SPLIT(Line),
	{Key_, _} = string:to_integer(Key),
	{Value_, _} = string:to_integer(Value),
	read_data(File, FileName, file:read_line(File), [{Key_, Value_} | Acc]);

read_data(File, FileName, eof, Acc) ->
	file:close(File),
	{ok, FileName, lists:reverse(Acc)}.

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

get_in(String) ->
	case io:fread(String, "~s") of
    {ok, In} -> In;
	{error, Reason} ->
		io:format("Error: ~p~n", [Reason]), get_in(String)
	end.
-module(pr). %processing
-export([get_input/0]).

-define(PARSE(TYPE, VAL, VAL_), 
				case TYPE of 
					int -> VAL_ = erlang:list_to_integer(VAL);
					_ -> VAL_ = erlang:list_to_atom(VAL)
				end).

-define(SPLIT(HEADER, OP, FUN, ARGS),
				[OP_, FUN_, ARGS_] = string:split(string:trim(HEADER), " ", all),
				OP = erlang:list_to_atom(OP_),
				FUN = erlang:list_to_atom(FUN_),
				case ARGS_ of
					[] -> ARGS = [];
					_ -> ARGS = erlang:list_to_integer(ARGS_)
				end).

-define(SPLIT_(TYPES, K, V), 
				[K_, V_] = string:split(string:trim(TYPES), " "),
				K = erlang:list_to_atom(K_),
				V = erlang:list_to_atom(V_)).

% Coordinator function to get the file contating the input
get_input() ->
	Read = string:chomp(io:get_line("Input file to process: ")),
	{ok, File} = file:open(Read, [read, raw]),
	read_file(File).

% Read the file line by line and process it
%The first line contains the operations to be performed, the funtions and evntually the arguments
%The second line contains the types of the variables
read_file(File) ->
	{ok, Header} = file:read_line(File),
	?SPLIT(Header, Op, Fun, Args),
	{ok, Types} = file:read_line(File),
	?SPLIT_(Types, Key_type, Value_type),
	{ok, Op, Fun, Args,
	 read_file(File, file:read_line(File), Key_type, Value_type, [])}.

read_file(File, {ok, Line}, Key_type, Value_type, Acc) ->
	[Key, Value] = string:split(string:trim(Line), " "),
	?PARSE(Key_type, Key, Key_),
	?PARSE(Value_type, Value, Value_),
	read_file(File, file:read_line(File), Key_type, Value_type, [{Key_, Value_} | Acc]);

read_file(File, eof, _, _, Acc) ->
	file:close(File),
	lists:reverse(Acc).

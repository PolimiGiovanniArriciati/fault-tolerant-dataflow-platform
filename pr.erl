-module(pr). %processing
-export([get_input/0]).
-define(SPLIT(X), string:split(string:trim(X), " ", all)).

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
	{Op, Fun, Args} = split_header(Header),
	{ok, Types} = file:read_line(File),
	{Key_type, Value_type} = split_types(Types),
	{ok, Op, Fun, Args,
	 read_file(File, file:read_line(File), Key_type, Value_type, [])}.

read_file(File, {ok, Line}, Key_type, Value_type, Acc) ->
	[Key, Value] = ?SPLIT(Line),
	Key_ = parse(Key, Key_type),
	Value_ = parse(Value, Value_type),
	read_file(File, file:read_line(File), Key_type, Value_type, [{Key_, Value_} | Acc]);

read_file(File, eof, _, _, Acc) ->
	file:close(File),
	lists:reverse(Acc).

% UTILITY FUNCTIONS:
% Split the types line into the types of the variables, converting them to atoms
split_header(Header) ->
	[Op_, Fun_, Args_] = ?SPLIT(Header),
	{erlang:list_to_atom(Op_),
	 erlang:list_to_atom(Fun_),
	 case Args_ of
	   [] -> [];
		 _ -> erlang:list_to_integer(Args_)
	 end}.

split_types(Types) ->
	[K_, V_] = ?SPLIT(Types),
	{erlang:list_to_atom(K_),
	 erlang:list_to_atom(V_)}.

% Split the header line into the operations to be performed, the functions and eventually the arguments
parse(V, V_type) ->
	case V_type of 
		int -> erlang:list_to_integer(V);
		_ -> erlang:list_to_atom(V)
	end.


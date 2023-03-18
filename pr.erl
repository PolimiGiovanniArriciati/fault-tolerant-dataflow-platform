-module(pr). %processing
-export([get_input/0]).

get_input() ->
	Read = string:chomp(io:get_line("Input file to process: ")),
	{ok, File} = file:open(Read, [read, raw]),
	{ok, Op, Data} = read_pr_file(File),
	[Op, Data].

% Read file
% Note: the input is parsed as a list of strings
% If a non ASCII / UTF-8 character is found, the string is broken and output as a list of integers
read_file(File) ->
	read_file(File, []).

read_file(File, Acc) ->
	case file:read_line(File) of
		{ok, Line} ->
			read_file(File, [string:trim(Line) | Acc]);
		eof ->
			file:close(File),
			{ok, lists:reverse(Acc)}
	end.

% Read processing file
read_pr_file(File) ->
	read_pr_file(File, []).

read_pr_file(File, []) ->
	{ok, Op} = file:read_line(File),
	[Module, Function | 	Args] = string:split(string:trim(Op), " ", all),
	{ok, Tuple} = file:read_line(File),
	{ok, Module, Function, Args, Tuple};
	%{ok, Op, read_pr_file(File, [{K,V}])};


read_pr_file(File, Acc) ->
	case file:read_line(File) of
		{ok, [K, " ", V]} ->
			read_pr_file(File, [ {K, V} | Acc]);
		eof ->
			file:close(File),
			lists:reverse(Acc)
	end.

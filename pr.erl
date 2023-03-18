-module(pr). %processing
-export([get_input/0]).

get_input() ->
	Read = string:chomp(io:get_line("Input file to process: ")),
	{ok, File} = file:open(Read, [read, raw]),
	{ok, Data} = read_file(File),
	Data.

% Read file
% Note: the input is parsed as a list of strings
% If a non ASCII / UTF-8 character is found, the string is broken and output as a list of integers
read_file(File) ->
	read_file(File, []).

read_file(File, Acc) ->
	case file:read_line(File) of
		{ok, Line} ->
			read_file(File, [string:chomp(Line) | Acc]);
		eof ->
			file:close(File),
			{ok, lists:reverse(Acc)}
	end.


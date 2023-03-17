-module('rpctest').
-export([worker/0, server_receive/1, server/0]).
worker() ->
    global:register_name(worker, self()),
    net_kernel:connect_node(server@Aspire),
    R = rpc:call(server@Aspire, 'rpctest', server_receive, [ciao]),
    %{ciao, ciao} ! server@Aspire,
    %io:format(R), 
    receive 
        R -> io:format("Ricevuto ", R)
end.

server_receive(Param) ->
    io:format("Stampo da remoto: "),
    io:format(Param).

server()->
    global:register_name(server, self()),
    receive 
        R -> io:format(["Ricevuto", R])
    end. 


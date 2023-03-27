-module(main).
-export([main/0]).

main() ->
    CoordinatorPid = spawn(coordinator, start, []),
    CoordinatorPid ! {start_work, "op1", "data5"},
    worker:start(8080),
    worker:start(8081).

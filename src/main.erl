-module(main).
-export([main/0]).

main() ->
    CoordinatorPid = spawn(coordinator, start, []),
    CoordinatorPid ! {start_work, "op", "data"},
    worker:start(8080),
    worker:start(8081).

-module(test).
-export([c/0, c1/0, c2w/0, c1w2/0, c1w/0, w/0, w1/0]).
-importlib([coordinator, worker]).

% Starts a coordinator listening on port 8080
c() ->
    coordinator:start(8080).

% Starts a coordinator listening on port 8081
c1() ->
    coordinator:start(8081).

w() ->
    worker:start("127.0.0.1", 8080).

w1() -> 
    worker:start("127.0.0.1", 8081).

% Starts a coordinator and a worker, ip: "127.0.0.1" port: 8080
c1w() ->
    spawn(coordinator, start, [8080]),
    spawn(worker, start, []).

% Starts a coordinator and two workers, ip: "127.0.0.1" port: 8080
c2w() ->
    spawn(coordinator, start, [8080]),
    spawn(worker, start, []),
    spawn(worker, start, []).

% Starts a coordinator and two workers, ip: "127.0.0.1" port: 8081
c1w2() ->
    spawn(coordinator, start, [8081]),
    spawn(worker, start, ["127.0.0.1", 8081]),
    spawn(worker, start, ["127.0.0.1", 8081]).
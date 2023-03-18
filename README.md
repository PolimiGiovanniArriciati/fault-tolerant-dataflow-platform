# Fault-tolerant dataflow platform

Optional project for the [Distributed System course][1]

We decided to implement the system in [Erlang](https://www.erlang.org/)

### Project objective:

You are to implement a distributed dataflow platform for processing key-value pairs where keys and values are integers.
The platform offers three operators, which are executed independently and in parallel for each key k:
- map(f: int -> int): for each input tuple <k, v>, it outputs a tuple <k, f(v)>
- changeKey(f: int -> int): for each input tuple <k, v>, it outputs a tuple <f(v), v>
- reduce(f: list<int> -> int): takes in input the list V of all values for key k, and outputs <k, f(V)>

The platform includes a coordinator and multiple workers.
The coordinator accepts dataflow programs specified as an arbitrarily long sequence of the above operators. For instance, programs may be defined in JSON format and may be submitted to the coordinator as input files. Each operator is executed in parallel over multiple partitions of the input data, where the number of partitions is specified as part of the dataflow program. The coordinator assigns individual tasks to workers and starts the computation. Implement fault-tolerance mechanisms that limit the amount of work that needs to be re- executed in the case a worker fails. The project can be implemented as a real distributed application (for example, in Java) or it can be simulated using OmNet++.
### Assumptions:
- Workers may fail at any time, while we assume the coordinator to be reliable.
- Links are reliable (use TCP to approximate reliable links and assume no network
partitions can occur).
- You may implement either a scheduling or a pipelining approach. In both cases, intermediate results are stored only in memory and may be lost if a worker fails. On the other end, nodes can rely on some durable store (the local file system or an external store) to implement fault-tolerance mechanisms.
- You may assume input data to be stored in one or more csv files, where each line represents a <k, v> tuple.
- You may assume that a set of predefined function exists and they can be referenced by name (for instance, function ADD(5) is the function that takes in input an integer x and returns integer x+5)

[1]: https://www11.ceda.polimi.it/schedaincarico/schedaincarico/controller/scheda_pubblica/SchedaPublic.do?&evn_default=evento&c_classe=744092&polij_device_category=DESKTOP&__pj0=0&__pj1=8e05c9bc851ffdc3fdcd7ab405195296

# redis-queue

A set of tiny libraries that implements some fault tolerance over Redis lists. The
original idea can be found [here](http://antirez.com/post/250).

I'll put here a simplified description of the idea later.

Suppose you have a Redis list as queue, where messages IDs are `lpush`ed by an 
application and `rpop`ed by workers. What does happen if a worker crashes while 
processing the message? There probably are a lot of possible strategies to solve
this problem, specially now that Redis allows atomic operations written in Lua.
This tiny library just implements one of this strategies, with available
interface for some different languages.

## Installation 

Python (the latest version for Python is ``0.2.1``):

```
pip install redis-queue-pyclj
```

Clojure (Leiningen):

```clojure
[redis-queue-clj "0.1.0"]
```

## Usage

A summary of the idea:

* An application posts a message ID `123` into the queue.
* Calling `safe_pop` will return the ID, but with a side effect: the ID is 
`lpush`ed with a timestamp into the same queue, becoming `123|1417696237`.
* If another worker uses `safe_pop` at this time, this function checks if 
the ID `123` is a member of the done set (created by another function, as 
described later) or if the timestamp indicates that its processing is late.
    * if `123` is a member of the done set, the ID is just discarded and 
removed from the done set.
    * if the timestamp indicates lateness, the ID is returned to the worker
and `lpush`ed again to the queue with a new timestamp.
    * otherwise, the same ID and timestamp is just `lpush`ed again into the
queue.
* When a worker finishes the message processing, it adds the message 
ID to the done set using `mark_done`. It's very important to give this 
signal when the message isn't useful anymore, otherwise, its ID can be never
removed from the queue.

For now, all implementations offer two useful functions, `safe_pop` and `mark_done`.

## Known issues

The functions are not using Lua script yet, so the operations are not 
atomic (and not completely safe).

## License

Copyright © 2014 Andre A. Boechat

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.

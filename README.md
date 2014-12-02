# redis-queue

A set of tiny libraries that implements some fault tolerance over Redis lists. The
original idea can be found [here](http://antirez.com/post/250).

I'll put here a simplified description of the idea later.

## Installation 

Python:

```
pip install redis-queue-pyclj
```

Clojure (Leiningen):

```clojure
[redis-queue-clj "0.1.0"]
```

## Usage

For now, all implementations offer two useful functions, `safe_pop` and `mark_done`.
Detailed description will come.

## License

Copyright © 2014 Andre A. Boechat

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.

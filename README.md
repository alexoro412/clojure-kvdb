# kvdb

This is a simple Key-Value database in Clojure. 

To run it, first run `lein run` in the top-level directory.

Then, in a new terminal, open the kvdb-client folder and run `lein run`.

The command syntax is based on Redis.

Example commands 

```
>> SET x 4
(:new 4)
>> GET x
(:ok 4)
>> DEL x
(:ok 1)   # number of keys deleted
>> HSET map key value
(:ok)
>> HGET map key 
(:ok value)
>> HDEL map key
(:ok 1)   # number of keys deleted
>> CLEAR
(:ok :clear)
>> EXISTS x
(:ok 0)   # number of keys that exist
```



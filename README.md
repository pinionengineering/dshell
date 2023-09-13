This is a silly little tool to manually interact with a datastore.

Datastores are not meant to be used this way, and this tool won't be too useful with storing or displaying binary data.
But sometimes it's useful for poking around and and seeing what's in the database while troubleshooting something.

It supports basic *very* basic queries and odering.

Let's see it.

Create a new datastore, or open up an existing one.
You have to spqcify the type of datastore and an appropriate path

```
$ dshell
dshell-> open badger ~/badger
```


This datastore is empty. Put in some values.

```
dshell(badger:/home/cory/badger)-> put /test1/k1 "this is the first message"
dshell(badger:/home/cory/badger)-> put /test2/k1 "test2 namespace"
dshell(badger:/home/cory/badger)-> put /test2/k2 "test2 again"
dshell(badger:/home/cory/badger)-> put /test3/k3 "wow another message in test3"
```

we can peer into these values with the get command

```
dshell(badger:/home/cory/badger)-> get /test1/k1
this is the first message

```

we can query for only certain messages by passing a prefix to the query command
By default, it will query *everything*

```
dshell(badger:/home/cory/badger)-> query
KEY:  /test1/k1
this is the first message
KEY:  /test2/k1
test2 namespace
KEY:  /test2/k2
test2 again
KEY:  /test3/k3
wow another message in test3

```

of course this might return a lot of data in a real datastore.
Put some constraints on that data using options. Let's say we want
to query by prefix to get just those keys that are *greater* than /test2
and limit the output to only 2 results, and with an offset.

To see what other filters are available, see the `help query`

```
dshell(badger:/home/cory/badger)-> query --fkg  /test2 -l 2 -o 1 /
KEY:  /test2/k2
test2 again
KEY:  /test3/k3
wow another message in test3

```

Finally, if we want to save data out of the datastore, 
some commands support a `--save` option. When this option is used,
contents are saved to files and a status message is printed to the
console.

```
dshell(badger:/home/cory/badger)-> query --save ~/out
wrote  25  bytes to  /home/cory/out/test1/k1
wrote  15  bytes to  /home/cory/out/test2/k1
wrote  11  bytes to  /home/cory/out/test2/k2
wrote  28  bytes to  /home/cory/out/test3/k3

```

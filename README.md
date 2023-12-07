A basic implementation of the Write-Ahead Log in chapter 3 of Patterns of Distributed Systems. A write-ahead log, sometimes called a commit log, is used to provide an append-only data store. In this case, the commit log stores
commands, which are played back to recreate the state of in-memory key-value store.

According to the chapter, similar implementations can be found in Zookeeper and Raft, the storage implementation in Kafka, as well as nosql databases like Cassandra to guarantee durability.
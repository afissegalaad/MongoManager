# MongoManager to start a whole cluster in one line [![Build Status](https://travis-ci.org/afissegalaad/MongoManager.svg?branch=release)](https://travis-ci.org/afissegalaad/MongoManager)

## MongoDB

MongoDB is a fast and scalable NoSQL database. It supports very high read and write operations. The main concepts are:

* **Cluster:** A group of nodes that manages scalability and replication.
* **Shard:** A group of nodes dedicated to a data subset. Each shard manages it's own replication. All shards together provide horizontal scalability to MongoDB.
* **Chunk:** Defines a range of data based on the key value. Each shard is in charge of a group of chunks. If a chunk grows to big it is split into two chunks, this is called migration.
* **Primary:** Is the entry node of a shard. It supports all read and write operations. Furthemore, the primary node is the only one that supports write operations in a shard.
* **Secondary:** Is the node providing replication to the shard (i.e. to MongoDB). It supports only write operation.

# MongoManager

MongoManager is a Python library launching, stopping, managing MongoDB cluster in a few lines of code.

* **Easy:** Just one class to start and manage a whole MongoDB cluster.
* **Flexible:** Can creates mongod/s processes or plug on them.
* **Clear:** Every step is clearly mentionned in a log or at the screen.

## Examples

```python
# Prepare data and log directories
cluster = MongoCluster().initialize()

# Start the whole cluster on all wanted  machines
cluster.start()

# Stop the cluster
cluster.stop()

# Clean the directories
cluster.clean()

```

This example will start a cluster with default configuration (1 config replica set, 2 data replica sets, with a replica factor of 3).

```python
# One line style
cluster = MongoCluster.initialize().start()
cluster.stop().clean()
```

## Installation

It's a python module. Import it.

## License

MongoManager is made under the terms of the MIT license.

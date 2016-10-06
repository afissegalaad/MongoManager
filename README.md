# MongoManager

[![Build Status](https://travis-ci.org/yoanncouillec/MongoManager.png)](https://travis-ci.org/yoanncouillec/MongoManager)

MongoManager is a Python library launching, stopping, managing MongoDB cluster in a few lines of code.

* **Easy:** Just one class to start and manage a whole MongoDB cluster.
* **Flexible:** Can creates mongod/s processes or plug on them.
* **Clear:** Every step is clearly mentionned in a log or at the screen.

## Examples

```python
cluster = MongoCluster()
cluster.init_cluster_env()
cluster.start_cluster()
```

This example will start a cluster with default configuration (1 config replica set, 2 data replica sets, with a replica factor of 3).

## Installation

The library is a Python module and can be imported directly in your code.
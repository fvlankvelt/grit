GRaph In Time (GRIT)
==

Uses RocksDB as backend to implement a time-travelling graph database.
At this moment mostly in the design phase - hopefully this changes soon.

Vertices, Edges and Indices
===

A vertex is modelled as a single key with a `<type>`, with labels and (incoming, outgoing) edges persisted in separate column families.  Each label is indexed - the index key is `<label>:<type>:<vertex_id>`.  The type of a vertex is fixed after creation.  Key-value properties can be modelled as `<label> := <value>:<property>`.

Indices and properties are modelled as vertices and edges themselves.  The index edge entry lets us find the vertices for a particular property value.

To prevent transactional conflicts, edges in indices are stored as separate keys:
    index:<label>:<type>:<vertex_id> => [Add @ ts] | [Remove @ ts]
An index lookup is then an iterator over a prefix.  A seek to `<label>:` can then be followed by an iteration that stops when the prefix no longer matches.

Labels for vertices are modeled as a set, stored in a column family:
  labels:<type>:<vertex_id>:<label> => [Add @ ts] | [Remove @ ts]

Edges in vertices are stored in a column family with the vertex key:
  edge_in:<type>:<vertex_id>:<target_type>:<vertex_id'> => [Add @ ts] | [Remove @ ts']
  edge_out:<type>:<vertex_id>:<target_type>:<vertex_id'> => [Add @ ts] | [Remove @ ts']
The complete set of edges is reconstructed by a scan/iterator with on the column family.

Locking of vertices in transactions consists of updating a "vertex" cell:
  vertex:<type>:<vertex_id> => <counter>
Transactions acquire this cell to detect conflicts.


User-defined timestamps for Time
===

RocksDB lets us use an Iterator (`db->NewIterator(readOptions)`).  
* time-travel: when `ReadOptions#timestamp` is set, it determines the visible data (latest ts for a key before the timestamp).  
* time-range: by also setting `ReadOptions#iter_start_ts`, also older entries can be read.


Other things to consider:

* *Compaction*: RocksDB compaction lets us specify a user-defined timestamp.  Values older than that will be deleted if they're no longer visible.
* *Transactions*: Multi-vertex operations, either by batching or by mutual dependence, can be executed in a RocksDB transaction.  Conflict detection is on a key level, so modelling needs to take this into account.  
* *HA*: High availability could be implemented with the use of `nuraft` - a C++ library that implements the RAFT protocol.  The `log_store` takes care of persisting RAFT log entries - e.g. for leader election.  The `state_machine` is the actual Graph Over Time store.  ClickHouse has a full implementation (HouseKeeper) that can be used as an alternative to an external ZooKeeper.  Nuraft snapshots can be based on RocksDB Checkpoints


Tentative API
===

Mutations:
- AddVertex(type: str): str (vertex_id)
- RemoveVertex(type: str, id: str): ()
- AddLabel(type: str, id: str, label: str): ()
- RemoveLabel(type: str, id: str, label: str): ()
- AddEdge(type: str, id: str, other_type: str, other_id: str): ()
- RemoveEdge(type: str, id: str, other_type: str, other_id: str): ()

Querying/fetching:
- GetVertices(label: str, type: str?): [{type: str, id: str}]
- GetEdges(type: str, id: str, direction: (in|out), other_type: str?): [{type: str, id: str})]
- GetLabels(type: str, id: str): [str]

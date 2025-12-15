GRaphs In Time (GRIT)
==

A highly available time-travelling graph database.  Supporting large transactions with SNAPSHOT isolation, fast ingestion and querying.

Vertices, Edges and Indices
===

A vertex is modelled as a single key with a `<type>`, with labels and (incoming, outgoing) edges persisted in separate column families.  Each label is indexed - the index key is `<label>:<type>:<vertex_id>`.  The type of a vertex is fixed after creation.  Key-value properties can be modelled as `<label> := <value>:<property>`.

An index entry is very similar to a vertex itself.  The vertices with its label are stored as edges - letting us find the vertices for a particular label.

To prevent transactional conflicts, edges in indices are stored as separate keys:
    index:<label>:<type>:<vertex_id> => [Add @ ts] | [Remove @ ts]
An index lookup is then an iterator over a prefix.  A seek to `<label>:` can then be followed by an iteration that stops when the prefix no longer matches.

Labels for vertices are modeled as a set, stored in a column family:
  labels:<type>:<vertex_id>:<label> => [Add @ ts] | [Remove @ ts]

Edges in vertices are stored in a column family with both vertex keys (so they are persisted twice - once with direction `in` and once with direction `out`):
  edges:<type>:<vertex_id>:<edge_label>:<direction>:<target_type>:<vertex_id'> => [Add @ ts] | [Remove @ ts']

The complete set of edges is reconstructed by a scan/iterator with on the column family.

Transactional conflicts are detected on commit.  the set of touched vertices is compared with that of concurrently running, successfully committed transactions.

Isolation is implemented by leveraging RocksDB merge operations.  Add/Remove updates are ignored for transactions that are not allowed to see it.

User-defined timestamps for Time
===

RocksDB lets us use an Iterator (`db->NewIterator(readOptions)`).  
* time-travel: when `ReadOptions#timestamp` is set, it determines the visible data (latest ts for a key before the timestamp).  
* time-range: by also setting `ReadOptions#iter_start_ts`, also older entries can be read.

The timestamps that are used are the NuRaft log index numbers.  For each transaction we keep track of the latest readable datum - additional filtering of operands that were written by concurrent transactions then gives us SNAPSHOT isolation.

Compaction
===

When an edge is often removed and added again, the number of updates that need to be processed can grow quite large.  Also when many transactions turn out invalid (e.g. due to a timeout) or are rolled back, many updates must be ignored.  Upon compaction by RocksDB, the updates (merge operands) are folded together into a plain value.

Slices
===

When there is a lot of churn on a vertex or an index entry, there are many updates that need to be fetched for the merge operator.  This makes reading vertices slow.  A way to mitigate this is by "slicing".  When the number of deletes is large both in absolute terms (say, #deleted-keys > 10) and relative (#deleted-keys > #non-deleted-keys), a new "slice" of the data can be made.  This is implemented by inserting a regular value (or delete tombstone) - exactly the same as what I compaction does but then on a fine-grained level.

Expiration
===

RocksDB let's us expire data by running a manual compaction with a history lower bound on the user-defined timestamp.

This mechanism lets compaction clean up the expired slices automatically.  Old slices should only be read with a timestamp before their archiving (or they appear empty).


High Availability 
===

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

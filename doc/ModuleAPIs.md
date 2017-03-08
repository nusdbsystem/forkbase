RequestHandler
 ### Members
 * Master List
 * Key Partitions in each Replica
 * Socket Connections to each Worker Node
 ### Cluster Management Methods
 + Init(List<Master_Node>)
 + UpdateClusterInfo(Master_Node)
 ### Data Management Methods
 + Value Get(Key, Branch = master, Version = head)
 + Put(Key, Value, Branch = master, Version = head, Forward = false, Force = false)
 + Branch(Key, Old_Branch, Version = head, New_Branch)
 + Move(Key, Old_Branch, New_Branch)
 + Abort Merge(Key, Value = null, Tgt_Branch, Ref_Branch, Ref_Version = head, Forward = false, Force = false)
 ### Internal Methods
 + List<Pair<Master_Node, Worker_Node>> ForwardWorkers(Key)

Master(zookeeper)
 ### Members
 * Worker List
 * Worker Status
 * Worker Partitions
 ###
 + List<Worker_Node, Key_Range> GetWorkerPartitions()

Worker
 ### Members
 * Worker ID
 * Managed Key Ranges
 * Key_Branch_Head Table (need to be dumped periodically)
 * UNode Manager
 ### Data Management Methods
 + Value Get(Key, Branch = master, Version = head)
 + Put(Key, Value, Branch = master, Version = head, Forward = false, Force = false)
 + Branch(Key, Old_Branch, Version = head, New_Branch)
 + Move(Key, Old_Branch, New_Branch)
 + Abort Merge(Key, Value = null, Tgt_Branch, Ref_Branch, Ref_Version = head, Forward = false, Force = false)

DataTypeManager
 ### Members
 * Chunk Storage
 ### UNode Management Methods
 + UNode LoadNode(version)
 + UNode NewNode(key, type)
 + UNode NewLinkedNode(UNode)
 + Commit(UNode)
 ### Blob Management Methods
 + Blob LoadBlob(hash)
 + Blob NewBlob(byte[])
 + Commit(Blob)
 ### String Management Methods
 + String LoadString(hash)
 + String NewString(String)
 + Commit(String)

struct UNode
 * key
 * type
 * version
 * pre_version
 * value/chunk_hash
 + SetValue(number/string/blob)

struct Blob
 * size
 * byte array

struct String
 * length
 * char array

ChunkStorage
 ### Chunk Management Methods
 * byte[] GetChunk(hash_value)
 * PutChunk(hash_value, byte[])
 ### note
 * chunk should be 3-way replicated
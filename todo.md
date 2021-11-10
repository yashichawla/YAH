Handle deleted data nodes and name nodes - wherever path of DN and NN is being referred, add a check and revive function

# Data Node
- Each Data Node must store only datanode_size blocks. If there is no more available space in a datanode, it must be removed from the block replication process.
- The block size for each file is provided by block_size. Each file must be split into the specified block size, and each block must be replicated replication_factor number of times. Each replicated block must now be stored in a Data Node.
- Choosing a Data Node for a block must be based on hashing (or any other algorithm). If there are collisions, or a Data Node is full, suitable measures must be taken. The allocation of blocks to Data Nodes must be efficient.
- Data Nodes can store logging information (such actions performed on a block, creation of new files and folders, etc) in datanode_log_path. Each Data Node must create its own logging file inside this directory to track only its blocks.
- Optional: It is possible for Data Nodes to crash or get deleted. If this happens, attempt to recreate the Data Node from the information stored in the Name Node about the deleted Data Node. You will be required to simulate the deletion of a Data Node to demonstrate this feature.

# Name Node

- The primary Name Node must keep track of all changes being made in path_to_fs. This includes creation or deletion of files and folders.
- The Name Node should also track details about each file such as the number of blocks it consists of, the location of the Data Nodes where its blocks are located and so on.
- The Name Node also logs information about Data Nodes. Meta data about Data Nodes (can be any information you wish to track, such as size of each Data Node, number of files, number of blocks occupied, etc) must be logged to namenode_log_path.
- Each Name Node must periodically (sync_period) send a heartbeat signal to every Data Node. During this process, it must ensure all Data Nodes are running, and update the status of each Data Node. If a Data Node is full, or if it cannot find a Data Node, it must take suitable actions to ensure there isn’t a failure while accessing the DFS.
- A secondary Name Node must be setup. It should back up all the data stored by the primary Name Node every sync_period seconds. If the primary Name Node ever fails, the secondary Name Node must take over as the primary Name Node and must retrieve all information from the last created checkpoint. It must also create a new secondary Name Node and restore information upto the last checkpoint.
- Optional: Implement an Edit Log. This log must be stored inside the Name Node and must be updated with all the operations that take place on the Data Node. The contents of the log must be merged with the Name Node’s meta data content about the file system mappings and block details at periodic intervals.
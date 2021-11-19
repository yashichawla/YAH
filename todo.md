- Handle deleted data nodes and name nodes - wherever path of DN and NN is being referred, add a check and revive function

# Data Node
- Hashing to choose DN
- Data Nodes can store logging information (such actions performed on a block, creation of new files and folders, etc) in datanode_log_path. Each Data Node must create its own logging file inside this directory to track only its blocks.
- Optional: It is possible for Data Nodes to crash or get deleted. If this happens, attempt to recreate the Data Node from the information stored in the Name Node about the deleted Data Node. You will be required to simulate the deletion of a Data Node to demonstrate this feature.

# Name Node

- The primary Name Node must keep track of all changes being made in path_to_fs. This includes creation or deletion of files and folders.
- The Name Node also logs information about Data Nodes. Meta data about Data Nodes (can be any information you wish to track, such as size of each Data Node, number of files, number of blocks occupied, etc) must be logged to namenode_log_path.
- Each Name Node must periodically (sync_period) send a heartbeat signal to every Data Node. During this process, it must ensure all Data Nodes are running, and update the status of each Data Node. If a Data Node is full, or if it cannot find a Data Node, it must take suitable actions to ensure there isn’t a failure while accessing the DFS.
- A secondary Name Node must be setup. It should back up all the data stored by the primary Name Node every sync_period seconds. If the primary Name Node ever fails, the secondary Name Node must take over as the primary Name Node and must retrieve all information from the last created checkpoint. It must also create a new secondary Name Node and restore information upto the last checkpoint.
- Optional: Implement an Edit Log. This log must be stored inside the Name Node and must be updated with all the operations that take place on the Data Node. The contents of the log must be merged with the Name Node’s meta data content about the file system mappings and block details at periodic intervals.

# Persistent storage
It must be possible to load the configuration of multiple DFS configurations, since multiple config files may be used to setup different DFS on the same machine.

# Loading the DFS
It should verify if the DFS can be loaded, by verifing the paths of the Data Nodes and Name Nodes. It must ensure and perform basic checks such as (but not limited to):
    Metadata contents about the Data Nodes stored by the Name Node exist and are correct
    Ensure that each Data Node contains all the blocks it should contain
    Ensure that the Name Node contains the correct mapping of blocks to Data Nodes
    Take suitable actions and attempt to restore the configuration in case of failures
- # If the DFS has been loaded for the first time, it must prompt the user to format the Name Node. The format operation must delete all data inside Data Nodes and Name Nodes and delete the contents of all the log files.

# Accesing the DFS
rm
rmdir

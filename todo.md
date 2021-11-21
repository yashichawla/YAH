- Handle deleted data nodes and name nodes - wherever path of DN and NN is being referred, add a check and revive function
- run script for a while, see if the timed tasks throw errors
- run mr job output is directory file

# Data Node
- Hashing to choose Data Node
- Data Nodes can store logging information (such actions performed on a block, creation of new files and folders, etc) in datanode_log_path. Each Data Node must create its own logging file inside this directory to track only its blocks.
- Optional: It is possible for Data Nodes to crash or get deleted. If this happens, attempt to recreate the Data Node from the information stored in the Name Node about the deleted Data Node. You will be required to simulate the deletion of a Data Node to demonstrate this feature.

# Name Node

- The Name Node also logs information about Data Nodes. Meta data about Data Nodes (can be any information you wish to track, such as size of each Data Node, number of files, number of blocks occupied, etc) must be logged to namenode_log_path.
- Each Name Node must periodically (sync_period) send a heartbeat signal to every Data Node. During this process, it must ensure all Data Nodes are running, and update the status of each Data Node. If a Data Node is full, or if it cannot find a Data Node, it must take suitable actions to ensure there isn’t a failure while accessing the DFS.

# Loading the DFS
- It should verify if the DFS can be loaded, by verifing the paths of the Data Nodes and Name Nodes. It must ensure and perform basic checks such as (but not limited to):
    - Metadata contents about the Data Nodes stored by the Name Node exist and are correct
    - Ensure that each Data Node contains all the blocks it should contain
    - Ensure that the Name Node contains the correct mapping of blocks to Data Nodes
    - Take suitable actions and attempt to restore the configuration in case of failures
- If the DFS has been loaded for the first time, it must prompt the user to format the Name Node. The format operation must delete all data inside Data Nodes and Name Nodes and delete the contents of all the log files.

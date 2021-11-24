- On rm- remove blocks from datanode, update block_info, update datanode_info
- Handle deleted data nodes using check and revive function - check data node, each datanode, contents of each
- check DFS can be loaded
- Output directory for run
- Hashing to choose data node

- It should verify if the DFS can be loaded, by verifing the paths of the Data Nodes and Name Nodes. It must ensure and perform basic checks such as (but not limited to):
    - Metadata contents about the Data Nodes stored by the Name Node exist and are correct
    - Ensure that each Data Node contains all the blocks it should contain
    - Ensure that the Name Node contains the correct mapping of blocks to Data Nodes
    - Take suitable actions and attempt to restore the configuration in case of failures


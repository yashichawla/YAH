- Handle deleted data nodes using check and revive function - check data node, each datanode, contents of each
- check DFS can be loaded
- Output directory for run
- Add logging for name node
- Add logging for data node
- Hashing to choose data node

- It should verify if the DFS can be loaded, by verifing the paths of the Data Nodes and Name Nodes. It must ensure and perform basic checks such as (but not limited to):
    - Metadata contents about the Data Nodes stored by the Name Node exist and are correct
    - Ensure that each Data Node contains all the blocks it should contain
    - Ensure that the Name Node contains the correct mapping of blocks to Data Nodes
    - Take suitable actions and attempt to restore the configuration in case of failures

24th
Logging- writing into datanodex.txt, writing into namenode_log
logging- put, rm, cat, operaton, last_update_time
def (datanode_id, block_id, message)
    write into the corresponding log txt for x datanode
    message=f"{timestamp} {message-put/rm/cat} {block_id}"

def (message)
    log txt 
    message=f"{timestamp} {message-put/rm/cat} {last check time}"
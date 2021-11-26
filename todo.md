- Handle deleted data nodes using check and revive function - check data node, each datanode, contents of each

- check datanode log folder exist
- inside -num_datanode log files
- if any not exist, create new file
- for i in range(num_datanodes) 
- check dn folder exist
- if not exist, create folder
- call function revive_datanode(datanode_id)
- call update commands

def revive_datanode(datanode_id)
- from datanode_info.json, get all blocks inside datanode_id
- for block in blocks:
- find datanode which the block is present in, datanode!=datanode_id
- copy block from datanode to current datanode_id folder

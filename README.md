# YAH- Yet Another Hadoop
This project is done as a part of the course requirements of the Big Data course (UE19CS322) at PES University, Bengaluru. 

Yet Another Hadoop is a simulation of the Hadoop Distributed File System. It is capable of replication of data across multiple nodes, tracking of file metdatda, carrying out operations on the file system and running map reduce jobs.

# How to run Yet Another Hadoop

## Setup Yet Another Hadoop

1. Install the dependencies.
    ```bash
    pip3 install -r requirements.txt
    ```

2. Setup the Distributed File System. The configuration to setup a distributed file system is provided in the `config_sample.json` file.
    ```bash
    usage: setup.py [-h] --CONFIG CONFIG [--CLEANUP CLEANUP]

    Setup the DFS

    optional arguments:
    -h, --help            show this help message and exit
    --CONFIG CONFIG, -f CONFIG
                            Path to the config file
    --CLEANUP CLEANUP, -c CLEANUP
                            Overwrite existing DFS
    ```
    Example:
    ```bash
    python3 setup.py --CONFIG config_sample.json --CLEANUP True
    ```

3. Running the `setup.py` script will generate a configuration file to load the distributed file system from. A sample one is provided in `dfs_setup.json`. Run the CLI to access the distributed file system.
    ```bash
    usage: hdfs.py [-h] --CONFIG CONFIG

    Load up the DFS

    optional arguments:
    -h, --help            show this help message and exit
    --CONFIG CONFIG, -f CONFIG
                            Path to the DFS config file
    ```
    Example:
    ```bash
    python3 hdfs.py --CONFIG dfs_setup.json
    ```

## Manipulate the Distributed File System

Listed below are the commands that can be run on the distributed file system. **Ensure that all paths used are absolute paths always**.

### `put`

Move a file to the distributed file system.

```bash
usage: put SOURCE_FILE_IN_LOCAL DESTINATION_FILE_IN_DFS
```

### `mkdir`

Create a directory in the distributed file system.

```bash
usage: mkdir DIR_NAME
```

### `rm`

Remove a file from the distributed file system.

```bash
usage: rm FILE_NAME
```

### `rmdir`

Remove a directory recursively from a distributed file system.

```bash
usage: rmdir DIR_NAME
```

### `cat`

View the contents of a file in the distributed file system.

```bash
usage: cat FILE_NAME
```

### `ls`

List all the files and folders in a directory in the distributed file system.

```bash
usage: ls DIR_NAME [-r RECURSIVE]
```

## Running Map-Reduce Jobs

Map-Reduce jobs can be run on the distributed file system via the CLI. The following command can be used to run a map-reduce job.

```bash
run -i INPUT_FILE_IN_DFS -o OUTPUT_FILE_IN_DFS -m MAPPER_FILENAME -r REDUCER_FILENAME
```
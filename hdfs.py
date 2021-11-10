import json
import time
import pytz
import shutil
import timeloop
import datetime
import argparse
from pathlib import Path

args = None
TIMED_TASK_LOOP = timeloop.Timeloop()
IST = pytz.timezone('Asia/Kolkata')


def load_config_from_json(filepath):
    global args
    with open(filepath, 'r') as f:
        config = json.load(f)
    for key, value in config.items():
        if isinstance(value, int):
            exec(f"args.{key.upper()} = {int(value)}")
        else:
            exec(f"args.{key.upper()} = '{value}'")


def load_args():
    global args
    parser = argparse.ArgumentParser(description='Load up the DFS')
    parser.add_argument('--CONFIG', '-c', type=str,
                        required=True, help='Path to the DFS config file')
    args = parser.parse_args()
    load_config_from_json(args.CONFIG)
    print(args)


def update_secondary_namenode():
    secondary_namenode_path = Path(
        args.PATH_TO_NAMENODES).joinpath(args.SECONDARY_NAMENODE_NAME)
    if not secondary_namenode_path.exists():
        secondary_namenode_path.mkdir(parents=True)

    files_in_secondary_namenode = secondary_namenode_path.glob('**/*')
    files_in_secondary_namenode = list(
        filter(Path.is_file, files_in_secondary_namenode))
    for filename in files_in_secondary_namenode:
        filename.unlink()

    primary_namenode_path = Path(
        args.PATH_TO_NAMENODES).joinpath(args.PRIMARY_NAMENODE_NAME)
    files_in_primary_namenode = primary_namenode_path.glob('**/*')
    files_in_primary_namenode = list(
        filter(Path.is_file, files_in_primary_namenode))

    for filename in files_in_primary_namenode:
        root_name = filename.name
        destination_path = str(secondary_namenode_path.joinpath(root_name))
        filename = str(filename)
        shutil.copy(filename, destination_path)


def create_namenode_checkpoints():
    current_time = datetime.datetime.now(IST)
    checkpoint_name = f"CHECKPOINT_{current_time}"
    checkpoint_path = Path(
        args.NAMENODE_CHECKPOINTS_PATH).joinpath(checkpoint_name)
    checkpoint_path.mkdir(parents=True)

    primary_namenode_path = Path(
        args.PATH_TO_NAMENODES).joinpath(args.PRIMARY_NAMENODE_NAME)
    files_in_primary_namenode = primary_namenode_path.glob('**/*')
    files_in_primary_namenode = list(
        filter(Path.is_file, files_in_primary_namenode))

    for filename in files_in_primary_namenode:
        root_name = filename.name
        destination_path = str(checkpoint_path.joinpath(root_name))
        filename = str(filename)
        shutil.copy(filename, destination_path)


def update_namenode_datanode_info():
    with open(Path(args.PATH_TO_NAMENODES).joinpath(args.PRIMARY_NAMENODE_NAME).joinpath(args.DATANODE_INFO_FILENAME), 'r') as f:
        datanode_info = json.load(f)

    for i in range(args.NUM_DATANODES):
        datanode_name = f"DATANODE{i}"
        datanode_path = Path(
            args.PATH_TO_DATANODES).joinpath(datanode_name)
        # handle deleted data node
        blocks_in_datanode = datanode_path.glob('**/*')
        blocks_in_datanode = list(filter(Path.is_file, blocks_in_datanode))
        blocks_in_datanode = list(
            map(lambda x: str(x.name), blocks_in_datanode))
        datanode_info[datanode_name] = blocks_in_datanode

    with open(Path(args.PATH_TO_NAMENODES).joinpath(args.PRIMARY_NAMENODE_NAME).joinpath(args.DATANODE_INFO_FILENAME), 'w') as f:
        json.dump(datanode_info, f)


def update_namenode_block_info():
    with open(Path(args.PATH_TO_NAMENODES).joinpath(args.PRIMARY_NAMENODE_NAME).joinpath(args.DATANODE_INFO_FILENAME), 'r') as f:
        datanode_info = json.load(f)

    block_info = dict()
    for datanode_id in datanode_info:
        for block_id in datanode_info[datanode_id]:
            if block_id not in block_info:
                block_info[block_id] = list()
            block_info[block_id].append(datanode_id)

    with open(Path(args.PATH_TO_NAMENODES).joinpath(args.PRIMARY_NAMENODE_NAME).joinpath(args.BLOCK_INFO_FILENAME), 'w') as f:
        json.dump(block_info, f)


load_args()


@ TIMED_TASK_LOOP.job(interval=datetime.timedelta(seconds=5))
def update_namenode():
    update_namenode_datanode_info()
    update_namenode_block_info()
    update_secondary_namenode()
    create_namenode_checkpoints()


TIMED_TASK_LOOP.start()

while True:
    _input = input(">>> ")
    if _input.lower() == "q":
        TIMED_TASK_LOOP.stop()
        exit(0)

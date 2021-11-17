import math
import json
import time
import pytz
import shutil
import random
import timeloop
import datetime
import argparse
from pathlib import Path
from fsplit.filesplit import Filesplit

args = None
file_splitter = Filesplit()
TIMED_TASK_LOOP = timeloop.Timeloop()
IST = pytz.timezone('Asia/Kolkata')


def load_config_from_json(filepath):
    global args
    with open(filepath, 'r') as f:
        config = json.load(f)
    for key, value in config.items():
        if isinstance(value, int):
            exec(f"args.{key.upper()} = {int(value)}")
        elif isinstance(value, float):
            exec(f"args.{key.upper()} = {float(value)}")
        else:
            exec(f"args.{key.upper()} = '{value}'")


def load_args():
    global args
    parser = argparse.ArgumentParser(description='Load up the DFS')
    parser.add_argument('--CONFIG', '-f', type=str,
                        required=True, help='Path to the DFS config file')
    args = parser.parse_args()
    load_config_from_json(args.CONFIG)

    with open(Path(args.PATH_TO_NAMENODES).joinpath(args.PRIMARY_NAMENODE_NAME).joinpath(
            args.FILESYSTEM_INFO_FILENAME), 'r') as f:
        args.FILESYSTEM = json.load(f)

    with open(Path(args.PATH_TO_NAMENODES).joinpath(args.PRIMARY_NAMENODE_NAME).joinpath(args.DATANODE_INFO_FILENAME), 'r') as f:
        args.DATANODE_INFO = json.load(f)

    with open(Path(args.PATH_TO_NAMENODES).joinpath(args.PRIMARY_NAMENODE_NAME).joinpath(args.BLOCK_INFO_FILENAME), 'r') as f:
        args.BLOCK_INFO = json.load(f)

    with open(Path(args.PATH_TO_NAMENODES).joinpath(args.PRIMARY_NAMENODE_NAME).joinpath(args.FILE_INFO_FILENAME), 'r') as f:
        args.FILE_INFO = json.load(f)

    # print(args)


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


def update_namenode_datanode_info_local():
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

    args.DATANODE_INFO = datanode_info


def update_namenode_block_info_local():
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

    args.BLOCK_INFO = block_info


def update_namenode_file_info():
    with open(Path(args.PATH_TO_NAMENODES).joinpath(args.PRIMARY_NAMENODE_NAME).joinpath(args.FILE_INFO_FILENAME), 'w') as f:
        json.dump(args.FILE_INFO, f)

    with open(Path(args.PATH_TO_NAMENODES).joinpath(args.PRIMARY_NAMENODE_NAME).joinpath(args.FILE_INFO_FILENAME), 'r') as f:
        args.FILE_INFO = json.load(f)


def update_namenode_datanode_info():
    with open(Path(args.PATH_TO_NAMENODES).joinpath(args.PRIMARY_NAMENODE_NAME).joinpath(args.DATANODE_INFO_FILENAME), 'w') as f:
        json.dump(args.DATANODE_INFO, f)

    with open(Path(args.PATH_TO_NAMENODES).joinpath(args.PRIMARY_NAMENODE_NAME).joinpath(args.DATANODE_INFO_FILENAME), 'r') as f:
        args.DATANODE_INFO = json.load(f)


def update_namenode_block_info():
    with open(Path(args.PATH_TO_NAMENODES).joinpath(args.PRIMARY_NAMENODE_NAME).joinpath(args.BLOCK_INFO_FILENAME), 'w') as f:
        json.dump(args.BLOCK_INFO, f)

    with open(Path(args.PATH_TO_NAMENODES).joinpath(args.PRIMARY_NAMENODE_NAME).joinpath(args.BLOCK_INFO_FILENAME), 'r') as f:
        args.BLOCK_INFO = json.load(f)


def update_namenode_filesystem_info():
    with open(Path(args.PATH_TO_NAMENODES).joinpath(args.PRIMARY_NAMENODE_NAME).joinpath(args.FILESYSTEM_INFO_FILENAME), 'w') as f:
        json.dump(args.FILESYSTEM, f)

    with open(Path(args.PATH_TO_NAMENODES).joinpath(args.PRIMARY_NAMENODE_NAME).joinpath(args.FILESYSTEM_INFO_FILENAME), 'r') as f:
        args.FILESYSTEM = json.load(f)


def get_file_block_details(file_path):
    # print(file_path)
    file_path = Path(file_path)
    if file_path.exists() and file_path.is_file():
        # handle 1024?
        file_size = file_path.stat().st_size
        # print(file_size)
        num_blocks = math.ceil(
            file_size / (args.BLOCK_SIZE * args.BLOCK_SIZE_UNIT))
        return num_blocks, file_size
    else:
        return None, None


def check_path_exists_in_hdfs(path):
    components = path.split('/')
    components = list(filter(bool, components))  # A/B/script.py
    # print(components)
    current = args.FILESYSTEM
    # print(args.FILESYSTEM)
    for index, component in enumerate(components):
        if component in current:
            if current[component] is None:  # and component == components[-1]:
                return True
            current = current[component]
        else:
            return False
    return True


def create_path_in_hdfs(destination_file_path):
    if not check_path_exists_in_hdfs(destination_file_path):
        components = destination_file_path.split('/')
        components = list(filter(bool, components))
        current = args.FILESYSTEM
        for component in components:
            if component not in current:
                current[component] = dict()
            current = current[component]
        return True
    else:
        return False


def create_file_in_hdfs(destination_file_path):
    if not check_path_exists_in_hdfs(destination_file_path):
        components = destination_file_path.split('/')
        components = list(filter(bool, components))
        filename = components[-1]
        current = args.FILESYSTEM
        for component in components[:-1]:
            if component not in current:
                current[component] = dict()
            current = current[component]
        current[filename] = None
        return True
    else:
        return False


def choose_datanode(mode="least"):
    all_datanodes = list(args.DATANODE_INFO.keys())
    datanode_capacities = list(
        map(lambda x: len(args.DATANODE_INFO[x]), all_datanodes))
    datanodes = list(zip(all_datanodes, datanode_capacities))
    available_datanodes = list(
        filter(lambda x: x[1] <= args.DATANODE_SIZE, datanodes))
    if not available_datanodes:
        return None
    else:
        if mode == "least":
            available_datanodes.sort(key=lambda x: x[1])
            return available_datanodes[0][0]
        elif mode == "random":
            return random.choice(available_datanodes)[0]
        elif mode == 'hashing':
            # TODO
            pass


def mkdir(*vargs):
    path = vargs[0]
    status = create_path_in_hdfs(path)
    if status:
        update_namenode_filesystem_info()
        return True
    else:
        print(f"Error: Directory {path} already exists.")
        return False


def ls(*vargs):
    path = vargs[0]
    # check if path exists
    # if path exists, navigate into path, print keys
    # else print path not found


def rm(*vargs):
    path = vargs[0]

    # check if path exists
    # if path exists, navigate into path, del key
    # else print path not found


def rmdir(*vargs):
    path = vargs[0]

    # check if path exists
    # if path exists, navigate into path, del key
    # else print path not found


def put(*vargs):
    source_file_path, destination_file_path = vargs
    num_blocks, file_size = get_file_block_details(source_file_path)
    # print(num_blocks, file_size)
    if num_blocks is None:
        return False
    else:
        check_file_created = create_file_in_hdfs(destination_file_path)
        # print(check_file_created, args.FILESYSTEM)
        if check_file_created:
            Path("./temp").mkdir(parents=True, exist_ok=True)

            file_splitter.split(
                file=source_file_path,
                split_size=args.BLOCK_SIZE * args.BLOCK_SIZE_UNIT,
                output_dir="./temp",
                newline=True
            )

            existing_file_ids = list(args.FILE_INFO.keys())
            if existing_file_ids:
                existing_file_ids = list(
                    map(lambda x: int(x[-3:]), existing_file_ids))
                incremented_id = str(max(existing_file_ids) + 1).zfill(3)
                new_file_id = f"FILE_{incremented_id}"
            else:
                new_file_id = "FILE_000"

            args.FILE_INFO[new_file_id] = {
                "file_path": destination_file_path,
                "num_blocks": num_blocks,
                "file_size": file_size
            }

            update_namenode_file_info()
            update_namenode_filesystem_info()

            block_root_path = Path("./temp")
            block_filename = Path(source_file_path).stem
            block_suffix = Path(source_file_path).suffix
            for i in range(1, num_blocks+1):
                current_block_name = f"{block_filename}_{i}"
                if block_suffix:
                    current_block_name += block_suffix
                # print(current_block_name)
                current_block_path = block_root_path.joinpath(
                    current_block_name)
                # print(current_block_path)
                destination_block_name = f"{new_file_id}__{i}"
                for j in range(args.REPLICATION_FACTOR):
                    datanode_id = choose_datanode()
                    if datanode_id is None:
                        print("Memory full")
                        break
                    else:
                        # print(args.PATH_TO_DATANODES,
                        #   type(args.PATH_TO_DATANODES))
                        path_to_datanode_block = Path(
                            args.PATH_TO_DATANODES).joinpath(datanode_id).joinpath(destination_block_name)
                        # update namenode info
                        # update file info
                        shutil.copy(str(current_block_path),
                                    str(path_to_datanode_block))
                        if destination_block_name not in args.BLOCK_INFO:
                            args.BLOCK_INFO[destination_block_name] = list()
                        args.BLOCK_INFO[destination_block_name].append(
                            datanode_id)
                        args.DATANODE_INFO[datanode_id].append(
                            destination_block_name)
            shutil.rmtree(block_root_path)
            update_namenode_block_info()
            update_namenode_datanode_info()
            return True
        else:
            print("File already exists")
            return False


load_args()
# args.SYNC_PERIOD in below task loop


@ TIMED_TASK_LOOP.job(interval=datetime.timedelta(seconds=60))
def update_namenode():
    update_namenode_datanode_info_local()
    update_namenode_block_info_local()
    update_secondary_namenode()
    create_namenode_checkpoints()


TIMED_TASK_LOOP.start()


command_map = {
    "put": put,
    # "rm": rm,
    "mkdir": mkdir,
    # "rmdir": rmdir,
    # "ls": ls,
    # "cat": cat
}


def process_input(_input):
    components = _input.split()
    command_string = components[0]
    _function = command_map.get(command_string)
    if _function is None:
        print(
            f"Invalid command. Valid commands are: {list(command_map.keys())}")
        return
    else:
        _function(*components[1:])


while True:
    _input = input("(hdfs) > ")
    if _input.lower() == "q":
        TIMED_TASK_LOOP.stop()
        exit(0)
    process_input(_input)

import os
import math
import json
import time
import pytz
import shutil
import random
import timeloop
import datetime
import argparse
import logging
import pprint
from pathlib import Path
from threading import Thread
from fsplit.filesplit import Filesplit
from multiprocessing import Process, Manager

args = None
file_splitter = Filesplit()
TIMED_TASK_LOOP = timeloop.Timeloop()
IST = pytz.timezone('Asia/Kolkata')
JOB_OUTPUT = list()
logging.getLogger("timeloop").setLevel(logging.CRITICAL)


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


def run_mapper(mapper_filename, input_filename, index):
    global JOB_OUTPUT
    output = os.popen(f'cat {input_filename} | python3 {mapper_filename}')
    JOB_OUTPUT.append((output.read().strip(), index))


def aggregate_and_sort(outputs):
    output = list()
    for o in outputs:
        output.extend(o[0].split('\n'))
    output.sort()
    return output


def run_reducer(reducer_filename, outputs, output_filename):
    global JOB_OUTPUT
    command = f"echo -e '{outputs}' | python3 {reducer_filename}"
    output = os.popen(command)
    with open('job_output', 'w') as f:
        f.write(output.read().strip())
    put('job_output', output_filename)


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
    file_path = Path(file_path)
    if file_path.exists() and file_path.is_file():
        # handle 1024?
        file_size = file_path.stat().st_size
        return file_size
    else:
        return None


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
    if vargs:
        path = vargs[0]
        if path == '.':
            path = '/'
    else:
        path = '/'
    recursive = False
    if len(vargs) > 1 and vargs[1].lower() == '-r':
        recursive = True
    status = check_path_exists_in_hdfs(path)
    if status:
        components = path.split('/')
        components = list(filter(bool, components))
        if components:
            current = args.FILESYSTEM
            for index, component in enumerate(components):
                if component in current:
                    current = current[component]
            if current is None:
                print(f"Error: Path {path} is a file, not a directory")
                return False
            else:
                if not recursive:
                    ls_items = '\n'.join(list(current.keys()))
                    print(ls_items)
                else:
                    print(json.dumps(current, indent=4))
                return True
        else:
            if not recursive:
                ls_items = '\n'.join(list(args.FILESYSTEM.keys()))
                print(ls_items)
            else:
                print(json.dumps(args.FILESYSTEM, indent=4))
            return True
    else:
        print(f"Error: Path {path} not found.")
        return False


def rm(*vargs):
    path = vargs[0]

    # check if path exists
    # if path exists, navigate into path, del key
    # delete from file_info also
    # else print path not found


def rmdir(*vargs):
    path = vargs[0]

    # check if path exists
    # if path exists, navigate into path, del key
    # del from file_info also
    # else print path not found


def get_file_id_from_hdfs_file_path(file_path):
    for file_id in args.FILE_INFO:
        if args.FILE_INFO[file_id]['file_path'] == file_path:
            return file_id
    return None


def get_datanode_id_from_block_id(block_id):
    if block_id in args.BLOCK_INFO:
        possible_datanodes = args.BLOCK_INFO[block_id]
        for datanode_id in possible_datanodes:
            if Path(args.PATH_TO_DATANODES).joinpath(datanode_id).joinpath(block_id).exists():
                return datanode_id
        return None
    else:
        return None


def put(*vargs):
    source_file_path, destination_file_path = vargs
    file_size = get_file_block_details(source_file_path)
    if file_size is None:
        print(f"Error: {source_file_path} not found")
        return False
    else:
        check_file_created = create_file_in_hdfs(destination_file_path)
        # print(check_file_created, args.FILESYSTEM)
        if check_file_created:
            Path("./temp").mkdir(parents=True, exist_ok=True)

            file_splitter.split(
                file=source_file_path,
                split_size=int(args.BLOCK_SIZE * args.BLOCK_SIZE_UNIT),
                output_dir="./temp",
                newline=True
            )
            num_blocks = len(list(Path("./temp").glob("*"))) - 1

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


def cat(*vargs):
    file_path = vargs[0]
    if check_path_exists_in_hdfs(file_path):
        file_id = get_file_id_from_hdfs_file_path(file_path)
        if file_id is None:
            print("Error: File not found.")
            return False
        num_blocks = args.FILE_INFO[file_id]['num_blocks']
        block_id = [f"{file_id}__{bid}" for bid in range(1, num_blocks+1)]
        block_paths = dict()
        for block_id in block_id:
            datanode_id = get_datanode_id_from_block_id(block_id)
            block_paths[block_id] = Path(
                args.PATH_TO_DATANODES).joinpath(datanode_id).joinpath(block_id)

        if None in block_paths.values():
            print("Error: Missing blocks or file not found.")
            return False
        else:
            block_paths = sorted(block_paths.items(),
                                 key=lambda x: int(x[0].split('__')[1]))
            for _, block_path in block_paths:
                with open(block_path, "r") as f:
                    print(f.read().strip())
            return True
    else:
        print(f"File {file_path} not found.")
        return False


def run(*vargs):
    global JOB_OUTPUT
    JOB_OUTPUT = list()
    JOB_OUTPUT.clear()

    run_parser = argparse.ArgumentParser()
    run_parser.add_argument(
        "--input", '-i', help="Input file path", type=str)
    run_parser.add_argument(
        "--output", '-o', help="Output file path", type=str)
    run_parser.add_argument(
        "--mapper", '-m', help="Mapper file path", type=str)
    run_parser.add_argument(
        "--reducer", '-r', help="Reducer file path", type=str)
    run_args = run_parser.parse_args(vargs)

    input_file_path = run_args.input
    output_file_path = run_args.output
    mapper_file_path = run_args.mapper
    reducer_file_path = run_args.reducer

    if check_path_exists_in_hdfs(input_file_path) and Path(mapper_file_path).exists() and Path(reducer_file_path).exists():
        file_id = get_file_id_from_hdfs_file_path(input_file_path)
        if file_id is None:
            print("Error: File not found.")
            return False
        else:
            num_blocks = args.FILE_INFO[file_id]['num_blocks']
            block_id = [f"{file_id}__{bid}" for bid in range(1, num_blocks+1)]
            block_paths = dict()
            for block_id in block_id:
                datanode_id = get_datanode_id_from_block_id(block_id)
                block_paths[block_id] = Path(
                    args.PATH_TO_DATANODES).joinpath(datanode_id).joinpath(block_id)
            if None in block_paths.values():
                print("Error: Missing blocks or file not found.")
                return False
            else:
                current_threads = list()
                for thread_index, block_path in enumerate(block_paths.values()):
                    current_thread = Thread(
                        target=run_mapper, args=(mapper_file_path, block_path, thread_index))
                    current_thread.start()
                    current_threads.append(current_thread)
                for current_thread in current_threads:
                    current_thread.join()
                current_threads.clear()

                JOB_OUTPUT = sorted(JOB_OUTPUT, key=lambda x: x[1])
                JOB_OUTPUT = '\n'.join(aggregate_and_sort(JOB_OUTPUT))
                run_reducer(reducer_file_path, JOB_OUTPUT, output_file_path)
    else:
        # find which file is not found
        print("Error: File not found.")
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
    "ls": ls,
    "cat": cat,
    "run": run
}


def process_input(_input):
    components = _input.split()
    command_string = components[0]
    _function = command_map.get(command_string)
    if _function is None:
        possible_commands = ', '.join(list(command_map.keys()))
        print(
            f"Invalid command. Valid commands are: {possible_commands}")
        return
    else:
        _function(*components[1:])


while True:
    _input = input("(hdfs) > ")
    if _input.lower() == "q":
        TIMED_TASK_LOOP.stop()
        exit(0)
    try:
        process_input(_input)
    except Exception as error_message:
        print(f"ERROR: {error_message}")

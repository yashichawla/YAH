import os
import pytz
import json
import argparse
import datetime
from pathlib import Path

args = None
IST = pytz.timezone("Asia/Kolkata")


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
    parser = argparse.ArgumentParser(description='Setup the DFS')
    parser.add_argument('--CONFIG', '-f', type=str,
                        required=True, help='Path to the config file')
    parser.add_argument('--CLEANUP', '-c', type=bool, required=False,
                        default=False, help='Overwrite existing DFS')
    args = parser.parse_args()
    load_config_from_json(args.CONFIG)
    # print(args)


def check_dfs_exists():
    return Path(args.PATH_TO_DATANODES).exists()


def setup_dfs_directories():
    for arg_key in args.__dict__:
        arg_value = args.__dict__[arg_key]
        if isinstance(arg_value, str) and "PATH" in arg_key and arg_key != 'FS_PATH':
            p = Path(arg_value)
            if p.suffix:
                p.touch(exist_ok=True)
            else:
                p.mkdir(parents=True, exist_ok=True)


def create_namenodes():
    for node_name in [args.PRIMARY_NAMENODE_NAME, args.SECONDARY_NAMENODE_NAME]:
        Path(args.PATH_TO_NAMENODES).joinpath(
            node_name).mkdir(parents=True, exist_ok=True)

    with open(Path(args.PATH_TO_NAMENODES).joinpath(args.PRIMARY_NAMENODE_NAME).joinpath(
            args.FILE_INFO_FILENAME), 'w') as f:
        json.dump(dict(), f)

    with open(Path(args.PATH_TO_NAMENODES).joinpath(args.PRIMARY_NAMENODE_NAME).joinpath(
            args.BLOCK_INFO_FILENAME), 'w') as f:
        json.dump(dict(), f)

    Path(args.PATH_TO_NAMENODES).joinpath(args.PRIMARY_NAMENODE_NAME).joinpath(
        args.DATANODE_INFO_FILENAME).touch(exist_ok=True)

    with open(Path(args.PATH_TO_NAMENODES).joinpath(args.PRIMARY_NAMENODE_NAME).joinpath(
            args.FILESYSTEM_INFO_FILENAME), 'w') as f:
        json.dump(dict(), f)


def create_datanodes():
    DATANODE_DICT = dict()
    for i in range(args.NUM_DATANODES):
        Path(args.PATH_TO_DATANODES).joinpath(
            f"DATANODE{i}").mkdir(parents=True, exist_ok=True)
        Path(args.DATANODE_LOG_PATH).joinpath(
            f"DATANODE{i}_LOG.txt").touch(exist_ok=True)
        DATANODE_DICT[f"DATANODE{i}"] = list()
    with open(Path(args.PATH_TO_NAMENODES).joinpath(args.PRIMARY_NAMENODE_NAME).joinpath(args.DATANODE_INFO_FILENAME), "w") as f:
        json.dump(DATANODE_DICT, f)


def create_dfs_setup_config():
    DFS_SETUP_CONFIG_DICT = dict()
    DFS_SETUP_CONFIG_DICT.update(args.__dict__)
    with open(args.DFS_SETUP_CONFIG, "w") as f:
        DFS_SETUP_CONFIG_DICT["TIMESTAMP"] = str(datetime.datetime.now(IST))
        DFS_SETUP_CONFIG_DICT["NUM_LOAD"] = 0
        json.dump(DFS_SETUP_CONFIG_DICT, f, indent=4)


load_args()

if args.CLEANUP:
    os.system("bash clean.sh")
elif check_dfs_exists():
    print('DFS already exists')
    exit(1)

setup_dfs_directories()
create_namenodes()
create_datanodes()
create_dfs_setup_config()

# add try-except to each function call
# if error caught, print error in X
# else, print success

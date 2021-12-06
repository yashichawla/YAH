#! /usr/bin/env python3

import os
import pytz
import json
import shutil
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
                        default="../config/config_sample.json", help='Path to the config file')
    parser.add_argument('--CLEANUP', '-c', type=bool, required=False,
                        default=False, help='Overwrite existing DFS')
    args = parser.parse_args()
    load_config_from_json(args.CONFIG)


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


def clean_up():
    if Path(args.PATH_TO_DATANODES).exists():
        shutil.rmtree(args.PATH_TO_DATANODES)
    if Path(args.PATH_TO_NAMENODES).exists():
        shutil.rmtree(args.PATH_TO_NAMENODES)
    if Path(args.CONFIG_LOG_PATH).exists():
        Path(args.CONFIG_LOG_PATH).unlink()


def check_missing_args():
    configs = [
        'BLOCK_SIZE', 
        'BLOCK_SIZE_UNIT', 
        'PATH_TO_DATANODES', 
        'PATH_TO_NAMENODES', 
        'REPLICATION_FACTOR', 
        'NUM_DATANODES', 
        'DATANODE_SIZE', 
        'SYNC_PERIOD', 
        'DATANODE_LOG_PATH', 
        'NAMENODE_LOG_PATH', 
        'CONFIG_LOG_PATH', 
        'NAMENODE_CHECKPOINTS_PATH', 
        'FS_PATH', 
        'DFS_SETUP_CONFIG', 
        'PRIMARY_NAMENODE_NAME', 
        'SECONDARY_NAMENODE_NAME', 
        'FILE_INFO_FILENAME', 
        'BLOCK_INFO_FILENAME', 
        'DATANODE_INFO_FILENAME', 
        'FILESYSTEM_INFO_FILENAME'
    ]
    for config in configs:
        if config not in args.__dict__:
            print(f"Error: Cannot find the following config: {config}")
            exit(1)

load_args()
check_missing_args()

if args.CLEANUP:
    print("Overwriting existing DFS setup...")
    clean_up()
elif check_dfs_exists():
    print('DFS already exists in location. Try another location or use the -c flag to overwrite.')
    exit(1)

try:
    print("Setting up directories...")
    setup_dfs_directories()
except Exception as error_message:
    print(f"Error while setting up DFS directories: {error_message}")
    clean_up()
    exit(1)

try:
    print("Creating Name Nodes...")
    create_namenodes()
except Exception as error_message:
    print(f"Error while setting up Name Nodes")
    clean_up()
    exit(1)

try:
    print("Creating Data Nodes...")
    create_datanodes()
except Exception as error_message:
    print(f"Error while setting up Data Nodes: {error_message}")
    clean_up()
    exit(1)


try:
    print("Creating DFS Configuration Variables...")
    create_dfs_setup_config()
except Exception as error_message:
    print(f"Error while creating Configuration Variables: {error_message}")
    clean_up()
    exit(1)

print("DFS setup successful!")

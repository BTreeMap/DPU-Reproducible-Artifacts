#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import shutil
import subprocess
import logging

def run_command(command, check=True, shell=False):
    """Run a shell command."""
    logging.info(f"Running command: {' '.join(command) if isinstance(command, list) else command}")
    subprocess.run(command, check=check, shell=shell)

def remove_directory(path):
    """Remove the specified directory if it exists, handling permission errors."""
    if os.path.exists(path):
        for root, dirs, files in os.walk(path):
            for name in dirs:
                try:
                    os.chmod(os.path.join(root, name), 0o777)
                except Exception as e:
                    logging.warning(f"Failed to change permission for directory {name}: {e}")
            for name in files:
                try:
                    os.chmod(os.path.join(root, name), 0o777)
                except Exception as e:
                    logging.warning(f"Failed to change permission for file {name}: {e}")
        try:
            shutil.rmtree(path)
            logging.info(f"Removed directory: {path}")
        except Exception as e:
            logging.error(f"Failed to remove directory {path}: {e}")
    else:
        logging.info(f"Directory not found: {path}")

def remove_file(path):
    """Remove the specified file if it exists."""
    if os.path.exists(path):
        try:
            os.remove(path)
            logging.info(f"Removed file: {path}")
        except Exception as e:
            logging.error(f"Failed to remove file {path}: {e}")
    else:
        logging.info(f"File not found: {path}")

def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    
  
    script_dir = os.path.dirname(os.path.abspath(__file__))
    logging.info(f"Script directory: {script_dir}")

   
    directories_to_clean = [
        os.path.join(script_dir, "datasets"),
        os.path.join(script_dir, "results"),
        os.path.join(script_dir, "queries"),
        os.path.join(script_dir, "output")
    ]
    
    for directory in directories_to_clean:
        logging.info(f"Cleaning directory: {directory}")
        remove_directory(directory)
    

    duckdb_path = os.path.join(script_dir, "duckdb")
    logging.info(f"Cleaning duckdb binary: {duckdb_path}")
    remove_file(duckdb_path)

    tpch_dbgen_dir = os.path.join(script_dir, "tpch-dbgen")
    logging.info(f"Cleaning tpch-dbgen directory: {tpch_dbgen_dir}")
    remove_directory(tpch_dbgen_dir)
    
    logging.info("Cleanup complete. All generated data and specified packages have been removed.")

if __name__ == "__main__":
    main()

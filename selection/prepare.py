#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import subprocess
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def run_command(command, check=True, shell=False):
    """Run a shell command."""
    logger.info(f"Running command: {' '.join(command) if isinstance(command, list) else command}")
    result = subprocess.run(command, check=check, shell=shell, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    return result

def install_packages():
    """Install required packages for the benchmark."""
    # Update package list
    try:
        run_command(['sudo', 'apt', 'update'])
        
        # Install required system packages
        packages = ['python3-pip', 'python3-dev', 'build-essential']
        run_command(['sudo', 'apt', 'install', '-y'] + packages)
        
        # Upgrade pip
        run_command([sys.executable, '-m', 'pip', 'install', '--upgrade', 'pip'])
        
        # Install required Python packages
        python_packages = ['duckdb', 'psutil']
        run_command([sys.executable, '-m', 'pip', 'install'] + python_packages)
        
        logger.info("All required packages installed successfully")
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to install packages: {e}")
        logger.error(f"Error output: {e.stderr.decode() if hasattr(e, 'stderr') else 'No error output'}")
        sys.exit(1)

def download_duckdb():
    """Download DuckDB CLI using pip."""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    duckdb_path = os.path.join(script_dir, "duckdb")
    
    if os.path.exists(duckdb_path):
        logger.info("DuckDB CLI already exists, skipping download.")
        return
    
    logger.info("Installing DuckDB using pip...")
    try:
        # Install or upgrade DuckDB using pip
        run_command([sys.executable, '-m', 'pip', 'install', 'duckdb', '--upgrade'])
        
        # Create a simple wrapper script for DuckDB CLI
        with open(duckdb_path, 'w') as f:
            f.write('#!/usr/bin/env python3\n')
            f.write('import sys\n')
            f.write('import duckdb\n\n')
            f.write('if __name__ == "__main__":\n')
            f.write('    args = sys.argv[1:]\n')
            f.write('    conn = duckdb.connect()\n')
            f.write('    if len(args) > 0 and args[0] == "-c":\n')
            f.write('        conn.execute(" ".join(args[1:]))\n')
            f.write('    else:\n')
            f.write('        print("DuckDB CLI wrapper. Use -c to execute SQL.")\n')
        
        # Make the wrapper script executable
        os.chmod(duckdb_path, 0o755)
        logger.info("DuckDB installed successfully and CLI wrapper created.")
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to install DuckDB: {e}")
        logger.error(f"Error output: {e.stderr.decode() if hasattr(e, 'stderr') else 'No error output'}")
        sys.exit(1)

def download_and_setup_dbgen():
    """Download and set up TPC-H dbgen tool if not already present."""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    dbgen_dir = os.path.join(script_dir, "tpch-dbgen")
    
    if os.path.exists(dbgen_dir):
        logger.info(f"TPC-H dbgen directory already exists at {dbgen_dir}")
    else:
        # Clone the TPC-H dbgen repository
        logger.info("Downloading TPC-H dbgen from GitHub...")
        try:
            run_command(['git', 'clone', 'https://github.com/electrum/tpch-dbgen.git', dbgen_dir])
            logger.info("TPC-H dbgen downloaded successfully.")
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to download TPC-H dbgen: {e}")
            logger.error(f"Error output: {e.stderr.decode() if hasattr(e, 'stderr') else 'No error output'}")
            sys.exit(1)
    
    # Compile dbgen
    dbgen_exe = os.path.join(dbgen_dir, "dbgen")
    if os.path.exists(dbgen_exe):
        logger.info("TPC-H dbgen is already compiled.")
    else:
        logger.info("Compiling TPC-H dbgen...")
        try:
            # Save current working directory
            cwd = os.getcwd()
            
            # Change to the dbgen directory
            os.chdir(dbgen_dir)
            
            # Create Makefile.custom with correct settings
            logger.info("Creating Makefile.custom...")
            with open("Makefile.custom", "w") as f:
                f.write("CC = gcc\n")
                f.write("DATABASE = ORACLE\n")
                f.write("MACHINE = LINUX\n")
                f.write("WORKLOAD = TPCH\n")
            
            # Run make directly in the dbgen directory
            logger.info("Running make in tpch-dbgen directory...")
            make_result = subprocess.run(
                ['make'],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=False  # Don't exit on error, we'll handle it
            )
            
            if make_result.returncode != 0:
                logger.warning(f"Make returned non-zero code: {make_result.returncode}")
                logger.info(f"Make stdout: {make_result.stdout.decode()}")
                logger.info(f"Make stderr: {make_result.stderr.decode()}")
                
                # Try with specific options
                logger.info("Trying alternative make command...")
                subprocess.run(['make', 'DATABASE=ORACLE', 'MACHINE=LINUX', 'WORKLOAD=TPCH'], check=False)
            
            # Return to original directory
            os.chdir(cwd)
            
            # Check if dbgen was created
            if os.path.exists(dbgen_exe):
                # Make it executable
                os.chmod(dbgen_exe, 0o755)
                logger.info("TPC-H dbgen compiled successfully.")
            else:
                logger.error("Failed to compile dbgen executable automatically.")
                logger.info("Please try to compile it manually with: cd tpch-dbgen && make")
        except Exception as e:
            # Make sure we go back to the original directory
            os.chdir(cwd)
            logger.error(f"Error compiling dbgen: {e}")
            logger.info("Please try to compile it manually with: cd tpch-dbgen && make")

def create_directories():
    """Create necessary directories for the benchmark."""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Create directories
    dirs = ["results", "datasets"]
    for directory in dirs:
        dir_path = os.path.join(script_dir, directory)
        os.makedirs(dir_path, exist_ok=True)
        logger.info(f"Created directory: {dir_path}")

def main():
    """Main function to prepare the benchmark environment."""
    logger.info("Starting preparation for the benchmark...")
    
    # Create necessary directories first
    logger.info("Creating necessary directories...")
    create_directories()
    
    # Install required packages
    logger.info("Installing required packages...")
    install_packages()
    
    # Download DuckDB CLI
    logger.info("Downloading DuckDB CLI...")
    download_duckdb()
    
    # Download and set up TPC-H dbgen
    logger.info("Setting up TPC-H dbgen...")
    download_and_setup_dbgen()
    
    logger.info("Preparation completed successfully. The environment is ready for benchmarking.")

if __name__ == "__main__":
    main()
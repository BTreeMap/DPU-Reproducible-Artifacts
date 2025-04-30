#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import subprocess
import json
import argparse
import logging
import time
import threading
import socket
import pickle
import duckdb
import psutil
import csv
import shutil
from multiprocessing import Process

# --------------
# Logging Setup
# --------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# -----------------
# Global Constants
# -----------------
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DUCKDB_CMD = os.path.join(SCRIPT_DIR, "duckdb")
DBGEN_DIR = os.path.join(SCRIPT_DIR, "tpch-dbgen")
DATASETS_DIR = os.path.join(SCRIPT_DIR, "datasets")
RESULTS_DIR = os.path.join(SCRIPT_DIR, "results")

TABLES = [
    "customer", "lineitem", "nation",
    "orders", "partsupp", "part",
    "region", "supplier"
]

# --------------------
# Argument Parsing
# --------------------
def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Run benchmark tests')
    parser.add_argument('--benchmark_items', type=str, help='Benchmark items to run (comma-separated)')
    parser.add_argument('--ScaleFactor', type=str, default="1", help='Scale factor for TPC-H data generation')
    parser.add_argument('--numProc', type=int, default=8, help='Number of processors/threads')
    parser.add_argument('--selectivity', type=str, default="1", help='Selectivity factor')
    parser.add_argument('--host_ip', type=str, default="0.0.0.0", help='Host IP address')
    parser.add_argument('--host_username', type=str, default="", help='Host username')
    parser.add_argument('--dpu_ip', type=str, default="127.0.0.1", help='DPU IP address')
    parser.add_argument('--port', type=str, default="9000", help='Port number')
    parser.add_argument('--metrics', type=str, help='Metrics to collect (JSON format)')
    return parser.parse_args()

# -------------------------
# Utility: Run Shell Command
# -------------------------
def run_command(command, check=True, shell=False):
    """Run a shell command."""
    logger.info(f"Running command: {' '.join(command) if isinstance(command, list) else command}")
    result = subprocess.run(command, check=check, shell=shell, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    return result

# -----------------------------------
# Step 1: Generate TPC-H Data (lineitem)
# -----------------------------------
# -----------------------------------
# Step 1: Generate TPC-H Data (lineitem) in TBL format
# -----------------------------------
def generate_tpch_data(scale_factor):
    logger.info(f"Generating TPC-H lineitem data (TBL format) with scale factor: {scale_factor}")
    
    try:
        sf_value = float(scale_factor)
        if sf_value < 1:
            logger.warning(f"Scale factor {sf_value} < 1. Using 1 as minimum.")
            sf_value = 1
    except ValueError:
        logger.error(f"Invalid scale factor: {scale_factor}. Using default value 1.")
        sf_value = 1

    original_dir = os.getcwd()

    for i in range(1, 11):
        dataset_dir = os.path.join(DATASETS_DIR, f"dataset_{i}")
        os.makedirs(dataset_dir, exist_ok=True)
        logger.info(f"===> Generating dataset #{i} with scale factor {sf_value}...")

  
        os.chdir(DBGEN_DIR)
        
        cmd = ["./dbgen", "-vf", "-s", str(sf_value), "-T", "L"]
        logger.info("Running command: " + " ".join(cmd))
        result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if result.returncode != 0:
            logger.error(f"dbgen for lineitem failed with error: {result.stderr.decode()}")

        src_tbl_file = os.path.join(DBGEN_DIR, "lineitem.tbl")
        dst_tbl_file = os.path.join(dataset_dir, "lineitem.tbl")
        if os.path.exists(src_tbl_file):
            try:
                shutil.move(src_tbl_file, dst_tbl_file)
                logger.info(f"Moved {src_tbl_file} to {dst_tbl_file}")
            except Exception as e:
                logger.error(f"Error moving {src_tbl_file} to {dst_tbl_file}: {e}")
        else:
            logger.warning(f"File {src_tbl_file} not found after dbgen for lineitem.")
        
 
        os.chdir(original_dir)
        logger.info(f"Done dataset #{i} in directory: {dataset_dir}\n")
    
    logger.info(f"All lineitem datasets generated under {DATASETS_DIR}")
    return DATASETS_DIR


# ------------------------------------
# Step 2: Generate SQL Queries (using TBL data)
# ------------------------------------
def generate_queries(selectivity):
    """Generate SQL query files for benchmarking (Query6 style) that run against TBL data."""
    logger.info("Generating SQL query files (using TBL data)...")
    
    query_dir = os.path.join(SCRIPT_DIR, "queries")
    os.makedirs(query_dir, exist_ok=True)
    
    for i in range(1, 11):
        query_file = os.path.join(query_dir, f"query6_{i}.sql")
        sel_factor = float(selectivity)
        
        query = f"""
        SELECT
            column06 AS revenue 
        FROM
            read_csv_auto('{os.path.join(DATASETS_DIR, f"dataset_{i}", "lineitem.tbl")}', delim='|')
        WHERE
            column04 < 6;
        """
        with open(query_file, 'w') as f:
            f.write(query)
        logger.info(f"Generated query file: {query_file}")
    
    return query_dir

   




# ------------------------------
# Step 3: Start DPU Server (Storage)
# ------------------------------
def start_dpu_server(host_ip, port, num_proc, scale_factor):
    logger.info(f"Starting DPU server on {host_ip}:{port} with {num_proc} threads and scale_factor={scale_factor}...")
    

    duckdb.default_connection().execute(f"PRAGMA threads={num_proc}")
    
    con = duckdb.connect(database=":memory:")
    
    process = psutil.Process()
    cpu_count = psutil.cpu_count(logical=True)
    
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    
    try:
        s.bind((host_ip, int(port)))
        s.listen(5)
        logger.info("[Storage] Listening for connections...")
        
        os.makedirs(RESULTS_DIR, exist_ok=True)
        result_file = os.path.join(RESULTS_DIR, f"dpu_results_{time.strftime('%Y%m%d_%H%M%S')}.csv")
        with open(result_file, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(["QueryID", "ExecutionTime", "RowsReturned", "ScannedRows", "CPUUsage", "WallClockTime"])
        
        while True:
            conn, addr = s.accept()
            logger.info(f"[Storage] Connected by {addr}")
            
            client_thread = threading.Thread(
                target=handle_client,
                args=(conn, addr, con, cpu_count, process, result_file, scale_factor)
            )
            client_thread.daemon = True
            client_thread.start()
    except KeyboardInterrupt:
        logger.info("DPU server stopping...")
    except Exception as e:
        logger.error(f"Server error: {e}")
    finally:
        s.close()

def handle_client(conn, addr, con, cpu_count, process, result_file, scale_factor):
    cpu_times_start = process.cpu_times()
    start_wall_time = time.time()
    
    try:
        data = conn.recv(4096)
        if not data:
            return
        
        query = data.decode("utf-8").strip()
        query_id = "unknown"
        
        if "query6_" in query:
            start_idx = query.find("query6_") + len("query6_")
            end_idx = query.find(".sql", start_idx)
            if end_idx > start_idx:
                query_id = query[start_idx:end_idx]
        
        logger.info(f"[Storage] Received query {query_id} from {addr}:\n  {query}")
        
        # ---------------------------
        # ---------------------------
        scanned_rows = 0
        lower_q = query.lower()
        
        if "lineitem" in lower_q:
            if scale_factor == 1:
                scanned_rows = 6001215
            elif scale_factor == 10:
                scanned_rows = 60012150
            else:
                scanned_rows = 6001215 * scale_factor
        
        query_start = time.time()
        result_data = con.execute(query).fetchall()
        query_end = time.time()
        query_time = query_end - query_start
        
        logger.info(f"[Storage] Query {query_id} executed in {query_time:.4f}s, returned rows={len(result_data)}.")
        
        payload_dict = {
            'rows': result_data,
            'scanned_rows': scanned_rows,
            'query_time': query_time
        }
        payload = pickle.dumps(payload_dict)
        conn.sendall(payload)
        logger.info(f"[Storage] Sent {len(payload)} bytes back to compute.")
        
    except Exception as e:
        logger.error(f"[Storage] Error while handling request from {addr}: {e}")
        query_time = 0
        result_data = []
        scanned_rows = 0
    finally:
        conn.close()
        
        cpu_times_end = process.cpu_times()
        end_wall_time = time.time()
        
        user_time_diff = cpu_times_end.user - cpu_times_start.user
        sys_time_diff = cpu_times_end.system - cpu_times_start.system
        wall_clock_diff = end_wall_time - start_wall_time
        
        if wall_clock_diff > 0:
            cpu_usage_pct = (user_time_diff + sys_time_diff) / (wall_clock_diff * cpu_count) * 100.0
        else:
            cpu_usage_pct = 0.0
        
        logger.info(f"[Storage] CPU usage for query {query_id}: {cpu_usage_pct:.2f}% (over ~{wall_clock_diff:.3f}s)")
        
        # 写入 CSV
        try:
            with open(result_file, 'a', newline='') as f:
                writer = csv.writer(f)
                writer.writerow([
                    query_id,
                    f"{query_time:.4f}",
                    len(result_data),
                    scanned_rows,
                    f"{cpu_usage_pct:.2f}",
                    f"{wall_clock_diff:.4f}"
                ])
        except Exception as e:
            logger.error(f"Failed to write results to CSV: {e}")

# -------------------------------
# Step 4: Host Client (Compute)
# -------------------------------
def send_query_and_get_result(query, host, port):
    """
    1) Connect to Storage
    2) Send query
    3) Receive pickled result (payload_dict)
    4) Return (result_rows, scanned_rows, data_time, data_size, server_query_time)
    """
    start_time = time.time()
    received_data = b""
    
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((host, int(port)))
            s.sendall(query.encode("utf-8"))
            
            while True:
                chunk = s.recv(4096)
                if not chunk:
                    break
                received_data += chunk
    except Exception as e:
        logger.error(f"Error communicating with storage: {e}")
        return [], 0, 0, 0, 0
    
    end_time = time.time()
    data_time = end_time - start_time
    data_size = len(received_data)
    
    try:
        payload_dict = pickle.loads(received_data)
        result_rows = payload_dict.get('rows', [])
        scanned_rows = payload_dict.get('scanned_rows', 0)
        server_query_time = payload_dict.get('query_time', 0.0)
        return result_rows, scanned_rows, data_time, data_size, server_query_time
    except Exception as e:
        logger.error(f"Error unpacking received data: {e}")
        return [], 0, data_time, data_size, 0.0

def run_host_client(dpu_ip, port, query_dir):
    logger.info(f"Starting host client connecting to DPU at {dpu_ip}:{port}...")
    
    os.makedirs(RESULTS_DIR, exist_ok=True)
    result_file = os.path.join(RESULTS_DIR, f"host_results_{time.strftime('%Y%m%d_%H%M%S')}.csv")
    with open(result_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow([
            "QueryFile", "ReturnedRows", "ScannedRows",
            "DataTransferTime", "DataSize", "ServerQueryTime",
            "CPUUsage", "Throughput"
        ])
    
    process = psutil.Process()
    cpu_times_start = process.cpu_times()
    overall_start = time.time()
    
    query_files = [f for f in os.listdir(query_dir) if f.startswith("query6_") and f.endswith(".sql")]
    query_files.sort()
    
    total_scanned = 0
    total_rows_returned = 0
    total_data_time = 0.0
    total_data_size = 0
    total_query_exec_time = 0.0
    executed_count = 0
    
    num_cpus = psutil.cpu_count(logical=True)
    
    for qf in query_files:
        qf_path = os.path.join(query_dir, qf)
        if not os.path.exists(qf_path):
            logger.warning(f"[Compute] Query file {qf} does not exist. Skipping.")
            continue
        
        with open(qf_path, "r", encoding="utf-8") as f:
            query = f.read().strip()
        
        logger.info(f"\n[Compute] ===== Executing {qf} =====")
        
        single_query_cpu_start = process.cpu_times()
        single_query_wall_start = time.time()
        
        result_rows, scanned_rows, data_time, data_size, server_query_time = send_query_and_get_result(
            query, dpu_ip, port
        )
        
        single_query_cpu_end = process.cpu_times()
        single_query_wall_end = time.time()
        
        user_diff = single_query_cpu_end.user - single_query_cpu_start.user
        sys_diff = single_query_cpu_end.system - single_query_cpu_start.system
        query_wall_diff = single_query_wall_end - single_query_wall_start
        
        single_query_cpu_usage_pct = (user_diff + sys_diff) / (query_wall_diff * num_cpus) * 100.0 if query_wall_diff > 0 else 0.0
        throughput = scanned_rows / query_wall_diff if query_wall_diff > 0 and scanned_rows > 0 else 0
        
        row_count = len(result_rows)
        total_rows_returned += row_count
        total_scanned += scanned_rows
        total_data_time += data_time
        total_data_size += data_size
        total_query_exec_time += server_query_time
        executed_count += 1
        
        logger.info(f"[Compute] {qf} => Returned {row_count} rows, scanned={scanned_rows}, "
                    f"data_time={data_time:.4f}s, data_size={data_size}, server_time={server_query_time:.4f}s")
        logger.info(f"[Compute] Single-query CPU usage: {single_query_cpu_usage_pct:.2f}% "
                    f"(measured over ~{query_wall_diff:.3f} sec)")
        logger.info(f"[Compute] Throughput: {throughput:.1f} rows/sec")

        if row_count > 0:
            logger.info(f"[Compute] Sample first row: {result_rows[0]}")
        
        with open(result_file, 'a', newline='') as f:
            writer = csv.writer(f)
            writer.writerow([
                qf,
                row_count,
                scanned_rows,
                f"{data_time:.4f}",
                data_size,
                f"{server_query_time:.4f}",
                f"{single_query_cpu_usage_pct:.2f}",
                f"{throughput:.1f}"
            ])

    cpu_times_end = process.cpu_times()
    overall_end = time.time()
    
    total_user_time = cpu_times_end.user - cpu_times_start.user
    total_sys_time = cpu_times_end.system - cpu_times_start.system
    overall_wall_time = overall_end - overall_start
    overall_cpu_usage_pct = ((total_user_time + total_sys_time) /
                             (overall_wall_time * num_cpus) * 100.0) if overall_wall_time > 0 else 0.0
    overall_throughput = total_scanned / overall_wall_time if overall_wall_time > 0 and total_scanned > 0 else 0
    
    # Summary
    logger.info("\n[Compute] ===================== Summary =====================")
    logger.info(f"[Compute] Total queries executed   : {executed_count}")
    logger.info(f"[Compute] Total scanned rows       : {total_scanned}")
    logger.info(f"[Compute] Total returned rows      : {total_rows_returned}")
    logger.info(f"[Compute] Total data transfer time : {total_data_time:.4f}s")
    logger.info(f"[Compute] Total data transfer size : {total_data_size} bytes")
    logger.info(f"[Compute] Overall time (wall-clock): {overall_wall_time:.4f}s")
    logger.info(f"[Compute] Average CPU usage        : {overall_cpu_usage_pct:.2f}%")
    logger.info(f"[Compute] Overall throughput       : {overall_throughput:.1f} rows/sec (scanned)")
    
    metrics_data = {
        "ThroughPut": overall_throughput,
        "execution_time": overall_wall_time,
        "cpu_usage": overall_cpu_usage_pct,
        "total_scanned_rows": total_scanned,
        "total_returned_rows": total_rows_returned
    }
    
    metrics_file = os.path.join(RESULTS_DIR, "benchmark_metrics.json")
    with open(metrics_file, 'w') as f:
        json.dump(metrics_data, f, indent=4)
    
    logger.info(f"Metrics saved to {metrics_file}")
    
    return metrics_data

# --------------------
# Main Entry
# --------------------
def main():
    """Main function to run the benchmark."""
    args = parse_args()
    
    metrics = json.loads(args.metrics) if args.metrics else ["ThroughPut"]

    try:
        scale_factor = int(args.ScaleFactor)
    except:
        logger.warning(f"Invalid ScaleFactor {args.ScaleFactor}, defaulting to 1.")
        scale_factor = 1
    
    logger.info("Running benchmark with the following arguments:")
    for arg, value in vars(args).items():
        logger.info(f"  {arg}: {value}")
    
    logger.info("Generating TPC-H data...")
    generate_tpch_data(args.ScaleFactor)  
    
    logger.info("Generating query files...")
    query_dir = generate_queries(args.selectivity)
    
    logger.info("Starting DPU server process...")
    dpu_process = Process(
        target=start_dpu_server,
        args=(args.host_ip, args.port, args.numProc, scale_factor)
    )
    dpu_process.daemon = True
    dpu_process.start()
    
    logger.info("Waiting for DPU server to initialize...")
    time.sleep(3)
    
    logger.info("Running host client to execute benchmark...")
    metrics_data = run_host_client(args.dpu_ip, args.port, query_dir)
    
    logger.info("Terminating DPU server process...")
    if dpu_process.is_alive():
        dpu_process.terminate()
        dpu_process.join(timeout=5)
    
    final_metrics = {metric: metrics_data.get(metric, 0) for metric in metrics}
    with open(os.path.join(RESULTS_DIR, "final_metrics.json"), 'w') as f:
        json.dump(final_metrics, f, indent=4)
    
    logger.info("Benchmark completed successfully.")

if __name__ == "__main__":
    main()

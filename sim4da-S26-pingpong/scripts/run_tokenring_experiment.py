import re
import subprocess
import time
from pathlib import Path

import polars as pl


NODE_COUNTS = [2, 4, 8, 16, 32, 64, 96]
BASE_PORT = 5000
MULTICAST_GROUP = "230.0.0.1"
MULTICAST_PORT = 45000
INITIAL_PROBABILITY = 0.5
SILENT_ROUNDS = 3
TIMEOUT_SECONDS = 60
CLASS_PATH = "build/manual-check"

RESULT_PATTERN = re.compile(
    r"RESULT n=(?P<n>\d+) rounds=(?P<rounds>\d+) multicasts=(?P<multicasts>\d+) "
    r"minMs=(?P<min_ms>\d+\.\d+) avgMs=(?P<avg_ms>\d+\.\d+) maxMs=(?P<max_ms>\d+\.\d+)"
)


def main():
    project_dir = Path(__file__).resolve().parents[1]
    timestamp = time.strftime("%Y%m%d-%H%M%S")
    run_dir = project_dir / "results" / f"tokenring-{timestamp}"
    run_dir.mkdir(parents=True, exist_ok=True)

    rows = []
    for run_index, node_count in enumerate(NODE_COUNTS):
        row = run_one_experiment(
            project_dir=project_dir,
            run_dir=run_dir,
            run_index=run_index,
            node_count=node_count,
            class_path=CLASS_PATH,
            initial_probability=INITIAL_PROBABILITY,
            silent_rounds=SILENT_ROUNDS,
            timeout_seconds=TIMEOUT_SECONDS,
            base_port=BASE_PORT + run_index * 1000,
            multicast_group=MULTICAST_GROUP,
            multicast_port=MULTICAST_PORT + run_index,
        )
        rows.append(row)
        print(row)

    results = pl.DataFrame(rows)
    csv_path = run_dir / "summary.csv"
    results.write_csv(csv_path)

    successful_ns = results.filter(pl.col("status") == "ok")["n"].to_list()
    max_successful_n = max(successful_ns) if successful_ns else None

    print(results)
    print(f"summary: {csv_path}")
    print(f"maximum successful n: {max_successful_n}")


def run_one_experiment(project_dir,
                       run_dir,
                       run_index,
                       node_count,
                       class_path,
                       initial_probability,
                       silent_rounds,
                       timeout_seconds,
                       base_port,
                       multicast_group,
                       multicast_port):
    processes = []
    log_files = []
    start_time = time.monotonic()

    try:
        for node_index in range(1, node_count):
            processes.append(start_node(
                project_dir, run_dir, run_index, node_index, node_count, class_path,
                base_port, multicast_group, multicast_port, initial_probability,
                silent_rounds, starts_with_token=False, log_files=log_files))

        time.sleep(1)

        initiator = start_node(
            project_dir, run_dir, run_index, 0, node_count, class_path,
            base_port, multicast_group, multicast_port, initial_probability,
            silent_rounds, starts_with_token=True, log_files=log_files)
        processes.append(initiator)

        status = "ok"
        try:
            initiator.wait(timeout=timeout_seconds)
        except subprocess.TimeoutExpired:
            status = "timeout"

        stop_processes(processes)
        close_files(log_files)

        duration_seconds = time.monotonic() - start_time
        result = parse_result(run_dir / f"run{run_index}-node0.log")

        if result is None and status == "ok":
            status = "no_result"

        return {
            "n": node_count,
            "status": status,
            "rounds": int(result["rounds"]) if result else None,
            "multicasts": int(result["multicasts"]) if result else None,
            "minMs": float(result["min_ms"]) if result else None,
            "avgMs": float(result["avg_ms"]) if result else None,
            "maxMs": float(result["max_ms"]) if result else None,
            "durationSeconds": round(duration_seconds, 3),
            "logDir": str(run_dir),
        }
    finally:
        stop_processes(processes)
        close_files(log_files)


def start_node(project_dir,
               run_dir,
               run_index,
               node_index,
               node_count,
               class_path,
               base_port,
               multicast_group,
               multicast_port,
               initial_probability,
               silent_rounds,
               starts_with_token,
               log_files):
    local_port = base_port + node_index
    next_port = base_port + ((node_index + 1) % node_count)
    log_file = open(run_dir / f"run{run_index}-node{node_index}.log", "w", encoding="utf-8")
    log_files.append(log_file)

    command = [
        "java",
        "-cp",
        class_path,
        "tokenring.RingNode",
        f"N{node_index}",
        str(node_index),
        str(node_count),
        str(local_port),
        "127.0.0.1",
        str(next_port),
        multicast_group,
        str(multicast_port),
        str(initial_probability),
        str(silent_rounds),
    ]

    if starts_with_token:
        command.append("start")

    return subprocess.Popen(
        command,
        cwd=project_dir,
        stdout=log_file,
        stderr=subprocess.STDOUT,
        text=True,
    )


def parse_result(log_path):
    if not log_path.exists():
        return None

    for line in log_path.read_text(encoding="utf-8", errors="replace").splitlines():
        match = RESULT_PATTERN.search(line)
        if match:
            return match.groupdict()

    return None


def stop_processes(processes):
    for process in processes:
        if process.poll() is None:
            process.terminate()

    for process in processes:
        try:
            process.wait(timeout=3)
        except subprocess.TimeoutExpired:
            process.kill()


def close_files(files):
    while files:
        file = files.pop()
        file.close()


if __name__ == "__main__":
    main()

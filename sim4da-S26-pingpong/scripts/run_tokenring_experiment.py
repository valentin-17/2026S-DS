import argparse
import csv
import os
import re
import subprocess
import time
from pathlib import Path


NODE_COUNTS = [2, 4, 8, 16, 32, 64, 128, 256, 512]
BASE_PORT = 5000
MULTICAST_GROUP = "230.0.0.1"
MULTICAST_PORT = 45000
INITIAL_PROBABILITY = 0.5
SILENT_ROUNDS = 3
TIMEOUT_SECONDS = 60
CLASS_PATH = "build/manual-check"
DEFAULT_HEAP_PERCENT = 85.0
MIN_HEAP_MB = 8

RESULT_PATTERN = re.compile(
    r"RESULT n=(?P<n>\d+) rounds=(?P<rounds>\d+) multicasts=(?P<multicasts>\d+) "
    r"minMs=(?P<min_ms>\d+\.\d+) avgMs=(?P<avg_ms>\d+\.\d+) maxMs=(?P<max_ms>\d+\.\d+)"
)


def main():
    args = parse_args()
    project_dir = Path(__file__).resolve().parents[1]
    timestamp = time.strftime("%Y%m%d-%H%M%S")
    run_dir = project_dir / "results" / f"tokenring-{timestamp}"
    run_dir.mkdir(parents=True, exist_ok=True)

    total_memory_mb = detect_total_memory_mb()
    print(f"total memory: {total_memory_mb} MiB")
    print(f"heap budget: {args.heap_percent:g}% split across live node JVMs")

    rows = []
    for run_index, node_count in enumerate(NODE_COUNTS):
        heap_mb = heap_mb_per_node(total_memory_mb, node_count, args.heap_percent)
        print(f"run {run_index}: n={node_count}, per-node JVM heap=-Xmx{heap_mb}m")
        row = run_one_experiment(
            project_dir=project_dir,
            run_dir=run_dir,
            run_index=run_index,
            node_count=node_count,
            class_path=CLASS_PATH,
            java_heap_mb=heap_mb,
            extra_jvm_args=args.jvm_arg,
            initial_probability=INITIAL_PROBABILITY,
            silent_rounds=SILENT_ROUNDS,
            timeout_seconds=TIMEOUT_SECONDS,
            base_port=BASE_PORT + run_index * 1000,
            multicast_group=MULTICAST_GROUP,
            multicast_port=MULTICAST_PORT + run_index,
        )
        rows.append(row)
        print(row)

    csv_path = run_dir / "summary.csv"
    write_summary_csv(csv_path, rows)

    successful_ns = [row["n"] for row in rows if row["status"] == "ok"]
    max_successful_n = max(successful_ns) if successful_ns else None

    print_summary(rows)
    print(f"summary: {csv_path}")
    print(f"maximum successful n: {max_successful_n}")


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--heap-percent",
        type=float,
        default=DEFAULT_HEAP_PERCENT,
        help=(
            "Percentage of physical RAM to make available to the experiment. "
            "The budget is split across the node JVMs in each run."
        ),
    )
    parser.add_argument(
        "--jvm-arg",
        action="append",
        default=[],
        help="Extra JVM argument to add to every node process. Can be repeated.",
    )
    return parser.parse_args()


def detect_total_memory_mb():
    env_value = os.environ.get("TOKENRING_TOTAL_MEMORY_MB")
    if env_value:
        return int(env_value)

    try:
        pages = os.sysconf("SC_PHYS_PAGES")
        page_size = os.sysconf("SC_PAGE_SIZE")
        if pages > 0 and page_size > 0:
            return pages * page_size // (1024 * 1024)
    except (AttributeError, OSError, ValueError):
        pass

    try:
        output = subprocess.check_output(
            ["sysctl", "-n", "hw.memsize"],
            text=True,
            stderr=subprocess.DEVNULL,
        )
        return int(output.strip()) // (1024 * 1024)
    except (OSError, subprocess.SubprocessError, ValueError):
        pass

    raise RuntimeError(
        "Could not detect physical RAM. Set TOKENRING_TOTAL_MEMORY_MB, for example "
        "TOKENRING_TOTAL_MEMORY_MB=32768."
    )


def heap_mb_per_node(total_memory_mb, node_count, heap_percent):
    if heap_percent <= 0 or heap_percent > 100:
        raise ValueError("--heap-percent must be greater than 0 and at most 100")

    experiment_budget_mb = int(total_memory_mb * heap_percent / 100)
    return max(MIN_HEAP_MB, experiment_budget_mb // node_count)


def write_summary_csv(csv_path, rows):
    with open(csv_path, "w", encoding="utf-8", newline="") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def print_summary(rows):
    if not rows:
        return

    fieldnames = list(rows[0].keys())
    widths = {
        field: max(len(field), *(len(str(row[field])) for row in rows))
        for field in fieldnames
    }
    print(" | ".join(field.ljust(widths[field]) for field in fieldnames))
    print("-+-".join("-" * widths[field] for field in fieldnames))
    for row in rows:
        print(" | ".join(str(row[field]).ljust(widths[field]) for field in fieldnames))


def run_one_experiment(project_dir,
                       run_dir,
                       run_index,
                       node_count,
                       class_path,
                       java_heap_mb,
                       extra_jvm_args,
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
                java_heap_mb, extra_jvm_args,
                base_port, multicast_group, multicast_port, initial_probability,
                silent_rounds, starts_with_token=False, log_files=log_files))

        time.sleep(1)

        initiator = start_node(
            project_dir, run_dir, run_index, 0, node_count, class_path,
            java_heap_mb, extra_jvm_args,
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
            "heapMbPerNode": java_heap_mb,
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
               java_heap_mb,
               extra_jvm_args,
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
        f"-Xmx{java_heap_mb}m",
        *extra_jvm_args,
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

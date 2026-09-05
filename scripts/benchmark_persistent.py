#!/usr/bin/env python3
"""Repeat a durable five-shard ingest workload using already-built binaries.

Each run gets fresh storage and free loopback ports. Reports and logs are retained;
only processes started by this script are stopped. No Cargo builds run during timing.
UNIRUST_* overrides are removed so a user's deployment configuration cannot leak in.
Optional macOS sampling is diagnostic only; exclude sampled runs from comparisons.
"""

import argparse
import hashlib
import os
from pathlib import Path
import platform
import re
import shutil
import socket
import subprocess
import time


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--bin-dir', type=Path, required=True)
    parser.add_argument('--output-dir', type=Path, required=True)
    parser.add_argument('--runs', type=int, default=3)
    parser.add_argument('--count', type=int, default=1_000_000)
    parser.add_argument('--shards', type=int, default=5)
    parser.add_argument('--streams', type=int, default=16)
    parser.add_argument('--batch', type=int, default=5_000)
    parser.add_argument('--overlap', type=float, default=0.1)
    parser.add_argument('--seed', type=int, default=42)
    parser.add_argument('--sample-seconds', type=int, default=0)
    parser.add_argument('--discard-data', action='store_true',
                        help='remove generated shard databases after successful runs; retain logs')
    args = parser.parse_args()
    for name in ('runs', 'count', 'shards', 'streams', 'batch'):
        if getattr(args, name) < 1:
            parser.error(f'--{name} must be positive')
    if not 0 <= args.overlap <= 1 or args.sample_seconds < 0:
        parser.error('overlap must be in [0, 1] and sample-seconds nonnegative')
    if args.sample_seconds and not shutil.which('sample'):
        parser.error('optional sampling requires the macOS sample tool')
    args.bin_dir = args.bin_dir.resolve(strict=True)
    args.output_dir.mkdir(parents=True, exist_ok=False)
    ontology = Path(__file__).resolve().parents[1] / 'examples/loadtest-ontology.json'
    env = {k: v for k, v in os.environ.items() if not k.startswith('UNIRUST_')}
    def digest(path):
        result = hashlib.sha256()
        with path.open('rb') as source:
            for chunk in iter(lambda: source.read(1024 * 1024), b''):
                result.update(chunk)
        return result.hexdigest()

    manifest = [f'platform={platform.platform()}', f'cpu_count={os.cpu_count()}',
                f'bin_dir={args.bin_dir}', 'profile=high-throughput',
                f'rayon_threads={env.get("RAYON_NUM_THREADS", "automatic")}',
                f'tokio_workers={env.get("TOKIO_WORKER_THREADS", "automatic")}',
                f'rust_log={env.get("RUST_LOG", "default")}',
                f'ontology_sha256={digest(ontology)}',
                'checkpoint_interval=0', f'arguments={args}']
    for name in ('unirust_shard', 'unirust_router', 'unirust_loadtest', 'unirust_healthcheck'):
        manifest.append(f'{name}_sha256={digest(args.bin_dir / name)}')
    (args.output_dir / 'configuration.txt').write_text('\n'.join(manifest) + '\n')
    summary = args.output_dir / 'summary.tsv'
    summary.write_text('run\tacked\trecords_per_second\taverage_rpc_ms\tsampled\n')

    for run in range(1, args.runs + 1):
        run_dir = args.output_dir / f'run-{run}'
        run_dir.mkdir()
        processes, files, samples = [], [], []
        completed = False
        sockets = [socket.socket() for _ in range(args.shards + 1)]
        try:
            for sock in sockets:
                sock.bind(('127.0.0.1', 0))
            ports = [sock.getsockname()[1] for sock in sockets]
        finally:
            for sock in sockets:
                sock.close()

        def start(binary, options, name):
            log = (run_dir / f'{name}.log').open('w')
            files.append(log)
            process = subprocess.Popen([str(args.bin_dir / binary), *options],
                                       stdout=log, stderr=subprocess.STDOUT, env=env)
            processes.append(process)
            return process

        def ready(kind, port):
            deadline = time.monotonic() + 120
            while time.monotonic() < deadline:
                if any(process.poll() is not None for process in processes):
                    raise RuntimeError(f'Cluster process exited; inspect {run_dir}')
                try:
                    result = subprocess.run(
                        [str(args.bin_dir / 'unirust_healthcheck'), kind,
                         f'http://127.0.0.1:{port}'], env=env, timeout=10,
                        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                    if result.returncode == 0:
                        return
                except subprocess.TimeoutExpired:
                    pass  # A slow probe does not consume the entire readiness window.
                time.sleep(0.1)
            raise RuntimeError(f'Cluster not ready; inspect {run_dir}')

        try:
            for shard, port in enumerate(ports[:-1]):
                start('unirust_shard', [
                    '--listen', f'127.0.0.1:{port}', '--shard-id', str(shard),
                    '--data-dir', str(run_dir / f'shard-{shard}'),
                    '--ontology', str(ontology), '--profile', 'high-throughput',
                    '--allow-colocated-checkpoints'], f'shard-{shard}')
            for port in ports[:-1]:
                ready('--shard', port)
            router = start('unirust_router', [
                '--listen', f'127.0.0.1:{ports[-1]}', '--shards',
                ','.join(f'127.0.0.1:{port}' for port in ports[:-1]),
                '--ontology', str(ontology), '--checkpoint-interval-secs', '0'], 'router')
            ready('--router', ports[-1])
            loadtest = start('unirust_loadtest', [
                '--router', f'http://127.0.0.1:{ports[-1]}', '--count', str(args.count),
                '--streams', str(args.streams), '--batch', str(args.batch),
                '--overlap', str(args.overlap), '--seed', str(args.seed), '--headless',
                '--log', str(run_dir / 'trace.log')], 'loadtest')
            if args.sample_seconds:
                time.sleep(3)
                for label, process in [('router', router), ('shard-0', processes[0])]:
                    samples.append(subprocess.Popen(
                        ['sample', str(process.pid), str(args.sample_seconds), '-file',
                         str(run_dir / f'{label}.sample.txt')],
                        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL))
            if loadtest.wait(timeout=1800) != 0:
                raise RuntimeError(f'Load test failed; inspect {run_dir}')
            report = (run_dir / 'loadtest.log').read_text()
            def metric(pattern):
                match = re.search(pattern, report)
                if not match:
                    raise RuntimeError(f'Missing metric {pattern!r}; inspect {run_dir}')
                return match.group(1)
            acked = int(metric(r'Records acked:\s+(\d+)'))
            if acked != args.count:
                raise RuntimeError(f'Incomplete acknowledgement: {acked}/{args.count}')
            rate = metric(r'Throughput:\s+([\d.]+)')
            latency = metric(r'Avg RPC latency:\s+([\d.]+)')
            line = f'{run}\t{acked}\t{rate}\t{latency}\t{bool(args.sample_seconds)}\n'
            with summary.open('a') as output:
                output.write(line)
            print(line, end='', flush=True)
            completed = True
        finally:
            for process in reversed(processes):
                if process.poll() is None:
                    process.terminate()
            for process in reversed(processes):
                try:
                    process.wait(timeout=30)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
            for process in samples:
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.terminate()
                    try:
                        process.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait()
            for log in files:
                log.close()
            if completed and args.discard_data:
                for shard in range(args.shards):
                    shutil.rmtree(run_dir / f'shard-{shard}')


if __name__ == '__main__':
    main()

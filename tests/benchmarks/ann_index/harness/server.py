"""
Minimal ClickHouse server lifecycle helper for the ANN benchmark harness.

Owns a single per-run server: writes a stripped-down `config.xml` under
`data_dir`, launches `<binary> server --daemon`, waits for `SELECT 1` to
succeed over the TCP port, and tears it down on exit. The data directory is
deleted on `__exit__` unless `keep_data_dir=True`.

Picks free ports via `socket.bind(0)` rather than a hardcoded range so multiple
concurrent harness runs can coexist.
"""
from __future__ import annotations

import logging
import os
import shutil
import signal
import socket
import subprocess
import time
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional


log = logging.getLogger(__name__)


def _free_tcp_port() -> int:
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _env_port(name: str) -> Optional[int]:
    v = os.environ.get(name)
    if not v:
        return None
    return int(v)


CONFIG_XML_TEMPLATE = """<clickhouse>
    <logger>
        <level>information</level>
        <log>{logs_dir}/server.log</log>
        <errorlog>{logs_dir}/server.err.log</errorlog>
        <size>200M</size>
        <count>3</count>
    </logger>
    <listen_host>{listen_host}</listen_host>
    <tcp_port>{tcp_port}</tcp_port>
    <http_port>{http_port}</http_port>
    <interserver_http_port>{interserver_port}</interserver_http_port>
    <path>{data_dir}/data/</path>
    <tmp_path>{data_dir}/tmp/</tmp_path>
    <user_files_path>{data_dir}/user_files/</user_files_path>
    <format_schema_path>{data_dir}/format_schemas/</format_schema_path>
    <access_control_path>{data_dir}/access/</access_control_path>
    <users>
        <default>
            <password></password>
            <networks><ip>::/0</ip></networks>
            <profile>default</profile>
            <quota>default</quota>
            <access_management>1</access_management>
            <named_collection_control>1</named_collection_control>
        </default>
    </users>
    <profiles><default/></profiles>
    <quotas><default/></quotas>
    <mark_cache_size>1073741824</mark_cache_size>
    <max_concurrent_queries>256</max_concurrent_queries>
    <max_server_memory_usage>0</max_server_memory_usage>
    <max_thread_pool_size>10000</max_thread_pool_size>
    <query_log>
        <database>system</database>
        <table>query_log</table>
        <partition_by>toYYYYMM(event_date)</partition_by>
        <flush_interval_milliseconds>1000</flush_interval_milliseconds>
    </query_log>
</clickhouse>
"""


@dataclass
class ServerHandle:
    binary: Path
    data_dir: Path
    tcp_port: int
    http_port: int
    interserver_port: int

    def client_argv(self) -> list[str]:
        return [str(self.binary), "client", "--port", str(self.tcp_port), "-d", "default"]


class ClickHouseServer:
    """Context-managed clickhouse-server. Use as `with ClickHouseServer(...) as srv:`."""

    def __init__(
        self,
        binary: Path,
        data_dir: Path,
        tcp_port: Optional[int] = None,
        startup_timeout_sec: float = 60.0,
        keep_data_dir: bool = False,
    ) -> None:
        self.binary = binary
        self.data_dir = data_dir
        # Env-vars override random allocation so a host clickhouse-client can
        # reach the in-container server on a known port. Listen on all
        # interfaces by default so `-p` mappings and `--network host` both work.
        self.tcp_port = (
            tcp_port if tcp_port is not None
            else _env_port("CH_BENCH_TCP_PORT") or _free_tcp_port()
        )
        self.http_port = _env_port("CH_BENCH_HTTP_PORT") or _free_tcp_port()
        self.interserver_port = _env_port("CH_BENCH_INTERSERVER_PORT") or _free_tcp_port()
        self.listen_host = os.environ.get("CH_BENCH_LISTEN_HOST", "0.0.0.0")
        self.startup_timeout_sec = startup_timeout_sec
        self.keep_data_dir = keep_data_dir
        self._pid: Optional[int] = None

    @property
    def handle(self) -> ServerHandle:
        return ServerHandle(
            binary=self.binary,
            data_dir=self.data_dir,
            tcp_port=self.tcp_port,
            http_port=self.http_port,
            interserver_port=self.interserver_port,
        )

    def _write_config(self) -> Path:
        (self.data_dir / "logs").mkdir(parents=True, exist_ok=True)
        (self.data_dir / "data").mkdir(parents=True, exist_ok=True)
        (self.data_dir / "tmp").mkdir(parents=True, exist_ok=True)
        (self.data_dir / "user_files").mkdir(parents=True, exist_ok=True)
        (self.data_dir / "format_schemas").mkdir(parents=True, exist_ok=True)
        (self.data_dir / "access").mkdir(parents=True, exist_ok=True)
        cfg = self.data_dir / "config.xml"
        cfg.write_text(CONFIG_XML_TEMPLATE.format(
            logs_dir=str(self.data_dir / "logs"),
            data_dir=str(self.data_dir),
            listen_host=self.listen_host,
            tcp_port=self.tcp_port,
            http_port=self.http_port,
            interserver_port=self.interserver_port,
        ))
        return cfg

    def start(self) -> None:
        cfg = self._write_config()
        pid_file = self.data_dir / "server.pid"
        log.info("starting clickhouse-server pid_file=%s tcp=%d", pid_file, self.tcp_port)
        subprocess.run(
            [str(self.binary), "server", "--daemon",
             f"--config-file={cfg}",
             f"--pid-file={pid_file}"],
            check=True,
        )
        deadline = time.monotonic() + self.startup_timeout_sec
        log.info("SQL (readiness probe, retried until ready) >>>\nSELECT 1\n<<<")
        while time.monotonic() < deadline:
            try:
                subprocess.run(
                    self.handle.client_argv() + ["-q", "SELECT 1"],
                    check=True, capture_output=True, timeout=5,
                )
                self._pid = int(pid_file.read_text().strip())
                # Expose the actual ports to the host through the data_dir
                # bind-mount so `clickhouse-client --port $(cat .../ports.env | grep TCP | cut -d= -f2)`
                # works without parsing the harness log.
                (self.data_dir / "ports.env").write_text(
                    f"TCP_PORT={self.tcp_port}\n"
                    f"HTTP_PORT={self.http_port}\n"
                    f"INTERSERVER_PORT={self.interserver_port}\n"
                )
                log.info("clickhouse-server ready pid=%d tcp=%d http=%d",
                         self._pid, self.tcp_port, self.http_port)
                return
            except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
                time.sleep(0.5)
        raise RuntimeError(f"clickhouse-server failed to become ready within {self.startup_timeout_sec}s")

    def stop(self) -> None:
        if self._pid is None:
            return
        try:
            import os
            os.kill(self._pid, signal.SIGTERM)
            deadline = time.monotonic() + 30.0
            while time.monotonic() < deadline:
                try:
                    os.kill(self._pid, 0)
                except ProcessLookupError:
                    self._pid = None
                    break
                time.sleep(0.2)
            else:
                log.warning("clickhouse-server did not exit gracefully, sending SIGKILL")
                os.kill(self._pid, signal.SIGKILL)
                self._pid = None
        except ProcessLookupError:
            self._pid = None

    def __enter__(self) -> "ClickHouseServer":
        self.start()
        return self

    def __exit__(self, *exc) -> None:
        self.stop()
        if not self.keep_data_dir and self.data_dir.exists():
            # Wipe contents, not the dir itself (it may be a bind-mount point).
            for child in self.data_dir.iterdir():
                try:
                    if child.is_dir() and not child.is_symlink():
                        shutil.rmtree(child)
                    else:
                        child.unlink()
                except OSError as e:
                    log.warning("failed to remove %s: %s", child, e)

    def query(self, sql: str, settings: Optional[dict] = None, multiquery: bool = False, receive_timeout: Optional[int] = None) -> str:
        argv = self.handle.client_argv()
        if multiquery:
            argv.append("--multiquery")
        if receive_timeout is not None:
            argv.append(f"--receive_timeout={receive_timeout}")
        for k, v in (settings or {}).items():
            argv.append(f"--{k}={v}")
        argv += ["-q", sql]
        # Echo the full SQL so every non-search statement shows up in the run log
        # (search-side recall and QPS bypass this method, so they stay quiet).
        log.info("SQL >>>\n%s\n<<<", sql.strip())
        result = subprocess.run(argv, capture_output=True, text=True)
        if result.returncode != 0:
            raise RuntimeError(
                f"clickhouse-client failed (rc={result.returncode})\n"
                f"--- query ---\n{sql}\n"
                f"--- stderr ---\n{result.stderr}"
            )
        return result.stdout

    def query_stream(self, sql: str, stdin_bytes: Iterable[bytes], settings: Optional[dict] = None) -> None:
        """Pipe stdin_bytes into clickhouse-client running `sql` (typically INSERT ... FORMAT RowBinary)."""
        argv = self.handle.client_argv()
        for k, v in (settings or {}).items():
            argv.append(f"--{k}={v}")
        argv += ["-q", sql]
        log.info("SQL (stream) >>>\n%s\n<<<", sql.strip())
        proc = subprocess.Popen(argv, stdin=subprocess.PIPE)
        try:
            for chunk in stdin_bytes:
                proc.stdin.write(chunk)
            proc.stdin.close()
            rc = proc.wait()
            if rc != 0:
                raise RuntimeError(f"clickhouse-client INSERT failed with exit {rc}")
        finally:
            if proc.poll() is None:
                proc.terminate()

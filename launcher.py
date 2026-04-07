from __future__ import annotations

import argparse
import logging
import os
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence

from multi_account_swap import (
    AccountConfig,
    configure_logging,
    load_env_file,
    load_json_file,
    load_runtime_settings,
    resolve_relative_path,
)


@dataclass(frozen=True)
class LauncherSettings:
    python_executable: str
    log_dir: Path
    startup_stagger_seconds: float
    child_log_level: str


@dataclass
class ChildProcess:
    account: AccountConfig
    process: subprocess.Popen[bytes]
    log_handle: object
    log_path: Path


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="批量拉起 Cantex 单账号 swap 进程。")
    parser.add_argument("--env-file", default=".env", help="环境变量文件路径。")
    parser.add_argument("--config-file", default="multi_account_swap.json", help="配置文件路径。")
    parser.add_argument("--python-executable", help="子进程使用的 Python 可执行文件。")
    parser.add_argument("--log-dir", help="launcher 默认日志目录。")
    parser.add_argument("--stagger-seconds", type=float, help="每个账号启动之间的等待秒数。")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    parser.add_argument("--child-log-level", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    parser.add_argument("--max-accounts", type=int, help="最多启动多少个启用账号。")
    parser.add_argument("--base-url", help="覆盖 API 地址。")
    parser.add_argument(
        "--dry-run",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="覆盖子进程 dry-run 模式。",
    )
    parser.add_argument("--run-once", action="store_true", help="让每个子进程只执行一轮。")
    parser.add_argument("--max-runs", type=int, help="覆盖每个子进程的最大轮数。")
    parser.add_argument("--interval-seconds", type=int, help="覆盖每个子进程轮询秒数。")
    parser.add_argument("--interval-minutes", type=int, help="覆盖每个子进程轮询分钟数。")
    return parser


def load_launcher_settings(args: argparse.Namespace) -> LauncherSettings:
    load_env_file(Path(args.env_file))
    config_path = Path(args.config_file).expanduser()
    config_data = load_json_file(config_path)
    launcher_section = config_data.get("launcher", {})
    if launcher_section is None:
        launcher_section = {}
    if not isinstance(launcher_section, dict):
        raise ValueError("launcher 必须是 JSON 对象。")

    config_dir = config_path.parent.resolve()
    raw_log_dir = args.log_dir or launcher_section.get("log_dir") or "logs"
    resolved_log_dir = resolve_relative_path(str(raw_log_dir), base_dir=config_dir)
    if resolved_log_dir is None:
        raise ValueError("launcher.log_dir 不能为空。")

    python_executable = (
        args.python_executable
        or launcher_section.get("python_executable")
        or sys.executable
    )
    startup_stagger_seconds = (
        args.stagger_seconds
        if args.stagger_seconds is not None
        else float(launcher_section.get("startup_stagger_seconds", 0))
    )
    if startup_stagger_seconds < 0:
        raise ValueError("startup_stagger_seconds 不能小于 0。")

    child_log_level = args.child_log_level or str(
        launcher_section.get("child_log_level", args.log_level)
    )

    return LauncherSettings(
        python_executable=str(python_executable),
        log_dir=Path(resolved_log_dir),
        startup_stagger_seconds=startup_stagger_seconds,
        child_log_level=child_log_level,
    )


def load_accounts(args: argparse.Namespace) -> list[AccountConfig]:
    runtime_args = argparse.Namespace(
        config_file=args.config_file,
        base_url=args.base_url,
        dry_run=args.dry_run,
        run_immediately=None,
        max_runs=args.max_runs,
        run_once=args.run_once,
        interval_seconds=args.interval_seconds,
        interval_minutes=args.interval_minutes,
        list_pools=False,
        account_name=None,
    )
    settings = load_runtime_settings(runtime_args)
    accounts = settings.accounts
    if args.max_accounts is not None:
        if args.max_accounts <= 0:
            raise ValueError("--max-accounts 必须大于 0。")
        accounts = accounts[: args.max_accounts]
    return accounts


def build_child_command(
    args: argparse.Namespace,
    launcher_settings: LauncherSettings,
    account: AccountConfig,
) -> list[str]:
    script_path = Path(__file__).with_name("multi_account_swap.py")
    command = [
        launcher_settings.python_executable,
        str(script_path),
        "--env-file",
        args.env_file,
        "--config-file",
        args.config_file,
        "--account-name",
        account.name,
        "--log-level",
        launcher_settings.child_log_level,
    ]
    if args.base_url:
        command.extend(["--base-url", args.base_url])
    if args.dry_run is not None:
        command.append("--dry-run" if args.dry_run else "--no-dry-run")
    if args.run_once:
        command.append("--run-once")
    if args.max_runs is not None:
        command.extend(["--max-runs", str(args.max_runs)])
    if args.interval_seconds is not None:
        command.extend(["--interval-seconds", str(args.interval_seconds)])
    elif args.interval_minutes is not None:
        command.extend(["--interval-minutes", str(args.interval_minutes)])
    return command


def prepare_child_env(account: AccountConfig) -> dict[str, str]:
    env = os.environ.copy()
    env["PYTHONIOENCODING"] = "utf-8"
    if account.proxy_url:
        env["HTTP_PROXY"] = account.proxy_url
        env["HTTPS_PROXY"] = account.proxy_url
        env["ALL_PROXY"] = account.proxy_url
    else:
        env.pop("HTTP_PROXY", None)
        env.pop("HTTPS_PROXY", None)
        env.pop("ALL_PROXY", None)
    return env


def start_child(
    args: argparse.Namespace,
    launcher_settings: LauncherSettings,
    account: AccountConfig,
) -> ChildProcess:
    launcher_settings.log_dir.mkdir(parents=True, exist_ok=True)
    log_path = Path(account.log_path) if account.log_path else launcher_settings.log_dir / f"{account.name}.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    log_handle = open(log_path, "ab")
    process = subprocess.Popen(
        build_child_command(args, launcher_settings, account),
        cwd=str(Path(__file__).resolve().parent),
        env=prepare_child_env(account),
        stdout=log_handle,
        stderr=subprocess.STDOUT,
    )
    logging.info(
        "已启动 %s | pid=%s | proxy=%s | log=%s",
        account.name,
        process.pid,
        account.proxy_url or "direct",
        log_path,
    )
    return ChildProcess(
        account=account,
        process=process,
        log_handle=log_handle,
        log_path=log_path,
    )


def stop_children(children: list[ChildProcess]) -> None:
    for child in children:
        if child.process.poll() is None:
            child.process.terminate()
    deadline = time.time() + 10
    for child in children:
        if child.process.poll() is None:
            timeout = max(0.0, deadline - time.time())
            try:
                child.process.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                child.process.kill()
    for child in children:
        child.log_handle.close()


def run_launcher(args: argparse.Namespace) -> int:
    launcher_settings = load_launcher_settings(args)
    accounts = load_accounts(args)
    if not accounts:
        logging.warning("没有可启动的账号。")
        return 0

    children: list[ChildProcess] = []
    try:
        for index, account in enumerate(accounts, start=1):
            children.append(start_child(args, launcher_settings, account))
            if index < len(accounts) and launcher_settings.startup_stagger_seconds > 0:
                time.sleep(launcher_settings.startup_stagger_seconds)

        while children:
            alive_children: list[ChildProcess] = []
            for child in children:
                return_code = child.process.poll()
                if return_code is None:
                    alive_children.append(child)
                    continue
                child.log_handle.close()
                logging.warning(
                    "账号进程已退出 | account=%s | code=%s | log=%s",
                    child.account.name,
                    return_code,
                    child.log_path,
                )
            children = alive_children
            if children:
                time.sleep(5)
    except KeyboardInterrupt:
        logging.warning("收到中断，正在终止所有账号进程。")
        stop_children(children)
        return 130

    logging.info("所有账号进程都已退出。")
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    configure_logging(args.log_level)
    try:
        return run_launcher(args)
    except ValueError as exc:
        parser.error(str(exc))
    return 2


if __name__ == "__main__":
    sys.exit(main())

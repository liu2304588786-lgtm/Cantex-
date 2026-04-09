from __future__ import annotations

import argparse
import asyncio
from contextlib import AsyncExitStack
import json
import logging
import os
import random
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_CEILING, ROUND_FLOOR
from pathlib import Path
from typing import Any, Iterable, Sequence

import aiohttp

try:
    from aiohttp_socks import ProxyConnector
except ImportError:
    ProxyConnector = None

try:
    from cantex_sdk import (
        CantexAPIError,
        CantexAuthError,
        CantexError,
        CantexSDK,
        CantexTimeoutError,
        InstrumentId,
        IntentTradingKeySigner,
        OperatorKeySigner,
    )
except ImportError as exc:
    raise SystemExit(
        "缺少依赖 'cantex_sdk'，请先执行：pip install -r requirements.txt"
    ) from exc


DEFAULT_BASE_URL = "https://api.cantex.io"
DEFAULT_AMOUNT_PRECISION = 10
DEFAULT_MAX_SLIPPAGE = Decimal("0.005")
BALANCE_LOGGER_NAME = "balance_snapshot"
FUNDS_LOGGER_NAME = "funds_overview"
FUNDS_STATE_STALE_SECONDS = 15 * 60
RNG = random.SystemRandom()
SOCKS_PROXY_SCHEMES = ("socks4://", "socks5://", "socks5h://")


class ProxyCantexSDK(CantexSDK):
    def __init__(
        self,
        operator_signer: OperatorKeySigner,
        intent_signer: IntentTradingKeySigner | None = None,
        *,
        base_url: str = DEFAULT_BASE_URL,
        api_key_path: str | None = "secrets/api_key.txt",
        timeout: aiohttp.ClientTimeout | None = None,
        max_retries: int = 3,
        retry_base_delay: float = 1.0,
        proxy_url: str | None = None,
    ) -> None:
        super().__init__(
            operator_signer,
            intent_signer,
            base_url=base_url,
            api_key_path=api_key_path,
            timeout=timeout,
            max_retries=max_retries,
            retry_base_delay=retry_base_delay,
        )
        self._proxy_url = proxy_url.strip() if proxy_url else None

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            connector: aiohttp.BaseConnector
            if self._proxy_url and self._proxy_url.lower().startswith(SOCKS_PROXY_SCHEMES):
                if ProxyConnector is None:
                    raise RuntimeError(
                        "SOCKS proxy requires aiohttp-socks. Run: pip install -r requirements.txt"
                    )
                connector = ProxyConnector.from_url(self._proxy_url)
            else:
                connector = aiohttp.TCPConnector(limit=20)
            self._session = aiohttp.ClientSession(
                timeout=self._timeout,
                connector=connector,
                headers={"User-Agent": "CantexSDK/1.0"},
                trust_env=True,
            )
        return self._session

    async def _request(
        self,
        method: str,
        path: str,
        *,
        json_data: dict | None = None,
        authenticated: bool = True,
    ) -> dict:
        session = await self._get_session()
        url = f"{self.base_url}{path}"
        headers = self._auth_headers() if authenticated else {}
        use_explicit_proxy = bool(
            self._proxy_url and not self._proxy_url.lower().startswith(SOCKS_PROXY_SCHEMES)
        )

        last_exc: Exception | None = None
        for attempt in range(1, self._max_retries + 1):
            try:
                request_kwargs: dict[str, Any] = {
                    "headers": headers,
                    "json": json_data,
                }
                if use_explicit_proxy:
                    request_kwargs["proxy"] = self._proxy_url
                async with session.request(method, url, **request_kwargs) as resp:
                    body = await resp.text()

                    if resp.status in (401, 403):
                        raise CantexAuthError(resp.status, body)

                    if resp.status >= 400:
                        if resp.status in {429, 502, 503, 504} and attempt < self._max_retries:
                            await asyncio.sleep(self._retry_base_delay * (2 ** (attempt - 1)))
                            continue
                        raise CantexAPIError(resp.status, body)

                    try:
                        return json.loads(body)
                    except json.JSONDecodeError as exc:
                        raise CantexError(
                            f"Invalid JSON in {resp.status} response from {method} {path}"
                        ) from exc
            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                last_exc = exc
                if attempt < self._max_retries:
                    await asyncio.sleep(self._retry_base_delay * (2 ** (attempt - 1)))
                    continue
                if isinstance(exc, asyncio.TimeoutError):
                    raise CantexTimeoutError(
                        f"{method} {path} timed out after {self._max_retries} attempts"
                    ) from exc
                raise CantexError(
                    f"{method} {path} failed after {self._max_retries} attempts: {exc}"
                ) from exc

        raise CantexError(
            f"{method} {path} failed after {self._max_retries} attempts"
        ) from last_exc

    async def get_egress_ip(self) -> str | None:
        session = await self._get_session()
        use_explicit_proxy = bool(
            self._proxy_url and not self._proxy_url.lower().startswith(SOCKS_PROXY_SCHEMES)
        )
        request_kwargs: dict[str, Any] = {}
        if use_explicit_proxy:
            request_kwargs["proxy"] = self._proxy_url

        for url in (
            "https://api.ipify.org",
            "https://ipv4.icanhazip.com",
            "https://ifconfig.me/ip",
        ):
            try:
                async with session.get(url, **request_kwargs) as resp:
                    if resp.status >= 400:
                        continue
                    ip_text = (await resp.text()).strip()
                    if ip_text:
                        return ip_text
            except (aiohttp.ClientError, asyncio.TimeoutError):
                continue
        return None


@dataclass(frozen=True)
class InstrumentSpec:
    instrument_id: str
    instrument_admin: str
    symbol: str | None = None

    @property
    def label(self) -> str:
        return f"{self.instrument_id} [{self.instrument_admin}]"

    @property
    def display_symbol(self) -> str:
        return self.symbol or self.instrument_id


@dataclass(frozen=True)
class AccountConfig:
    name: str
    operator_key: str
    trading_key: str | None
    api_key_path: str | None
    proxy_url: str | None
    log_path: str | None


@dataclass(frozen=True)
class StrategySettings:
    fixed_sell_instrument: InstrumentSpec | None
    fixed_buy_instrument: InstrumentSpec | None
    instruments: list[InstrumentSpec]
    randomize_pair: bool
    prefer_low_buy_high_sell: bool
    reference_instrument: InstrumentSpec
    min_reference_ticket_size: Decimal
    sell_ratio_min: Decimal
    sell_ratio_max: Decimal
    min_sell_amount: Decimal
    max_network_fee: Decimal
    min_value_edge_ratio: Decimal
    max_slippage: Decimal
    min_buy_amount: Decimal | None
    create_intent_account: bool
    amount_precision: int


@dataclass(frozen=True)
class RuntimeSettings:
    base_url: str
    dry_run: bool
    run_immediately: bool
    interval_seconds_range: tuple[int, int] | None
    swap_interval_seconds_range: tuple[int, int] | None
    max_runs: int | None
    strategy: StrategySettings
    accounts: list[AccountConfig]


@dataclass
class AccountRuntime:
    account: AccountConfig
    sdk: CantexSDK
    operator_signer: OperatorKeySigner
    intent_signer: IntentTradingKeySigner | None
    egress_ip: str | None = None
    next_regular_swap_not_before: float = 0.0


@dataclass(frozen=True)
class AccountCycleResult:
    account_name: str
    sell_symbol: str
    buy_symbol: str | None
    balance: Decimal | None
    sell_amount: Decimal | None
    sell_ratio: Decimal | None
    network_fee: Decimal | None
    threshold_passed: bool
    action: str | None
    trade_mode: str | None = None
    egress_ip: str | None = None
    value_edge: Decimal | None = None
    value_edge_ratio: Decimal | None = None
    balance_snapshot: tuple[tuple[str, Decimal], ...] = ()
    total_reference_value: Decimal | None = None
    unvalued_symbols: tuple[str, ...] = ()
    swaps_24h: int | None = None
    cc_rebates: Decimal | None = None
    cc_rebates_period: str | None = None
    cc_rebates_status: str | None = None


@dataclass(frozen=True)
class AccountRewardSnapshot:
    swaps_24h: int | None
    cc_rebates: Decimal | None
    cc_rebates_period: str | None = None
    cc_rebates_status: str | None = None


@dataclass(frozen=True)
class PlannedTrade:
    sell_instrument: InstrumentSpec
    buy_instrument: InstrumentSpec
    balance: Decimal
    sell_amount: Decimal
    mode: str = "regular"
    quote: Any | None = None
    sell_reference_value: Decimal | None = None
    buy_reference_value: Decimal | None = None
    net_reference_edge: Decimal | None = None
    edge_ratio: Decimal | None = None
    fee_balance_sufficient: bool = True


def load_env_file(path: Path) -> None:
    if not path.exists():
        return

    for line_number, raw_line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            raise ValueError(f"{path} 第 {line_number} 行格式不正确：{raw_line!r}")

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if value and value[0] == value[-1] and value[0] in {'"', "'"}:
            value = value[1:-1]
        os.environ.setdefault(key, value)


def load_json_file(path: Path) -> dict[str, Any]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise ValueError(f"找不到配置文件：{path}") from exc
    except json.JSONDecodeError as exc:
        raise ValueError(f"配置文件 JSON 格式错误 {path}：{exc}") from exc

    if not isinstance(data, dict):
        raise ValueError(f"配置文件顶层必须是 JSON 对象：{path}")
    return data


def parse_decimal(value: str, field_name: str) -> Decimal:
    try:
        parsed = Decimal(value)
    except InvalidOperation as exc:
        raise ValueError(f"{field_name} 必须是合法的小数：{value!r}") from exc
    return parsed


def parse_bool(value: Any, field_name: str) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, int) and value in (0, 1):
        return bool(value)
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"1", "true", "yes", "y", "on"}:
            return True
        if normalized in {"0", "false", "no", "n", "off"}:
            return False
    raise ValueError(f"{field_name} 必须是布尔值。")


def decimal_text(value: Decimal) -> str:
    normalized = format(value, "f")
    if "." in normalized:
        normalized = normalized.rstrip("0").rstrip(".")
    return normalized or "0"


def percentage_text(value: Decimal, places: int = 4) -> str:
    quantized = (value * Decimal("100")).quantize(Decimal(1).scaleb(-places))
    return f"{decimal_text(quantized)}%"


def resolve_shared_log_dir(accounts: Sequence[AccountConfig]) -> Path:
    for account in accounts:
        if account.log_path:
            return Path(account.log_path).expanduser().resolve().parent
    return Path("logs").resolve()


def resolve_funds_log_path(accounts: Sequence[AccountConfig]) -> Path:
    return resolve_shared_log_dir(accounts) / "funds_overview.log"


def resolve_funds_state_dir(accounts: Sequence[AccountConfig]) -> Path:
    return resolve_shared_log_dir(accounts) / ".funds_state"


def configure_logging(level: str, *, funds_log_path: str | None = None) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    logging.getLogger("cantex_sdk").setLevel(logging.WARNING)
    logging.getLogger("cantex_sdk._sdk").setLevel(logging.WARNING)
    balance_logger = logging.getLogger(BALANCE_LOGGER_NAME)
    balance_logger.handlers.clear()
    balance_logger.setLevel(logging.INFO)
    balance_logger.propagate = False
    balance_handler = logging.FileHandler("balance.log", encoding="utf-8")
    balance_handler.setFormatter(
        logging.Formatter("%(asctime)s | %(message)s", "%Y-%m-%d %H:%M:%S")
    )
    balance_logger.addHandler(balance_handler)
    funds_logger = logging.getLogger(FUNDS_LOGGER_NAME)
    funds_logger.handlers.clear()
    funds_logger.setLevel(logging.INFO)
    funds_logger.propagate = False
    if funds_log_path:
        funds_log_file = Path(funds_log_path).expanduser().resolve()
        funds_log_file.parent.mkdir(parents=True, exist_ok=True)
        funds_handler = logging.FileHandler(funds_log_file, encoding="utf-8")
        funds_handler.setFormatter(
            logging.Formatter("%(asctime)s | %(message)s", "%Y-%m-%d %H:%M:%S")
        )
        funds_logger.addHandler(funds_handler)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="使用 JSON 配置文件运行 Cantex 多账号正式 swap 脚本。"
    )
    parser.add_argument("--env-file", default=".env", help="环境变量文件路径，默认是 .env")
    parser.add_argument(
        "--config-file",
        default="multi_account_swap.json",
        help="多账号 JSON 配置文件路径。",
    )
    parser.add_argument("--account-name", help="只运行指定账号名称。")
    parser.add_argument("--base-url", help="覆盖配置文件中的 API 地址。")
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="日志级别。",
    )
    parser.add_argument(
        "--list-pools",
        action="store_true",
        help="列出可用池子后退出。",
    )
    parser.add_argument(
        "--run-once",
        action="store_true",
        help="立即执行一轮后退出。",
    )
    parser.add_argument(
        "--dry-run",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="覆盖配置，只获取报价不真正下单。",
    )
    parser.add_argument(
        "--run-immediately",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="覆盖配置，决定启动后是否立即执行第一轮。",
    )
    parser.add_argument(
        "--max-runs",
        type=int,
        help="覆盖配置，最多执行多少轮后停止。",
    )

    interval_group = parser.add_mutually_exclusive_group()
    interval_group.add_argument("--interval-seconds", type=int, help="覆盖配置，将轮询间隔设为多少秒。")
    interval_group.add_argument("--interval-minutes", type=int, help="覆盖配置，将轮询间隔设为多少分钟。")
    return parser


def to_sdk_instrument(spec: InstrumentSpec) -> InstrumentId:
    return InstrumentId(id=spec.instrument_id, admin=spec.instrument_admin)


def instrument_matches(left: InstrumentSpec | InstrumentId, right: InstrumentSpec | InstrumentId) -> bool:
    left_id = left.instrument_id if isinstance(left, InstrumentSpec) else left.id
    left_admin = left.instrument_admin if isinstance(left, InstrumentSpec) else left.admin
    right_id = right.instrument_id if isinstance(right, InstrumentSpec) else right.id
    right_admin = right.instrument_admin if isinstance(right, InstrumentSpec) else right.admin
    return left_id == right_id and left_admin == right_admin


def resolve_relative_path(raw_path: str | None, *, base_dir: Path) -> str | None:
    if not raw_path:
        return None

    path = Path(raw_path).expanduser()
    if not path.is_absolute():
        path = (base_dir / path).resolve()
    else:
        path = path.resolve()
    return str(path)


def parse_instrument_object(raw: Any, *, label: str) -> InstrumentSpec:
    if not isinstance(raw, dict):
        raise ValueError(f"{label} 必须是 JSON 对象。")

    instrument_id = raw.get("instrument_id")
    instrument_admin = raw.get("instrument_admin")
    if not instrument_id or not instrument_admin:
        raise ValueError(f"{label} 必须包含 instrument_id 和 instrument_admin。")
    symbol = raw.get("symbol")
    return InstrumentSpec(
        instrument_id=str(instrument_id),
        instrument_admin=str(instrument_admin),
        symbol=(str(symbol) if symbol else None),
    )


def slugify_account_name(name: str) -> str:
    slug = "".join(ch.lower() if ch.isalnum() else "_" for ch in name).strip("_")
    return slug or "account"


def resolve_secret(
    entry: dict[str, Any],
    *,
    direct_field: str,
    env_field: str,
    label: str,
    required: bool,
) -> str | None:
    direct_value = entry.get(direct_field)
    if direct_value is not None:
        value = str(direct_value).strip()
        if value:
            return value

    env_name = entry.get(env_field)
    if env_name is not None:
        env_name_text = str(env_name).strip()
        if not env_name_text:
            raise ValueError(f"{label}：{env_field} 不能为空。")
        value = os.getenv(env_name_text)
        if not value:
            raise ValueError(f"{label}：环境变量 '{env_name_text}' 未设置。")
        return value.strip()

    if required:
        raise ValueError(f"{label}：必须提供 '{direct_field}' 或 '{env_field}'。")
    return None


def resolve_optional_setting(
    entry: dict[str, Any],
    *,
    direct_field: str,
    env_field: str,
) -> str | None:
    direct_value = entry.get(direct_field)
    if direct_value is not None:
        value = str(direct_value).strip()
        if value:
            return value

    env_name = entry.get(env_field)
    if env_name is None:
        return None
    env_name_text = str(env_name).strip()
    if not env_name_text:
        return None
    value = os.getenv(env_name_text)
    if not value:
        return None
    return value.strip()


def sell_amount_bounds(
    balance: Decimal,
    strategy: StrategySettings,
    sell_instrument: InstrumentSpec,
) -> tuple[Decimal, Decimal] | None:
    unit = Decimal(1).scaleb(-strategy.amount_precision)
    lower_bound = balance * strategy.sell_ratio_min
    if instrument_matches(sell_instrument, strategy.reference_instrument):
        lower_bound = max(
            lower_bound,
            strategy.min_sell_amount + unit,
            strategy.min_reference_ticket_size + unit,
        )
    else:
        lower_bound = max(lower_bound, unit)
    upper_bound = balance * strategy.sell_ratio_max
    if upper_bound < lower_bound:
        return None

    return lower_bound, upper_bound


def choose_random_sell_amount(
    balance: Decimal,
    strategy: StrategySettings,
    sell_instrument: InstrumentSpec,
    *,
    lower_bound_override: Decimal | None = None,
) -> Decimal | None:
    unit = Decimal(1).scaleb(-strategy.amount_precision)
    bounds = sell_amount_bounds(balance, strategy, sell_instrument)
    if bounds is None:
        return None

    lower_bound, upper_bound = bounds
    if lower_bound_override is not None:
        lower_bound = max(lower_bound, lower_bound_override)
    if upper_bound < lower_bound:
        return None

    low_units = int((lower_bound / unit).to_integral_value(rounding=ROUND_CEILING))
    high_units = int((upper_bound / unit).to_integral_value(rounding=ROUND_FLOOR))
    if low_units > high_units:
        return None

    return Decimal(RNG.randint(low_units, high_units)) * unit


def parse_account_config(
    entry: dict[str, Any],
    *,
    base_dir: Path,
    default_api_key_dir: str,
    require_trading_key: bool,
    index: int,
) -> AccountConfig | None:
    if not isinstance(entry, dict):
        raise ValueError(f"accounts[{index}] 必须是 JSON 对象。")

    name = str(entry.get("name") or f"account-{index + 1:02d}")
    if "enabled" in entry and not parse_bool(entry["enabled"], f"accounts[{index}].enabled"):
        return None

    operator_key = resolve_secret(
        entry,
        direct_field="operator_key",
        env_field="operator_key_env",
        label=f"accounts[{index}] operator key",
        required=True,
    )
    trading_key = resolve_secret(
        entry,
        direct_field="trading_key",
        env_field="trading_key_env",
        label=f"accounts[{index}] trading key",
        required=require_trading_key,
    )

    raw_api_key_path = entry.get("api_key_path")
    if raw_api_key_path is None:
        raw_api_key_path = f"{default_api_key_dir}/{slugify_account_name(name)}_api_key.txt"
    api_key_path = resolve_relative_path(str(raw_api_key_path), base_dir=base_dir)
    proxy_url = resolve_optional_setting(
        entry,
        direct_field="proxy_url",
        env_field="proxy_url_env",
    )
    raw_log_path = entry.get("log_path")
    if raw_log_path is None:
        raw_log_path = f"logs/{slugify_account_name(name)}.log"
    log_path = resolve_relative_path(str(raw_log_path), base_dir=base_dir)

    return AccountConfig(
        name=name,
        operator_key=operator_key or "",
        trading_key=trading_key,
        api_key_path=api_key_path,
        proxy_url=proxy_url,
        log_path=log_path,
    )


def compute_interval_seconds(
    args: argparse.Namespace,
    config_run: dict[str, Any],
) -> int | None:
    if args.run_once or args.list_pools:
        return None

    if args.interval_seconds is not None:
        interval_seconds = args.interval_seconds
    elif args.interval_minutes is not None:
        interval_seconds = args.interval_minutes * 60
    else:
        config_seconds = config_run.get("interval_seconds")
        config_minutes = config_run.get("interval_minutes")
        if config_seconds is not None and config_minutes is not None:
            raise ValueError("run 配置里不能同时定义 interval_seconds 和 interval_minutes。")
        if config_seconds is not None:
            interval_seconds = int(config_seconds)
        elif config_minutes is not None:
            interval_seconds = int(config_minutes) * 60
        else:
            raise ValueError("请在 JSON 配置文件里设置 interval_seconds 或 interval_minutes。")

    if interval_seconds <= 0:
        raise ValueError("轮询间隔必须大于 0。")
    return interval_seconds


def validate_interval_range(
    minimum_seconds: int,
    maximum_seconds: int,
    *,
    label: str,
) -> tuple[int, int]:
    if minimum_seconds <= 0 or maximum_seconds <= 0:
        raise ValueError(f"{label} 必须大于 0。")
    if minimum_seconds > maximum_seconds:
        raise ValueError(f"{label} 的最小值不能大于最大值。")
    return minimum_seconds, maximum_seconds


def load_interval_range_from_run_config(
    config_run: dict[str, Any],
    *,
    prefix: str,
    default_range: tuple[int, int] | None = None,
) -> tuple[int, int]:
    seconds_key = f"{prefix}_seconds"
    minutes_key = f"{prefix}_minutes"
    seconds_min_key = f"{prefix}_seconds_min"
    seconds_max_key = f"{prefix}_seconds_max"
    minutes_min_key = f"{prefix}_minutes_min"
    minutes_max_key = f"{prefix}_minutes_max"

    has_fixed_seconds = config_run.get(seconds_key) is not None
    has_fixed_minutes = config_run.get(minutes_key) is not None
    has_range_seconds = (
        config_run.get(seconds_min_key) is not None
        or config_run.get(seconds_max_key) is not None
    )
    has_range_minutes = (
        config_run.get(minutes_min_key) is not None
        or config_run.get(minutes_max_key) is not None
    )

    defined_modes = sum(
        1
        for flag in (has_fixed_seconds, has_fixed_minutes, has_range_seconds, has_range_minutes)
        if flag
    )
    if defined_modes > 1:
        raise ValueError(
            f"run.{prefix} 只能配置一种形式：固定秒、固定分钟、秒区间或分钟区间。"
        )

    if has_fixed_seconds:
        seconds = int(config_run[seconds_key])
        return validate_interval_range(seconds, seconds, label=f"run.{seconds_key}")
    if has_fixed_minutes:
        seconds = int(config_run[minutes_key]) * 60
        return validate_interval_range(seconds, seconds, label=f"run.{minutes_key}")
    if has_range_seconds:
        if config_run.get(seconds_min_key) is None or config_run.get(seconds_max_key) is None:
            raise ValueError(f"run.{seconds_min_key} 和 run.{seconds_max_key} 必须同时设置。")
        return validate_interval_range(
            int(config_run[seconds_min_key]),
            int(config_run[seconds_max_key]),
            label=f"run.{seconds_min_key}/{seconds_max_key}",
        )
    if has_range_minutes:
        if config_run.get(minutes_min_key) is None or config_run.get(minutes_max_key) is None:
            raise ValueError(f"run.{minutes_min_key} 和 run.{minutes_max_key} 必须同时设置。")
        return validate_interval_range(
            int(config_run[minutes_min_key]) * 60,
            int(config_run[minutes_max_key]) * 60,
            label=f"run.{minutes_min_key}/{minutes_max_key}",
        )
    if default_range is not None:
        return default_range
    raise ValueError(
        f"请在 JSON 配置文件里设置 {seconds_key}/{minutes_key} 或区间版本。"
    )


def has_interval_range_config(config_run: dict[str, Any], *, prefix: str) -> bool:
    for suffix in (
        "_seconds",
        "_minutes",
        "_seconds_min",
        "_seconds_max",
        "_minutes_min",
        "_minutes_max",
    ):
        if config_run.get(f"{prefix}{suffix}") is not None:
            return True
    return False


def compute_interval_range(
    args: argparse.Namespace,
    config_run: dict[str, Any],
) -> tuple[int, int] | None:
    if args.run_once or args.list_pools:
        return None
    if args.interval_seconds is not None or args.interval_minutes is not None:
        fixed_interval_seconds = compute_interval_seconds(args, config_run)
        assert fixed_interval_seconds is not None
        return validate_interval_range(
            fixed_interval_seconds,
            fixed_interval_seconds,
            label="CLI interval override",
        )
    return load_interval_range_from_run_config(config_run, prefix="interval")


def choose_next_interval_seconds(interval_range: tuple[int, int] | None) -> int | None:
    if interval_range is None:
        return None
    minimum_seconds, maximum_seconds = interval_range
    if minimum_seconds == maximum_seconds:
        return minimum_seconds
    return RNG.randint(minimum_seconds, maximum_seconds)


def regular_swap_allowed(runtime: AccountRuntime) -> bool:
    return time.monotonic() >= runtime.next_regular_swap_not_before


def schedule_next_regular_swap(runtime: AccountRuntime, settings: RuntimeSettings) -> int | None:
    cooldown_seconds = choose_next_interval_seconds(settings.swap_interval_seconds_range)
    if cooldown_seconds is None:
        runtime.next_regular_swap_not_before = 0.0
        return None
    runtime.next_regular_swap_not_before = time.monotonic() + cooldown_seconds
    return cooldown_seconds


def load_runtime_settings(args: argparse.Namespace) -> RuntimeSettings:
    config_path = Path(args.config_file).expanduser()
    config_data = load_json_file(config_path)
    config_dir = config_path.parent.resolve()

    run_section = config_data.get("run", {})
    strategy_section = config_data.get("strategy", {})
    accounts_section = config_data.get("accounts", [])
    if not isinstance(run_section, dict):
        raise ValueError("配置项 'run' 必须是 JSON 对象。")
    if not isinstance(strategy_section, dict):
        raise ValueError("配置项 'strategy' 必须是 JSON 对象。")
    if not isinstance(accounts_section, list) or not accounts_section:
        raise ValueError("配置项 'accounts' 必须是非空数组。")

    base_url = args.base_url or str(config_data.get("base_url") or DEFAULT_BASE_URL)
    dry_run = (
        args.dry_run
        if args.dry_run is not None
        else parse_bool(run_section.get("dry_run", True), "run.dry_run")
    )
    run_immediately = (
        args.run_immediately
        if args.run_immediately is not None
        else parse_bool(run_section.get("run_immediately", True), "run.run_immediately")
    )
    interval_seconds_range = compute_interval_range(args, run_section)
    swap_interval_seconds_range = None
    if interval_seconds_range is not None:
        swap_interval_prefix = (
            "swap_interval"
            if has_interval_range_config(run_section, prefix="swap_interval")
            else "success_interval"
        )
        swap_interval_seconds_range = load_interval_range_from_run_config(
            run_section,
            prefix=swap_interval_prefix,
            default_range=interval_seconds_range,
        )
    max_runs = 1 if args.run_once else args.max_runs
    if max_runs is None and not args.run_once and run_section.get("max_runs") is not None:
        max_runs = int(run_section["max_runs"])
    if max_runs is not None and max_runs <= 0:
        raise ValueError("max_runs 必须大于 0。")

    has_random_instruments = strategy_section.get("instruments") is not None
    required_strategy_fields = [
        "sell_ratio_min",
        "sell_ratio_max",
        "max_network_fee",
    ]
    if not has_random_instruments:
        required_strategy_fields.extend(
            [
                "sell_instrument_id",
                "sell_instrument_admin",
                "buy_instrument_id",
                "buy_instrument_admin",
            ]
        )
    missing_strategy_fields = [
        f"strategy.{field_name}"
        for field_name in required_strategy_fields
        if strategy_section.get(field_name) is None
    ]
    if missing_strategy_fields:
        raise ValueError(f"缺少必要配置项：{', '.join(missing_strategy_fields)}")

    create_intent_account = parse_bool(
        strategy_section.get("create_intent_account", False),
        "strategy.create_intent_account",
    )
    randomize_pair = parse_bool(
        strategy_section.get("randomize_pair", has_random_instruments),
        "strategy.randomize_pair",
    )
    prefer_low_buy_high_sell = parse_bool(
        strategy_section.get("prefer_low_buy_high_sell", has_random_instruments),
        "strategy.prefer_low_buy_high_sell",
    )
    max_slippage = parse_decimal(
        str(strategy_section.get("max_slippage", DEFAULT_MAX_SLIPPAGE)),
        "strategy.max_slippage",
    )
    min_buy_amount = None
    if strategy_section.get("min_buy_amount") is not None:
        min_buy_amount = parse_decimal(
            str(strategy_section["min_buy_amount"]),
            "strategy.min_buy_amount",
        )
    min_sell_amount = parse_decimal(
        str(strategy_section.get("min_sell_amount", "6")),
        "strategy.min_sell_amount",
    )
    sell_ratio_min = parse_decimal(str(strategy_section["sell_ratio_min"]), "strategy.sell_ratio_min")
    sell_ratio_max = parse_decimal(str(strategy_section["sell_ratio_max"]), "strategy.sell_ratio_max")
    if sell_ratio_min <= 0 or sell_ratio_min >= 1:
        raise ValueError("strategy.sell_ratio_min 必须大于 0 且小于 1。")
    if sell_ratio_max <= 0 or sell_ratio_max >= 1:
        raise ValueError("strategy.sell_ratio_max 必须大于 0 且小于 1。")
    if sell_ratio_min > sell_ratio_max:
        raise ValueError("strategy.sell_ratio_min 不能大于 strategy.sell_ratio_max。")

    max_network_fee = parse_decimal(str(strategy_section["max_network_fee"]), "strategy.max_network_fee")
    min_value_edge_ratio = parse_decimal(
        str(strategy_section.get("min_value_edge_ratio", "0")),
        "strategy.min_value_edge_ratio",
    )
    if min_value_edge_ratio < 0:
        raise ValueError("strategy.min_value_edge_ratio 不能小于 0。")
    min_reference_ticket_size = parse_decimal(
        str(strategy_section.get("min_reference_ticket_size", "10")),
        "strategy.min_reference_ticket_size",
    )
    amount_precision = int(strategy_section.get("amount_precision", DEFAULT_AMOUNT_PRECISION))
    if amount_precision <= 0 or amount_precision > 18:
        raise ValueError("strategy.amount_precision 必须在 1 到 18 之间。")

    instruments: list[InstrumentSpec]
    fixed_sell_instrument: InstrumentSpec | None
    fixed_buy_instrument: InstrumentSpec | None
    if has_random_instruments:
        raw_instruments = strategy_section["instruments"]
        if not isinstance(raw_instruments, list) or len(raw_instruments) < 2:
            raise ValueError("strategy.instruments 必须是至少包含 2 个币种的数组。")
        instruments = [
            parse_instrument_object(raw, label=f"strategy.instruments[{index}]")
            for index, raw in enumerate(raw_instruments)
        ]
        fixed_sell_instrument = None
        fixed_buy_instrument = None
    else:
        fixed_sell_instrument = InstrumentSpec(
            str(strategy_section["sell_instrument_id"]),
            str(strategy_section["sell_instrument_admin"]),
        )
        fixed_buy_instrument = InstrumentSpec(
            str(strategy_section["buy_instrument_id"]),
            str(strategy_section["buy_instrument_admin"]),
        )
        instruments = [fixed_sell_instrument, fixed_buy_instrument]
        randomize_pair = False

    raw_reference_instrument = strategy_section.get("reference_instrument")
    if raw_reference_instrument is not None:
        reference_instrument = parse_instrument_object(
            raw_reference_instrument,
            label="strategy.reference_instrument",
        )
    else:
        explicit_reference_id = strategy_section.get("reference_instrument_id")
        explicit_reference_admin = strategy_section.get("reference_instrument_admin")
        if explicit_reference_id and explicit_reference_admin:
            reference_instrument = InstrumentSpec(
                str(explicit_reference_id),
                str(explicit_reference_admin),
            )
        else:
            inferred_reference = next(
                (instrument for instrument in instruments if instrument.instrument_id == "Amulet"),
                None,
            )
            if inferred_reference is None:
                raise ValueError("请配置 strategy.reference_instrument 或 reference_instrument_id/admin。")
            reference_instrument = inferred_reference

    default_api_key_dir = str(config_data.get("api_key_dir", "secrets"))
    require_trading_key = (not dry_run) or create_intent_account
    accounts: list[AccountConfig] = []
    for index, entry in enumerate(accounts_section):
        account = parse_account_config(
            entry,
            base_dir=config_dir,
            default_api_key_dir=default_api_key_dir,
            require_trading_key=require_trading_key,
            index=index,
        )
        if account is not None:
            accounts.append(account)
    if args.account_name:
        accounts = [account for account in accounts if account.name == args.account_name]
        if not accounts:
            raise ValueError(f"未找到启用的账号：{args.account_name}")
    if not accounts:
        raise ValueError("配置文件里没有可用的启用账号。")

    return RuntimeSettings(
        base_url=base_url,
        dry_run=dry_run,
        run_immediately=run_immediately,
        interval_seconds_range=interval_seconds_range,
        swap_interval_seconds_range=swap_interval_seconds_range,
        max_runs=max_runs,
        strategy=StrategySettings(
            fixed_sell_instrument=fixed_sell_instrument,
            fixed_buy_instrument=fixed_buy_instrument,
            instruments=instruments,
            randomize_pair=randomize_pair,
            prefer_low_buy_high_sell=prefer_low_buy_high_sell,
            reference_instrument=reference_instrument,
            min_reference_ticket_size=min_reference_ticket_size,
            sell_ratio_min=sell_ratio_min,
            sell_ratio_max=sell_ratio_max,
            min_sell_amount=min_sell_amount,
            max_network_fee=max_network_fee,
            min_value_edge_ratio=min_value_edge_ratio,
            max_slippage=max_slippage,
            min_buy_amount=min_buy_amount,
            create_intent_account=create_intent_account,
            amount_precision=amount_precision,
        ),
        accounts=accounts,
    )


def build_account_runtime(account: AccountConfig, *, base_url: str, needs_intent_signer: bool) -> AccountRuntime:
    try:
        operator_signer = OperatorKeySigner.from_hex(account.operator_key)
    except Exception as exc:
        raise SystemExit(
            f"[{account.name}] operator key 不是合法的 Ed25519 十六进制私钥。"
        ) from exc

    intent_signer = None
    if needs_intent_signer:
        if not account.trading_key:
            raise SystemExit(
                f"[{account.name}] 实盘下单或创建 intent 账户时必须提供 trading key。"
            )
        try:
            intent_signer = IntentTradingKeySigner.from_hex(account.trading_key)
        except Exception as exc:
            raise SystemExit(
                f"[{account.name}] trading key 不是合法的 secp256k1 十六进制私钥。"
            ) from exc

    if account.api_key_path:
        Path(account.api_key_path).expanduser().resolve().parent.mkdir(parents=True, exist_ok=True)

    return AccountRuntime(
        account=account,
        sdk=ProxyCantexSDK(
            operator_signer,
            intent_signer,
            base_url=base_url,
            api_key_path=account.api_key_path,
            proxy_url=account.proxy_url,
        ),
        operator_signer=operator_signer,
        intent_signer=intent_signer,
    )


def find_token(tokens: Iterable[object], instrument: InstrumentSpec) -> object | None:
    for token in tokens:
        token_instrument = getattr(token, "instrument", None)
        if (
            getattr(token_instrument, "id", None) == instrument.instrument_id
            and getattr(token_instrument, "admin", None) == instrument.instrument_admin
        ):
            return token
    return None


def current_balance(tokens: Iterable[object], instrument: InstrumentSpec) -> Decimal:
    token = find_token(tokens, instrument)
    if token is None:
        return Decimal("0")
    return getattr(token, "unlocked_amount", Decimal("0"))


def has_sufficient_fee_balance(
    tokens: Iterable[object],
    strategy: StrategySettings,
    *,
    sell_instrument: InstrumentSpec,
    sell_amount: Decimal,
    network_fee: Decimal,
) -> bool:
    if instrument_matches(sell_instrument, strategy.reference_instrument):
        return current_balance(tokens, sell_instrument) >= sell_amount + network_fee
    return current_balance(tokens, sell_instrument) >= sell_amount and (
        current_balance(tokens, strategy.reference_instrument) >= network_fee
    )


async def list_pools(sdk: CantexSDK) -> None:
    pools = await sdk.get_pool_info()
    if not pools.pools:
        logging.info("接口没有返回任何池子。")
        return

    for index, pool in enumerate(pools.pools, start=1):
        logging.info(
            "池子 %s | %s [%s] <-> %s [%s] | contract=%s",
            index,
            pool.token_a.id,
            pool.token_a.admin,
            pool.token_b.id,
            pool.token_b.admin,
            pool.contract_id,
        )


def explain_auth_error(exc: CantexAPIError, base_url: str, operator_signer: OperatorKeySigner) -> str:
    operator_public_key_b64 = operator_signer.get_public_key_b64()
    operator_public_key_hex = operator_signer.get_public_key_hex()

    if exc.status == 400 and "invalid publicKey" in exc.body:
        return (
            "鉴权失败：Cantex 拒绝了当前 operator 公钥。\n"
            f"当前 API 地址：{base_url}\n"
            f"推导出的 operator 公钥（base64）：{operator_public_key_b64}\n"
            f"推导出的 operator 公钥（hex）：{operator_public_key_hex}\n"
            "可能原因：\n"
            "1. 这把 key 属于另一个网络。\n"
            "2. operator key 不是该账号的 Ed25519 私钥。\n"
            "3. 把 trading key 或其他钱包私钥填到了 operator key 位置。"
        )

    return (
        "与 Cantex API 通信时鉴权失败。\n"
        f"当前 API 地址：{base_url}\n"
        f"Operator 公钥（base64）：{operator_public_key_b64}\n"
        f"接口返回：{exc}"
    )


async def authenticate_runtime(runtime: AccountRuntime) -> bool:
    try:
        await runtime.sdk.authenticate()
        return True
    except CantexAuthError as exc:
        logging.error("[%s] 鉴权失败：%s", runtime.account.name, exc)
        return False
    except CantexAPIError as exc:
        logging.error("[%s] %s", runtime.account.name, explain_auth_error(exc, runtime.sdk.base_url, runtime.operator_signer))
        return False


async def ensure_intent_account(runtime: AccountRuntime, *, create_intent_account: bool) -> None:
    account_admin = await runtime.sdk.get_account_admin()
    if account_admin.has_intent_account:
        return
    if not create_intent_account:
        raise RuntimeError(
            f"[{runtime.account.name}] 缺少 intent trading account，请先开启 create_intent_account。"
        )

    logging.warning("[%s] 未找到 intent trading account，正在自动创建。", runtime.account.name)
    result = await runtime.sdk.create_intent_trading_account()
    logging.info("[%s] intent trading account 创建成功：%s", runtime.account.name, result)


def infer_token_symbol(token: object | None, fallback: str) -> str:
    if token is None:
        return fallback
    symbol = getattr(token, "instrument_symbol", "")
    if symbol:
        return str(symbol)
    return fallback


def instrument_symbol_from_spec(spec: InstrumentSpec) -> str:
    return spec.display_symbol


def tracked_balance_instruments(strategy: StrategySettings) -> list[InstrumentSpec]:
    instruments: list[InstrumentSpec] = []
    for instrument in [*strategy.instruments, strategy.reference_instrument]:
        if any(instrument_matches(instrument, existing) for existing in instruments):
            continue
        instruments.append(instrument)
    return instruments


def capture_balance_snapshot(
    tokens: Iterable[object],
    strategy: StrategySettings,
) -> tuple[tuple[str, Decimal], ...]:
    snapshot: list[tuple[str, Decimal]] = []
    for instrument in tracked_balance_instruments(strategy):
        balance = current_balance(tokens, instrument)
        if balance == 0:
            continue
        token = find_token(tokens, instrument)
        symbol = infer_token_symbol(token, instrument_symbol_from_spec(instrument))
        snapshot.append((symbol, balance))
    return tuple(snapshot)


async def estimate_total_reference_value(
    runtime: AccountRuntime,
    tokens: Iterable[object],
    strategy: StrategySettings,
) -> tuple[Decimal, tuple[str, ...]]:
    total_reference_value = Decimal("0")
    unvalued_symbols: list[str] = []
    for instrument in tracked_balance_instruments(strategy):
        balance = current_balance(tokens, instrument)
        if balance == 0:
            continue
        token = find_token(tokens, instrument)
        symbol = infer_token_symbol(token, instrument_symbol_from_spec(instrument))
        reference_value = await quote_reference_amount(
            runtime,
            sell_amount=balance,
            sell_instrument=instrument,
            reference_instrument=strategy.reference_instrument,
        )
        if reference_value is None:
            unvalued_symbols.append(symbol)
            continue
        total_reference_value += reference_value
    return total_reference_value, tuple(unvalued_symbols)


def format_balance_snapshot_summary(
    results: list[AccountCycleResult],
    total_value_symbol: str,
) -> str | None:
    balance_parts: list[str] = []
    for result in sorted(results, key=lambda item: item.account_name):
        token_parts = [
            f"{symbol}={decimal_text(amount)}"
            for symbol, amount in result.balance_snapshot
        ]
        account_summary = f"{result.account_name}["
        account_summary += ", ".join(token_parts) if token_parts else "无非零余额"
        account_summary += "]"
        if result.total_reference_value is not None:
            account_summary += (
                f" 总估值={decimal_text(result.total_reference_value)} {total_value_symbol}"
            )
        if result.unvalued_symbols:
            account_summary += f" 未估值={','.join(result.unvalued_symbols)}"
        balance_parts.append(account_summary)
    if not balance_parts:
        return None
    return "账户余额 | " + " | ".join(balance_parts)


def parse_decimal_or_none(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None


def parse_int_or_none(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


async def fetch_account_reward_snapshot(runtime: AccountRuntime) -> AccountRewardSnapshot:
    try:
        data = await runtime.sdk._request("GET", "/v1/account/reward_activity")
    except CantexAPIError as exc:
        if exc.status != 404:
            logging.warning("[%s] failed to fetch reward activity: %s", runtime.account.name, exc)
        return AccountRewardSnapshot(
            swaps_24h=None,
            cc_rebates=None,
        )
    except Exception as exc:
        logging.warning("[%s] failed to fetch reward activity: %s", runtime.account.name, exc)
        return AccountRewardSnapshot(
            swaps_24h=None,
            cc_rebates=None,
        )

    if not isinstance(data, dict):
        return AccountRewardSnapshot(
            swaps_24h=None,
            cc_rebates=None,
        )

    stats = data.get("stats", {})
    rebates = data.get("rebates", {})
    rebate_period = None
    rebate_entry: dict[str, Any] | None = None
    if isinstance(rebates, dict):
        for candidate in ("this_week", "yesterday", "last_week"):
            current_entry = rebates.get(candidate)
            if isinstance(current_entry, dict):
                rebate_period = candidate
                rebate_entry = current_entry
                break

    rebate_status = None
    rebate_amount = None
    if rebate_entry is not None:
        rebate_amount = parse_decimal_or_none(rebate_entry.get("cc_amount"))
        raw_status = str(rebate_entry.get("status") or "").strip()
        rebate_status = raw_status or None

    swaps_24h = None
    if isinstance(stats, dict):
        swaps_24h = parse_int_or_none(stats.get("count_24h"))

    return AccountRewardSnapshot(
        swaps_24h=swaps_24h,
        cc_rebates=rebate_amount,
        cc_rebates_period=rebate_period,
        cc_rebates_status=rebate_status,
    )


def atomic_write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = path.with_suffix(f"{path.suffix}.tmp")
    temporary_path.write_text(content, encoding="utf-8")
    os.replace(temporary_path, path)


def write_account_funds_state(
    accounts: Sequence[AccountConfig],
    result: AccountCycleResult,
    *,
    cycle_number: int,
    total_value_symbol: str,
) -> None:
    state_path = resolve_funds_state_dir(accounts) / f"{slugify_account_name(result.account_name)}.json"
    payload = {
        "account_name": result.account_name,
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "updated_at_epoch": time.time(),
        "cycle_number": cycle_number,
        "swaps_24h": result.swaps_24h,
        "estimated_value": (
            decimal_text(result.total_reference_value)
            if result.total_reference_value is not None
            else None
        ),
        "estimated_value_symbol": total_value_symbol,
        "cc_rebates": decimal_text(result.cc_rebates) if result.cc_rebates is not None else None,
        "cc_rebates_period": result.cc_rebates_period,
        "cc_rebates_status": result.cc_rebates_status,
        "balances": [
            {"symbol": symbol, "amount": decimal_text(amount)}
            for symbol, amount in result.balance_snapshot
        ],
        "unvalued_symbols": list(result.unvalued_symbols),
    }
    atomic_write_text(
        state_path,
        json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2),
    )


def load_account_funds_states(accounts: Sequence[AccountConfig]) -> list[dict[str, Any]]:
    state_dir = resolve_funds_state_dir(accounts)
    if not state_dir.exists():
        return []

    now = time.time()
    states: list[dict[str, Any]] = []
    for path in sorted(state_dir.glob("*.json")):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if not isinstance(payload, dict):
            continue

        updated_at_epoch = payload.get("updated_at_epoch")
        if updated_at_epoch is not None:
            try:
                if now - float(updated_at_epoch) > FUNDS_STATE_STALE_SECONDS:
                    continue
            except (TypeError, ValueError):
                pass
        states.append(payload)

    states.sort(key=lambda item: str(item.get("account_name") or ""))
    return states


def format_funds_state_summary(state: dict[str, Any], total_value_symbol: str) -> str:
    account_name = str(state.get("account_name") or "unknown")
    estimated_value_symbol = str(state.get("estimated_value_symbol") or total_value_symbol)
    swaps_24h = state.get("swaps_24h")
    parts = [
        f"24h swaps={swaps_24h if swaps_24h is not None else '?'}",
        f"Estimated value={state.get('estimated_value') or '?'} {estimated_value_symbol}",
    ]

    rebates_text = "?"
    if state.get("cc_rebates") is not None:
        rebates_text = f"{state['cc_rebates']} CC"
        if state.get("cc_rebates_period"):
            rebates_text += f" ({state['cc_rebates_period']})"
    parts.append(f"CC REBATES={rebates_text}")

    if state.get("cc_rebates_status"):
        parts.append(f"rebate_status={state['cc_rebates_status']}")

    balances = state.get("balances", [])
    if isinstance(balances, list) and balances:
        balance_parts = []
        for balance in balances:
            if not isinstance(balance, dict):
                continue
            symbol = balance.get("symbol")
            amount = balance.get("amount")
            if symbol is None or amount is None:
                continue
            balance_parts.append(f"{symbol}={amount}")
        if balance_parts:
            parts.append("balances=[" + ", ".join(balance_parts) + "]")

    unvalued_symbols = state.get("unvalued_symbols", [])
    if isinstance(unvalued_symbols, list) and unvalued_symbols:
        parts.append("unvalued=" + ",".join(str(symbol) for symbol in unvalued_symbols))

    return account_name + "{" + ", ".join(parts) + "}"


def format_funds_overview_summary(
    accounts: Sequence[AccountConfig],
    total_value_symbol: str,
) -> str | None:
    states = load_account_funds_states(accounts)
    if not states:
        return None

    total_estimated_value = Decimal("0")
    has_estimated_value = False
    for state in states:
        estimated_value = parse_decimal_or_none(state.get("estimated_value"))
        if estimated_value is None:
            continue
        total_estimated_value += estimated_value
        has_estimated_value = True

    summary = "Funds | "
    if has_estimated_value:
        summary += f"Total Estimated value={decimal_text(total_estimated_value)} {total_value_symbol}"
    else:
        summary += "Total Estimated value=?"
    summary += f" | fresh_accounts={len(states)}"
    summary += " | " + " | ".join(
        format_funds_state_summary(state, total_value_symbol)
        for state in states
    )
    return summary


def is_too_small_amount_error(exc: CantexAPIError) -> bool:
    body = getattr(exc, "body", "")
    return exc.status == 400 and (
        "Too small amount" in body or "Minimum ticket size is 10 CC" in body
    )


async def get_runtime_egress_ip(runtime: AccountRuntime) -> str | None:
    if runtime.egress_ip is not None:
        return runtime.egress_ip
    sdk = runtime.sdk
    if isinstance(sdk, ProxyCantexSDK):
        runtime.egress_ip = await sdk.get_egress_ip()
    return runtime.egress_ip


async def quote_reference_amount(
    runtime: AccountRuntime,
    *,
    sell_amount: Decimal,
    sell_instrument: InstrumentSpec,
    reference_instrument: InstrumentSpec,
) -> Decimal | None:
    if sell_amount <= 0:
        return None
    if instrument_matches(sell_instrument, reference_instrument):
        return sell_amount

    try:
        reference_quote = await runtime.sdk.get_swap_quote(
            sell_amount,
            to_sdk_instrument(sell_instrument),
            to_sdk_instrument(reference_instrument),
        )
    except CantexAPIError as exc:
        if is_too_small_amount_error(exc):
            return None
        raise
    return reference_quote.returned_amount


async def quote_buy_reference_amount(
    runtime: AccountRuntime,
    *,
    buy_amount: Decimal,
    buy_instrument: InstrumentSpec,
    reference_instrument: InstrumentSpec,
) -> Decimal | None:
    if buy_amount <= 0:
        return None
    if instrument_matches(buy_instrument, reference_instrument):
        return buy_amount

    try:
        reference_quote = await runtime.sdk.get_swap_quote(
            buy_amount,
            to_sdk_instrument(buy_instrument),
            to_sdk_instrument(reference_instrument),
        )
    except CantexAPIError as exc:
        if is_too_small_amount_error(exc):
            return None
        raise
    return reference_quote.returned_amount


async def select_sell_amount(
    runtime: AccountRuntime,
    *,
    strategy: StrategySettings,
    sell_instrument: InstrumentSpec,
    balance: Decimal,
) -> Decimal | None:
    spendable_balance = balance
    if instrument_matches(sell_instrument, strategy.reference_instrument):
        spendable_balance -= strategy.max_network_fee
        if spendable_balance <= 0:
            return None

    bounds = sell_amount_bounds(spendable_balance, strategy, sell_instrument)
    if bounds is None:
        return None

    lower_bound, upper_bound = bounds
    if instrument_matches(sell_instrument, strategy.reference_instrument):
        return choose_random_sell_amount(spendable_balance, strategy, sell_instrument)

    upper_reference_value = await quote_reference_amount(
        runtime,
        sell_amount=upper_bound,
        sell_instrument=sell_instrument,
        reference_instrument=strategy.reference_instrument,
    )
    if (
        upper_reference_value is None
        or upper_reference_value < strategy.min_reference_ticket_size
    ):
        return None

    unit = Decimal(1).scaleb(-strategy.amount_precision)
    estimated_required = (
        upper_bound * strategy.min_reference_ticket_size / upper_reference_value
    )
    lower_bound_override = estimated_required
    max_amount = (upper_bound / unit).to_integral_value(rounding=ROUND_FLOOR) * unit

    candidate_amounts: list[Decimal] = []
    for _ in range(5):
        candidate_amount = choose_random_sell_amount(
            spendable_balance,
            strategy,
            sell_instrument,
            lower_bound_override=lower_bound_override,
        )
        if candidate_amount is not None:
            candidate_amounts.append(candidate_amount)
    if max_amount > 0:
        candidate_amounts.append(max_amount)

    seen_amounts: set[Decimal] = set()
    for candidate_amount in candidate_amounts:
        if candidate_amount in seen_amounts:
            continue
        seen_amounts.add(candidate_amount)
        reference_value = await quote_reference_amount(
            runtime,
            sell_amount=candidate_amount,
            sell_instrument=sell_instrument,
            reference_instrument=strategy.reference_instrument,
        )
        if (
            reference_value is not None
            and reference_value >= strategy.min_reference_ticket_size
        ):
            return candidate_amount
    return None


async def evaluate_trade_opportunity(
    runtime: AccountRuntime,
    *,
    strategy: StrategySettings,
    tokens: Iterable[object],
    sell_instrument: InstrumentSpec,
    buy_instrument: InstrumentSpec,
    balance: Decimal,
    sell_amount: Decimal,
) -> PlannedTrade | None:
    sell_reference_value = await quote_reference_amount(
        runtime,
        sell_amount=sell_amount,
        sell_instrument=sell_instrument,
        reference_instrument=strategy.reference_instrument,
    )
    if sell_reference_value is None or sell_reference_value <= 0:
        return None

    try:
        quote = await runtime.sdk.get_swap_quote(
            sell_amount,
            to_sdk_instrument(sell_instrument),
            to_sdk_instrument(buy_instrument),
        )
    except CantexAPIError as exc:
        if is_too_small_amount_error(exc):
            return None
        raise

    network_fee = quote.fees.network_fee
    if not instrument_matches(network_fee.instrument, strategy.reference_instrument):
        return None
    if network_fee.amount >= strategy.max_network_fee:
        return None
    fee_balance_sufficient = has_sufficient_fee_balance(
        tokens,
        strategy,
        sell_instrument=sell_instrument,
        sell_amount=sell_amount,
        network_fee=network_fee.amount,
    )

    buy_reference_value = await quote_buy_reference_amount(
        runtime,
        buy_amount=quote.returned_amount,
        buy_instrument=buy_instrument,
        reference_instrument=strategy.reference_instrument,
    )
    if buy_reference_value is None or buy_reference_value <= 0:
        return None

    net_reference_edge = buy_reference_value - sell_reference_value - network_fee.amount
    edge_ratio = net_reference_edge / sell_reference_value
    if net_reference_edge <= 0 or edge_ratio < strategy.min_value_edge_ratio:
        return None

    return PlannedTrade(
        sell_instrument=sell_instrument,
        buy_instrument=buy_instrument,
        balance=balance,
        sell_amount=sell_amount,
        mode="opportunity",
        quote=quote,
        sell_reference_value=sell_reference_value,
        buy_reference_value=buy_reference_value,
        net_reference_edge=net_reference_edge,
        edge_ratio=edge_ratio,
        fee_balance_sufficient=fee_balance_sufficient,
    )


async def build_regular_trade_candidate(
    runtime: AccountRuntime,
    *,
    strategy: StrategySettings,
    tokens: Iterable[object],
    sell_instrument: InstrumentSpec,
    buy_instrument: InstrumentSpec,
    balance: Decimal,
    sell_amount: Decimal,
) -> PlannedTrade | None:
    try:
        quote = await runtime.sdk.get_swap_quote(
            sell_amount,
            to_sdk_instrument(sell_instrument),
            to_sdk_instrument(buy_instrument),
        )
    except CantexAPIError as exc:
        if is_too_small_amount_error(exc):
            return None
        raise

    network_fee = quote.fees.network_fee
    if not instrument_matches(network_fee.instrument, strategy.reference_instrument):
        return None

    sell_reference_value = await quote_reference_amount(
        runtime,
        sell_amount=sell_amount,
        sell_instrument=sell_instrument,
        reference_instrument=strategy.reference_instrument,
    )
    if sell_reference_value is None or sell_reference_value <= 0:
        return None

    return PlannedTrade(
        sell_instrument=sell_instrument,
        buy_instrument=buy_instrument,
        balance=balance,
        sell_amount=sell_amount,
        mode="regular",
        quote=quote,
        sell_reference_value=sell_reference_value,
        fee_balance_sufficient=has_sufficient_fee_balance(
            tokens,
            strategy,
            sell_instrument=sell_instrument,
            sell_amount=sell_amount,
            network_fee=network_fee.amount,
        ),
    )


async def choose_opportunity_trade_pair(
    runtime: AccountRuntime,
    strategy: StrategySettings,
    tokens: Iterable[object],
) -> PlannedTrade | None:
    if not strategy.prefer_low_buy_high_sell:
        return None

    reference_balance = current_balance(tokens, strategy.reference_instrument)
    candidate_trades: list[PlannedTrade] = []
    fallback_trades: list[PlannedTrade] = []
    for instrument in strategy.instruments:
        balance = current_balance(tokens, instrument)
        if balance <= 0:
            continue

        sell_amount = await select_sell_amount(
            runtime,
            strategy=strategy,
            sell_instrument=instrument,
            balance=balance,
        )
        if sell_amount is None:
            continue

        buy_candidates = [
            candidate
            for candidate in strategy.instruments
            if not instrument_matches(candidate, instrument)
        ]
        for buy_candidate in buy_candidates:
            planned_trade = await evaluate_trade_opportunity(
                runtime,
                strategy=strategy,
                tokens=tokens,
                sell_instrument=instrument,
                buy_instrument=buy_candidate,
                balance=balance,
                sell_amount=sell_amount,
            )
            if planned_trade is not None:
                if planned_trade.fee_balance_sufficient:
                    candidate_trades.append(planned_trade)
                else:
                    fallback_trades.append(planned_trade)

    if not candidate_trades:
        if reference_balance <= 0:
            return None
        candidate_trades = fallback_trades
        if not candidate_trades:
            return None

    candidate_trades.sort(
        key=lambda trade: (
            trade.net_reference_edge or Decimal("-Infinity"),
            trade.edge_ratio or Decimal("-Infinity"),
            trade.sell_reference_value or Decimal("0"),
        ),
        reverse=True,
    )
    return candidate_trades[0]


async def choose_trade_pair(
    runtime: AccountRuntime,
    strategy: StrategySettings,
    tokens: Iterable[object],
) -> PlannedTrade | None:
    if not strategy.randomize_pair:
        assert strategy.fixed_sell_instrument is not None
        assert strategy.fixed_buy_instrument is not None
        balance = current_balance(tokens, strategy.fixed_sell_instrument)
        if balance <= 0:
            return None
        sell_amount = await select_sell_amount(
            runtime,
            strategy=strategy,
            sell_instrument=strategy.fixed_sell_instrument,
            balance=balance,
        )
        if sell_amount is None:
            return None
        candidate = await build_regular_trade_candidate(
            runtime,
            strategy=strategy,
            tokens=tokens,
            sell_instrument=strategy.fixed_sell_instrument,
            buy_instrument=strategy.fixed_buy_instrument,
            balance=balance,
            sell_amount=sell_amount,
        )
        return candidate

    reference_balance = current_balance(tokens, strategy.reference_instrument)
    candidate_trades: list[PlannedTrade] = []
    fallback_trades: list[PlannedTrade] = []
    for instrument in strategy.instruments:
        balance = current_balance(tokens, instrument)
        if balance <= 0:
            continue

        sell_amount = await select_sell_amount(
            runtime,
            strategy=strategy,
            sell_instrument=instrument,
            balance=balance,
        )
        if sell_amount is None:
            continue

        buy_candidates = [
            candidate
            for candidate in strategy.instruments
            if not instrument_matches(candidate, instrument)
        ]
        if not buy_candidates:
            continue

        for buy_candidate in buy_candidates:
            candidate_trade = await build_regular_trade_candidate(
                runtime,
                strategy=strategy,
                tokens=tokens,
                sell_instrument=instrument,
                buy_instrument=buy_candidate,
                balance=balance,
                sell_amount=sell_amount,
            )
            if candidate_trade is None:
                continue
            if candidate_trade.fee_balance_sufficient:
                candidate_trades.append(candidate_trade)
            else:
                fallback_trades.append(candidate_trade)

    if not candidate_trades:
        if reference_balance <= 0:
            return None
        candidate_trades = fallback_trades
        if not candidate_trades:
            return None

    candidate_trades.sort(
        key=lambda trade: (
            trade.quote is not None
            and trade.quote.fees.network_fee.amount < strategy.max_network_fee,
            trade.fee_balance_sufficient,
            trade.sell_reference_value or Decimal("0"),
            trade.balance,
        ),
        reverse=True,
    )
    return candidate_trades[0]


async def reference_equivalent_amount(
    runtime: AccountRuntime,
    *,
    sell_amount: Decimal,
    sell_instrument: InstrumentSpec,
    buy_instrument: InstrumentSpec,
    quote: Any,
    reference_instrument: InstrumentSpec,
) -> Decimal:
    if instrument_matches(sell_instrument, reference_instrument):
        return sell_amount

    if instrument_matches(buy_instrument, reference_instrument):
        return quote.returned_amount

    reference_quote = await runtime.sdk.get_swap_quote(
        sell_amount,
        to_sdk_instrument(sell_instrument),
        to_sdk_instrument(reference_instrument),
    )
    return reference_quote.returned_amount


def format_cycle_summary(
    cycle_number: int,
    threshold: Decimal,
    threshold_symbol: str,
    results: list[AccountCycleResult],
    next_interval_seconds: int | None,
) -> str:
    fee_results = [result for result in results if result.network_fee is not None]
    if not fee_results:
        if any(result.action in {"no-opportunity", "cooldown"} for result in results):
            summary = f"第{cycle_number}轮 | 未发现可执行交易"
            if next_interval_seconds is not None:
                summary += f" | 下次间隔={next_interval_seconds}秒"
            return summary
        summary = f"第 {cycle_number} 轮 | 未拿到有效网络费报价"
        if next_interval_seconds is not None:
            summary += f" | 下次间隔={next_interval_seconds}秒"
        return summary

    lowest = min(fee_results, key=lambda result: result.network_fee or Decimal("Infinity"))
    summary = (
        f"第 {cycle_number} 轮 | 当前最低网络费="
        f"{decimal_text(lowest.network_fee or Decimal('0'))} {threshold_symbol}"
        f" | 账号={lowest.account_name}"
        f" | 方向={lowest.sell_symbol}->{lowest.buy_symbol or '?'}"
        f" | 阈值={decimal_text(threshold)} {threshold_symbol}"
    )

    triggered = [result for result in results if result.threshold_passed]
    if not triggered:
        if next_interval_seconds is not None:
            return summary + f" | 未触发 | 下次间隔={next_interval_seconds}秒"
        return summary + " | 未触发"

    preferred = next(
        (result for result in triggered if result.action in {"submitted", "dry-run"}),
        triggered[0],
    )
    if (
        preferred.balance is not None
        and preferred.sell_amount is not None
        and preferred.sell_ratio is not None
        and preferred.network_fee is not None
    ):
        summary += (
            f" | 触发账号={preferred.account_name}"
            f" | 类型={'低买高卖' if preferred.trade_mode == 'opportunity' else '常规轮询'}"
            f" | 方向={preferred.sell_symbol}->{preferred.buy_symbol or '?'}"
            f" | 当前余额={decimal_text(preferred.balance)} {preferred.sell_symbol}"
            f" | 投入金额={decimal_text(preferred.sell_amount)} {preferred.sell_symbol}"
            f" | 投入比例={percentage_text(preferred.sell_ratio)}"
            f" | 网络费={decimal_text(preferred.network_fee)} {threshold_symbol}"
        )
        if preferred.value_edge is not None and preferred.value_edge_ratio is not None:
            summary += (
                f" | 预估净价差={decimal_text(preferred.value_edge)} {threshold_symbol}"
                f" | 价差率={percentage_text(preferred.value_edge_ratio)}"
            )

    completed = [result for result in results if result.action in {"submitted", "dry-run"}]
    if completed:
        completed_parts = []
        for result in completed:
            ip_text = result.egress_ip or "unknown"
            status_text = "已下单" if result.action == "submitted" else "dry-run"
            completed_parts.append(f"{result.account_name}@{ip_text}({status_text})")
        summary += " | 完成=" + ", ".join(completed_parts)
    elif preferred.action == "balance":
        summary += " | 余额不足未下单"
    elif preferred.action == "slippage":
        summary += " | 滑点超限未下单"
    elif preferred.action == "min-buy":
        summary += " | 回报过低未下单"
    elif preferred.action == "too-small":
        summary += " | 小于10CC等值未下单"
    else:
        summary += " | 已命中阈值"

    extra_triggered = len(triggered) - 1
    if extra_triggered > 0:
        summary += f" | 另有{extra_triggered}个账号也满足阈值"
    if next_interval_seconds is not None:
        summary += f" | 下次间隔={next_interval_seconds}秒"
    return summary


async def execute_account_cycle(
    runtime: AccountRuntime,
    settings: RuntimeSettings,
    cycle_number: int,
) -> AccountCycleResult:
    strategy = settings.strategy
    account_info = await runtime.sdk.get_account_info()
    balance_snapshot = capture_balance_snapshot(account_info.tokens, strategy)
    (
        total_reference_value,
        unvalued_symbols,
    ), reward_snapshot = await asyncio.gather(
        estimate_total_reference_value(
            runtime,
            account_info.tokens,
            strategy,
        ),
        fetch_account_reward_snapshot(runtime),
    )
    reward_kwargs = {
        "swaps_24h": reward_snapshot.swaps_24h,
        "cc_rebates": reward_snapshot.cc_rebates,
        "cc_rebates_period": reward_snapshot.cc_rebates_period,
        "cc_rebates_status": reward_snapshot.cc_rebates_status,
    }
    opportunity_trade = await choose_opportunity_trade_pair(runtime, strategy, account_info.tokens)
    planned_trade = opportunity_trade
    regular_trade_ready = regular_swap_allowed(runtime)
    if planned_trade is None and regular_trade_ready:
        planned_trade = await choose_trade_pair(runtime, strategy, account_info.tokens)
    if planned_trade is None:
        return AccountCycleResult(
            account_name=runtime.account.name,
            sell_symbol="?",
            buy_symbol=None,
            balance=None,
            sell_amount=None,
            sell_ratio=None,
            network_fee=None,
            threshold_passed=False,
            action="cooldown" if not regular_trade_ready else "no-opportunity",
            trade_mode="regular" if not regular_trade_ready else None,
            egress_ip=None,
            balance_snapshot=balance_snapshot,
            total_reference_value=total_reference_value,
            unvalued_symbols=unvalued_symbols,
            **reward_kwargs,
        )

    sell_spec = planned_trade.sell_instrument
    buy_spec = planned_trade.buy_instrument
    sell_instrument = to_sdk_instrument(sell_spec)
    buy_instrument = to_sdk_instrument(buy_spec)

    sell_token = find_token(account_info.tokens, sell_spec)
    unlocked_balance = planned_trade.balance
    sell_symbol = infer_token_symbol(sell_token, instrument_symbol_from_spec(sell_spec))
    buy_symbol = instrument_symbol_from_spec(buy_spec)
    sell_amount = planned_trade.sell_amount

    selected_ratio = sell_amount / unlocked_balance if unlocked_balance > 0 else Decimal("0")

    quote = planned_trade.quote
    if quote is None:
        try:
            quote = await runtime.sdk.get_swap_quote(
                sell_amount,
                sell_instrument,
                buy_instrument,
            )
        except CantexAPIError as exc:
            if is_too_small_amount_error(exc):
                return AccountCycleResult(
                    account_name=runtime.account.name,
                    sell_symbol=sell_symbol,
                    buy_symbol=buy_symbol,
                    balance=unlocked_balance,
                    sell_amount=sell_amount,
                    sell_ratio=selected_ratio,
                    network_fee=None,
                    threshold_passed=False,
                    action="too-small",
                    trade_mode=planned_trade.mode,
                    egress_ip=None,
                    balance_snapshot=balance_snapshot,
                    total_reference_value=total_reference_value,
                    unvalued_symbols=unvalued_symbols,
                    **reward_kwargs,
                )
            raise

    network_fee = quote.fees.network_fee
    if not instrument_matches(network_fee.instrument, strategy.reference_instrument):
        return AccountCycleResult(
            account_name=runtime.account.name,
            sell_symbol=sell_symbol,
            buy_symbol=buy_symbol,
            balance=unlocked_balance,
            sell_amount=sell_amount,
            sell_ratio=selected_ratio,
            network_fee=None,
            threshold_passed=False,
            action=None,
            trade_mode=planned_trade.mode,
            egress_ip=None,
            value_edge=planned_trade.net_reference_edge,
            value_edge_ratio=planned_trade.edge_ratio,
            balance_snapshot=balance_snapshot,
            total_reference_value=total_reference_value,
            unvalued_symbols=unvalued_symbols,
            **reward_kwargs,
        )

    if network_fee.amount >= strategy.max_network_fee:
        return AccountCycleResult(
            account_name=runtime.account.name,
            sell_symbol=sell_symbol,
            buy_symbol=buy_symbol,
            balance=unlocked_balance,
            sell_amount=sell_amount,
            sell_ratio=selected_ratio,
            network_fee=network_fee.amount,
            threshold_passed=False,
            action=None,
            trade_mode=planned_trade.mode,
            egress_ip=None,
            value_edge=planned_trade.net_reference_edge,
            value_edge_ratio=planned_trade.edge_ratio,
            balance_snapshot=balance_snapshot,
            total_reference_value=total_reference_value,
            unvalued_symbols=unvalued_symbols,
            **reward_kwargs,
        )

    result = AccountCycleResult(
        account_name=runtime.account.name,
        sell_symbol=sell_symbol,
        buy_symbol=buy_symbol,
        balance=unlocked_balance,
        sell_amount=sell_amount,
        sell_ratio=selected_ratio,
        network_fee=network_fee.amount,
        threshold_passed=True,
        action=None,
        trade_mode=planned_trade.mode,
        egress_ip=None,
        value_edge=planned_trade.net_reference_edge,
        value_edge_ratio=planned_trade.edge_ratio,
        balance_snapshot=balance_snapshot,
        total_reference_value=total_reference_value,
        unvalued_symbols=unvalued_symbols,
        **reward_kwargs,
    )

    try:
        reference_equivalent = await reference_equivalent_amount(
            runtime,
            sell_amount=sell_amount,
            sell_instrument=sell_spec,
            buy_instrument=buy_spec,
            quote=quote,
            reference_instrument=strategy.reference_instrument,
        )
    except CantexAPIError as exc:
        if is_too_small_amount_error(exc):
            return AccountCycleResult(
                account_name=result.account_name,
                sell_symbol=result.sell_symbol,
                buy_symbol=result.buy_symbol,
                balance=result.balance,
                sell_amount=result.sell_amount,
                sell_ratio=result.sell_ratio,
                network_fee=result.network_fee,
                threshold_passed=True,
                action="too-small",
                trade_mode=result.trade_mode,
                egress_ip=None,
                value_edge=result.value_edge,
                value_edge_ratio=result.value_edge_ratio,
                balance_snapshot=balance_snapshot,
                total_reference_value=total_reference_value,
                unvalued_symbols=unvalued_symbols,
                **reward_kwargs,
            )
        raise
    if reference_equivalent < strategy.min_reference_ticket_size:
        return AccountCycleResult(
            account_name=result.account_name,
            sell_symbol=result.sell_symbol,
            buy_symbol=result.buy_symbol,
            balance=result.balance,
            sell_amount=result.sell_amount,
            sell_ratio=result.sell_ratio,
            network_fee=result.network_fee,
            threshold_passed=True,
            action="too-small",
            trade_mode=result.trade_mode,
            egress_ip=None,
            value_edge=result.value_edge,
            value_edge_ratio=result.value_edge_ratio,
            balance_snapshot=balance_snapshot,
            total_reference_value=total_reference_value,
            unvalued_symbols=unvalued_symbols,
            **reward_kwargs,
        )

    if not has_sufficient_fee_balance(
        account_info.tokens,
        strategy,
        sell_instrument=sell_spec,
        sell_amount=sell_amount,
        network_fee=network_fee.amount,
    ):
        return AccountCycleResult(
            account_name=result.account_name,
            sell_symbol=result.sell_symbol,
            buy_symbol=result.buy_symbol,
            balance=result.balance,
            sell_amount=result.sell_amount,
            sell_ratio=result.sell_ratio,
            network_fee=result.network_fee,
            threshold_passed=True,
            action="balance",
            trade_mode=result.trade_mode,
            egress_ip=None,
            value_edge=result.value_edge,
            value_edge_ratio=result.value_edge_ratio,
            balance_snapshot=balance_snapshot,
            total_reference_value=total_reference_value,
            unvalued_symbols=unvalued_symbols,
            **reward_kwargs,
        )

    if quote.prices.slippage > strategy.max_slippage:
        return AccountCycleResult(
            account_name=result.account_name,
            sell_symbol=result.sell_symbol,
            buy_symbol=result.buy_symbol,
            balance=result.balance,
            sell_amount=result.sell_amount,
            sell_ratio=result.sell_ratio,
            network_fee=result.network_fee,
            threshold_passed=True,
            action="slippage",
            trade_mode=result.trade_mode,
            egress_ip=None,
            value_edge=result.value_edge,
            value_edge_ratio=result.value_edge_ratio,
            balance_snapshot=balance_snapshot,
            total_reference_value=total_reference_value,
            unvalued_symbols=unvalued_symbols,
            **reward_kwargs,
        )

    if strategy.min_buy_amount is not None and quote.returned_amount < strategy.min_buy_amount:
        return AccountCycleResult(
            account_name=result.account_name,
            sell_symbol=result.sell_symbol,
            buy_symbol=result.buy_symbol,
            balance=result.balance,
            sell_amount=result.sell_amount,
            sell_ratio=result.sell_ratio,
            network_fee=result.network_fee,
            threshold_passed=True,
            action="min-buy",
            trade_mode=result.trade_mode,
            egress_ip=None,
            value_edge=result.value_edge,
            value_edge_ratio=result.value_edge_ratio,
            balance_snapshot=balance_snapshot,
            total_reference_value=total_reference_value,
            unvalued_symbols=unvalued_symbols,
            **reward_kwargs,
        )

    if settings.dry_run:
        egress_ip = await get_runtime_egress_ip(runtime)
        if planned_trade.mode == "regular":
            schedule_next_regular_swap(runtime, settings)
        return AccountCycleResult(
            account_name=result.account_name,
            sell_symbol=result.sell_symbol,
            buy_symbol=result.buy_symbol,
            balance=result.balance,
            sell_amount=result.sell_amount,
            sell_ratio=result.sell_ratio,
            network_fee=result.network_fee,
            threshold_passed=True,
            action="dry-run",
            trade_mode=result.trade_mode,
            egress_ip=egress_ip,
            value_edge=result.value_edge,
            value_edge_ratio=result.value_edge_ratio,
            balance_snapshot=balance_snapshot,
            total_reference_value=total_reference_value,
            unvalued_symbols=unvalued_symbols,
            **reward_kwargs,
        )

    await ensure_intent_account(runtime, create_intent_account=strategy.create_intent_account)
    try:
        await runtime.sdk.swap(
            sell_amount,
            sell_instrument,
            buy_instrument,
        )
    except CantexAPIError as exc:
        if is_too_small_amount_error(exc):
            return AccountCycleResult(
                account_name=result.account_name,
                sell_symbol=result.sell_symbol,
                buy_symbol=result.buy_symbol,
                balance=result.balance,
                sell_amount=result.sell_amount,
                sell_ratio=result.sell_ratio,
                network_fee=result.network_fee,
                threshold_passed=True,
                action="too-small",
                trade_mode=result.trade_mode,
                egress_ip=None,
                value_edge=result.value_edge,
                value_edge_ratio=result.value_edge_ratio,
                balance_snapshot=balance_snapshot,
                total_reference_value=total_reference_value,
                unvalued_symbols=unvalued_symbols,
                **reward_kwargs,
            )
        raise
    egress_ip = await get_runtime_egress_ip(runtime)
    if planned_trade.mode == "regular":
        schedule_next_regular_swap(runtime, settings)
    return AccountCycleResult(
        account_name=result.account_name,
        sell_symbol=result.sell_symbol,
        buy_symbol=result.buy_symbol,
        balance=result.balance,
        sell_amount=result.sell_amount,
        sell_ratio=result.sell_ratio,
        network_fee=result.network_fee,
        threshold_passed=True,
        action="submitted",
        trade_mode=result.trade_mode,
        egress_ip=egress_ip,
        value_edge=result.value_edge,
        value_edge_ratio=result.value_edge_ratio,
        balance_snapshot=balance_snapshot,
        total_reference_value=total_reference_value,
        unvalued_symbols=unvalued_symbols,
        **reward_kwargs,
    )


async def process_account_cycle(
    runtime: AccountRuntime,
    settings: RuntimeSettings,
    cycle_number: int,
) -> AccountCycleResult | None:
    if not await authenticate_runtime(runtime):
        return None
    result = await execute_account_cycle(runtime, settings, cycle_number)
    if result.action in {"submitted", "dry-run"}:
        logging.info(
            "[%s] 第 %s 轮完成 | 方向=%s->%s | 金额=%s %s | 出口IP=%s | 状态=%s",
            result.account_name,
            cycle_number,
            result.sell_symbol,
            result.buy_symbol or "?",
            decimal_text(result.sell_amount or Decimal("0")),
            result.sell_symbol,
            result.egress_ip or "unknown",
            "已下单" if result.action == "submitted" else "dry-run",
        )
    return result


async def run_schedule(runtimes: list[AccountRuntime], settings: RuntimeSettings) -> None:
    cycle_number = 0
    next_interval_seconds = choose_next_interval_seconds(settings.interval_seconds_range)
    threshold_symbol = settings.strategy.reference_instrument.display_symbol
    while settings.max_runs is None or cycle_number < settings.max_runs:
        if cycle_number > 0 or not settings.run_immediately:
            assert next_interval_seconds is not None
            await asyncio.sleep(next_interval_seconds)

        cycle_number += 1
        outcomes = await asyncio.gather(
            *(process_account_cycle(runtime, settings, cycle_number) for runtime in runtimes),
            return_exceptions=True,
        )

        cycle_results: list[AccountCycleResult] = []
        for runtime, outcome in zip(runtimes, outcomes, strict=False):
            if isinstance(outcome, Exception):
                if isinstance(outcome, (CantexAPIError, CantexTimeoutError)):
                    logging.exception("[%s] 第 %s 轮发生 Cantex SDK 错误：%s", runtime.account.name, cycle_number, outcome)
                else:
                    logging.exception("[%s] 第 %s 轮发生未预期错误：%s", runtime.account.name, cycle_number, outcome)
                continue
            if outcome is not None:
                cycle_results.append(outcome)

        next_interval_seconds = choose_next_interval_seconds(settings.interval_seconds_range)
        logging.info(
            format_cycle_summary(
                cycle_number,
                settings.strategy.max_network_fee,
                threshold_symbol,
                cycle_results,
                next_interval_seconds,
            )
        )
        balance_summary = format_balance_snapshot_summary(cycle_results, threshold_symbol)
        if balance_summary:
            logging.getLogger(BALANCE_LOGGER_NAME).info(balance_summary)
        for result in cycle_results:
            write_account_funds_state(
                settings.accounts,
                result,
                cycle_number=cycle_number,
                total_value_symbol=threshold_symbol,
            )
        funds_summary = format_funds_overview_summary(settings.accounts, threshold_symbol)
        if funds_summary:
            logging.getLogger(FUNDS_LOGGER_NAME).info(funds_summary)


async def async_main(args: argparse.Namespace, settings: RuntimeSettings) -> int:
    needs_intent_signer = (not settings.dry_run) or settings.strategy.create_intent_account
    runtimes = [
        build_account_runtime(
            account,
            base_url=settings.base_url,
            needs_intent_signer=needs_intent_signer,
        )
        for account in settings.accounts
    ]

    async with AsyncExitStack() as stack:
        for runtime in runtimes:
            await stack.enter_async_context(runtime.sdk)

        if args.list_pools:
            first_runtime = runtimes[0]
            if not await authenticate_runtime(first_runtime):
                return 2
            logging.info("使用账号 %s 列出池子信息。", first_runtime.account.name)
            await list_pools(first_runtime.sdk)
            return 0
        await run_schedule(runtimes, settings)
        return 0


def configure_asyncio_policy() -> None:
    if sys.platform != "win32":
        return
    try:
        policy_cls = asyncio.WindowsSelectorEventLoopPolicy
    except AttributeError:
        return
    asyncio.set_event_loop_policy(policy_cls())


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        load_env_file(Path(args.env_file))
        settings = load_runtime_settings(args)
    except ValueError as exc:
        parser.error(str(exc))

    configure_logging(
        args.log_level,
        funds_log_path=str(resolve_funds_log_path(settings.accounts)),
    )

    try:
        try:
            sys.stdout.reconfigure(encoding="utf-8")
            sys.stderr.reconfigure(encoding="utf-8")
        except AttributeError:
            pass
        configure_asyncio_policy()
        return asyncio.run(async_main(args, settings))
    except KeyboardInterrupt:
        logging.warning("已被手动中断，脚本退出。")
        return 130


if __name__ == "__main__":
    sys.exit(main())

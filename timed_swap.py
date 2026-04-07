from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Iterable

try:
    from cantex_sdk import (
        CantexAPIError,
        CantexAuthError,
        CantexSDK,
        CantexTimeoutError,
        InstrumentId,
        IntentTradingKeySigner,
        OperatorKeySigner,
    )
except ImportError as exc:
    raise SystemExit(
        "Missing dependency 'cantex_sdk'. Install it with: pip install -r requirements.txt"
    ) from exc


DEFAULT_BASE_URL = "https://api.testnet.cantex.io"


@dataclass(frozen=True)
class InstrumentSpec:
    instrument_id: str
    instrument_admin: str

    @property
    def label(self) -> str:
        return f"{self.instrument_id} [{self.instrument_admin}]"


@dataclass(frozen=True)
class SwapSettings:
    base_url: str
    api_key_path: str | None
    sell_amount: Decimal
    sell_instrument: InstrumentSpec
    buy_instrument: InstrumentSpec
    max_slippage: Decimal
    min_buy_amount: Decimal | None
    interval_seconds: int | None
    max_runs: int | None
    dry_run: bool
    run_immediately: bool
    create_intent_account: bool


def load_env_file(path: Path) -> None:
    if not path.exists():
        return

    for line_number, raw_line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            raise ValueError(f"Invalid line in {path} at {line_number}: {raw_line!r}")

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if value and value[0] == value[-1] and value[0] in {'"', "'"}:
            value = value[1:-1]
        os.environ.setdefault(key, value)


def parse_decimal(value: str, field_name: str) -> Decimal:
    try:
        parsed = Decimal(value)
    except InvalidOperation as exc:
        raise ValueError(f"{field_name} must be a valid decimal number: {value!r}") from exc
    return parsed


def decimal_text(value: Decimal) -> str:
    normalized = format(value, "f")
    if "." in normalized:
        normalized = normalized.rstrip("0").rstrip(".")
    return normalized or "0"


def configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run fixed-amount swaps on a schedule using the official Cantex SDK."
    )
    parser.add_argument("--env-file", default=".env", help="Path to the env file. Default: .env")
    parser.add_argument("--base-url", help="Override CANTEX_BASE_URL from the env file.")
    parser.add_argument(
        "--api-key-path",
        help="Override CANTEX_API_KEY_PATH. Use 'none' to disable local API key caching.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging verbosity.",
    )
    parser.add_argument(
        "--list-pools",
        action="store_true",
        help="List available pools for the configured account and exit.",
    )
    parser.add_argument(
        "--run-once",
        action="store_true",
        help="Execute a single quote/swap cycle immediately and exit.",
    )
    parser.add_argument("--sell-amount", help="Fixed sell amount for each run.")
    parser.add_argument("--sell-instrument-id", help="Instrument ID to sell.")
    parser.add_argument("--sell-instrument-admin", help="Instrument admin party for the sell token.")
    parser.add_argument("--buy-instrument-id", help="Instrument ID to buy.")
    parser.add_argument("--buy-instrument-admin", help="Instrument admin party for the buy token.")
    parser.add_argument(
        "--max-slippage",
        default="0.005",
        help="Maximum allowed slippage as a decimal fraction. Default: 0.005",
    )
    parser.add_argument(
        "--min-buy-amount",
        help="Optional minimum returned amount. Skip the swap if the quote is lower.",
    )
    parser.add_argument(
        "--max-runs",
        type=int,
        help="Optional maximum number of cycles before exiting.",
    )
    parser.add_argument(
        "--run-immediately",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Whether to execute the first cycle immediately after startup.",
    )
    parser.add_argument(
        "--dry-run",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="When true, fetch quotes but do not submit swaps. Default: true",
    )
    parser.add_argument(
        "--create-intent-account",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Create the intent account automatically if it does not exist.",
    )

    interval_group = parser.add_mutually_exclusive_group()
    interval_group.add_argument("--interval-seconds", type=int, help="Swap interval in seconds.")
    interval_group.add_argument("--interval-minutes", type=int, help="Swap interval in minutes.")

    return parser


def validate_args(args: argparse.Namespace, parser: argparse.ArgumentParser) -> SwapSettings | None:
    base_url = args.base_url or os.getenv("CANTEX_BASE_URL", DEFAULT_BASE_URL)
    raw_api_key_path = args.api_key_path or os.getenv("CANTEX_API_KEY_PATH", "secrets/api_key.txt")
    api_key_path = None if str(raw_api_key_path).lower() == "none" else raw_api_key_path

    if args.list_pools:
        return None

    required_fields = {
        "--sell-amount": args.sell_amount,
        "--sell-instrument-id": args.sell_instrument_id,
        "--sell-instrument-admin": args.sell_instrument_admin,
        "--buy-instrument-id": args.buy_instrument_id,
        "--buy-instrument-admin": args.buy_instrument_admin,
    }
    missing = [flag for flag, value in required_fields.items() if not value]
    if missing:
        parser.error(f"Missing required arguments: {', '.join(missing)}")

    if args.run_once:
        interval_seconds = None
        max_runs = 1
        run_immediately = True
    else:
        if args.interval_seconds is None and args.interval_minutes is None:
            parser.error("Provide --interval-seconds, --interval-minutes, or use --run-once.")
        interval_seconds = args.interval_seconds or args.interval_minutes * 60
        if interval_seconds <= 0:
            parser.error("Interval must be greater than zero.")
        max_runs = args.max_runs
        run_immediately = args.run_immediately

    if max_runs is not None and max_runs <= 0:
        parser.error("--max-runs must be greater than zero.")

    sell_amount = parse_decimal(args.sell_amount, "sell amount")
    if sell_amount <= 0:
        parser.error("--sell-amount must be greater than zero.")

    max_slippage = parse_decimal(args.max_slippage, "max slippage")
    if max_slippage < 0:
        parser.error("--max-slippage must be zero or positive.")

    min_buy_amount = None
    if args.min_buy_amount is not None:
        min_buy_amount = parse_decimal(args.min_buy_amount, "minimum buy amount")
        if min_buy_amount <= 0:
            parser.error("--min-buy-amount must be greater than zero.")

    return SwapSettings(
        base_url=base_url,
        api_key_path=api_key_path,
        sell_amount=sell_amount,
        sell_instrument=InstrumentSpec(args.sell_instrument_id, args.sell_instrument_admin),
        buy_instrument=InstrumentSpec(args.buy_instrument_id, args.buy_instrument_admin),
        max_slippage=max_slippage,
        min_buy_amount=min_buy_amount,
        interval_seconds=interval_seconds,
        max_runs=max_runs,
        dry_run=args.dry_run,
        run_immediately=run_immediately,
        create_intent_account=args.create_intent_account,
    )


def require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise SystemExit(f"Missing required environment variable: {name}")
    return value


def to_sdk_instrument(spec: InstrumentSpec) -> InstrumentId:
    return InstrumentId(id=spec.instrument_id, admin=spec.instrument_admin)


def build_sdk(
    settings: SwapSettings | None,
    list_only: bool,
    *,
    base_url: str,
    api_key_path: str | None,
) -> tuple[CantexSDK, OperatorKeySigner, IntentTradingKeySigner | None]:
    try:
        operator_signer = OperatorKeySigner.from_hex(require_env("CANTEX_OPERATOR_KEY"))
    except Exception as exc:
        raise SystemExit(
            "CANTEX_OPERATOR_KEY is not a valid Ed25519 private key hex string."
        ) from exc

    trading_key = os.getenv("CANTEX_TRADING_KEY")
    needs_intent_signer = False
    if not list_only and settings is not None:
        needs_intent_signer = not settings.dry_run or settings.create_intent_account

    intent_signer = None
    if needs_intent_signer:
        if not trading_key:
            raise SystemExit(
                "CANTEX_TRADING_KEY is required when --no-dry-run or --create-intent-account is used."
            )
        try:
            intent_signer = IntentTradingKeySigner.from_hex(trading_key)
        except Exception as exc:
            raise SystemExit(
                "CANTEX_TRADING_KEY is not a valid secp256k1 private key hex string."
            ) from exc

    if api_key_path:
        Path(api_key_path).expanduser().resolve().parent.mkdir(parents=True, exist_ok=True)

    return (
        CantexSDK(
            operator_signer,
            intent_signer,
            base_url=base_url,
            api_key_path=api_key_path,
        ),
        operator_signer,
        intent_signer,
    )


def current_balance(tokens: Iterable[object], instrument: InstrumentSpec) -> Decimal:
    for token in tokens:
        token_instrument = getattr(token, "instrument", None)
        if (
            getattr(token_instrument, "id", None) == instrument.instrument_id
            and getattr(token_instrument, "admin", None) == instrument.instrument_admin
        ):
            return getattr(token, "unlocked_amount", Decimal("0"))
    return Decimal("0")


async def list_pools(sdk: CantexSDK) -> None:
    pools = await sdk.get_pool_info()
    if not pools.pools:
        logging.info("No pools were returned by the API.")
        return

    for index, pool in enumerate(pools.pools, start=1):
        logging.info(
            "Pool %s | %s [%s] <-> %s [%s] | contract=%s",
            index,
            pool.token_a.id,
            pool.token_a.admin,
            pool.token_b.id,
            pool.token_b.admin,
            pool.contract_id,
        )


async def ensure_intent_account(sdk: CantexSDK, settings: SwapSettings) -> None:
    account_admin = await sdk.get_account_admin()
    if account_admin.has_intent_account:
        return
    if not settings.create_intent_account:
        raise RuntimeError(
            "Intent account is missing. Re-run with --create-intent-account to create it automatically."
        )

    logging.warning("Intent account not found. Creating it now.")
    result = await sdk.create_intent_trading_account()
    logging.info("Intent account created: %s", result)


async def execute_cycle(sdk: CantexSDK, settings: SwapSettings, cycle_number: int) -> None:
    sell_instrument = to_sdk_instrument(settings.sell_instrument)
    buy_instrument = to_sdk_instrument(settings.buy_instrument)

    logging.info(
        "Cycle %s starting | sell=%s %s | buy=%s | dry_run=%s",
        cycle_number,
        decimal_text(settings.sell_amount),
        settings.sell_instrument.label,
        settings.buy_instrument.label,
        settings.dry_run,
    )

    account_info = await sdk.get_account_info()
    unlocked_balance = current_balance(account_info.tokens, settings.sell_instrument)
    logging.info(
        "Available balance for %s: %s",
        settings.sell_instrument.label,
        decimal_text(unlocked_balance),
    )

    if unlocked_balance < settings.sell_amount:
        logging.warning(
            "Skipping cycle %s: balance %s is below required sell amount %s.",
            cycle_number,
            decimal_text(unlocked_balance),
            decimal_text(settings.sell_amount),
        )
        return

    quote = await sdk.get_swap_quote(
        settings.sell_amount,
        sell_instrument,
        buy_instrument,
    )

    logging.info(
        "Quote | receive=%s %s | slippage=%s | trade_price=%s | total_fee=%s | eta=%ss",
        decimal_text(quote.returned_amount),
        settings.buy_instrument.label,
        decimal_text(quote.prices.slippage),
        decimal_text(quote.prices.trade),
        decimal_text(quote.fees.fee_percentage),
        decimal_text(quote.estimated_time_seconds),
    )

    if quote.prices.slippage > settings.max_slippage:
        logging.warning(
            "Skipping cycle %s: slippage %s is above limit %s.",
            cycle_number,
            decimal_text(quote.prices.slippage),
            decimal_text(settings.max_slippage),
        )
        return

    if settings.min_buy_amount is not None and quote.returned_amount < settings.min_buy_amount:
        logging.warning(
            "Skipping cycle %s: returned amount %s is below minimum %s.",
            cycle_number,
            decimal_text(quote.returned_amount),
            decimal_text(settings.min_buy_amount),
        )
        return

    if settings.dry_run:
        logging.info("Dry-run enabled, quote accepted but no swap was submitted.")
        return

    await ensure_intent_account(sdk, settings)
    result = await sdk.swap(
        settings.sell_amount,
        sell_instrument,
        buy_instrument,
    )
    logging.info("Swap submitted successfully: %s", result)


async def run_schedule(sdk: CantexSDK, settings: SwapSettings) -> None:
    cycle_number = 0
    while settings.max_runs is None or cycle_number < settings.max_runs:
        if cycle_number > 0 or not settings.run_immediately:
            assert settings.interval_seconds is not None
            next_run = datetime.now().astimezone()
            logging.info(
                "Sleeping %s seconds before the next cycle. Current local time: %s",
                settings.interval_seconds,
                next_run.strftime("%Y-%m-%d %H:%M:%S %Z"),
            )
            await asyncio.sleep(settings.interval_seconds)

        cycle_number += 1
        try:
            await execute_cycle(sdk, settings, cycle_number)
        except (CantexAuthError, CantexAPIError, CantexTimeoutError) as exc:
            logging.exception("Cantex SDK error in cycle %s: %s", cycle_number, exc)
        except Exception as exc:
            logging.exception("Unexpected error in cycle %s: %s", cycle_number, exc)


def explain_auth_error(exc: CantexAPIError, base_url: str, operator_signer: OperatorKeySigner) -> str:
    operator_public_key_b64 = operator_signer.get_public_key_b64()
    operator_public_key_hex = operator_signer.get_public_key_hex()

    if exc.status == 400 and "invalid publicKey" in exc.body:
        return (
            "Authentication failed: Cantex rejected the operator public key.\n"
            f"Current API base URL: {base_url}\n"
            f"Derived operator public key (base64): {operator_public_key_b64}\n"
            f"Derived operator public key (hex): {operator_public_key_hex}\n"
            "Most likely causes:\n"
            "1. You are pointing at the wrong network. The current .env is set to testnet, but "
            "the key may belong to a mainnet account created in https://www.cantex.io/app .\n"
            "2. CANTEX_OPERATOR_KEY is not the account's Ed25519 operator private key.\n"
            "3. You pasted the trading key or another wallet key into CANTEX_OPERATOR_KEY.\n"
            "If your account was created on the production app, set CANTEX_BASE_URL=https://api.cantex.io "
            "and try again."
        )

    return (
        "Authentication failed while talking to the Cantex API.\n"
        f"Current API base URL: {base_url}\n"
        f"Operator public key (base64): {operator_public_key_b64}\n"
        f"API response: {exc}"
    )


async def async_main(args: argparse.Namespace, settings: SwapSettings | None) -> int:
    resolved_base_url = args.base_url or (settings.base_url if settings else os.getenv("CANTEX_BASE_URL", DEFAULT_BASE_URL))
    raw_api_key_path = args.api_key_path or (
        settings.api_key_path if settings else os.getenv("CANTEX_API_KEY_PATH", "secrets/api_key.txt")
    )
    resolved_api_key_path = None if str(raw_api_key_path).lower() == "none" else raw_api_key_path

    sdk, operator_signer, _intent_signer = build_sdk(
        settings,
        list_only=args.list_pools,
        base_url=resolved_base_url,
        api_key_path=resolved_api_key_path,
    )
    async with sdk:
        try:
            await sdk.authenticate()
        except CantexAPIError as exc:
            logging.error(explain_auth_error(exc, sdk.base_url, operator_signer))
            return 2
        except CantexAuthError as exc:
            logging.error("Authentication failed: %s", exc)
            return 2

        if args.list_pools:
            await list_pools(sdk)
            return 0

        assert settings is not None
        logging.info(
            "Connected to %s | pair=%s -> %s | amount=%s | dry_run=%s",
            settings.base_url,
            settings.sell_instrument.label,
            settings.buy_instrument.label,
            decimal_text(settings.sell_amount),
            settings.dry_run,
        )
        await run_schedule(sdk, settings)
        return 0


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    try:
        load_env_file(Path(args.env_file))
    except ValueError as exc:
        parser.error(str(exc))

    configure_logging(args.log_level)
    settings = validate_args(args, parser)

    try:
        return asyncio.run(async_main(args, settings))
    except KeyboardInterrupt:
        logging.warning("Interrupted by user, exiting.")
        return 130


if __name__ == "__main__":
    sys.exit(main())

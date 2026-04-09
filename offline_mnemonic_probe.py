from __future__ import annotations

import argparse
import getpass
import hashlib
import hmac
import sys
import unicodedata
from dataclasses import dataclass

from ecdsa import SECP256k1, SigningKey

try:
    from cantex_sdk import IntentTradingKeySigner, OperatorKeySigner
except ImportError as exc:
    raise SystemExit(
        "缺少依赖 'cantex_sdk'，请先在当前项目环境里执行：pip install -r requirements.txt"
    ) from exc


HARDENED_OFFSET = 0x80000000
SECP256K1_ORDER = SECP256k1.order

DEFAULT_SECP256K1_PATHS = (
    "m/44'/0'/0'/0/{index}",
    "m/44'/60'/0'/0/{index}",
    "m/44'/60'/0'/{index}",
    "m/44'/118'/0'/0/{index}",
    "m/44'/118'/0'/{index}",
    "m/44'/931'/0'/0/{index}",
    "m/44'/931'/0'/{index}",
)

DEFAULT_ED25519_PATHS = (
    "m/44'/0'/0'/0'/{index}'",
    "m/44'/60'/0'/0'/{index}'",
    "m/44'/60'/0'/{index}'",
    "m/44'/118'/0'/0'/{index}'",
    "m/44'/118'/0'/{index}'",
    "m/44'/931'/0'/0'/{index}'",
    "m/44'/931'/0'/{index}'",
)


@dataclass(frozen=True)
class DerivedCandidate:
    curve: str
    path: str
    private_key_hex: str
    public_key_hex: str
    public_key_b64: str | None = None
    public_key_der_hex: str | None = None
    matched_private: tuple[str, ...] = ()
    matched_public: tuple[str, ...] = ()


def normalize_text(value: str) -> str:
    return unicodedata.normalize("NFKD", value.strip())


def normalize_hex(value: str) -> str:
    return value.strip().lower().removeprefix("0x")


def build_seed(mnemonic: str, passphrase: str) -> bytes:
    normalized_mnemonic = normalize_text(mnemonic)
    normalized_passphrase = normalize_text(passphrase)
    return hashlib.pbkdf2_hmac(
        "sha512",
        normalized_mnemonic.encode("utf-8"),
        ("mnemonic" + normalized_passphrase).encode("utf-8"),
        2048,
        dklen=64,
    )


def parse_derivation_path(path: str) -> list[int]:
    raw = path.strip()
    if not raw.startswith("m"):
        raise ValueError(f"派生路径必须以 m 开头：{path}")
    if raw == "m":
        return []
    if not raw.startswith("m/"):
        raise ValueError(f"派生路径格式不正确：{path}")

    segments: list[int] = []
    for part in raw[2:].split("/"):
        hardened = part.endswith("'")
        number_text = part[:-1] if hardened else part
        if not number_text.isdigit():
            raise ValueError(f"派生路径段不是数字：{part}")
        number = int(number_text)
        if number < 0 or number >= HARDENED_OFFSET:
            raise ValueError(f"派生路径段超出范围：{part}")
        segments.append(number | HARDENED_OFFSET if hardened else number)
    return segments


def mnemonic_words(mnemonic: str) -> list[str]:
    return [word for word in normalize_text(mnemonic).split() if word]


def format_indexed_paths(templates: list[str], start_index: int, count: int) -> list[str]:
    results: list[str] = []
    for template in templates:
        for offset in range(count):
            index = start_index + offset
            results.append(template.format(index=index))
    return results


def hmac_sha512(key: bytes, data: bytes) -> bytes:
    return hmac.new(key, data, hashlib.sha512).digest()


def ser32(index: int) -> bytes:
    return index.to_bytes(4, byteorder="big", signed=False)


def ser256(value: int) -> bytes:
    return value.to_bytes(32, byteorder="big", signed=False)


def compressed_pubkey_from_secret(secret_int: int) -> bytes:
    signing_key = SigningKey.from_secret_exponent(secret_int, curve=SECP256k1)
    verifying_key = signing_key.get_verifying_key()
    raw = verifying_key.to_string()
    x_bytes = raw[:32]
    y_int = int.from_bytes(raw[32:], byteorder="big", signed=False)
    prefix = b"\x03" if (y_int & 1) else b"\x02"
    return prefix + x_bytes


def derive_secp256k1_private_key(seed: bytes, path: str) -> bytes:
    key_material = hmac_sha512(b"Bitcoin seed", seed)
    private_key = int.from_bytes(key_material[:32], byteorder="big", signed=False)
    chain_code = key_material[32:]
    if private_key == 0 or private_key >= SECP256K1_ORDER:
        raise ValueError("BIP32 master private key 无效。")

    for index in parse_derivation_path(path):
        if index & HARDENED_OFFSET:
            data = b"\x00" + ser256(private_key) + ser32(index)
        else:
            data = compressed_pubkey_from_secret(private_key) + ser32(index)
        child_material = hmac_sha512(chain_code, data)
        child_offset = int.from_bytes(child_material[:32], byteorder="big", signed=False)
        if child_offset >= SECP256K1_ORDER:
            raise ValueError(f"路径 {path} 派生出无效 secp256k1 子密钥。")
        private_key = (child_offset + private_key) % SECP256K1_ORDER
        if private_key == 0:
            raise ValueError(f"路径 {path} 派生出零值 secp256k1 子密钥。")
        chain_code = child_material[32:]

    return ser256(private_key)


def derive_ed25519_private_key(seed: bytes, path: str) -> bytes:
    key_material = hmac_sha512(b"ed25519 seed", seed)
    private_key = key_material[:32]
    chain_code = key_material[32:]

    for index in parse_derivation_path(path):
        if not (index & HARDENED_OFFSET):
            raise ValueError(f"Ed25519 仅支持 hardened 路径：{path}")
        data = b"\x00" + private_key + ser32(index)
        child_material = hmac_sha512(chain_code, data)
        private_key = child_material[:32]
        chain_code = child_material[32:]

    return private_key


def build_ed25519_candidate(
    path: str,
    private_key: bytes,
    match_private: set[str],
    match_public: set[str],
) -> DerivedCandidate:
    private_key_hex = private_key.hex()
    signer = OperatorKeySigner.from_hex(private_key_hex)
    public_key_hex = signer.get_public_key_hex().lower()
    return DerivedCandidate(
        curve="ed25519",
        path=path,
        private_key_hex=private_key_hex,
        public_key_hex=public_key_hex,
        public_key_b64=signer.get_public_key_b64(),
        matched_private=tuple(
            target for target in sorted(match_private) if target == private_key_hex
        ),
        matched_public=tuple(
            target for target in sorted(match_public) if target == public_key_hex
        ),
    )


def build_secp256k1_candidate(
    path: str,
    private_key: bytes,
    match_private: set[str],
    match_public: set[str],
) -> DerivedCandidate:
    private_key_hex = private_key.hex()
    signer = IntentTradingKeySigner.from_hex(private_key_hex)
    public_key_hex = signer.get_public_key_hex().lower()
    public_key_der_hex = signer.get_public_key_hex_der().lower()
    matched_public = [
        target
        for target in sorted(match_public)
        if target in {public_key_hex, public_key_der_hex}
    ]
    return DerivedCandidate(
        curve="secp256k1",
        path=path,
        private_key_hex=private_key_hex,
        public_key_hex=public_key_hex,
        public_key_der_hex=public_key_der_hex,
        matched_private=tuple(
            target for target in sorted(match_private) if target == private_key_hex
        ),
        matched_public=tuple(matched_public),
    )


def derive_candidates(
    seed: bytes,
    secp_paths: list[str],
    ed25519_paths: list[str],
    match_private: set[str],
    match_public: set[str],
) -> tuple[list[DerivedCandidate], list[str]]:
    candidates: list[DerivedCandidate] = []
    errors: list[str] = []

    for path in ed25519_paths:
        try:
            private_key = derive_ed25519_private_key(seed, path)
            candidates.append(
                build_ed25519_candidate(path, private_key, match_private, match_public)
            )
        except Exception as exc:
            errors.append(f"[ed25519] {path}: {exc}")

    for path in secp_paths:
        try:
            private_key = derive_secp256k1_private_key(seed, path)
            candidates.append(
                build_secp256k1_candidate(path, private_key, match_private, match_public)
            )
        except Exception as exc:
            errors.append(f"[secp256k1] {path}: {exc}")

    return candidates, errors


def print_candidate(candidate: DerivedCandidate, show_private: bool) -> None:
    role = "operator" if candidate.curve == "ed25519" else "trading"
    print(f"[{candidate.curve}] {candidate.path}")
    print(f"  {role}_public_key_hex: {candidate.public_key_hex}")
    if candidate.public_key_b64 is not None:
        print(f"  {role}_public_key_b64: {candidate.public_key_b64}")
    if candidate.public_key_der_hex is not None:
        print(f"  {role}_public_key_der_hex: {candidate.public_key_der_hex}")
    if show_private:
        print(f"  {role}_private_key_hex: {candidate.private_key_hex}")
    if candidate.matched_private:
        print("  matched_private: " + ", ".join(candidate.matched_private))
    if candidate.matched_public:
        print("  matched_public: " + ", ".join(candidate.matched_public))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "本地离线助记词探针：从 BIP39 助记词派生候选 Ed25519 / secp256k1 key，"
            "用于排查 Cantex operator / trading key 是否拿错。"
        )
    )
    parser.add_argument(
        "--mnemonic",
        help="BIP39 助记词。建议留空，脚本会安全地交互输入，不会回显。",
    )
    parser.add_argument(
        "--passphrase",
        default=None,
        help="BIP39 passphrase。建议留空，脚本会安全地交互输入，不会回显。",
    )
    parser.add_argument(
        "--start-index",
        type=int,
        default=0,
        help="从哪个地址索引开始枚举，默认 0。",
    )
    parser.add_argument(
        "--count",
        type=int,
        default=5,
        help="每条路径模板枚举多少个索引，默认 5。",
    )
    parser.add_argument(
        "--show-private",
        action="store_true",
        help="显示派生出的私钥。默认不显示，只展示公钥和匹配结果。",
    )
    parser.add_argument(
        "--match-private",
        action="append",
        default=[],
        help="要比对的可疑私钥 hex，可重复传入。",
    )
    parser.add_argument(
        "--match-public",
        action="append",
        default=[],
        help="要比对的可疑公钥 hex，可重复传入。",
    )
    parser.add_argument(
        "--extra-secp-path",
        action="append",
        default=[],
        help="额外 secp256k1 路径模板，可用 {index} 占位，例如 m/44'/60'/0'/0/{index}。",
    )
    parser.add_argument(
        "--extra-ed25519-path",
        action="append",
        default=[],
        help="额外 Ed25519 路径模板，可用 {index} 占位，例如 m/44'/118'/0'/0'/{index}'。",
    )
    return parser.parse_args()


def validate_args(args: argparse.Namespace) -> None:
    if args.start_index < 0:
        raise SystemExit("--start-index 不能小于 0。")
    if args.count <= 0:
        raise SystemExit("--count 必须大于 0。")


def main() -> int:
    args = parse_args()
    validate_args(args)

    mnemonic = args.mnemonic
    if mnemonic is None:
        mnemonic = getpass.getpass("请输入助记词（不会回显）：").strip()
    passphrase = args.passphrase
    if passphrase is None:
        passphrase = getpass.getpass("请输入 BIP39 passphrase（可直接回车留空，不会回显）：")

    words = mnemonic_words(mnemonic)
    if len(words) not in {12, 15, 18, 21, 24}:
        print(
            f"警告：当前词数为 {len(words)}，不像标准 BIP39 助记词。脚本仍会继续尝试派生。",
            file=sys.stderr,
        )

    secp_paths = format_indexed_paths(
        [*DEFAULT_SECP256K1_PATHS, *args.extra_secp_path],
        start_index=args.start_index,
        count=args.count,
    )
    ed25519_paths = format_indexed_paths(
        [*DEFAULT_ED25519_PATHS, *args.extra_ed25519_path],
        start_index=args.start_index,
        count=args.count,
    )

    match_private = {normalize_hex(item) for item in args.match_private if item.strip()}
    match_public = {normalize_hex(item) for item in args.match_public if item.strip()}
    seed = build_seed(mnemonic, passphrase)

    print("离线派生开始。")
    print(
        f"词数={len(words)} | secp256k1路径数={len(secp_paths)} | "
        f"ed25519路径数={len(ed25519_paths)}"
    )
    print("说明：本脚本不会发起任何网络请求，也不会自动保存助记词。")
    print()

    candidates, errors = derive_candidates(
        seed,
        secp_paths=secp_paths,
        ed25519_paths=ed25519_paths,
        match_private=match_private,
        match_public=match_public,
    )

    public_matches = [candidate for candidate in candidates if candidate.matched_public]
    private_matches = [candidate for candidate in candidates if candidate.matched_private]

    if public_matches or private_matches:
        print("发现候选匹配：")
        print()
        seen_ids: set[tuple[str, str]] = set()
        for candidate in [*public_matches, *private_matches]:
            key = (candidate.curve, candidate.path)
            if key in seen_ids:
                continue
            seen_ids.add(key)
            print_candidate(candidate, show_private=args.show_private)
            print()
    else:
        print("在当前默认路径范围内没有发现任何匹配。")
        print("你可以稍后加大 --count，或者追加 --extra-secp-path / --extra-ed25519-path 重试。")
        print()

    print("全部候选：")
    print()
    for candidate in candidates:
        print_candidate(candidate, show_private=args.show_private)
        print()

    if errors:
        print("派生报错：")
        for message in errors:
            print(f"  - {message}")
        print()

    print(
        "排查建议：\n"
        "1. 先看 ed25519 条目，把 operator_public_key_hex / operator_public_key_b64 "
        "和 Cantex 日志里的鉴权公钥对比。\n"
        "2. 再看 secp256k1 条目，把 matched_private / matched_public "
        "和你手头可疑 trading key 对比。\n"
        "3. 如果默认路径没有命中，优先补你原钱包对应的派生路径。"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

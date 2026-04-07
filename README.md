# Cantex Swap Scripts

这个目录现在有两种运行方式：

- `multi_account_swap.py`
  - 核心交易脚本
  - 支持多账号并发
  - 也支持 `--account-name` 只跑单个账号
- `launcher.py`
  - 按账号批量拉起独立 Python 进程
  - 适合 10-100 个账号长期运行
  - 每个账号可以绑定自己的代理和日志文件

## 1. 安装依赖

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

## 2. 环境变量

先复制：

```powershell
Copy-Item .env.example .env
```

如果你的账号是在 `https://www.cantex.io/app` 创建的，请使用主网：

```env
CANTEX_BASE_URL=https://api.cantex.io
```

`.env.example` 里已经给了多账号代理变量的写法，例如：

```env
CANTEX_PROXY_ACCOUNT_01=http://username:password@127.0.0.1:8001
CANTEX_PROXY_ACCOUNT_02=http://username:password@127.0.0.1:8002
```

如果你用的是 `socks5://` 代理，`requirements.txt` 里已经补了 `aiohttp-socks`。

## 3. 配置文件

先复制示例：

```powershell
Copy-Item multi_account_swap.example.json multi_account_swap.json
```

当前配置的核心规则是：

- 优先选择当前账号里按 `CC` 等值计算后价值最高的币种
- 在该币余额的 `30%-99%` 之间随机卖出
- 本地最小卖出下限仍保留 `> 6 CC`
- 平台真实最小票据仍是 `10 CC` 等值，脚本会提前跳过不满足的单子
- 只有当 `Network Fee < 0.15 CC` 时才继续
- 成功触发后下一轮等待 `60s`，平时每 `10s` 检查一次

每个账号现在支持这些字段：

- `name`
- `operator_key_env`
- `trading_key_env`
- `proxy_url` 或 `proxy_url_env`
- `api_key_path`
- `log_path`
- `enabled`

示例：

```json
{
  "name": "account-01",
  "operator_key_env": "CANTEX_OPERATOR_KEY",
  "trading_key_env": "CANTEX_TRADING_KEY",
  "proxy_url_env": "CANTEX_PROXY_ACCOUNT_01",
  "log_path": "logs/account-01.log"
}
```

`launcher` 段是给批量启动器用的：

```json
"launcher": {
  "log_dir": "logs",
  "startup_stagger_seconds": 0
}
```

## 4. 单账号运行

先做只读测试：

```powershell
python multi_account_swap.py --config-file multi_account_swap.json --account-name account-01 --run-once --dry-run
```

正式运行单账号：

```powershell
python multi_account_swap.py --config-file multi_account_swap.json --account-name account-01 --no-dry-run
```

## 5. 多账号并发运行

如果你只是想在一个进程里并发所有启用账号：

```powershell
python multi_account_swap.py --config-file multi_account_swap.json --no-dry-run
```

如果你想按“每个账号一个独立进程”的方式运行：

```powershell
python launcher.py --config-file multi_account_swap.json --no-dry-run
```

这时所有启用账号会按各自代理并行启动；成交或 `dry-run` 命中时，日志会额外带上 `出口IP=`。

常用 launcher 参数：

- `--max-accounts 10`
  - 先只启动前 10 个账号做灰度
- `--stagger-seconds 3`
  - 每个账号进程启动之间等 3 秒
- `--dry-run`
  - 临时覆盖为只读模式
- `--interval-seconds 15`
  - 临时覆盖轮询间隔

例如：

```powershell
python launcher.py --config-file multi_account_swap.json --max-accounts 5 --dry-run --stagger-seconds 3
```

## 6. 100 号建议架构

如果你准备扩到 100 个号，建议这样组织：

- `1` 个 launcher
- `N` 个账号块写在同一个 `multi_account_swap.json`
- 每个账号单独一个代理
- 每个账号单独一个 `api_key_path`
- 每个账号单独一个 `log_path`

推荐分阶段推进：

1. 先开 `5` 个号验证代理、下单和日志。
2. 再扩到 `20` 个号观察稳定性。
3. 最后再补齐到 `100` 个号。

## 7. 风险提醒

- 先用 `--dry-run` 跑一段时间再切实盘。
- 每个账号都要先单独验证密钥、代理和 intent account。
- 当前 Cantex 平台最小票据仍是 `10 CC` 等值，这不是脚本本地能绕过的限制。
- 多账号和代理运行前，请确认你的使用方式符合平台规则。

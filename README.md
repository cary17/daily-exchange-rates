# Daily Exchange Rates

手动采集 Visa、Mastercard、银联官方汇率。源码位于 `main`，JSON 数据和归档位于 `data`，不发布 Releases。

Visa、Mastercard 获取 USD、EUR、JPY、GBP、CNY、CHF、CAD、AUD、NZD、HKD、SGD 共 11 个核心币种的 110 个有向组合；银联保留官方完整 JSON。汇率不附加手续费，不反推或补造缺失币对。

## 安装与本地使用

使用 Python 3.13，在源码目录执行：

```sh
python3.13 -m venv .venv
. .venv/bin/activate
python -m pip install -r requirements.lock
python -m pip install --no-deps .
python -m exchange_rates fetch --provider visa --data-dir ./rates-data
python -m exchange_rates fetch --provider mastercard --start-date 2026-01-01 --end-date 2026-01-03 --data-dir ./rates-data
```

`--provider` 可选 `visa`、`mastercard`、`unionpay`。发布到已配置的 Git 远端时，追加 `--publish --remote origin --branch data`，并使用新的空 `--data-dir`；程序从远端同步后采集、提交与推送，不清空已有本地数据目录。

## 手动采集

在 GitHub **Settings → Actions → General → Workflow permissions** 允许读写权限，并确保 `data` 的分支保护或规则集允许工作流推送及年度快照的强制推送。将 `main` 设为默认分支。

GitHub 仓库的 **Actions** 中分别运行 **Fetch Visa**、**Fetch Mastercard** 或 **Fetch UnionPay**，选择 `main`：

- `start_date`：`YYYY-MM-DD`，留空使用运行时北京时间当天。
- `end_date`：`YYYY-MM-DD`，含结束日；留空仅采集起始日。
- 日期区间无程序硬上限，单次工作流最长运行 360 分钟。建议按实际耗时拆分长区间。

采集没有定时触发，也不会自动回退到其他日期。请求日期、机构实际日期等采集信息保存在独立的 `metadata` 数据中。已封存日期仍可手动补录并重建对应归档。

四个写入工作流共用一个并发组，串行写入 `data`，最多排队 100 个任务；满队列后新增任务会被 GitHub 取消。

## 数据访问

浏览仓库时切换到 `data` 分支。`provider` 为 `visa`、`mastercard` 或 `unionpay`。

| 内容 | 路径 |
| --- | --- |
| 最新汇率 | `latest/provider.json` |
| 最新原始响应 | `raw/latest/provider.json` |
| 最新采集信息 | `metadata/latest/provider.json` |
| 每日汇率 | `history/YYYY/MM/provider/YYYY-MM-DD.json` |
| 每日原始响应 | `raw/history/YYYY/MM/provider/YYYY-MM-DD.json` |
| 每日采集信息 | `metadata/history/YYYY/MM/provider/YYYY-MM-DD.json` |
| 月度归档 | `history/YYYY/MM/YYYY-MM.tar.gz` |
| 年度归档 | `history/YYYY.tar.gz` |

将以下模板中的 `OWNER`、`REPO` 替换为仓库信息，可直接读取 `data` 分支数据；每日数据、清单和归档沿用相同 URL 前缀：

```text
https://raw.githubusercontent.com/OWNER/REPO/data/latest/visa.json
https://raw.githubusercontent.com/OWNER/REPO/data/raw/latest/unionpay.json
https://raw.githubusercontent.com/OWNER/REPO/data/metadata/latest/mastercard.json
```

Visa、Mastercard 汇率 JSON 仅含 `exchangeRateJson` 列表；`rateData` 表示 1 单位 `transCur` 可兑换的 `baseCur` 数量。原始响应与采集信息单独保存，元数据的 `response_dates` 保留接口返回的日期字段。银联完整 JSON 按机构原文保留。

## 归档与补跑

**Archive** 于每月北京时间 3 日 00:00 运行，也可在 **Actions** 手动运行。只处理已到期月份，扫描并补齐所有遗漏的到期归档，不自动抓取汇率。本地补跑：

```sh
python -m exchange_rates archive --data-dir ./rates-data
```

月包包含 `history`、`raw`、`metadata` 三类每日文件，并保留每日路径；同目录的 `manifest.json` 记录文件日期、机构、大小及哈希，`SHA256SUMS` 校验月包和清单。

一月先归档上一年十二月，再生成上一年年度包。年度包直接包含展平后的三类每日 JSON，不嵌套月包；旁附 `YYYY.manifest.json`、`YYYY.sha256`。验证成功后删除该年已归档的每日文件和月包，保留 `latest`，并将 `data` 重建为一次快照。其他时间保留正常 Git 历史。

年度快照会更换 `data` 的历史链；本地数据副本需重新同步，最直接的方式是重新克隆 `data` 分支：

```sh
git clone --single-branch --branch data https://github.com/OWNER/REPO.git rates-data
```

机构地址、核心币种与请求设置见 `config/providers.json`；新增机构需实现 `src/exchange_rates/providers/` 下的 provider 接口并注册。

## 许可

代码采用 **AGPL-3.0-only**，完整文本见 [LICENSE](LICENSE)。机构原始响应及由其整理的汇率数据不套用代码许可，其权利与使用条件以各机构原始来源为准。

# Daily Exchange Rates

手动采集 Visa、Mastercard、银联官方汇率。源码在 `main`，当前数据在 `data`，月度和年度归档在 [Releases](https://github.com/cary17/daily-exchange-rates/releases)。

Visa、Mastercard 自动读取各自官方支持的全部币种，交易币种平均分成九组；每组由独立 Actions job 串行查询全部账单币种，全部成功后合并为每机构一份 JSON。银联继续直接下载完整 JSON。手续费为零，反向独立查询，不取倒数。

## 使用 GitHub Actions

将 `main` 设为默认分支，允许 Actions 读写仓库；`data` 的分支规则需允许年度快照的强制推送，并确保账户有可用的 Git LFS 配额。

在 [Actions](https://github.com/cary17/daily-exchange-rates/actions) 分别运行 **Fetch Visa**、**Fetch Mastercard** 或 **Fetch UnionPay**，选择 `main`：

- `start_date`：`YYYY-MM-DD`，留空使用运行时北京时间当天。
- `end_date`：包含结束日，留空仅抓起始日。
- `interval`：每个会话的请求间隔，单位秒，默认 `0.3`；本次运行可填写 `0.5`、`1` 等非负数。
- `shards`：并行分片 job 数，默认 `9`；每个 job 使用独立 runner 与独立出口地址。
- 日期严格匹配，错误日期的响应计入待补抓项，不回退到其他日期。
- 每个分片在自身进程内串行查询，只补抓本分片失败、缺失或校验未通过的币对；成功币对保留，不重复请求。默认最多补抓两轮，每轮只处理仍未成功的部分。
- 官方目录包含的历史币种同样查询；补抓耗尽仍有错漏时，该日不发布、不覆盖已有完整数据。摘要只列关键统计，完整错漏与响应证据保存在诊断 Artifact。长区间请分批运行，单次工作流最多 360 分钟。

各机构的 `recovery_rounds` 在 `config/providers.json` 配置，默认 `2`，设为 `0` 关闭末尾补抓；它独立于 HTTP 传输重试。银联仅提供整份 JSON，校验失败时重新下载该日完整文件。原始记录保留失败与补抓请求，元数据记录补抓轮数和恢复数量。

抓取没有定时任务。Visa 与 Mastercard 各自编排为 `prepare → shard × N → plan-retry → retry × 失败分片数 → merge`：`prepare` 计算日期与分片矩阵，`shard` 每个 job 只抓一个分片并把结果上传为 Artifact；首轮全部结束后，`plan-retry` 仅按结果文件是否存在找出缺失分片及日期，不重复读取大响应或执行完整校验。没有缺片时跳过 `retry`，否则用新 runner 补跑一轮，成功分片及已成功的日期不重复采集。`merge` 先下载首轮结果，再覆盖补跑结果，校验币对完整性与目录一致性后一次性发布；补跑仍有缺片时该日不发布部分数据。银联仍是单 job 直接下载整份 JSON。工作流之间串行发布。

默认每个会话的 `http.interval` 为 `0.3` 秒，三个抓取工作流的 `interval` 输入可在每次运行时修改；留空回退到默认值，支持有限的非负数，`0` 表示关闭会话间隔。本地使用 `--interval 0.8` 临时覆盖机构与全局配置，不传参数则沿用配置。`http.global_interval` 保持 `0`，不启用跨会话共享限速。

分片之间不共享出口地址，因此每个分片都是独立串行流。单分片实测约 3 次请求/秒，9 片并行时全长查询约 15 至 20 分钟；实际速度取决于接口响应。

收到 403 后暂停该分片 60 秒，冷却结束只放行一个探测请求；HTTP 200 清零连续拒绝计数，连续 3 个拒绝批次则停止该分片。同一批已在途的响应完整保留在诊断中，但不会重复累计拒绝次数或用迟到的成功响应误判恢复。403 不做即时传输重试，失败币对仍由末尾补抓处理；不会发布部分数据。冷却与阈值可在配置中调整。

每次运行均保存简短统计和诊断文件；失败时也通过 `actions/upload-artifact` 上传，保留 7 天，摘要提供下载链接。摘要累计最多 512 KiB，完整错误与原始分片不写入摘要。访问控制提前停止时保留已尝试记录，未查询的币对不会伪报为 HTTP 失败。

## 本地使用

需要 Python 3.13 和 `git-lfs`：

```sh
python3.13 -m venv .venv
. .venv/bin/activate
python -m pip install -r requirements.lock
python -m pip install --no-deps .
python -m exchange_rates fetch --provider visa --data-dir ./rates-data
python -m exchange_rates fetch --provider mastercard --start-date 2026-09-29 --end-date 2026-09-30 --data-dir ./rates-data
```

本地归档导出到独立的 `release-output`；`--release-dir` 可指定其它目录。向 GitHub 发布时设置 `GITHUB_REPOSITORY`、`GITHUB_TOKEN` 和 Git 推送认证，追加 `--publish --remote origin --branch data`，并使用新的空 `--data-dir`。

`fetch --provider visa|mastercard` 在单进程内并发抓取全部九个分片，适合本机快速验证；Actions 使用等价的 `fetch-shard` 与 `merge-shards`，把每个分片放到独立 runner：

```sh
python -m exchange_rates fetch-shard --provider mastercard --start-date 2026-10-03 \
  --shard-index 0 --shard-count 9 --shard-dir ./shards
python -m exchange_rates merge-shards --provider mastercard --start-date 2026-10-03 \
  --shard-dir ./shards --data-dir ./rates-data
```

`fetch-shard` 只写 `--shard-dir/<provider>/<date>/shard-NN.json`，不写已发布数据；`merge-shards` 校验分片齐全、目录一致、币对无缺失与重复后写入 `--data-dir`。分片缺失或校验失败时该日不发布，其余日期不受影响。

## 数据读取

| 内容 | `data` 分支路径 |
| --- | --- |
| 最新统一汇率 | `latest/provider.json` |
| 最新原始响应或索引 | `raw/latest/provider.json` |
| 最新原始响应分片 | `raw/latest/provider.parts/shard-01.json` 至 `shard-09.json` |
| 最新采集元数据 | `metadata/latest/provider.json` |
| 未归档每日汇率 | `history/YYYY/MM/provider/YYYY-MM-DD.json` |
| 每日原始响应或索引 | `raw/history/YYYY/MM/provider/YYYY-MM-DD.json` |
| 每日原始响应分片 | `raw/history/YYYY/MM/provider/YYYY-MM-DD.parts/shard-01.json` 至 `shard-09.json` |
| 每日元数据 | `metadata/history/YYYY/MM/provider/YYYY-MM-DD.json` |
| 最近三个月的月包 | `history/YYYY/MM/YYYY-MM.tar.gz`，大包可能分卷 |

`provider` 为 `visa`、`mastercard` 或 `unionpay`。银联原始记录仍为单文件，无分片。Visa、Mastercard 原始索引包含相对分片目录、文件大小和 SHA256；每片保留请求及完整响应原文。元数据记录目录范围、分片覆盖和接口返回日期。

统一数据使用 `exchangeRateJson`，`rateData` 表示 1 单位 `transCur` 可兑换的 `baseCur` 数量。例：

```text
https://raw.githubusercontent.com/cary17/daily-exchange-rates/data/latest/visa.json
```

单文件超过 100 MiB 自动使用 Git LFS；网页原始链接可能返回 LFS 指针，使用安装了 Git LFS 的 Git 客户端读取完整文件。

## 归档与下载

**Archive** 于每月北京时间 3 日 00:00 运行，也可手动补跑；仅归档已有数据，不抓取汇率。月包发布成功并校验后清理对应明细；所有月包均保留在 Release，仓库只留最近三个已归档日历月的月包。年度包只在 Release，不入 `data`。

一月先完成上年十二月归档，再从月包流式生成展平的年包，成功后重建一次 `data` 分支快照。大归档按 1900 MiB 分卷。封存日期可手动补录，相关月包及已存在的年包一起更新；部分发布失败需重跑原补录，避免月年内容不一致。

Release 标签为 `rates-YYYY-MM` 或 `rates-YYYY`，附件采用内容哈希名称。用以下命令下载、恢复原文件名并验证 SHA256：

```sh
python -m exchange_rates download --repository cary17/daily-exchange-rates --period 2026-09 --output-dir downloads/2026-09
# 单包直接解压；分卷先按编号合并，再解压
if [ -f downloads/2026-09/2026-09.tar.gz.part001 ]; then
  cat downloads/2026-09/2026-09.tar.gz.part* > downloads/2026-09/2026-09.tar.gz
fi
tar -xzf downloads/2026-09/2026-09.tar.gz
```

年度快照后本地 `data` 副本需重新同步，最直接是重新克隆：

```sh
git clone --single-branch --branch data https://github.com/cary17/daily-exchange-rates.git rates-data
```

三个月留存指当前分支文件；Git/LFS 历史对象回收及配额由 GitHub 管理。机构配置见 `config/providers.json`；新增查询源可复用 `CurrencyCatalog` 和九分片并发接口，并增加独立工作流。

## 许可

代码采用 **AGPL-3.0-only**，完整文本见 [LICENSE](LICENSE)。机构原始响应及汇率数据不套用代码许可，使用条件以原始来源为准。

# Daily Exchange Rates

Visa、Mastercard、银联、JCB 官方参考汇率数据。最新数据和未归档明细位于 [`data` 分支](https://github.com/cary17/daily-exchange-rates/tree/data)，月度、年度归档位于 [Releases](https://github.com/cary17/daily-exchange-rates/releases)。

## 汇率文件

| 内容 | 最新文件 |
| --- | --- |
| Visa | [visa.json](https://raw.githubusercontent.com/cary17/daily-exchange-rates/data/latest/visa.json) |
| Mastercard | [mastercard.json](https://raw.githubusercontent.com/cary17/daily-exchange-rates/data/latest/mastercard.json) |
| 银联 | [unionpay.json](https://raw.githubusercontent.com/cary17/daily-exchange-rates/data/latest/unionpay.json) |
| JCB · JPY | [jcb-jpy.json](https://raw.githubusercontent.com/cary17/daily-exchange-rates/data/latest/jcb-jpy.json) |
| JCB · USD Buy | [jcb-usd-buy.json](https://raw.githubusercontent.com/cary17/daily-exchange-rates/data/latest/jcb-usd-buy.json) |
| JCB · USD Mid | [jcb-usd-mid.json](https://raw.githubusercontent.com/cary17/daily-exchange-rates/data/latest/jcb-usd-mid.json) |
| JCB · USD Sell | [jcb-usd-sell.json](https://raw.githubusercontent.com/cary17/daily-exchange-rates/data/latest/jcb-usd-sell.json) |

### 格式与精度

汇率位于 `exchangeRateJson` 数组中，结构示例（数值仅作示意）：

```json
{
  "exchangeRateJson": [
    {"transCur": "USD", "baseCur": "CNY", "rateData": 7.1}
  ]
}
```

- `transCur`：交易币种；`baseCur`：换算后的币种。
- `rateData`：**1 单位 `transCur` 可兑换的 `baseCur` 数量**；换算金额为交易金额乘以该值。
- JCB JPY 的 `baseCur=JPY`：优先保留同日 JPY 页面显示值；页面日期不匹配时，用同日 USD 原表的 `JPY Sell / 交易币种 Buy` 重建 15 个币种，精确截断至小数点后 **3 位**。元数据 `derived` 区分直接采集与重建，`rate_formula`、`source_urls` 记录公式和来源。
- JCB USD 三份文件分别采用原表 Buy、Mid、Sell 列的倒数，`baseCur=USD`，由使用者选择参考口径。
- JCB USD 的 `rateData` 最多保留 **100 位有效数字，向零截断，不四舍五入**；循环小数仍是近似值。每行另有整数 `rateNumerator`、`rateDenominator`，两者之比为原始报价倒数的精确约分值。需要精确值时请使用分子／分母，并采用任意精度整数、十进制或分数运算，避免普通浮点解析丢失精度。

汇率日期及来源见对应元数据，不要把最新文件的下载日期当作汇率日期。文件不保证覆盖每个日历日，缺失不表示汇率为零，也不应自动沿用前一日数据。JCB 存在非发布日。数据用于参考，实际账单可能因处理日期、发卡行手续费等因素而不同。

## 读取与下载

最新文件可直接通过上表链接读取，例如：

```sh
curl -fL https://raw.githubusercontent.com/cary17/daily-exchange-rates/data/latest/visa.json -o visa.json
```

`data` 分支中的路径如下，`name` 为上表文件名去掉 `.json` 后的名称：

| 内容 | 路径 |
| --- | --- |
| 最新汇率 | `latest/name.json` |
| 每日汇率（未归档） | `history/YYYY/MM/name/YYYY-MM-DD.json` |
| 最新原始响应或分片索引 | `raw/latest/name.json` |
| 每日原始响应或分片索引 | `raw/history/YYYY/MM/name/YYYY-MM-DD.json` |
| 最新元数据 | `metadata/latest/name.json` |
| 每日元数据 | `metadata/history/YYYY/MM/name/YYYY-MM-DD.json` |
| 最近三个月的月包 | `history/YYYY/MM/YYYY-MM.tar.gz`，大包可能分卷 |

Visa、Mastercard 的原始响应按索引中的 `parts_directory` 和 `parts` 读取分片，索引包含大小和 SHA256。银联、JCB 的原始响应为单文件 JSON 封装；JCB USD 三份原始记录均保留完整 USD 页面及其三列数据；JPY 直接采集时保留 JPY 页面，重建时保留用于计算的 USD 页面。

单文件超过 100 MiB 时使用 Git LFS，原始链接可能返回指针而非文件内容。需要完整数据副本时，先安装 Git LFS，再克隆 `data` 分支：

```sh
git clone --single-branch --branch data https://github.com/cary17/daily-exchange-rates.git rates-data
```

## 历史归档

月包和年包均包含汇率、原始响应及元数据。已归档明细从 `data` 分支移除；该分支仅保留最近三个已归档日历月的月包，全部月包和年包从 [Releases](https://github.com/cary17/daily-exchange-rates/releases) 下载。

- 月包标签：`rates-YYYY-MM`；年包标签：`rates-YYYY`。
- Release 说明中的 JSON `files` 列表提供附件的 `asset_name`、原文件名 `filename`、大小和 SHA256。下载列出的附件，按 `filename` 恢复文件名后校验。
- 单包为 `YYYY-MM.tar.gz` 或 `YYYY.tar.gz`；分卷为相同名称加 `.part001`、`.part002` 等，需按编号合并后解压到新的空目录。

以已下载并恢复文件名的月包为例：

```sh
sha256sum -c SHA256SUMS
if [ -f 2026-09.tar.gz.part001 ]; then
  cat 2026-09.tar.gz.part* > 2026-09.tar.gz
fi
tar -xzf 2026-09.tar.gz
```

## 许可

代码采用 **AGPL-3.0-only**，全文见 [LICENSE](LICENSE)。卡组织原始响应及汇率数据不套用代码许可，使用、转载或再分发须遵守对应原始来源的使用条件。

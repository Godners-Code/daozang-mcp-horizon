# DaoZang Reasoner MCP — Prefect Horizon 在线版

《中华道藏》全文的**可溯源检索 + 原文取证** MCP 服务器，为 [Prefect Horizon](https://horizon.prefect.io)
的托管环境（FastMCP / HTTP MCP）而构建。

- 语料：**3,152 篇**经文、**285,117 个文本块**
- 引擎：关键词（IDF）检索 + Markdown 原文精确回定位
- 端点：`https://<server-name>.fastmcp.app/mcp`
- 许可证：**MIT**（见 [LICENSE](LICENSE)；语料数据的来源与条款见文末）

---

## 在线使用

已部署在 Prefect Horizon，**无需本地安装**：

| 项 | 值 |
|---|---|
| 端点 | `https://daozang-reasoner.fastmcp.app/mcp` |
| 传输 | Streamable HTTP |
| 认证 | `Authorization: Bearer <HORIZON_API_KEY>`（无认证返回 401） |

客户端配置（Claude Desktop / Claude Code / Cursor / Python / curl）、6 个工具的参数、
检索技巧、冷启动与限额、故障排查 → **见 [USAGE.md](USAGE.md)**。

> 首次调用含冷启动（加载索引约 40–60 s），之后单次 1.7–2.1 s。

---

## 为什么是「关键词引擎」而不是向量检索？

这不是偷懒，而是被 Horizon 的运行时限倒推出来的结论。

| Horizon 限制 | 值 | 对本服务的含义 |
|---|---|---|
| 内存配额 | **1024 MB**（所有档位统一，不可调） | bge-m3 ONNX 量化会话约 **570 MB**，加上向量矩阵必然 OOM |
| 请求超时 | **170 s** | 冷启动必须在此窗口内完成 |
| 本地文件系统 | **Ephemeral**（跨实例/重新部署不保留） | 无法依赖运行时下载语料 |
| 构建产物 | = 仓库内容 | 索引必须固化进仓库 |

进一步核实（2026-10-12）：

- 285,117 × 1024 维 **float32 向量矩阵本身 = 1113.7 MiB**，已超过 1024 MB 配额；
  实测（Python 3.13 + numpy）仅该矩阵就占 **+1113.8 MB** 常驻。
- 降精度到 int8 后向量为 278 MiB，但 ONNX 会话仍需约 570 MB，
  合计仍逼近/超过上限（详见 `docs/HORIZON.md`）。
- 远程嵌入 API 亦不可用：Hugging Face Inference Providers 返回
  **402（无可用额度）**，且账户非 PRO。
- **换向量数据库引擎解决不了这个问题**——瓶颈是「查询向量从哪来」（嵌入模型的内存），
  而不是「向量存哪里」。任何向量库都需要查询向量，而嵌入模型在 1024 MB 里放不下。

因此在线版只提供关键词检索 + 原文提取，即 `dao-zang` skill 的**默认 `text` 引擎**行为。
需要语义检索时，请在本地用完整的 MCPB / stdio 版（有充足内存）。

---

## 数据格式

数据来自 `Godners/DaoZang`，已重新拆分为 **13 个 parquet 分片**，每个 **≤ 92.9 MiB**，
全部在 GitHub 100 MiB 单文件硬上限内（**无需 Git LFS**）：

```
data/shards/
  chunks-01.parquet … chunks-13.parquet   每片 23,383 行（末片 4,521 行）
  manifest.json                            行数、字节数、SHA-256、行区间
```

列与原始数据集一致：`source` / `title` / `chunk_index` / `chars` / `text` / `embedding(1024×float32)`。

| 项 | 值 |
|---|---|
| 行数 | 285,117（= 285,117 行校验通过） |
| 分片数 | 13 |
| 合计 | 1,126.2 MiB（1.10 GiB），zstd 压缩 |
| 最大分片 | 92.82 MiB |
| 原始（未拆） | 6 片 × 约 217 MB，均超 100 MiB 无法入库 |

**⚠️ 运行时只读非向量列。** 向量列合计约 1.17 GB，而 Horizon 只有 1024 MB 内存，
因此 `daozang/store.py` 用 `pq.read_table(..., columns=[source,title,chunk_index,chars,text])`
做**列裁剪**，向量留在磁盘上不加载。

**运行时内存策略**：逐片流式读取 → 正文编码进一个 `bytearray`（222 MiB）→
建立偏移表；检索在拼接后的 UTF-8 字节流上做子串扫描（C 实现），
命中偏移经二分映射回行号，只对最终 top-k 解码文本。

实测（Python 3.13 + pyarrow）：

| 指标 | 值 |
|---|---|
| 加载耗时 | 2.2 – 2.8 s |
| 加载期峰值 RSS | **669 MB** |
| 稳态常驻 | **442 MB** |
| 单次检索 | 56 – 130 ms（常见词）；高频词「黄庭」130 ms |
| Horizon 上限 | 1024 MB |

重建分片：

```bash
python tools/split_parquet.py /path/to/parquet_dir
```

### 推送 `markdowns/` 原文（多线程）

`markdowns/`（3,152 篇）必须随仓库发布：`get_original` / `read_source` 要在运行时
回到 Markdown 原文里做定位，`daozang/store.py` 从 `<root>/markdowns/` 读取。

`git push` 无法并行（同分支并发 push 会 non-fast-forward），因此原文用
**GitHub Git Data API + 线程池**推送：

```bash
export GH_PAT=github_pat_xxx        # 或写入本地 .env（已 gitignore）
tools/push_markdowns.sh --workers 48 --chunk 500 --interval 60
```

实测（本机 → 代理 → api.github.com，真实 markdown 样本，各并发档用互不重叠文件
以排除 GitHub 按 blob SHA 去重带来的假加速）：

| 并发 | 吞吐 |
|---|---|
| 1 | 0.67 文件/s |
| 16 | 7.97 文件/s |
| 32 | 12.58 文件/s |
| **48** | **13.88 文件/s（饱和点）** |
| 64 | 13.98 文件/s |

脚本特性：每 60 秒输出一次瞬时/平均速度与 ETA、每 500 文件一个提交、
逐文件指数退避重试、checkpoint 落盘可断点续传、结束时自动
`git fetch` + `git reset --hard origin/main` 对齐本地分支。


---

## 本地运行

```bash
pip install -r requirements.txt

# HTTP（默认 127.0.0.1:7860，端点 /mcp）
python server.py

# stdio
MCP_TRANSPORT=stdio python server.py

# 检查入口能否被 Horizon 识别（注意：没有 --json，用 --format）
fastmcp inspect server.py:mcp --format fastmcp
```

### 环境变量

| 变量 | 默认 | 说明 |
|---|---|---|
| `DAOZANG_ROOT` | 本文件所在目录 | 索引与 `markdowns/` 的根目录 |
| `MCP_TRANSPORT` | `http` | `http` 或 `stdio` |
| `HOST` / `PORT` | `127.0.0.1` / `7860` | HTTP 监听地址 |

---

## 工具

| 工具 | 用途 |
|---|---|
| `daozang_protocol` | 道藏考据方法论（先读这个） |
| `daozang_status` | 馆藏状态：块数、篇数、正文规模 |
| `list_sources` | 按文件名关键词列经目，用于缩小范围 |
| `search_daozang` | 关键词（IDF）检索，返回相关度 / 源文件 / 块序号 |
| `get_original` | 把命中块还原成**带行号**的 Markdown 原文，`⟦…⟧` 标出命中范围 |
| `read_source` | 读取某篇经文全文 |

推荐工作流：`daozang_protocol` → `list_sources` → `search_daozang` → `get_original`。

> 检索用**专名或原句短语**（如「周天火候」「天地悉皆归」），不要用自然语言问句——
> 本引擎是关键词匹配，不是语义匹配。

---

## 部署到 Horizon

本仓库**已经部署上线**，端点 `https://daozang-reasoner.fastmcp.app/mcp`。
重新部署或首次部署：

```bash
export HORIZON_API_KEY=fmcp_你的密钥

# 创建版本 → 构建 → 部署（每 5 秒打印构建状态与日志最新一行）
python tools/deploy_horizon.py --repo Godners-Code/daozang-mcp-horizon \
  --branch main --entrypoint server.py:mcp --dependency requirements.txt \
  --interval 5 --poll 180

# 端到端验证：断言 285,117 块 / 3,152 篇，并调用全部 6 个工具
python tools/verify_horizon.py
```

要点：

- **构建 `ready` ≠ 已上线**。之后还有 `deploying` 阶段（实测约 1 分 47 秒），
  期间访问返回 `404`。以 `GET /deployments` 的 `succeeded` 为准。
- 入口点 `server.py:mcp`；依赖文件 `requirements.txt`。
- 服务器名（slug）决定 URL，如 `daozang-reasoner` → `https://daozang-reasoner.fastmcp.app/mcp`。
- 客户端连接需带 Horizon 凭据（Horizon 认证默认开启，**免费层无法关闭**）。

更完整的客户端配置与排错见 [USAGE.md](USAGE.md)。

---

## 许可证与数据来源

**本仓库的代码以 MIT 协议开源**，全文见 [LICENSE](LICENSE)：

> MIT License — Copyright (c) 2026 Godners-Code
> 允许任意使用、复制、修改、合并、发布、分发、再授权与销售，
> 唯一要求是保留版权声明与许可声明；软件按「原样」提供，不含任何担保。

**语料数据的来源与条款另计。** 经文原文与向量数据来自 Hugging Face 数据集
[`Godners/DaoZang`](https://huggingface.co/datasets/Godners/DaoZang)，
`markdowns/` 与 `data/shards/` 均由该数据集派生，**不适用上述 MIT 条款**，
请遵循数据集自身的许可说明，仅供研究与文献检索使用。

即：**代码 MIT，语料从其原许可**。若需将语料用于商业用途，请先确认原数据集授权。

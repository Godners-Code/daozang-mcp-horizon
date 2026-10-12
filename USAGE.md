# 使用与配置说明 — DaoZang Reasoner MCP（Prefect Horizon 在线版）

> 线上端点：`https://daozang-reasoner.fastmcp.app/mcp`
> 传输：**Streamable HTTP**（MCP） · 服务器 `daozang-reasoner` v1.1.0
> 语料：3,152 篇经文 / 285,117 个文本块 · 全部工具**只读**

---

## 1. 端点与认证

| 项 | 值 |
|---|---|
| URL | `https://daozang-reasoner.fastmcp.app/mcp` |
| 传输 | Streamable HTTP |
| 认证 | **必需**：`Authorization: Bearer <HORIZON_API_KEY>` |
| 无认证时 | `HTTP 401` |

Horizon 托管服务器默认开启认证（`authMode=fastmcp-cloud`），**免费层无法关闭**。
这里的 `HORIZON_API_KEY` 就是你注册 Horizon 时拿到的 `fmcp_...` 开头的密钥
（Horizon 控制台 → API Keys）。

> 该密钥等同于访问凭据，请勿提交到仓库或粘贴到公开场合。

---

## 2. 客户端配置

### 2.1 Claude Desktop

编辑 `claude_desktop_config.json`（macOS：`~/Library/Application Support/Claude/`；
Windows：`%APPDATA%\Claude\`）：

```json
{
  "mcpServers": {
    "daozang-reasoner": {
      "url": "https://daozang-reasoner.fastmcp.app/mcp",
      "headers": {
        "Authorization": "Bearer fmcp_你的密钥"
      }
    }
  }
}
```

### 2.2 Claude Code（CLI）

```bash
claude mcp add --transport http daozang-reasoner \
  https://daozang-reasoner.fastmcp.app/mcp \
  --header "Authorization: Bearer fmcp_你的密钥"
```

### 2.3 Cursor / VS Code / 通用 `mcp.json`

```json
{
  "mcpServers": {
    "daozang-reasoner": {
      "url": "https://daozang-reasoner.fastmcp.app/mcp",
      "headers": {
        "Authorization": "Bearer fmcp_你的密钥"
      }
    }
  }
}
```

### 2.4 Python（FastMCP 客户端）

```python
import asyncio
from fastmcp import Client

KEY = "fmcp_你的密钥"
URL = "https://daozang-reasoner.fastmcp.app/mcp"


async def main():
    async with Client(URL, auth=KEY) as c:
        for t in await c.list_tools():
            print(t.name, "—", t.title)

        r = await c.call_tool("search_daozang",
                              {"query": "天地悉皆归", "top_k": 3,
                               "include_original": True})
        for h in r.structured_content["hits"]:
            o = h["original"]
            print(f'{h["score"]:.4f}  {h["source"]} #{h["chunk_index"]}  '
                  f'行 {o["line_start"]}~{o["line_end"]}  located={o["located"]}')


asyncio.run(main())
```

### 2.5 裸 HTTP（curl，用于连通性排查）

```bash
curl -sS https://daozang-reasoner.fastmcp.app/mcp \
  -H 'Content-Type: application/json' \
  -H 'Accept: application/json, text/event-stream' \
  -H 'Authorization: Bearer fmcp_你的密钥' \
  -d '{"jsonrpc":"2.0","id":1,"method":"initialize",
       "params":{"protocolVersion":"2025-06-18","capabilities":{},
                 "clientInfo":{"name":"probe","version":"1"}}}'
```

返回 `200` + `event: message` 且 `serverInfo.name = daozang-reasoner` 即正常；
返回 `401` 是密钥缺失/错误；返回 `404 {"error":"Not Found"}` 说明该 target
还没有生效的部署（见第 6 节）。

---

## 3. 工具清单

| 工具 | 用途 | 关键参数 |
|---|---|---|
| `daozang_protocol` | 道藏考据方法论（**建议第一步**） | — |
| `daozang_status` | 馆藏状态：块数、篇数、正文规模、引擎 | — |
| `list_sources` | 按**文件名关键词**列经目，用于缩小范围 | `keyword`、`limit`(1–500) |
| `search_daozang` | 关键词（IDF）检索 285,117 块 | `query`、`top_k`(1–50)、`source_filter`、`include_original`、`context`(0–2000) |
| `get_original` | 把命中块还原成**带行号**原文，`⟦…⟧` 标出命中范围 | `source`、`chunk_index`、`context` |
| `read_source` | 读取某篇经文全文（超长截断） | `source`、`max_chars`(100–200000) |

### 推荐工作流

```
daozang_protocol → list_sources → search_daozang → get_original →（需要通读时）read_source
```

1. **`list_sources`**：先用关键词把范围缩到某几部经（如 `keyword="清静"` → 24 篇）。
2. **`search_daozang`**：拿到 `source` + `chunk_index` + `score`。
   可传 `source_filter` 直接限定经名，省掉第 1 步。
3. **`get_original`**：**引用经文前的必要一步**。检索返回的是清洗后正文，
   本工具返回原始 Markdown 且带行号，便于标注出处。
4. **`read_source`**：需要通读全文或核对上下文时用。

---

## 4. 使用技巧（很重要）

- **本服务是关键词匹配，不是语义检索。** 不要问「道家怎么看待天人关系」这类自然语言问句；
  应先抽出其中的**专名或原句短语**，例如：
  - ✅ `周天火候`、`天地悉皆归`、`三官大帝`、`黄庭`
  - ❌ `道教如何理解宇宙生成`
- **词越具体越准。** 生僻专名的 IDF 权重高，排序质量最好。
- **先缩小范围再检索。** 命中过散时，用 `source_filter`（如 `"清静经"`）或先 `list_sources`。
- **`include_original=true` 更慢但可直接引用**；批量探索时先用默认值，确定目标后再取原文。
- **不返回的内容**：本在线版**没有语义/向量检索**（原因见 README），也没有联网检索；
  只覆盖这 3,152 篇语料。

---

## 5. 运行特性与限额

| 项 | 值 | 说明 |
|---|---|---|
| 冷启动 | **约 40–60 s** | 首次工具调用要加载索引（实测 39.6 s）；之后单次调用 **1.7–2.1 s** |
| 请求超时 | 170 s | 冷启动 + 检索仍在限额内 |
| 内存上限 | 1024 MB | 稳态实测 442 MB，加载峰值 669 MB |
| 单次载荷 | 6 MB（每向） | `read_source` 的 `max_chars` 过大可能触顶 |
| 会话路由 TTL | 24 h | 超时需重新初始化 |
| 文件系统 | **临时（ephemeral）** | 运行时不能写入持久数据 |
| 工具权限 | 全部只读 | 无破坏性操作 |

> 冷启动只发生在实例被回收后的第一次调用。若客户端报超时，**重试一次**通常即可
> （第一次的请求已经把实例唤醒）。

---

## 6. 部署与验证

### 部署（更新线上版本）

```bash
export HORIZON_API_KEY=fmcp_你的密钥
export NET_PROXY=http://192.168.109.5:8080      # 本机出网代理，按需

python tools/deploy_horizon.py \
  --repo Godners-Code/daozang-mcp-horizon \
  --branch main --entrypoint server.py:mcp --dependency requirements.txt \
  --interval 5 --poll 180
```

脚本会：复用/创建项目 → 创建版本（从 GitHub 构建）→ **每 5 秒打印构建状态与构建日志最新一行**
→ 等待部署 `deploying → succeeded` → 输出 serving URL。

> 注意：**构建 `ready` ≠ 已上线**。构建完成后还有一个 `deploying` 阶段
> （实测约 1 分 47 秒），期间访问会返回 `404`。

### 验证

```bash
python tools/verify_horizon.py --url https://daozang-reasoner.fastmcp.app/mcp
```

逐项断言 `n_chunks == 285,117`、`n_files == 3,152`，并依次调用全部 6 个工具。

---

## 7. 故障排查

| 现象 | 原因 | 处理 |
|---|---|---|
| `404 {"error":"Not Found"}` | 部署未完成 / target 未切换 | 查 `GET /deployments` 直到 `succeeded`；再等 1–2 分钟 |
| `401` | 缺 `Authorization: Bearer` 或密钥错误 | 检查 header 与密钥 |
| `Session terminated`（initialize 阶段） | 通常是部署尚未生效或冷启动被中断 | 确认部署 `succeeded` 后重试 |
| 首次调用很慢（40–60 s） | 冷启动加载索引 | 正常现象，重试即快 |
| 检索结果不相关 | 用了自然语言问句 | 改用具名/原句短语，或加 `source_filter` |
| `source_filter` 无结果 | 关键词与文件名不匹配 | 先用 `list_sources` 确认经名写法 |

---

## 8. 相关文档

- [`README.md`](README.md) — 项目说明、数据格式、为何用关键词引擎
- [`LICENSE`](LICENSE) — MIT（**代码 MIT，语料从其原许可**）

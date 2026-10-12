# -*- coding: utf-8 -*-
"""道藏考据 MCP 服务器（Prefect Horizon / Streamable HTTP 在线版）。

入口对象：`mcp`（Horizon 入口填 `server.py:mcp`）
"""
from __future__ import annotations

import logging
import os
from typing import Annotated, Optional

from mcp.types import ToolAnnotations
from pydantic import BaseModel, ConfigDict, Field

from fastmcp import FastMCP

from daozang.store import DaoZangStore
from daozang import textloc

logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO").upper(),
                    format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger("daozang.mcp")

SERVER_NAME = "daozang-reasoner"
SERVER_VERSION = "1.1.0"

INSTRUCTIONS = """\
道藏考据（DaoZang Reasoner）—— 在《中华道藏》全文中做可溯源的检索与原文取证。

语料：3,152 篇经文、285,117 个文本块（由 Markdown 原文切分并清洗而来）。

推荐工作流：
1. `daozang_protocol`  先读方法论（若不确定怎么问）；
2. `list_sources`      用关键词把范围缩到某几部经；
3. `search_daozang`    检索命中块（关键词/IDF 排序，返回相关度、源文件、块序号）；
4. `get_original`      把命中块还原成**带行号的 Markdown 原文**，用 ⟦…⟧ 标出精确命中范围；
5. `read_source`       需要通读时取全文。

检索技巧：本服务使用关键词匹配（非语义）。自然语言问句请先抽出其中的**专名或原句短语**
（如「周天火候」「天地悉皆归」），或配合 `source_filter` 限定经名。词越具体越准。
"""

READ_ONLY = ToolAnnotations(readOnlyHint=True, destructiveHint=False,
                            idempotentHint=True, openWorldHint=False)

_store: Optional[DaoZangStore] = None


def store() -> DaoZangStore:
    global _store
    if _store is None:
        _store = DaoZangStore()
        _store.load()
    return _store


# ====================================================================== 输出模型
class _Base(BaseModel):
    model_config = ConfigDict(extra="allow")


class ProtocolOut(_Base):
    methodology: str = Field(description="道藏考据方法论全文（Markdown）")


class CorpusStatusOut(_Base):
    ready: bool = Field(description="索引是否已就绪")
    n_chunks: int = Field(description="文本块总数")
    n_files: int = Field(description="经文篇数")
    text_bytes: int = Field(description="索引中正文的 UTF-8 字节数")
    parts: int = Field(description="正文分片数")
    engine: str = Field(description="检索引擎标识，本在线版为 text")
    semantic_available: bool = Field(description="语义检索是否可用；在线版恒为 false")


class SourceItem(_Base):
    source: str = Field(description="Markdown 源文件名（可作为 source_filter 使用）")
    chunks: int = Field(description="该篇的文本块数")


class ListSourcesOut(_Base):
    keyword: str = Field(description="实际使用的文件名关键词")
    count: int = Field(description="返回条数")
    total_matched: int = Field(description="匹配到的总篇数（可能多于返回条数）")
    items: list[SourceItem] = Field(description="匹配到的经文清单")


class OriginalWindow(_Base):
    source: str = Field(description="Markdown 源文件名")
    chunk_index: int = Field(description="块序号")
    line_start: int = Field(description="命中范围起始行号（1 起）")
    line_end: int = Field(description="命中范围结束行号（1 起）")
    window_line_start: int = Field(description="展示窗口起始行号")
    window_line_end: int = Field(description="展示窗口结束行号")
    text: str = Field(description="保留原文格式的窗口文本，⟦…⟧ 标出精确命中范围")
    located: bool = Field(description="是否成功在原文中定位该块")


class Hit(_Base):
    score: float = Field(description="相关度分数（词频 × IDF ÷ 长度归一）")
    source: str = Field(description="Markdown 源文件名")
    title: str = Field(description="经名")
    chunk_index: int = Field(description="块序号，可传给 get_original")
    text: str = Field(description="命中块正文（已清洗）")
    original: Optional[OriginalWindow] = Field(default=None,
                                              description="命中块的原文窗口（include_original=true 时返回）")


class SearchOut(_Base):
    query: str = Field(description="原始查询串")
    terms: list[str] = Field(description="实际参与检索的关键词")
    engine: str = Field(description="实际使用的引擎，本在线版为 text")
    count: int = Field(description="返回命中数")
    hits: list[Hit] = Field(description="命中列表，按相关度降序")


class SourceTextOut(_Base):
    source: str = Field(description="Markdown 源文件名")
    total_chars: int = Field(description="全文总字符数")
    truncated: bool = Field(description="是否因 max_chars 被截断")
    text: str = Field(description="经文全文（Markdown）")


# ====================================================================== 工具
mcp = FastMCP(SERVER_NAME, instructions=INSTRUCTIONS, version=SERVER_VERSION)

METHODOLOGY = """\
# 道藏考据方法论

## 一、先定范围，再问问题
《中华道藏》收经 3,152 篇、28.5 万段。直接抛自然语言长问句，关键词匹配容易失焦。
先用 `list_sources` 以经名关键词（如「清静」「度人」「悟真」）缩小到若干部，
再把范围交给 `search_daozang` 的 `source_filter`。

## 二、检索用「原句短语」，不要用「问句」
本服务是关键词（IDF）引擎，不是语义引擎：
* ✅ 好：「周天火候」「天地悉皆归」「三官大帝」「黄庭」
* ❌ 差：「什么是内丹修炼中的周天火候？它和筑基有什么关系？」
问句请自行切出其中的专名、术语或原句片段，可分多次检索。

## 三、必须回到原文取证
检索返回的是**建库时清洗过的正文**（已去 Markdown 记号、压缩空白），
可用于判断相关性，但**不宜直接引用**。引用前一律用 `get_original`：
它会重放同一套清洗规则，把块精确映射回原文，给出**行号**并用 ⟦…⟧ 标出命中范围。

## 四、结论要能溯源
每条论断都应附 `源文件名 + 块序号（或行号）`，便于他人复核。

## 五、注意语料的边界
* 本库为《中华道藏》文本，不含敦煌本、辑佚本等库外材料；
* 同一经文可能有异名、异体字（如「无／無」「炁／气」），检索时两种写法都试；
* 若某词零命中，先换更短的核心词，或去掉 `source_filter` 再试。
"""


@mcp.tool(title="道藏考据方法论", annotations=READ_ONLY)
def daozang_protocol() -> ProtocolOut:
    """返回道藏考据的推荐工作流与检索技巧（先读这个再动手）。"""
    return ProtocolOut(methodology=METHODOLOGY)


@mcp.tool(title="馆藏状态", annotations=READ_ONLY)
def daozang_status() -> CorpusStatusOut:
    """查看索引状态：块数、篇数、正文规模、可用引擎。"""
    return CorpusStatusOut(**store().status())


@mcp.tool(title="按关键词列经目", annotations=READ_ONLY)
def list_sources(
    keyword: Annotated[str, Field(description="经名/文件名关键词，如「清静」「悟真」；留空则从首篇开始列出",
                                  examples=["清静", "度人", "悟真"])] = "",
    limit: Annotated[int, Field(description="最多返回篇数", ge=1, le=500)] = 50,
) -> ListSourcesOut:
    """按文件名关键词列出匹配的经文及各自块数，用于缩小检索范围。"""
    st = store()
    items = st.sources_matching(keyword, limit=limit)
    total = sum(1 for s in st.sources if not keyword or keyword in s)
    return ListSourcesOut(
        keyword=keyword,
        count=len(items),
        total_matched=total,
        items=[SourceItem(source=s, chunks=c) for s, c in items],
    )


@mcp.tool(title="检索道藏", annotations=READ_ONLY)
def search_daozang(
    query: Annotated[str, Field(description="检索词，建议用专名或原句短语，如「周天火候」「天地悉皆归」",
                                min_length=1, examples=["周天火候", "天地悉皆归", "三官大帝"])],
    top_k: Annotated[int, Field(description="返回条数", ge=1, le=50)] = 5,
    source_filter: Annotated[Optional[str], Field(
        description="按源文件名关键词限定范围（如「清静经」），等价于先 list_sources 再筛",
        default=None)] = None,
    include_original: Annotated[bool, Field(
        description="是否同时返回原文窗口（更慢，但便于直接引用）", default=False)] = False,
    context: Annotated[int, Field(description="原文窗口每侧扩展字符数（include_original=true 时生效）",
                                  ge=0, le=2000)] = 300,
) -> SearchOut:
    """在 285,117 个文本块中做关键词（IDF）检索，返回相关度、源文件名与块序号。

    检索到结果后，引用原文请改用 `get_original`（带行号、保留原文格式）。
    """
    st = store()
    source_files = None
    if source_filter:
        source_files = textloc.match_source_files(source_filter, st.md_dir)
        if not source_files:
            return SearchOut(query=query, terms=st.split_terms(query), engine="text",
                             count=0, hits=[])
    raw = st.search(query, top_k=top_k, source_files=source_files)

    hits: list[Hit] = []
    for score, text, src, ci, title in raw:
        orig = None
        if include_original:
            orig = _original_for(src, ci, text, context)
        hits.append(Hit(score=round(score, 4), source=src, title=title,
                        chunk_index=ci, text=text, original=orig))
    return SearchOut(query=query, terms=st.split_terms(query), engine="text",
                     count=len(hits), hits=hits)


def _original_for(src: str, chunk_index: int, chunk_text: str, context: int):
    st = store()
    path = os.path.join(st.md_dir, src)
    if not os.path.exists(path):
        return None
    with open(path, encoding="utf-8", errors="replace") as f:
        raw = f.read()
    cleaned, origin = textloc.replay_clean(raw)
    loc = textloc.locate_chunk(cleaned, origin, chunk_text)
    if loc is None:
        return OriginalWindow(source=src, chunk_index=chunk_index, line_start=0, line_end=0,
                              window_line_start=0, window_line_end=0,
                              text="（清洗重放与建库不一致，未能定位）", located=False)
    rs, re_ = loc
    ws, we = textloc.extract_window(raw, rs, re_, context)
    return OriginalWindow(
        source=src, chunk_index=chunk_index,
        line_start=textloc.line_no(raw, rs), line_end=textloc.line_no(raw, re_),
        window_line_start=textloc.line_no(raw, ws), window_line_end=textloc.line_no(raw, we),
        text=textloc.render_window(raw, ws, we, rs, re_), located=True)


@mcp.tool(title="取带行号原文", annotations=READ_ONLY)
def get_original(
    source: Annotated[str, Field(description="Markdown 源文件名（取自检索结果的 source 字段，须完整）",
                                 min_length=1,
                                 examples=["中华道藏-三洞真经-三洞经教-一切道经音义妙门由起.md"])],
    chunk_index: Annotated[int, Field(description="块序号（取自检索结果的 chunk_index）", ge=0)],
    context: Annotated[int, Field(description="窗口每侧扩展字符数", ge=0, le=2000)] = 300,
) -> OriginalWindow:
    """把指定块还原成**保留原文格式、带行号**的窗口，并用 ⟦…⟧ 标出精确命中范围。

    这是引用经文前的必要一步：检索返回的是清洗后正文，本工具返回的是原始 Markdown。
    """
    st = store()
    path = os.path.join(st.md_dir, source)
    if not os.path.exists(path):
        raise ValueError(f"源文件不存在：{source}")

    ids = st.find_row(source, chunk_index)
    if ids is None:
        raise ValueError(f"{source} 中不存在块序号 {chunk_index}")
    chunk_text = st.text_at(ids)
    out = _original_for(source, chunk_index, chunk_text, context)
    if out is None:
        raise ValueError(f"源文件不存在：{source}")
    return out


@mcp.tool(title="读取经文全文", annotations=READ_ONLY)
def read_source(
    source: Annotated[str, Field(description="Markdown 源文件名（须完整，可用 list_sources 获取）",
                                 min_length=1)],
    max_chars: Annotated[int, Field(description="最多返回字符数，超出则截断",
                                    ge=100, le=200000)] = 20000,
) -> SourceTextOut:
    """读取某篇经文的 Markdown 全文（超长时截断），用于通读或核对上下文。"""
    st = store()
    path = os.path.join(st.md_dir, source)
    if not os.path.exists(path):
        raise ValueError(f"源文件不存在：{source}")
    with open(path, encoding="utf-8", errors="replace") as f:
        raw = f.read()
    trunc = len(raw) > max_chars
    return SourceTextOut(source=source, total_chars=len(raw), truncated=trunc,
                         text=raw[:max_chars])


# ====================================================================== 本地运行
app = mcp.http_app(path="/mcp")


def main():
    transport = os.environ.get("MCP_TRANSPORT", "http").strip().lower()
    if transport == "stdio":
        mcp.run(transport="stdio")
    else:
        import uvicorn
        uvicorn.run(app, host=os.environ.get("HOST", "127.0.0.1"),
                    port=int(os.environ.get("PORT", "7860")), log_level="info")


if __name__ == "__main__":
    main()

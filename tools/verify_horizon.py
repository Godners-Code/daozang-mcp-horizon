# -*- coding: utf-8 -*-
"""验证 Horizon 线上端点：连接 → 列出工具 → 逐个调用 → 校验结果。

Horizon 认证默认开启（authMode=fastmcp-cloud），因此必须带 API Key。
用法：
    python tools/verify_horizon.py [--url https://daozang-reasoner.fastmcp.app/mcp]
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time

DEFAULT_URL = "https://daozang-reasoner.fastmcp.app/mcp"
# 密钥一律从环境变量读取，仓库是公开的，绝不能硬编码。
KEY = os.environ.get("HORIZON_API_KEY", "")


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default=DEFAULT_URL)
    ap.add_argument("--key", default=KEY)
    args = ap.parse_args()
    if not args.key:
        sys.exit("缺少 HORIZON_API_KEY（或用 --key 指定）")

    from fastmcp import Client

    t0 = time.perf_counter()
    async with Client(args.url, auth=args.key) as c:
        init_s = time.perf_counter() - t0
        tools = await c.list_tools()
        print(f"✅ 连接成功（含冷启动 {init_s:.1f}s）")
        print(f"   工具 {len(tools)} 个：")
        for t in tools:
            print(f"     - {t.name}  「{t.title}」")

        # ---- 1) 状态 ----
        t0 = time.perf_counter()
        r = await c.call_tool("daozang_status", {})
        st = r.structured_content
        print(f"\n[daozang_status] {(time.perf_counter() - t0) * 1000:.0f} ms")
        print(f"   块数 {st['n_chunks']:,} | 篇数 {st['n_files']:,} | "
              f"正文 {st['text_bytes'] / 1048576:.1f} MiB | 分片 {st['parts']} | "
              f"引擎 {st['engine']} | 加载 {st.get('load_seconds')}s")
        assert st["n_chunks"] == 285_117, st
        assert st["n_files"] == 3_152, st

        # ---- 2) 检索 ----
        for q in ("天地悉皆归", "周天火候", "三官大帝"):
            t0 = time.perf_counter()
            r = await c.call_tool("search_daozang", {"query": q, "top_k": 3})
            sc = r.structured_content
            ms = (time.perf_counter() - t0) * 1000
            print(f"\n[search_daozang] 「{q}」 {sc['count']} 条  {ms:.0f} ms")
            for h in sc["hits"]:
                print(f"   [{h['score']:.4f}] {h['source']} #{h['chunk_index']}")

        # ---- 3) 原文回定位 ----
        r = await c.call_tool("search_daozang", {"query": "天地悉皆归", "top_k": 1,
                                                 "include_original": True})
        h = r.structured_content["hits"][0]
        o = h["original"]
        print(f"\n[include_original] located={o['located']} 原文行 {o['line_start']}~{o['line_end']}")
        print("   " + o["text"][:150].replace("\n", " "))

        r = await c.call_tool("get_original", {"source": h["source"],
                                               "chunk_index": h["chunk_index"], "context": 100})
        g = r.structured_content
        print(f"[get_original] located={g['located']} 行 {g['line_start']}~{g['line_end']}")
        assert g["located"] is True

        # ---- 4) 列经目 + 全文 ----
        r = await c.call_tool("list_sources", {"keyword": "清静", "limit": 3})
        ls = r.structured_content
        print(f"\n[list_sources] 「清静」匹配 {ls['total_matched']} 篇，返回 {ls['count']} 条")
        for it in ls["items"]:
            print(f"   {it['chunks']:>5} 块  {it['source']}")

        r = await c.call_tool("read_source", {"source": ls["items"][0]["source"],
                                              "max_chars": 300})
        rs = r.structured_content
        print(f"[read_source] 全文 {rs['total_chars']:,} 字，截断={rs['truncated']}")

        r = await c.call_tool("daozang_protocol", {})
        print(f"[daozang_protocol] 方法论 {len(r.structured_content['methodology'])} 字")

    print("\n" + "=" * 60)
    print("✅ 全部通过")


if __name__ == "__main__":
    _px = os.environ.get("NET_PROXY") or "http://192.168.109.5:8080"
    for k in ("HTTPS_PROXY", "HTTP_PROXY", "https_proxy", "http_proxy"):
        os.environ[k] = _px
    asyncio.run(main())

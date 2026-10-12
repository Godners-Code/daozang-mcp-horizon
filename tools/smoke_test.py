# -*- coding: utf-8 -*-
"""Horizon 版服务器本地冒烟测试：功能正确性 + 常驻内存。"""
import gc
import os
import sys
import time

import psutil

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def rss():
    gc.collect()
    return psutil.Process(os.getpid()).memory_info().rss / 1024 / 1024


print(f"[基线] {rss():.1f} MB")
t0 = time.perf_counter()
from daozang.store import DaoZangStore                      # noqa: E402

# 采样加载期间的峰值 RSS（Horizon 的 1024 MB 是硬上限，峰值触发即 OOM）
import threading                                             # noqa: E402
_peak = [rss()]
_stop = threading.Event()


def _sample():
    while not _stop.is_set():
        _peak[0] = max(_peak[0], rss())
        time.sleep(0.02)


_th = threading.Thread(target=_sample, daemon=True)
_th.start()

st = DaoZangStore()
st.load()
_stop.set()
_th.join(timeout=1)

print(f"[加载索引] {rss():.1f} MB   耗时 {st.load_seconds}s   加载峰值 {_peak[0]:.1f} MB")

s = st.status()
print(f"  块数 {s['n_chunks']:,} | 篇数 {s['n_files']:,} | "
      f"正文 {s['text_bytes'] / 1048576:,.1f} MiB | 分片 {s['parts']}")
print(f"[仅加载索引后常驻] {rss():.1f} MB")

# ---- 检索 ----
for q in ("周天火候", "天地悉皆归", "三官大帝", "黄庭"):
    t0 = time.perf_counter()
    r = st.search(q, top_k=3)
    dt = time.perf_counter() - t0
    print(f"\n查询「{q}」 {len(r)} 条  用时 {dt * 1000:.0f} ms   常驻 {rss():.1f} MB")
    for score, text, src, ci, title in r[:3]:
        print(f"  [{score:.4f}] {src} (第{ci}块)  {text[:60].replace(chr(10), ' ')}…")

# ---- 原文回定位 ----
from daozang import textloc                                 # noqa: E402
r = st.search("天地悉皆归", top_k=1)
if r:
    _, text, src, ci, _ = r[0]
    raw = open(os.path.join(st.md_dir, src), encoding="utf-8", errors="replace").read()
    cleaned, origin = textloc.replay_clean(raw)
    loc = textloc.locate_chunk(cleaned, origin, text)
    print(f"\n原文回定位: {src} 第{ci}块 -> {loc}")
    if loc:
        rs, re_ = loc
        ws, we = textloc.extract_window(raw, rs, re_, 60)
        print(f"  行号 {textloc.line_no(raw, rs)}~{textloc.line_no(raw, re_)}")
        print("  " + textloc.render_window(raw, ws, we, rs, re_).replace("\n", "\n  ")[:400])

# ---- 列经目 ----
print("\n列经目「清静」:")
for src, n in st.sources_matching("清静", limit=5):
    print(f"  {n:>5} 块  {src}")

# ---- source_filter ----
r = st.search("天地", top_k=2, source_files=textloc.match_source_files("清静", st.md_dir))
print(f"\n限定「清静」+ 查「天地」: {len(r)} 条")
for score, text, src, ci, _ in r:
    print(f"  [{score:.4f}] {src} (第{ci}块)")

print(f"\n[峰值常驻] {rss():.1f} MB   （Horizon 上限 1024 MB）")

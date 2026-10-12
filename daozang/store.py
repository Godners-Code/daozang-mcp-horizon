# -*- coding: utf-8 -*-
"""道藏索引仓库（parquet 版）：只读、低常驻内存的块索引与关键词（IDF）检索。

数据源：`data/shards/chunks-NN.parquet`（每片 ≤95 MiB，含 source/title/chunk_index/
chars/text/embedding 六列，与原始 Godners/DaoZang 一致）。

**关键点：只读非向量列。** 向量列（1024×float32）合计约 1.17 GB，本服务的运行环境
（Prefect Horizon）只有 1024 MB 内存，因此运行时用 `columns=[...]` 做列裁剪，
只把正文与元数据读进内存，向量留在磁盘上不加载。

内存构成（实测目标）：
  pyarrow/numpy 运行时     约  60 MiB
  正文拼接缓冲（222 MiB）  约 222 MiB
  元数据数组              约  10 MiB
  ────────────────────────────────
  合计                    约 300 MiB   （上限 1024 MiB）

检索直接在拼接后的 UTF-8 字节流上做子串扫描（C 实现），命中偏移经二分映射回行号，
只对最终 top-k 解码文本。
"""
from __future__ import annotations

import json
import logging
import math
import os
import re
import threading
import time

import numpy as np

log = logging.getLogger("daozang.store")

TEXT_COLUMNS = ["source", "title", "chunk_index", "chars", "text"]


def _default_root():
    env = os.environ.get("DAOZANG_ROOT")
    if env:
        return os.path.abspath(env)
    # <root>/daozang/store.py -> <root>
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class DaoZangStore:
    def __init__(self, root=None):
        self.root = _default_root() if root is None else os.path.abspath(root)
        self.shard_dir = os.path.join(self.root, "data", "shards")
        self.md_dir = os.path.join(self.root, "markdowns")

        self._lock = threading.RLock()
        self._loaded = False
        self.error = None
        self.load_seconds = None

        self.manifest = None
        self.blob = b""             # 全部正文的 UTF-8 拼接
        self.offsets = None         # np.int64 [N]，每行正文在 blob 中的起始偏移
        self.lengths = None         # np.int32 [N]
        self.chars = None           # np.int32 [N]
        self.chunk_idx = None       # np.int32 [N]
        self.source_ids = None      # np.int32 [N]
        self.title_ids = None       # np.int32 [N]
        self.sources = []           # list[str]
        self.titles = []            # list[str]
        self._per_source = None

    # ------------------------------------------------------------------ 加载
    def load(self):
        with self._lock:
            if self._loaded:
                return
            import pyarrow.parquet as pq

            t0 = time.time()
            try:
                with open(os.path.join(self.shard_dir, "manifest.json"), encoding="utf-8") as f:
                    self.manifest = json.load(f)
                shards = [s["name"] for s in self.manifest["shards"]]

                # 逐片流式处理：每片读完立刻编码进 bytearray 并释放，
                # 避免同时持有 28.5 万个 Python 字符串 + 一份拼接副本（会把峰值推高到 560 MB+）。
                blob = bytearray()
                offs, lens, chs, cis = [], [], [], []
                src_list, s_map = [], {}
                tit_list, t_map = [], {}
                sid, tid = [], []

                for name in shards:
                    t = pq.read_table(os.path.join(self.shard_dir, name), columns=TEXT_COLUMNS)
                    for s in t.column("source").to_pylist():
                        j = s_map.get(s)
                        if j is None:
                            j = s_map[s] = len(src_list)
                            src_list.append(s)
                        sid.append(j)
                    for s in t.column("title").to_pylist():
                        j = t_map.get(s)
                        if j is None:
                            j = t_map[s] = len(tit_list)
                            tit_list.append(s)
                        tid.append(j)
                    for x in t.column("text").to_pylist():
                        b = (x or "").encode("utf-8")
                        offs.append(len(blob))
                        lens.append(len(b))
                        blob += b
                    chs.append(t.column("chars").to_numpy(zero_copy_only=False))
                    cis.append(t.column("chunk_index").to_numpy(zero_copy_only=False))
                    del t

                self.sources, self.titles = src_list, tit_list
                self.blob = bytes(blob)
                del blob
                self.offsets = np.array(offs, dtype=np.int64)
                self.lengths = np.array(lens, dtype=np.int32)
                self.chars = np.concatenate(chs).astype(np.int32)
                self.chunk_idx = np.concatenate(cis).astype(np.int32)
                self.source_ids = np.array(sid, dtype=np.int32)
                self.title_ids = np.array(tid, dtype=np.int32)
                del offs, lens, chs, cis, sid, tid

                self._per_source = np.bincount(self.source_ids.astype(np.int64),
                                               minlength=len(self.sources))
                n = self.manifest["nRows"]
                if self.offsets.shape[0] != n:
                    raise RuntimeError(f"读取行数 {self.offsets.shape[0]:,} ≠ manifest {n:,}")
                self._loaded = True
                self.load_seconds = round(time.time() - t0, 3)
                log.info("索引就绪：%d 行 / %d 篇 / 正文 %.1f MiB，%.3fs",
                         self.offsets.shape[0], len(self.sources),
                         len(self.blob) / 1048576, self.load_seconds)
            except Exception as e:                       # noqa: BLE001
                self.error = f"{type(e).__name__}: {e}"
                raise

    def _ensure(self):
        if not self._loaded:
            self.load()

    # ------------------------------------------------------------------ 读取
    def text_at(self, i):
        off = int(self.offsets[i])
        return self.blob[off:off + int(self.lengths[i])].decode("utf-8", "replace")

    def source_of(self, i):
        return self.sources[int(self.source_ids[i])]

    def title_of(self, i):
        return self.titles[int(self.title_ids[i])]

    def chunk_of(self, i):
        return int(self.chunk_idx[i])

    def count(self):
        self._ensure()
        return int(self.offsets.shape[0])

    def n_files(self):
        self._ensure()
        return len(self.sources)

    def status(self):
        self._ensure()
        return {
            "ready": self.error is None,
            "error": self.error,
            "n_chunks": self.count(),
            "n_files": self.n_files(),
            "text_bytes": len(self.blob),
            "parts": self.manifest["nShards"],
            "load_seconds": self.load_seconds,
            "engine": "text",
            "semantic_available": False,
        }

    # ------------------------------------------------------------------ 检索
    @staticmethod
    def split_terms(question):
        return [t for t in re.split(r"[\s,，。;；、:：!！?？\"'“”‘’()（）\[\]【】]+", question) if t]

    def _source_mask(self, source_files):
        if not source_files:
            return None
        wanted = {s for s in source_files}
        ids = np.array([i for i, s in enumerate(self.sources) if s in wanted], dtype=np.int64)
        if ids.size == 0:
            return np.zeros(self.count(), dtype=bool)
        return np.isin(self.source_ids.astype(np.int64), ids)

    def search(self, question, top_k=5, source_files=None, per_term_limit=20000,
               max_hits=400000):
        """关键词检索：子串扫描 + 词频/IDF 排序（近似 BM25 的长度归一）。"""
        self._ensure()
        terms = self.split_terms(question or "")
        if not terms:
            return []
        mask = self._source_mask(source_files)
        buf = self.blob

        hits: dict[int, int] = {}
        dfs: list[int] = []
        for t in terms:
            tb = t.encode("utf-8")
            if not tb:
                continue
            df = 0
            pos = buf.find(tb)
            while pos >= 0 and df < per_term_limit:
                i = int(np.searchsorted(self.offsets, pos, side="right")) - 1
                if i >= 0:
                    off = int(self.offsets[i])
                    if off <= pos and pos + len(tb) <= off + int(self.lengths[i]):
                        if mask is None or mask[i]:
                            if i not in hits:
                                if len(hits) >= max_hits:
                                    break
                                hits[i] = 0
                                df += 1
                            hits[i] += 1
                pos = buf.find(tb, pos + 1)
            dfs.append(df)
            if len(hits) >= max_hits:
                break

        if not hits or not any(dfs):
            return []
        total = max(1, sum(dfs))
        idf = sum(math.log(1.0 + total / (1.0 + d)) for d in dfs if d > 0) / max(1, len(dfs))

        scored = []
        for i, cnt in hits.items():
            if cnt <= 0:
                continue
            score = cnt * idf / (1.0 + math.log(1.0 + int(self.chars[i]) / 300.0))
            scored.append((score, i))
        scored.sort(key=lambda x: -x[0])

        return [(score, self.text_at(i), self.source_of(i), self.chunk_of(i), self.title_of(i))
                for score, i in scored[:max(1, top_k)]]

    def find_row(self, source, chunk_index):
        """定位 (source, chunk_index) 对应的行号；不存在返回 None。

        行按 source 连续、篇内按 chunk_index 递增排列，故用 source_id 掩码 +
        searchsorted 即可。
        """
        self._ensure()
        try:
            sid = self.sources.index(source)
        except ValueError:
            return None
        sel = np.where(self.source_ids == sid)[0]
        if sel.size == 0:
            return None
        sub = self.chunk_idx[sel]
        j = int(np.searchsorted(sub, chunk_index))
        if j < sub.size and int(sub[j]) == chunk_index:
            return int(sel[j])
        return None

    def sources_matching(self, keyword, limit=50):
        """按文件名关键词列经目（含每篇块数）。"""
        self._ensure()
        kw = (keyword or "").strip()
        out = []
        for i, s in enumerate(self.sources):
            if not kw or kw in s:
                out.append((s, int(self._per_source[i])))
                if len(out) >= limit:
                    break
        return out

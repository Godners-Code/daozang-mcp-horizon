# -*- coding: utf-8 -*-
"""把 Godners/DaoZang 的 data/*.parquet 重新拆分为每个 ≤95 MiB 的分片。

为什么拆：GitHub 单文件硬上限 100 MiB，原始 6 个分片每个约 217 MB 无法入库。
拆分后每个分片都能用普通 git 提交（无需 Git LFS）。

流程：
  1. 标定：先写一个 3,000 行的小分片，量出「每行压缩后字节数」；
  2. 分片：按 target/bytes_per_row 估算行数，逐片写出并校验；
  3. 兜底：若某片超出 target，用更少的行数重写该片（最多重试 5 次）；
  4. 产出 manifest.json（行数、字节数、SHA-256、行区间）。

列与原始数据一致：source / title / chunk_index / chars / text / embedding(1024×float32)
"""
from __future__ import annotations

import glob
import hashlib
import json
import os
import sys

import pyarrow as pa
import pyarrow.parquet as pq

TARGET = 95 * 1024 * 1024          # 95 MiB，留 5 MiB 余量给 GitHub 100 MiB 硬上限
SAFETY = 0.97                      # 估算时再留 3% 余量
PROBE_ROWS = 3000
COMPRESSION = "zstd"


def sha256_file(path, chunk=1 << 20):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            b = f.read(chunk)
            if not b:
                break
            h.update(b)
    return h.hexdigest()


def write_shard(table, path):
    pq.write_table(table, path, compression=COMPRESSION,
                   use_dictionary=["source", "title"], row_group_size=8192)
    return os.path.getsize(path)


def main():
    repo = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    src_dir = sys.argv[1] if len(sys.argv) > 1 else os.path.join(repo, "..", "data")
    out_dir = os.path.join(repo, "data", "shards")
    os.makedirs(out_dir, exist_ok=True)
    for old in glob.glob(os.path.join(out_dir, "*.parquet")):
        os.remove(old)

    files = sorted(glob.glob(os.path.join(src_dir, "*.parquet")))
    if not files:
        sys.exit(f"未找到 parquet：{src_dir}")

    print("读取原始分片…", flush=True)
    tables = [pq.read_table(f) for f in files]
    full = pa.concat_tables(tables)
    n_rows = full.num_rows
    schema = full.schema
    print(f"  合计 {n_rows:,} 行；列：{full.column_names}", flush=True)
    print(f"  原始内存占用 {full.nbytes / 1024 / 1024:,.1f} MiB", flush=True)

    # ---------------- 1) 标定 ----------------
    probe = os.path.join(out_dir, "_probe.parquet")
    size = write_shard(full.slice(0, PROBE_ROWS), probe)
    bpr = size / PROBE_ROWS
    os.remove(probe)
    print(f"标定：{PROBE_ROWS:,} 行 → {size / 1024 / 1024:.1f} MiB，"
          f"每行约 {bpr:,.0f} B", flush=True)

    rows_per_shard = max(1000, int(TARGET * SAFETY / bpr))
    n_shards = (n_rows + rows_per_shard - 1) // rows_per_shard
    print(f"预估每片 {rows_per_shard:,} 行 → {n_shards} 片\n", flush=True)

    # ---------------- 2) 分片 ----------------
    manifest = []
    pos = 0
    idx = 0
    while pos < n_rows:
        idx += 1
        take = min(rows_per_shard, n_rows - pos)
        name = f"chunks-{idx:02d}.parquet"
        path = os.path.join(out_dir, name)

        for attempt in range(6):
            size = write_shard(full.slice(pos, take), path)
            if size <= TARGET:
                break
            take = int(take * TARGET / size * 0.95)
            print(f"  片 {idx} 超出 {size / 1024 / 1024:.1f} MiB，"
                  f"缩到 {take:,} 行重写", flush=True)
        else:
            sys.exit(f"片 {idx} 反复超出上限，放弃")

        manifest.append({
            "name": name,
            "rowStart": pos,
            "rows": take,
            "bytes": size,
            "sha256": sha256_file(path),
        })
        flag = "✅" if size <= 95 * 1024 * 1024 else "❌"
        print(f"  {name}  {take:>7,} 行  {size / 1024 / 1024:7.2f} MiB  {flag}", flush=True)
        pos += take

    total = sum(m["bytes"] for m in manifest)
    out = {
        "nRows": n_rows,
        "nShards": len(manifest),
        "compression": COMPRESSION,
        "columns": full.column_names,
        "embeddingDim": 1024,
        "totalBytes": total,
        "shards": manifest,
    }
    with open(os.path.join(out_dir, "manifest.json"), "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)

    over = [m for m in manifest if m["bytes"] > 100 * 1024 * 1024]
    print(f"\n合计 {len(manifest)} 片，{total / 1024 / 1024:,.1f} MiB "
          f"({total / 1024 / 1024 / 1024:,.2f} GiB)")
    print(f"超过 100 MiB 的片：{len(over)} 个")
    print(f"最大片：{max(m['bytes'] for m in manifest) / 1024 / 1024:.2f} MiB")
    print(f"行数校验：{sum(m['rows'] for m in manifest):,} / {n_rows:,}")


if __name__ == "__main__":
    main()

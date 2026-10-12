#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""打包发布制品（**源码 + 文档**，不含语料）。

产出 `dist/daozang-mcp-horizon-<version>-src.zip`，内含：

    server.py  requirements.txt  README.md  USAGE.md  LICENSE  .gitignore
    daozang/*.py                    运行时模块
    tools/*.py  tools/*.sh          部署 / 验证 / 打包脚本
    data/shards/manifest.json       分片清单（行数、字节、SHA-256），不含 parquet 本体
    RELEASE.md                      自动生成的发版说明（提交、端点、校验值）

**刻意排除**：`data/shards/*.parquet`（1.1 GB）与 `markdowns/`（190 MB）。
两者是部署产物，已在 GitHub 仓库与 HuggingFace 上；本包只保证「代码可读、可重建」。

用法：
    python tools/make_release.py                 # 版本取 server.py 里的 SERVER_VERSION
    python tools/make_release.py --version 1.2.0 --out ../dist
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
import sys
import time
import zipfile

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

ALWAYS = ["server.py", "requirements.txt", "README.md", "USAGE.md", "LICENSE", ".gitignore"]
DIRS = ["daozang", "tools"]
EXTRA = ["data/shards/manifest.json"]
# 绝不入包的路径前缀（大语料 / 版本控制 / 临时产物）
EXCLUDE_PREFIX = ("markdowns/", "data/shards/chunks-", ".git/", ".tmp/", "__pycache__/")


def read_version() -> str:
    with open(os.path.join(ROOT, "server.py"), encoding="utf-8") as f:
        m = re.search(r'SERVER_VERSION\s*=\s*"([^"]+)"', f.read())
    return m.group(1) if m else "0.0.0"


def git(*args: str) -> str:
    try:
        return subprocess.run(["git", *args], cwd=ROOT, capture_output=True,
                              text=True, encoding="utf-8", check=True).stdout.strip()
    except Exception:                                           # noqa: BLE001
        return ""


def collect() -> list[str]:
    files: list[str] = []
    for f in ALWAYS:
        if os.path.isfile(os.path.join(ROOT, f)):
            files.append(f)
    for d in DIRS:
        base = os.path.join(ROOT, d)
        if not os.path.isdir(base):
            continue
        for cur, _dirs, names in os.walk(base):
            _dirs[:] = [x for x in _dirs if x != "__pycache__"]
            for n in sorted(names):
                if n.endswith((".pyc", ".pyo")):
                    continue
                files.append(os.path.relpath(os.path.join(cur, n), ROOT).replace("\\", "/"))
    for f in EXTRA:
        if os.path.isfile(os.path.join(ROOT, f)):
            files.append(f)
    return sorted(set(f for f in files
                      if not f.startswith(EXCLUDE_PREFIX)))


def sha256(path: str, limit: int | None = None) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            b = f.read(1 << 20)
            if not b:
                break
            h.update(b)
    return h.hexdigest()


def build_release_md(version: str, files: list[str]) -> str:
    commit = git("rev-parse", "--short", "HEAD")
    commit_full = git("rev-parse", "HEAD")
    branch = git("rev-parse", "--abbrev-ref", "HEAD")
    remote = git("remote", "get-url", "origin")
    ts = time.strftime("%Y-%m-%d %H:%M:%S")

    shards = rows = 0
    man_path = os.path.join(ROOT, "data", "shards", "manifest.json")
    if os.path.isfile(man_path):
        with open(man_path, encoding="utf-8") as f:
            m = json.load(f)
        shards = m.get("nShards", len(m.get("shards", [])))
        rows = m.get("nRows", 0)

    lines = [
        f"# DaoZang Reasoner MCP — 发布制品 v{version}",
        "",
        f"- 打包时间：{ts}",
        f"- 源码提交：`{commit}`（完整 `{commit_full}`，分支 `{branch}`）",
        f"- 远端仓库：{remote}",
        f"- 线上端点：https://daozang-reasoner.fastmcp.app/mcp",
        f"- 传输 / 认证：Streamable HTTP / `Authorization: Bearer <HORIZON_API_KEY>`",
        "",
        "## 本包内容",
        "",
        "**仅源码与文档**，不含语料（`data/shards/*.parquet` 与 `markdowns/`）。",
        "",
        f"- 文件数：{len(files)}",
        "- 运行时模块：`daozang/`（store.py 检索、textloc.py 原文回定位）",
        "- 脚本：`tools/`（部署、验证、多线程推送、分片、打包）",
        "- `data/shards/manifest.json`：分片清单（行数、字节、SHA-256），便于校验外部语料",
        "",
        "## 语料来源（本包不含，需另行获取）",
        "",
        "| 资产 | 来源 | 用途 |",
        "|---|---|---|",
        "| 6 个原始 parquet | HF `Godners/DaoZang` | `tools/split_parquet.py` 的输入 |",
        "| 13 个分片 | 由上一项拆分生成 | 运行时检索数据（`data/shards/`）|",
        "| 3,152 篇 markdown | HF `Godners/DaoZang` | `get_original` / `read_source` 回定位 |",
        "| bge-m3 ONNX | HF `BAAI/bge-m3` | 语义检索（**在线版不使用**）|",
        "",
        "## 语料规模（校验基线）",
        "",
        "| 项 | 值 |",
        "|---|---|",
        f"| 文本块 | {rows:,} |",
        "| 经文篇数 | 3,152 |",
        f"| 分片数 | {shards} |",
        "| 正文体积 | 221.9 MiB |",
        "| 稳态内存 / 加载峰值 | 442 MB / 669 MB |",
        "",
        "## 部署与验证",
        "",
        "```bash",
        "export HORIZON_API_KEY=fmcp_你的密钥",
        "python tools/deploy_horizon.py --repo Godners-Code/daozang-mcp-horizon \\",
        "  --branch main --entrypoint server.py:mcp --dependency requirements.txt",
        "python tools/verify_horizon.py",
        "```",
        "",
        "验证基线：`n_chunks == 285,117`、`n_files == 3,152`，6 个工具全部可用。",
        "",
        "## 文件清单",
        "",
    ]
    lines += [f"- `{f}`" for f in files]
    lines += ["", "## 许可证", "",
              "代码 MIT（见 `LICENSE`）；语料派生自 HF `Godners/DaoZang`，遵循其原许可。", ""]
    return "\n".join(lines)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--version", default=None, help="默认取 server.py 的 SERVER_VERSION")
    ap.add_argument("--out", default=os.path.join(ROOT, "..", "dist"))
    args = ap.parse_args()

    version = args.version or read_version()
    out_dir = os.path.abspath(args.out)
    os.makedirs(out_dir, exist_ok=True)
    zip_path = os.path.join(out_dir, f"daozang-mcp-horizon-{version}-src.zip")

    files = collect()
    if not files:
        sys.exit("没有可打包的文件")

    release_md = build_release_md(version, files)
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED, compresslevel=9) as z:
        for f in files:
            z.write(os.path.join(ROOT, f), f)
        z.writestr("RELEASE.md", release_md)

    total = sum(os.path.getsize(os.path.join(ROOT, f)) for f in files)
    print(f"版本    : {version}")
    print(f"提交    : {git('rev-parse', '--short', 'HEAD')}")
    print(f"文件数  : {len(files)}（+ RELEASE.md）")
    print(f"原始体积: {total / 1024:.1f} KiB")
    print(f"压缩后  : {os.path.getsize(zip_path) / 1024:.1f} KiB")
    print(f"制品    : {zip_path}")
    print(f"sha256  : {sha256(zip_path)}")


if __name__ == "__main__":
    main()

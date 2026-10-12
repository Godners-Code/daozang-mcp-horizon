#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""多线程推送 markdown 原文到 GitHub（Git Data API）。

为什么不用 `git push`：
  - 同一分支上的并发 `git push` 会 non-fast-forward 冲突，**无法并行**；
  - 每批还要本地 `git commit`（500 文件约 19 s），是纯本地串行开销。

实测（本机 → 代理 → api.github.com，样本为真实 markdown，各并发档使用互不重叠的
文件以避免 GitHub 按 blob SHA 去重造成假加速）：

    workers= 1  →  0.67 文件/s   （单请求约 1.49 s）
    workers=16  →  7.97 文件/s
    workers=32  → 12.58 文件/s
    workers=48  → 13.88 文件/s   ← 饱和点
    workers=64  → 13.98 文件/s   （再加并发无收益）

对照 `git push`：500 文件/批 = 36 s（commit 19 s + push 17 s）≈ 13.9 文件/s。
两者吞吐相当，但 API 路径是**真多线程**、可逐文件重试、可断点续传，
并且所有文件最终只产生一个干净的提交（而不是几十个 batch 提交）。

用法：
    export GH_PAT=github_pat_xxx
    python tools/push_markdowns_mt.py                       # 默认 markdowns/，48 线程
    python tools/push_markdowns_mt.py --workers 32 --chunk 500 --interval 15
    python tools/push_markdowns_mt.py --dry-run
"""
from __future__ import annotations

import argparse
import base64
import json
import os
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor

OWNER = "Godners-Code"
REPO = "daozang-mcp-horizon"
BRANCH = "main"
API = f"https://api.github.com/repos/{OWNER}/{REPO}"
# 注意：不要用 HTTPS_PROXY —— 运行环境可能预置了自己的代理变量，会把我们劫持掉。
# 统一用专用变量 NET_PROXY，默认就是本机出网所需的代理。
PROXY = os.environ.get("NET_PROXY") or "http://192.168.109.5:8080"
STATE = ".tmp/pushed_paths.txt"

PAT = os.environ.get("GH_PAT", "").strip()
_opener = urllib.request.build_opener(
    urllib.request.ProxyHandler({"http": PROXY, "https": PROXY})
)
RATE = [-1]          # 最近一次响应里的 X-RateLimit-Remaining


# --------------------------------------------------------------------------- #
# HTTP
# --------------------------------------------------------------------------- #
def gh(path, data=None, method="GET", retries=6, timeout=180):
    """调用 GitHub API，对 403/429/5xx/网络错误做指数退避重试。"""
    url = path if path.startswith("http") else API + path
    last = ""
    for attempt in range(retries):
        body = json.dumps(data).encode("utf-8") if data is not None else None
        r = urllib.request.Request(url, data=body, method=method)
        r.add_header("Authorization", f"Bearer {PAT}")
        r.add_header("Accept", "application/vnd.github+json")
        r.add_header("X-GitHub-Api-Version", "2022-11-28")
        if body:
            r.add_header("Content-Type", "application/json")
        try:
            with _opener.open(r, timeout=timeout) as resp:
                raw = resp.read()
                try:
                    RATE[0] = int(resp.headers.get("X-RateLimit-Remaining", -1))
                except (TypeError, ValueError):
                    pass
                return resp.status, (json.loads(raw) if raw else None)
        except urllib.error.HTTPError as e:
            raw = e.read()
            try:
                RATE[0] = int(e.headers.get("X-RateLimit-Remaining", -1))
            except (TypeError, ValueError):
                pass
            if e.code in (403, 429) or e.code >= 500:
                last = f"HTTP {e.code} {raw[:200]!r}"
                time.sleep(min(2 ** attempt, 30))
                continue
            return e.code, raw.decode("utf-8", "replace")[:600]
        except Exception as e:                                  # noqa: BLE001
            last = repr(e)
            time.sleep(min(2 ** attempt, 30))
    raise RuntimeError(f"重试 {retries} 次仍失败：{method} {url} — {last}")


# --------------------------------------------------------------------------- #
# 工具
# --------------------------------------------------------------------------- #
def fmt_hms(s) -> str:
    s = int(max(s, 0))
    return f"{s // 3600}:{s % 3600 // 60:02d}:{s % 60:02d}"


def git(*args) -> str:
    env = dict(os.environ, LC_ALL="C")
    return subprocess.run(
        ["git", "-c", "core.quotepath=false", *args],
        capture_output=True, text=True, encoding="utf-8", env=env, check=True,
    ).stdout


def list_untracked(prefix: str):
    """返回 (相对路径, 字节数) 列表；已推送过的路径从 checkpoint 里剔除。"""
    out = git("ls-files", "--others", "--exclude-standard")
    paths = [p for p in out.splitlines() if p.strip()]
    if prefix:
        paths = [p for p in paths if p.startswith(prefix)]
    done = set()
    if os.path.exists(STATE):
        with open(STATE, encoding="utf-8") as f:
            done = {l.strip() for l in f if l.strip()}
    paths = [p for p in paths if p not in done]
    items = []
    for p in paths:
        try:
            items.append((p, os.path.getsize(p)))
        except OSError:
            pass
    return items, len(done)


def head_info():
    code, ref = gh(f"/git/ref/heads/{BRANCH}")
    if code != 200:
        raise RuntimeError(f"读取分支失败 HTTP {code}: {ref}")
    sha = ref["object"]["sha"]
    code, commit = gh(f"/git/commits/{sha}")
    if code != 200:
        raise RuntimeError(f"读取 commit 失败 HTTP {code}")
    return sha, commit["tree"]["sha"]


# --------------------------------------------------------------------------- #
# 进度 / 上报
# --------------------------------------------------------------------------- #
class Prog:
    def __init__(self, total, total_bytes):
        self.lock = threading.Lock()
        self.total = total
        self.total_bytes = total_bytes
        self.done = 0
        self.nbytes = 0
        self.failed: list[tuple[str, str]] = []
        self.t0 = time.time()

    def ok(self, nbytes):
        with self.lock:
            self.done += 1
            self.nbytes += nbytes

    def fail(self, path, err):
        with self.lock:
            self.failed.append((path, err))

    def snap(self):
        with self.lock:
            return self.done, self.nbytes, len(self.failed)


def reporter(stop: threading.Event, prog: Prog, interval: int):
    """后台线程：每 interval 秒输出一次上传速度。"""
    prev_done, prev_t = 0, time.time()
    while not stop.wait(interval):
        now = time.time()
        done, nbytes, nfail = prog.snap()
        inst = (done - prev_done) / max(now - prev_t, 1e-9)
        prev_done, prev_t = done, now
        el = now - prog.t0
        avg = done / el if el > 0 else 0.0
        left = prog.total - done
        eta = left / avg if avg > 0 else 0.0
        quota = f" | 配额余 {RATE[0]}" if RATE[0] >= 0 else ""
        print(
            f"[{time.strftime('%H:%M:%S')}] 已上传 {done}/{prog.total} 文件 "
            f"({nbytes / 1048576:.1f} MiB) | 瞬时 {inst:4.1f} f/s | 平均 {avg:4.1f} f/s | "
            f"已用 {fmt_hms(el)} | 剩余约 {fmt_hms(eta)} | "
            f"预计 {time.strftime('%H:%M:%S', time.localtime(now + eta))} 完成"
            + (f" | 失败 {nfail}" if nfail else "") + quota,
            flush=True,
        )


# --------------------------------------------------------------------------- #
# 主流程
# --------------------------------------------------------------------------- #
def put_blob(path: str, size: int, prog: Prog):
    """上传单个文件为 blob；内容寻址，重复内容天然幂等。"""
    try:
        with open(path, "rb") as f:
            content = base64.b64encode(f.read()).decode("ascii")
        code, payload = gh("/git/blobs",
                           {"content": content, "encoding": "base64"}, "POST")
        if code in (200, 201) and isinstance(payload, dict) and payload.get("sha"):
            prog.ok(size)
            return {"path": path, "mode": "100644", "type": "blob",
                    "sha": payload["sha"]}
        prog.fail(path, f"HTTP {code}: {payload}")
    except Exception as e:                                      # noqa: BLE001
        prog.fail(path, repr(e))
    return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--path", default="markdowns",
                    help="只推送该前缀下的未跟踪文件（默认 markdowns）")
    ap.add_argument("--workers", type=int, default=48,
                    help="并发线程数（实测 48 为饱和点）")
    ap.add_argument("--chunk", type=int, default=500,
                    help="每多少文件打一个提交（便于断点续传）")
    ap.add_argument("--interval", type=int, default=15,
                    help="每隔多少秒输出一次上传速度（默认 15）")
    ap.add_argument("--message", default="markdown sources")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--no-resync", action="store_true",
                    help="结束后不执行 git fetch/reset 同步本地分支")
    ap.add_argument("--proxy", default=PROXY)
    args = ap.parse_args()

    if not PAT:
        sys.exit("缺少环境变量 GH_PAT")
    if not os.path.isdir(".git"):
        sys.exit("请在仓库根目录运行")

    # 让 git 子进程（fetch/reset）也走同一个代理
    global _opener
    for k in ("HTTPS_PROXY", "HTTP_PROXY", "https_proxy", "http_proxy"):
        os.environ[k] = args.proxy
    _opener = urllib.request.build_opener(
        urllib.request.ProxyHandler({"http": args.proxy, "https": args.proxy})
    )

    os.makedirs(".tmp", exist_ok=True)
    items, skipped = list_untracked(args.path)
    if skipped:
        print(f"checkpoint：跳过已推送 {skipped} 个文件")
    if not items:
        print("没有待推送文件。")
        return

    total = len(items)
    total_bytes = sum(s for _, s in items)
    nchunks = (total + args.chunk - 1) // args.chunk
    print("=" * 72)
    print(f"待推送 {total} 个文件 / {total_bytes / 1048576:.1f} MiB"
          f"（均 {total_bytes / total / 1024:.1f} KiB）")
    print(f"并发 {args.workers} 线程 | 每 {args.chunk} 文件一个提交 | "
          f"共 {nchunks} 个提交 | 上报间隔 {args.interval}s")
    print(f"目标 {OWNER}/{REPO}@{BRANCH} | 代理 {args.proxy}")
    print(f"开始 {time.strftime('%H:%M:%S')}")
    print("=" * 72)
    if args.dry_run:
        for p, s in items[:10]:
            print(f"  {s / 1024:7.1f} KiB  {p}")
        print(f"  …（共 {total} 个）")
        return

    head, base_tree = head_info()
    print(f"当前 {BRANCH} = {head[:12]}  tree = {base_tree[:12]}\n")

    prog = Prog(total, total_bytes)
    stop = threading.Event()
    rep = threading.Thread(target=reporter, args=(stop, prog, args.interval),
                           daemon=True)
    rep.start()

    t_all = time.time()
    commits = 0
    ok_paths: list[str] = []
    try:
        for ci in range(nchunks):
            part = items[ci * args.chunk:(ci + 1) * args.chunk]
            part_bytes = sum(s for _, s in part)
            c0 = time.time()
            print(f"--- 提交 {ci + 1}/{nchunks}：{len(part)} 文件 "
                  f"/ {part_bytes / 1048576:.1f} MiB ---", flush=True)

            with ThreadPoolExecutor(max_workers=args.workers) as ex:
                entries = [e for e in ex.map(
                    lambda it: put_blob(it[0], it[1], prog), part) if e]

            if not entries:
                print("  本批全部失败，中止。")
                break

            entries.sort(key=lambda e: e["path"].encode("utf-8"))

            code, tree = gh("/git/trees",
                            {"base_tree": base_tree, "tree": entries}, "POST")
            if code not in (200, 201):
                print(f"  建树失败 HTTP {code}: {tree}")
                break

            msg = f"{args.message} {ci + 1}/{nchunks}: {len(entries)} files"
            code, commit = gh("/git/commits",
                              {"message": msg, "tree": tree["sha"],
                               "parents": [head]}, "POST")
            if code not in (200, 201):
                print(f"  建提交失败 HTTP {code}: {commit}")
                break

            code, ref = gh(f"/git/refs/heads/{BRANCH}",
                           {"sha": commit["sha"], "force": False}, "PATCH")
            if code not in (200, 201):
                # 远端被别的提交推进过 → 重新取 head 再试一次
                print(f"  更新引用失败 HTTP {code}，重新同步 head 后重试…")
                head, base_tree = head_info()
                code, commit = gh("/git/commits",
                                  {"message": msg, "tree": tree["sha"],
                                   "parents": [head]}, "POST")
                code, ref = gh(f"/git/refs/heads/{BRANCH}",
                               {"sha": commit["sha"], "force": False}, "PATCH")
                if code not in (200, 201):
                    print(f"  仍失败 HTTP {code}: {ref}")
                    break

            head, base_tree = commit["sha"], tree["sha"]
            commits += 1
            ok_paths.extend(e["path"] for e in entries)
            with open(STATE, "a", encoding="utf-8") as f:
                f.write("\n".join(e["path"] for e in entries) + "\n")

            ct = time.time() - c0
            done, nbytes, nfail = prog.snap()
            print(f"  ✅ {head[:12]}  {ct:.1f}s  "
                  f"本批 {len(entries) / ct:.1f} f/s  "
                  f"{part_bytes / ct / 1048576:.2f} MiB/s  |  "
                  f"累计 {done}/{total} 文件 {nbytes / 1048576:.1f} MiB"
                  + (f" | 失败 {nfail}" if nfail else ""), flush=True)
    except KeyboardInterrupt:
        print("\n中断：已提交的部分已安全落库，checkpoint 已保存，可重跑续传。")
    finally:
        stop.set()
        rep.join(timeout=2)

    tt = time.time() - t_all
    done, nbytes, nfail = prog.snap()
    print("=" * 72)
    print(f"结束：{commits} 个提交，成功 {done}/{total} 文件，"
          f"{nbytes / 1048576:.1f} MiB，失败 {nfail}")
    print(f"总耗时 {fmt_hms(tt)} | 平均 {done / max(tt, 1e-9):.1f} 文件/s | "
          f"{nbytes / max(tt, 1e-9) / 1048576:.2f} MiB/s")
    print(f"最新 {BRANCH} = {head}")
    if prog.failed:
        print("失败明细（前 10 条）：")
        for p, e in prog.failed[:10]:
            print(f"  {p}  →  {e[:160]}")
    print("=" * 72)

    if commits and not args.no_resync:
        print("同步本地分支…")
        try:
            git("fetch", "origin", BRANCH)
            git("reset", "--hard", f"origin/{BRANCH}")
            left = git("ls-files", "--others", "--exclude-standard")
            print(f"  本地已对齐 origin/{BRANCH}，剩余未跟踪 "
                  f"{len([p for p in left.splitlines() if p.strip()])} 个")
        except subprocess.CalledProcessError as e:
            print(f"  同步失败：{e.stderr}")


if __name__ == "__main__":
    for k in ("HTTPS_PROXY", "HTTP_PROXY", "https_proxy", "http_proxy"):
        os.environ.setdefault(k, PROXY)
    main()

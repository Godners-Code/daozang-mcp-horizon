# -*- coding: utf-8 -*-
"""在 Prefect Horizon 上创建项目并部署 DaoZang Reasoner MCP。

流程（Horizon REST API v0）：
  1. POST /organizations/{org}/projects            创建 hosted 项目（幂等：已存在则复用）
  2. GET  /organizations/{org}/projects/{p}/targets 取 production target
  3. POST /organizations/{org}/projects/{p}/versions 从 GitHub 分支构建并部署
  4. GET  /organizations/{org}/projects/{p}/versions/{v} 轮询 building→ready/failed
  5. GET  /organizations/{org}/projects/{p}/targets  取最终 serving URL

用法：
  python tools/deploy_horizon.py [--slug daozang-reasoner] [--repo Godners-Code/daozang-mcp-horizon]
                                 [--branch main] [--entrypoint server.py:mcp] [--no-deploy]
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request

API = "https://horizon.prefect.io/api/v0"
ORG = os.environ.get("HORIZON_ORG_ID",
                     "f47ecb20-bbc9-4cee-86c2-60c77c9e1e64")   # Godners
# 密钥一律从环境变量读取，仓库是公开的，绝不能硬编码。
KEY = os.environ.get("HORIZON_API_KEY", "")
PROXY = os.environ.get("NET_PROXY") or "http://192.168.109.5:8080"


def _opener():
    h = urllib.request.HTTPSHandler()
    h.set_http_debuglevel(0)
    op = urllib.request.build_opener(h)
    op.addheaders = []
    return op


def req(method, path, body=None, timeout=120):
    url = f"{API}{path}"
    data = json.dumps(body).encode() if body is not None else None
    r = urllib.request.Request(url, data=data, method=method)
    r.add_header("Authorization", f"Bearer {KEY}")
    r.add_header("Accept", "application/json")
    if data:
        r.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(r, timeout=timeout) as resp:
            raw = resp.read()
            return resp.status, (json.loads(raw) if raw else None)
    except urllib.error.HTTPError as e:
        raw = e.read()
        try:
            payload = json.loads(raw)
        except Exception:                                # noqa: BLE001
            payload = raw[:600].decode("utf-8", "replace")
        return e.code, payload


def show(tag, code, payload):
    print(f"[{tag}] HTTP {code}")
    if isinstance(payload, dict) and payload.get("message"):
        print("   message:", payload["message"])
        if payload.get("errors"):
            print("   errors :", json.dumps(payload["errors"], ensure_ascii=False)[:600])
    return payload


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--slug", default="daozang-reasoner",
                    help="项目 slug，决定 serving 主机名（6-32 位小写字母/数字/连字符）")
    ap.add_argument("--repo", default="Godners-Code/daozang-mcp-horizon")
    ap.add_argument("--branch", default="main")
    ap.add_argument("--entrypoint", default="server.py:mcp")
    ap.add_argument("--dependency", default="requirements.txt")
    ap.add_argument("--name", default="DaoZang Reasoner")
    ap.add_argument("--summary", default="《中华道藏》全文可溯源检索与原文取证")
    ap.add_argument("--description",
                    default="3,152 篇经文 / 285,117 个文本块。关键词(IDF)检索 + "
                            "Markdown 原文回定位（带行号）。")
    ap.add_argument("--no-deploy", action="store_true")
    ap.add_argument("--poll", type=int, default=60, help="构建轮询次数上限")
    ap.add_argument("--interval", type=float, default=5.0,
                    help="构建轮询间隔秒数（默认 5；调小可获得更密的进度输出）")
    args = ap.parse_args()

    if not KEY:
        sys.exit("缺少环境变量 HORIZON_API_KEY（本仓库为公开仓库，密钥不写入代码）")

    # ---------- 1) 项目 ----------
    code, payload = req("GET", f"/organizations/{ORG}/projects")
    projects = (payload or {}).get("items", []) if isinstance(payload, dict) else []
    proj = next((p for p in projects if p.get("slug") == args.slug), None)
    if proj:
        print(f"复用已有项目：{proj['slug']}  id={proj['id']}")
    else:
        code, payload = req("POST", f"/organizations/{ORG}/projects", {
            "kind": "hosted",
            "slug": args.slug,
            "name": args.name,
            "summary": args.summary[:100],
            "description": args.description,
        })
        show("创建项目", code, payload)
        if code not in (200, 201) or not isinstance(payload, dict) or "id" not in payload:
            sys.exit("项目创建失败")
        proj = payload
    pid = proj["id"]
    print(f"  projectId = {pid}")
    print(f"  authMode  = {proj.get('authMode')}")

    # ---------- 2) targets ----------
    code, payload = req("GET", f"/organizations/{ORG}/projects/{pid}/targets")
    targets = (payload or {}).get("items", []) if isinstance(payload, dict) else []
    print(f"\n[targets] {[(t.get('slug'), t.get('id')) for t in targets]}")
    prod = next((t for t in targets if t.get("slug") == "production"), None) or (targets[0] if targets else None)
    if not prod:
        sys.exit("未找到 target")
    tid = prod["id"]
    print(f"  production targetId = {tid}")

    # ---------- 3) 版本（构建 + 部署） ----------
    body = {
        "source": {
            "kind": "github",
            "repository": args.repo,
            "revision": {"kind": "branch", "name": args.branch},
            "access": {"kind": "public"},
        },
        "build": {"entrypoint": args.entrypoint, "dependencyPath": args.dependency},
    }
    if not args.no_deploy:
        body["deploy"] = {"targetIds": [tid]}

    print(f"\n[创建版本] {args.repo}@{args.branch}  entrypoint={args.entrypoint}")
    code, payload = req("POST", f"/organizations/{ORG}/projects/{pid}/versions", body, timeout=180)
    show("创建版本", code, payload)
    if code not in (200, 201, 202) or not isinstance(payload, dict) or "id" not in payload:
        sys.exit("版本创建失败")
    vid = payload["id"]
    print(f"  versionId = {vid}  status = {payload.get('status')}")

    # ---------- 4) 轮询构建（每次都带构建日志最新一行，等待期间也有进展可见） ----------
    status = payload.get("status")
    t0 = time.time()
    last_msg = None
    for k in range(args.poll):
        if status in ("ready", "failed"):
            break
        time.sleep(args.interval)
        code, payload = req("GET", f"/organizations/{ORG}/projects/{pid}/versions/{vid}")
        if code != 200:
            show("轮询", code, payload)
            break
        status = payload.get("status")

        line = ""
        lcode, logs = req("GET", f"/organizations/{ORG}/projects/{pid}/versions/{vid}/logs")
        if lcode == 200:
            items = logs.get("items") if isinstance(logs, dict) else None
            if items:
                msg = str((items[-1] or {}).get("message", "")).strip()
                if msg and msg != last_msg:
                    last_msg = msg
                    line = "  ← " + msg[:110]
        print(f"  [{k + 1:3d}] {time.time() - t0:6.1f}s  status={status}{line}", flush=True)
    print(f"  构建结束：status={status}  用时 {time.time() - t0:.1f}s", flush=True)

    # ---------- 5) 构建日志 ----------
    code, logs = req("GET", f"/organizations/{ORG}/projects/{pid}/versions/{vid}/logs")
    if code == 200:
        txt = logs if isinstance(logs, str) else json.dumps(logs, ensure_ascii=False)
        print("\n===== 构建日志（末尾 4000 字符）=====")
        print(txt[-4000:])

    # ---------- 5.5) 等待部署就绪（构建 ready ≠ 已上线，还要 provisioning） ----------
    dep_status = None
    if not args.no_deploy:
        d0 = time.time()
        for k in range(args.poll):
            code, dl = req("GET", f"/organizations/{ORG}/projects/{pid}/deployments")
            dep = (dl or {}).get("items", [{}])[0] if isinstance(dl, dict) else {}
            dep_status = dep.get("status")
            print(f"  [部署 {k + 1:3d}] {time.time() - d0:6.1f}s  status={dep_status}"
                  + (f"  error={dep['error']}" if dep.get("error") else ""), flush=True)
            if dep_status in ("succeeded", "deployed", "active", "failed", "error"):
                break
            time.sleep(args.interval)
        print(f"  部署结束：status={dep_status}  用时 {time.time() - d0:.1f}s", flush=True)

    # ---------- 6) serving URL ----------
    code, payload = req("GET", f"/organizations/{ORG}/projects/{pid}/targets")
    for t in (payload or {}).get("items", []):
        print(f"\ntarget {t.get('slug')}: currentDeploymentId={t.get('currentDeploymentId')} "
              f"url={t.get('servingUrl') or t.get('url')}")
    print(f"\n最终：version status = {status} | deployment status = {dep_status}")
    print(f"端点：https://{args.slug}.fastmcp.app/mcp")
    if dep_status in ("succeeded", "deployed", "active"):
        print("提示：用 tools/verify_horizon.py 做一次端到端验证（首次调用含冷启动，约 40-60s）。")


if __name__ == "__main__":
    for k in ("HTTPS_PROXY", "HTTP_PROXY", "https_proxy", "http_proxy"):
        os.environ[k] = PROXY
    main()

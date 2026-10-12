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
    if not KEY:
        sys.exit("缺少环境变量 HORIZON_API_KEY（本仓库为公开仓库，密钥不写入代码）")
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
    args = ap.parse_args()

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

    # ---------- 4) 轮询构建 ----------
    status = payload.get("status")
    for k in range(args.poll):
        if status in ("ready", "failed"):
            break
        time.sleep(5)
        code, payload = req("GET", f"/organizations/{ORG}/projects/{pid}/versions/{vid}")
        if code != 200:
            show("轮询", code, payload)
            break
        status = payload.get("status")
        print(f"  [{k + 1}] status = {status}")

    # ---------- 5) 构建日志 ----------
    code, logs = req("GET", f"/organizations/{ORG}/projects/{pid}/versions/{vid}/logs")
    if code == 200:
        txt = logs if isinstance(logs, str) else json.dumps(logs, ensure_ascii=False)
        print("\n===== 构建日志（末尾 4000 字符）=====")
        print(txt[-4000:])

    # ---------- 6) serving URL ----------
    code, payload = req("GET", f"/organizations/{ORG}/projects/{pid}/targets")
    for t in (payload or {}).get("items", []):
        print(f"\ntarget {t.get('slug')}: status={t.get('status')} "
              f"url={t.get('url') or t.get('servingUrl')}")
    print(f"\n最终：version status = {status}")
    print(f"端点应为：https://{args.slug}.fastmcp.app/mcp")


if __name__ == "__main__":
    for k in ("HTTPS_PROXY", "HTTP_PROXY", "https_proxy", "http_proxy"):
        os.environ[k] = PROXY
    main()

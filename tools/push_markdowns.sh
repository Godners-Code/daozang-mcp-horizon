#!/usr/bin/env bash
# 推送 markdown 原文到 GitHub —— 多线程版（Git Data API）。
#
# 本脚本只是薄封装：设置代理 / 取令牌 / 选解释器，真正的多线程上传逻辑在
#   tools/push_markdowns_mt.py
# 里面，那里有完整的实测依据（为什么不用 `git push`）与参数说明。
#
# 特性：
#   - 多线程上传（默认 48 线程，实测饱和点）
#   - 每 60 秒输出一次瞬时/平均上传速度、累计进度、ETA 与预计完成时刻
#   - 每 500 文件打一个提交，checkpoint 落盘，中断后可重跑续传
#   - 逐文件重试（指数退避），最后同步本地分支到 origin/main
#
# 用法：
#   export GH_PAT=github_pat_xxx
#   tools/push_markdowns.sh                        # 默认：markdowns/，48 线程
#   tools/push_markdowns.sh --workers 32 --interval 60
#   tools/push_markdowns.sh --dry-run
set -uo pipefail

cd "$(dirname "$0")/.." || exit 1

# 本机直连 GitHub 需要走代理（GitHub ↔ Horizon 之间不需要，那是平台侧的事）
# 这里用专用变量 NET_PROXY 并【强制覆盖】标准代理变量：
# 运行环境有时会预置自己的 HTTPS_PROXY，不覆盖的话会被劫持。
export NET_PROXY="${NET_PROXY:-http://192.168.109.5:8080}"
export HTTPS_PROXY="$NET_PROXY"
export HTTP_PROXY="$NET_PROXY"
export https_proxy="$NET_PROXY"
export http_proxy="$NET_PROXY"
export GIT_TERMINAL_PROMPT=0
git config core.quotepath false

# 令牌：优先环境变量；其次读本地 .env（.env 已在 .gitignore 中，绝不入库）
if [ -z "${GH_PAT:-}" ] && [ -f .env ]; then
  # shellcheck disable=SC1091
  . ./.env
fi
if [ -z "${GH_PAT:-}" ]; then
  echo "缺少 GH_PAT。请先： export GH_PAT=github_pat_xxx   或写入本地 .env" >&2
  exit 1
fi

# 解释器：优先 $PYTHON，其次托管解释器，最后 PATH 里的 python
PY="${PYTHON:-}"
if [ -z "$PY" ]; then
  for c in \
    "C:/Users/renty/.workbuddy-ai/binaries/python/versions/3.13.12/python.exe" \
    "$(command -v python3 2>/dev/null)" \
    "$(command -v python 2>/dev/null)"; do
    [ -n "$c" ] && [ -x "$c" ] && { PY="$c"; break; }
  done
fi
if [ -z "$PY" ]; then echo "找不到可用的 python 解释器" >&2; exit 1; fi

exec "$PY" tools/push_markdowns_mt.py "$@"

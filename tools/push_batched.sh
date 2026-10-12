#!/usr/bin/env bash
# 分批提交并推送。
#   规则：小文件每 100 个一批；**超过 50 MiB 的文件单独一批**（13 个 92 MiB 的
#   parquet 分片若挤在一批里，单次 push 会变成 1.1 GB，容易超时/被拒）。
#   顺序：代码与配置 → data/shards 分片 → markdowns 原文
set -uo pipefail

cd "$(dirname "$0")/.." || exit 1
export HTTPS_PROXY="http://192.168.109.5:8080"
export HTTP_PROXY="http://192.168.109.5:8080"
export https_proxy="http://192.168.109.5:8080"
export http_proxy="http://192.168.109.5:8080"
export GIT_TERMINAL_PROMPT=0

BATCH=100
BIG=$((50 * 1024 * 1024))
BRANCH=main

git reset -q

grp() { git ls-files --others --exclude-standard | grep -E "$1" | LC_ALL=C sort; }
{
  grp '^(\.gitignore|README\.md|requirements\.txt|server\.py|daozang/|tools/)'
  grp '^data/'
  grp '^markdowns/'
} > /tmp/bf.txt
TOTAL=$(grep -c . /tmp/bf.txt)
echo "待推送 $TOTAL 个文件（大文件阈值 50 MiB）"
echo "================================================================"

i=0; n=0; fails=0
while [ "$i" -lt "$TOTAL" ]; do
  # 组装一批
  : > /tmp/batch.bin
  cnt=0; bytes=0; i0=$i
  while [ "$i" -lt "$TOTAL" ] && [ "$cnt" -lt "$BATCH" ]; do
    f=$(sed -n "$((i + 1))p" /tmp/bf.txt)
    [ -z "$f" ] && { i=$((i + 1)); continue; }
    sz=$(stat -c %s "$f" 2>/dev/null || echo 0)
    # 大文件：必须独占一批
    if [ "$sz" -gt "$BIG" ]; then
      [ "$cnt" -gt 0 ] && break
      printf '%s\0' "$f" >> /tmp/batch.bin
      cnt=1; bytes=$sz; i=$((i + 1)); break
    fi
    printf '%s\0' "$f" >> /tmp/batch.bin
    cnt=$((cnt + 1)); bytes=$((bytes + sz)); i=$((i + 1))
  done
  [ "$cnt" -eq 0 ] && break

  n=$((n + 1))
  xargs -0 -a /tmp/batch.bin git add --
  git commit -q -m "batch ${n}: ${cnt} files, $((bytes / 1024 / 1024)) MiB" || { echo "  [批 $n] commit 失败"; fails=$((fails + 1)); }

  if git push -q origin "$BRANCH" 2>/tmp/pe.txt; then
    printf '  [批 %2d] %4d 文件  %6d MiB  已推送（累计 %d/%d）\n' "$n" "$cnt" "$((bytes / 1024 / 1024))" "$i" "$TOTAL"
  else
    echo "  [批 $n] ❌ push 失败：$(head -c 500 /tmp/pe.txt)"
    fails=$((fails + 1)); break
  fi
done

echo "================================================================"
echo "完成：$n 批，失败 $fails，本地提交 $(git rev-list --count HEAD 2>/dev/null)"
echo "剩余未跟踪：$(git ls-files --others --exclude-standard | grep -c . || true)"

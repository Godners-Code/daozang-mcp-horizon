# -*- coding: utf-8 -*-
"""原文定位：把索引中的「清洗后文本块」精确映射回 Markdown 原文，并输出带行号的窗口。

清洗规则与建库时完全一致（去 markdown 记号、压缩空白），重放时同步记录每个清洗
字符在原文中的偏移，因此能把块边界还原到原文位置。
"""
from __future__ import annotations

import os
import re


def _apply_keep_group(pattern, text, origin, group=1, flags=0):
    out, oout = [], []
    pos = 0
    for m in re.finditer(pattern, text, flags):
        out.append(text[pos:m.start()])
        oout.extend(origin[pos:m.start()])
        gs, ge = m.start(group), m.end(group)
        out.append(m.group(group))
        oout.extend(origin[gs:ge])
        pos = m.end()
    out.append(text[pos:])
    oout.extend(origin[pos:])
    return "".join(out), oout


def _apply_delete(pattern, text, origin, flags=0):
    out, oout = [], []
    pos = 0
    for m in re.finditer(pattern, text, flags):
        out.append(text[pos:m.start()])
        oout.extend(origin[pos:m.start()])
        pos = m.end()
    out.append(text[pos:])
    oout.extend(origin[pos:])
    return "".join(out), oout


def _apply_replace(pattern, text, origin, repl, flags=0):
    out, oout = [], []
    pos = 0
    for m in re.finditer(pattern, text, flags):
        out.append(text[pos:m.start()])
        oout.extend(origin[pos:m.start()])
        for ch in repl:
            out.append(ch)
            oout.append(origin[m.start()])
        pos = m.end()
    out.append(text[pos:])
    oout.extend(origin[pos:])
    return "".join(out), oout


def replay_clean(raw):
    """重放建库时的 clean_md，返回 (cleaned_text, origin)。"""
    lines = raw.splitlines()
    body, body_origin = raw, None
    if lines and lines[0].strip() == "---":
        cut = None
        for i in range(1, min(len(lines), 60)):
            if lines[i].strip() == "---":
                cut = i
                break
        if cut is not None:
            offs, pos = [], 0
            for ln in raw.splitlines(keepends=True):
                offs.append(pos)
                pos += len(ln)
            body_lines = lines[cut + 1:]
            body = "\n".join(body_lines)
            body_origin = []
            for j, ln in enumerate(body_lines):
                base = offs[cut + 1 + j]
                body_origin.extend([base + k for k in range(len(ln))])
                if j < len(body_lines) - 1:
                    body_origin.append(base + len(ln))
    if body_origin is None:
        body_origin = list(range(len(body)))

    t, org = body, body_origin
    t, org = _apply_delete(r"```.*?```", t, org, re.S)
    t, org = _apply_keep_group(r"`([^`]*)`", t, org, 1)
    t, org = _apply_keep_group(r"!\[([^\]]*)\]\([^)]*\)", t, org, 1)
    t, org = _apply_keep_group(r"\[([^\]]+)\]\([^)]*\)", t, org, 1)
    t, org = _apply_delete(r"<[^>]+>", t, org)
    t, org = _apply_delete(r"^\s{0,3}#{1,6}\s*", t, org, re.M)
    t, org = _apply_keep_group(r"\*\*([^*]+)\*\*", t, org, 1)
    t, org = _apply_keep_group(r"__([^_]+)__", t, org, 1)
    t, org = _apply_keep_group(r"(?<!\*)\*(?!\*)([^*\n]+)\*(?!\*)", t, org, 1)
    t, org = _apply_keep_group(r"(?<!_)__(?!_)([^_\n]+)__(?!_)", t, org, 1)
    t, org = _apply_replace(r"\r", t, org, "\n")
    t, org = _apply_replace(r"[ \t\u3000]+", t, org, " ")
    t, org = _apply_replace(r"\n{3,}", t, org, "\n\n")
    ls = len(t) - len(t.lstrip())
    t = t.strip()
    org = org[ls:ls + len(t)]
    return t, org


def locate_chunk(cleaned, origin, chunk):
    """返回 chunk 在原文中的 (start, end)；找不到返回 None。"""
    s = cleaned.find(chunk)
    if s < 0 or s + len(chunk) > len(origin):
        return None
    e = s + len(chunk)
    return origin[s], origin[e - 1] + 1


def extract_window(raw, rs, re_, context=300):
    """把 [rs, re_) 扩展到最近段落边界，每侧最多扩展 context 字符。"""
    para = re.compile(r"(?:\r?\n){2,}")
    best = -1
    for m in para.finditer(raw[:rs]):
        best = m.end()
    if best >= 0 and rs - best <= context:
        ws = best
    else:
        ws = max(0, rs - context)
    m = para.search(raw[re_:])
    if m and m.start() <= context:
        we = re_ + m.start()
    else:
        we = min(len(raw), re_ + context)
    return ws, we


def render_window(raw, ws, we, rs, re_):
    win = raw[ws:we]
    return win[:rs - ws] + "\u27e6" + win[rs - ws:re_ - ws] + "\u27e7" + win[re_ - ws:]


def line_no(raw, pos):
    return raw.count("\n", 0, pos) + 1


def match_source_files(kw, md_dir):
    """markdowns 下文件名包含 kw 的文件（与索引中 source 字段一致）。"""
    if not kw:
        return []
    return [f for f in sorted(os.listdir(md_dir))
            if f.lower().endswith(".md") and kw in f]

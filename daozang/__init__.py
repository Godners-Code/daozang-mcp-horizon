# -*- coding: utf-8 -*-
"""道藏考据 MCP — Prefect Horizon 在线版。

与本地版（MCPB / stdio）的区别：
  * 数据在**构建期**已固化为紧凑块索引，运行时不下载、不建索引；
  * 只提供关键词（IDF）检索 + 原文提取（即 dao-zang skill 的默认 `text` 引擎）；
    语义检索需要 bge-m3 ONNX 会话（约 570 MB），超出 Horizon 1024 MB 上限，故不提供。
"""
from __future__ import annotations

from .store import DaoZangStore

__all__ = ["DaoZangStore"]
__version__ = "1.1.0"

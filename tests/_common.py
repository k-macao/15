#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试公共工具 (Shared testing utilities)

提供:
  - 仓库根目录 / 脚本目录的绝对路径
  - load_script(name): 以隔离命名空间动态加载 scripts/*.py 模块
    (不依赖 sys.path 配置, 同时兼容 `python -m unittest` 与 `pytest`)

测试套件统一从这里加载被测脚本, 避免对运行目录的假设。
"""
import importlib.util
import os

# 仓库根目录 (tests/ 的上一级)
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# 核心脚本目录
SCRIPTS_DIR = os.path.join(ROOT_DIR, "scripts")
# 评测数据目录
EVALS_DIR = os.path.join(ROOT_DIR, "evals")
EVALS_FILE = os.path.join(EVALS_DIR, "evals.json")
# 模板目录 (template_manager 依赖)
TEMPLATES_DIR = os.path.join(ROOT_DIR, "assets", "templates")


def load_script(name: str):
    """动态加载 scripts/<name>.py 并返回一个模块对象。

    每个脚本以独立模块名 (bettafish_<name>) 加载, 互不污染全局命名空间。
    """
    path = os.path.join(SCRIPTS_DIR, f"{name}.py")
    if not os.path.exists(path):
        raise FileNotFoundError(f"被测脚本不存在: {path}")
    spec = importlib.util.spec_from_file_location(f"bettafish_{name}", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def load_json(path: str):
    import json
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)

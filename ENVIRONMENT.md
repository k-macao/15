# 环境与依赖 (Environment & Libraries)

本文件整理 **BettaFish-skill** 的测试环境、关键数据与第三方依赖, 供本地开发与
`.github/workflows/ci.yml` (GitHub Actions) 调用。

## 1. 运行环境 (Environment)

| 项目 | 要求 |
| --- | --- |
| Python | **3.11+** (脚本使用 dataclasses / 类型注解, 实测 3.11.2 通过) |
| 操作系统 | Linux (CI 使用 `ubuntu-latest`) / macOS / Windows |
| Shell | `bash` (子技能脚本 `frame.sh` / `soffice.py` 需要) |
| Node.js | 可选, 报告生成 guidance 中的 `docx` JS 库示例需要运行时 |
| 网络 | **不需要** —— 测试套件完全离线, 数据来自 `tests/data/` |

### 系统级依赖 (apt / 渲染用)

| 软件 | 用途 | 安装 |
| --- | --- | --- |
| **LibreOffice** (`soffice`) | DOCX/PPTX 转换、接受修订痕迹 | `sudo apt-get install -y libreoffice` |
| **Poppler** (`pdftoppm`) | PDF 转图片 (pdf subskill) | `sudo apt-get install -y poppler-utils` |

> 这两项仅用于子技能的实际渲染; 运行 `tests/` 单元测试**不依赖**它们。

## 2. 第三方库 (Libraries)

完整清单见 `requirements.txt`。核心分层:

| 类别 | 库 | 用途 |
| --- | --- | --- |
| 标准库 | `json` `re` `datetime` `dataclasses` `collections` `hashlib` `pathlib` | 所有核心脚本, 零依赖 |
| PDF 渲染 | `reportlab` | PDF 报告生成 |
| DOCX 子技能 | `defusedxml` | 安全解析 OOXML |
| PDF 子技能 | `pdfplumber` `Pillow` `pdf2image` | PDF 抽取 / 图像处理 / 转图 |
| 数据处理 | `pandas` `numpy` | 参考示例 (可选) |
| 前端可视化 | `echarts@5.4.3` `d3@7` (CDN) | HTML 报告图表, **无需 pip 安装** |

### 安装

```bash
pip install -r requirements.txt
```

## 3. 关键数据 (Key Data)

| 位置 | 内容 | 说明 |
| --- | --- | --- |
| `evals/evals.json` | 8 个端到端评测用例 | 由 `tests/test_evals.py` 校验 |
| `tests/data/sample_search_results.json` | 搜索结果样例 | 平台识别 / 解析测试 |
| `tests/data/sample_social_posts.json` | 社媒帖子样例 | 聚合 / 情感测试 |
| `assets/templates/*.md` | 6 套报告模板 | `template_manager` 解析测试 |
| `references/*.md` | 数据源 / 设计 / 情感指引 | 知识库, 非测试输入 |

## 4. 测试工具 (Testing Tools)

详见 [`tests/README.md`](tests/README.md)。一键运行:

```bash
python -m unittest discover -s tests -p "test_*.py" -v
```

## 5. CI 调用

GitHub Actions 工作流在 `.github/workflows/ci.yml`。提交即触发, 自动完成:
环境准备 → 安装依赖 → 安装系统库 → 运行测试套件 → 校验 evals 结构。

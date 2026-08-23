# 测试工具 (Testing Tools)

本目录是 **BettaFish-skill** 的测试工具集, 用于验证核心脚本、报告模板与评测用例的正确性。

## 结构

```
tests/
├── _common.py                 # 公共工具: 加载脚本 / 路径常量 / JSON 读取
├── __init__.py                # 测试包标识
├── test_sentiment_analyzer.py # 情感分析器 (lexicon + 方面情感)
├── test_data_collector.py     # 数据采集器 (平台识别 / 聚合 / 热度)
├── test_graph_generator.py    # 知识图谱生成器 (D3 力导向图数据)
├── test_template_manager.py   # 报告模板管理器 (assets/templates/*.md)
├── test_report_generator.py   # 报告生成器 (8 章结构 / HTML 生成)
├── test_evals.py              # evals/evals.json 结构校验
└── data/                      # 关键测试数据 (fixtures)
    ├── sample_search_results.json
    ├── sample_social_posts.json
    └── README.md
```

## 运行方式

测试套件基于标准库 `unittest` 编写, 同时兼容 `pytest`, **无需安装任何第三方依赖**即可运行。

```bash
# 方式一: unittest (零依赖)
python -m unittest discover -s tests -t . -p "test_*.py" -v

# 方式二: pytest (若已安装)
pip install pytest
pytest tests/ -v

# 仅运行单个模块
python -m unittest tests.test_sentiment_analyzer -v
```

## 覆盖范围

| 模块 | 覆盖点 |
| --- | --- |
| `sentiment_analyzer` | 单条/批量情感分析、分布统计、关键词提取、热度指数、风险点识别 |
| `data_collector` | 平台识别、搜索结果解析、平台聚合、热度指数、@提及/#话题 提取、视频提取 |
| `graph_generator` | 节点/连线增删、JSON 序列化、HTML 输出、子图提取、分析结果建图 |
| `template_manager` | 模板列举、按查询选择、结构解析、校验、章节指引、按查询加载 |
| `report_generator` | 8 核心章节结构、内容元素指引、三格式 guidance、HTML/Word 字符串生成 |
| `evals.json` | 必填字段、eval_name/id 唯一性、prompt 非空 |

## 说明

- `report_generator` 的 **PDF/Word 实际渲染** 依赖 `reportlab` / `docx` / `LibreOffice`,
  属于重集成测试, 不在本套件内; 本套件仅验证纯 Python 的字符串生成与结构。
- 所有数据均来自 `tests/data/` 中的样例, 不依赖网络或外部服务, 可在离线 CI 环境运行。

详见仓库根目录 `ENVIRONMENT.md` 获取完整环境与依赖说明。

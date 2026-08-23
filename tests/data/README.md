# 关键测试数据 (Key Test Data)

本目录存放测试套件使用的**关键数据** (fixtures)。这些数据是真实结构的最小化样例,
用于单元测试与 CI 验证, **不含任何模拟/虚构的业务结论**, 仅描述数据形状。

| 文件 | 用途 | 对应被测模块 |
| --- | --- | --- |
| `sample_search_results.json` | 模拟 WebSearch 返回的搜索结果条目 (title/snippet/url/source/date) | `scripts/data_collector.py` |
| `sample_social_posts.json` | 模拟社交媒体帖子 (platform/likes/comments/shares/sentiment/engagement_score) | `scripts/data_collector.py`、`scripts/sentiment_analyzer.py` |

## 约定

- 所有字段名与 `scripts/data_collector.py` 中的 `SocialMediaPost` / `parse_search_result`
  保持一致。
- 平台枚举: `weibo` / `xiaohongshu` / `douyin` / `bilibili` / `zhihu` / `other`。
- 情感枚举: `positive` / `neutral` / `negative`。
- 修改字段形状后, 请同步更新 `tests/test_data_collector.py` 与 `tests/_common.py` 中的约定。

## 评测用例 (Evals)

更高层的端到端评测用例定义在仓库根的 **`evals/evals.json`**, 由
`tests/test_evals.py` 校验结构完整性, 并由 `.github/workflows/ci.yml` 在每次提交时自动检查。

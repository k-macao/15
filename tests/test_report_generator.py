#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""报告生成器测试 (report_generator)

注意: 本测试仅覆盖"纯 Python / 字符串生成"部分 (guidance 函数、HTML 生成)。
PDF/Word 实际渲染依赖 reportlab / docx / LibreOffice, 在独立集成测试中验证。
"""
import unittest

from ._common import load_script

rg = load_script("report_generator")

# 评测期望的 8 个核心章节 (get_report_structure 返回的英文键)
EXPECTED_SECTIONS = [
    "executive_summary", "brand_volume_analysis", "key_events_review",
    "sentiment_cognition_analysis", "user_persona_analysis",
    "risk_opportunity_insights", "conclusions_recommendations", "appendix",
]


class TestReportGenerator(unittest.TestCase):
    def test_get_report_structure(self):
        structure = rg.get_report_structure()
        self.assertIsInstance(structure, dict)
        for sec in EXPECTED_SECTIONS:
            self.assertIn(sec, structure, f"缺少核心章节: {sec}")

    def test_get_content_elements_guide(self):
        guide = rg.get_content_elements_guide()
        self.assertIsInstance(guide, dict)

    def test_guidance_functions_return_strings(self):
        self.assertIsInstance(rg.generate_word_report_guidance(), str)
        self.assertIsInstance(rg.generate_pdf_report_guidance(), str)
        self.assertIsInstance(rg.generate_html_report_guidance(), str)

    def test_prepare_report_data(self):
        data = rg.prepare_report_data(
            topic="测试品牌",
            query_results=[{"title": "搜索结果", "source": "weibo"}],
            media_results=[{"title": "视频", "platform": "douyin"}],
            insight_results={"summary": "洞察"},
            forum_discussion=[{"speaker": "QueryAgent", "content": "分析结论"}],
            knowledge_graph={},
        )
        self.assertIsInstance(data, dict)
        self.assertIn("topic", data)

    def test_generate_rich_html_report(self):
        html = rg.generate_rich_html_report(
            topic="测试品牌舆情",
            data={"summary": "本季度口碑整体向好"},
        )
        self.assertIsInstance(html, str)
        self.assertIn("舆情分析报告", html)
        self.assertIn("执行摘要", html)
        # 图表库引用 (embedded guidance)
        self.assertIn("echarts", html)

    def test_generate_rich_word_report(self):
        word = rg.generate_rich_word_report(
            topic="测试品牌舆情",
            data={"summary": "本季度口碑整体向好"},
        )
        self.assertIsInstance(word, str)
        self.assertIn("舆情分析报告", word)


if __name__ == "__main__":
    unittest.main()

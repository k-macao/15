#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""情感分析器测试 (sentiment_analyzer)"""
import unittest

from ._common import load_script

sa = load_script("sentiment_analyzer")
_ANALYZER = sa.SentimentAnalyzer()


def _result_dict(text: str) -> dict:
    """返回与 analyze() 等价的、含 confidence/fine_emotions 的结果字典。"""
    return vars(_ANALYZER.analyze(text))


class TestSentimentAnalyzer(unittest.TestCase):
    def test_simple_sentiment_analyze_structure(self):
        result = sa.simple_sentiment_analyze("这个产品体验很棒，我非常喜欢")
        self.assertIsInstance(result, dict)
        for key in ("label", "confidence", "positive_score",
                    "negative_score", "neutral_score", "fine_emotions", "aspects"):
            self.assertIn(key, result, f"缺少字段: {key}")
        self.assertIn(result["label"], ("positive", "negative", "neutral"))

    def test_simple_sentiment_returns_label(self):
        self.assertIn(
            sa.simple_sentiment_analyze("服务态度恶劣，太差了")["label"],
            ("positive", "negative", "neutral"),
        )

    def test_analyze_batch(self):
        texts = ["服务态度很好", "质量太差了", "一般般吧"]
        results = sa.batch_sentiment_analyze(texts)
        self.assertEqual(len(results), 3)
        for r in results:
            self.assertIn(r["label"], ("positive", "negative", "neutral"))

    def test_sentiment_distribution(self):
        results = [_result_dict(t) for t in
                   ["服务态度很好", "质量太差了", "一般般吧"]]
        dist = sa.analyze_sentiment_distribution(results)
        self.assertIn("total", dist)
        self.assertEqual(dist["total"], 3)
        self.assertIn("positive_count", dist)
        self.assertIn("average_confidence", dist)

    def test_extract_keywords(self):
        keywords = sa.extract_keywords(
            ["这款手机续航很强", "这款手机拍照清晰", "续航是亮点"], top_k=5
        )
        self.assertIsInstance(keywords, list)
        self.assertLessEqual(len(keywords), 5)
        for item in keywords:
            self.assertEqual(len(item), 2)  # (词, 频次)

    def test_calculate_heat_index(self):
        heat = sa.calculate_heat_index(
            ["帖子一", "帖子二"], timestamps=["2024-01-01", "2024-01-02"]
        )
        self.assertIn("heat_score", heat)
        self.assertIn("heat_level", heat)
        self.assertIsInstance(heat["heat_score"], (int, float))

    def test_identify_risk_points(self):
        texts = ["这个品牌欺骗消费者", "服务态度恶劣"]
        results = [_result_dict(t) for t in texts]
        risks = sa.identify_risk_points(texts, results)
        self.assertIsInstance(risks, list)
        # 含风险关键词的文本应被识别
        self.assertGreaterEqual(len(risks), 1)


if __name__ == "__main__":
    unittest.main()

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""数据采集器测试 (data_collector)

注意: collect_from_search_results / extract_video_urls 为模块级函数,
其余为 DataCollector 实例方法。
"""
import os
import unittest

from ._common import load_script, load_json

dc = load_script("data_collector")
SAMPLE = os.path.join(os.path.dirname(__file__), "data", "sample_search_results.json")


class TestDataCollector(unittest.TestCase):
    def setUp(self):
        self.collector = dc.DataCollector()
        self.sample = load_json(SAMPLE)

    def test_identify_platform(self):
        cases = {
            "https://weibo.com/123": "weibo",
            "https://www.xiaohongshu.com/abc": "xiaohongshu",
            "https://www.douyin.com/video/1": "douyin",
            "https://www.bilibili.com/video/BV1": "bilibili",
            "https://www.zhihu.com/question/1": "zhihu",
        }
        for url, expected in cases.items():
            self.assertEqual(self.collector.identify_platform(url), expected, url)

    def test_identify_platform_unknown(self):
        self.assertEqual(self.collector.identify_platform("https://example.com"), "other")

    def test_parse_search_result(self):
        raw = self.sample[0]
        parsed = self.collector.parse_search_result(raw)
        self.assertIsInstance(parsed, dict)
        self.assertIn("platform", parsed)

    def test_aggregate_platform_stats(self):
        parsed = [self.collector.parse_search_result(r) for r in self.sample]
        stats = self.collector.aggregate_platform_stats(parsed)
        self.assertIsInstance(stats, dict)
        self.assertIn("weibo", stats)
        # 每个平台统计应含占比与互动均值
        self.assertIn("percentage", stats["weibo"])
        self.assertIn("avg_likes", stats["weibo"])

    def test_calculate_heat_index(self):
        heat = self.collector.calculate_heat_index(
            ["内容一", "内容二"], timestamps=["2024-01-01", "2024-01-02"]
        )
        self.assertIn("heat_score", heat)
        self.assertIn("heat_level", heat)

    def test_extract_mentions_and_hashtags(self):
        text = "感谢 @品牌官博 支持，一起来 #国货之光# 活动"
        # 函数返回已去除 @ / # 的纯净词
        self.assertIn("品牌官博", self.collector.extract_mentions(text))
        self.assertIn("国货之光", self.collector.extract_hashtags(text))

    def test_collect_from_search_results(self):
        # 模块级函数
        results = dc.collect_from_search_results(self.sample)
        self.assertIsInstance(results, list)
        self.assertGreaterEqual(len(results), 1)

    def test_extract_video_urls(self):
        # 模块级函数
        videos = dc.extract_video_urls(self.sample)
        self.assertIsInstance(videos, list)


if __name__ == "__main__":
    unittest.main()

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Tests for scripts/generate_wechat_push.py"""

import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

import generate_wechat_push as gwp  # noqa: E402

SAMPLE_RSS = """<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0"><channel>
  <title>demo feed</title>
  <item>
    <title>某品牌新品发布获好评</title>
    <link>https://example.com/news/1</link>
    <description>&lt;a href="#"&gt;某品牌新品发布获好评&lt;/a&gt; 用户反馈体验流畅。</description>
    <pubDate>Mon, 18 Aug 2025 08:00:00 GMT</pubDate>
    <source url="https://media.example.com">示例媒体</source>
  </item>
  <item>
    <title>用户投诉售后问题引关注</title>
    <link>https://example.com/news/2</link>
    <description>部分用户投诉售后响应慢，要求退款赔偿。</description>
    <pubDate>Tue, 19 Aug 2025 09:30:00 GMT</pubDate>
  </item>
  <item>
    <title></title>
    <link>https://example.com/skip-me</link>
  </item>
</channel></rss>
"""

SAMPLE_ATOM = """<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <entry>
    <title>Climate technology update</title>
    <link href="https://example.com/atom/1" />
    <summary>New research and policy context.</summary>
    <updated>2025-08-20T10:30:00Z</updated>
  </entry>
</feed>
"""


class GenerateWechatPushTests(unittest.TestCase):
    """All generate()/main() tests run in offline mode for determinism."""

    def setUp(self):
        self._old_offline = gwp.os.environ.get("WECHAT_PUSH_OFFLINE")
        gwp.os.environ["WECHAT_PUSH_OFFLINE"] = "1"

    def tearDown(self):
        if self._old_offline is None:
            gwp.os.environ.pop("WECHAT_PUSH_OFFLINE", None)
        else:
            gwp.os.environ["WECHAT_PUSH_OFFLINE"] = self._old_offline

    def test_generate_html_contains_topic(self):
        topic = "AI 行业本周舆情"
        result = gwp.generate(topic)
        self.assertIn(topic, result["html"])
        self.assertIn("<!DOCTYPE html>", result["html"])
        self.assertTrue(result["template"])

    def test_generate_html_has_rich_sections(self):
        result = gwp.generate("测试主题")
        html_doc = result["html"]
        for section in ("热度", "情感", "热门关键词", "渠道分布", "风险提示", "要点", "信源列表"):
            self.assertIn(section, html_doc)
        # linked sources must be rendered as anchors
        self.assertIn("<a href=", html_doc)

    def test_generate_offline_reports_stub_source(self):
        result = gwp.generate("测试主题", offline=True)
        self.assertEqual(result["data_source"], "offline_stub")
        self.assertIn("离线示例数据", result["html"])

    def test_bilingual_rss_coverage_is_explicit(self):
        self.assertEqual(len(gwp.ENGLISH_RSS_FEEDS), 50)
        self.assertEqual(len(gwp.CHINESE_RSS_FEEDS), 20)
        html_doc = gwp.generate("测试主题", offline=True)["html"]
        self.assertIn("English RSS feeds", html_doc)
        self.assertIn("中文 RSS feeds", html_doc)

    def test_cli_writes_output(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "wechat_push.html"
            rc = gwp.main(["--topic", "测试主题", "--output", str(out), "--offline"])
            self.assertEqual(rc, 0)
            self.assertTrue(out.is_file())
            self.assertIn("测试主题", out.read_text(encoding="utf-8"))

    def test_push_skipped_without_token(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "wechat_push.html"
            old = gwp.os.environ.pop("PUSHPLUS_TOKEN", None)
            try:
                rc = gwp.main(["--topic", "skip", "--output", str(out), "--push", "--offline"])
            finally:
                if old is not None:
                    gwp.os.environ["PUSHPLUS_TOKEN"] = old
            self.assertEqual(rc, 0)


class ParseRssTests(unittest.TestCase):
    def test_parse_rss_extracts_items(self):
        items = gwp._parse_rss(SAMPLE_RSS.encode("utf-8"), default_source="demo")
        self.assertEqual(len(items), 2)  # empty-title item skipped
        first = items[0]
        self.assertEqual(first["title"], "某品牌新品发布获好评")
        self.assertEqual(first["url"], "https://example.com/news/1")
        self.assertEqual(first["source"], "示例媒体")
        self.assertEqual(first["date"], "2025-08-18")
        self.assertNotIn("<", first["snippet"])  # tags stripped
        # item without <source> falls back to default
        self.assertEqual(items[1]["source"], "demo")

    def test_parse_rss_invalid_xml(self):
        self.assertEqual(gwp._parse_rss(b"not xml at all"), [])

    def test_parse_atom_feed(self):
        items = gwp._parse_rss(SAMPLE_ATOM.encode("utf-8"), default_source="atom-demo")
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]["url"], "https://example.com/atom/1")
        self.assertEqual(items[0]["date"], "2025-08-20")
        self.assertEqual(items[0]["source"], "atom-demo")

    def test_build_insight_falls_back_to_stub(self):
        old = gwp.fetch_search_results
        gwp.fetch_search_results = lambda topic, limit=12: []
        try:
            insight = gwp.build_insight("任意主题", offline=False)
        finally:
            gwp.fetch_search_results = old
        self.assertEqual(insight["data_source"], "offline_stub")
        self.assertGreaterEqual(len(insight["posts"]), 3)

    def test_build_insight_uses_fetched_results(self):
        fetched = gwp._parse_rss(SAMPLE_RSS.encode("utf-8"))
        old = gwp.fetch_search_results
        gwp.fetch_search_results = lambda topic, limit=12: fetched
        try:
            insight = gwp.build_insight("某品牌", offline=False)
        finally:
            gwp.fetch_search_results = old
        self.assertEqual(insight["data_source"], "rss_news")
        self.assertEqual(len(insight["posts"]), 2)
        # the complaint item should surface as a risk point
        self.assertTrue(insight["risks"])
        self.assertTrue(insight["keywords"])


if __name__ == "__main__":
    unittest.main()

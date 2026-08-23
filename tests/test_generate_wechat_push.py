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


class GenerateWechatPushTests(unittest.TestCase):
    def test_generate_html_contains_topic(self):
        topic = "AI 行业本周舆情"
        result = gwp.generate(topic)
        self.assertIn(topic, result["html"])
        self.assertIn("<!DOCTYPE html>", result["html"])
        self.assertTrue(result["template"])

    def test_cli_writes_output(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "wechat_push.html"
            rc = gwp.main(["--topic", "测试主题", "--output", str(out)])
            self.assertEqual(rc, 0)
            self.assertTrue(out.is_file())
            self.assertIn("测试主题", out.read_text(encoding="utf-8"))

    def test_push_skipped_without_token(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "wechat_push.html"
            old = gwp.os.environ.pop("PUSHPLUS_TOKEN", None)
            try:
                rc = gwp.main(["--topic", "skip", "--output", str(out), "--push"])
            finally:
                if old is not None:
                    gwp.os.environ["PUSHPLUS_TOKEN"] = old
            self.assertEqual(rc, 0)


if __name__ == "__main__":
    unittest.main()

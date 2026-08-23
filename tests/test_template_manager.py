#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""报告模板管理器测试 (template_manager)

依赖仓库内 assets/templates/*.md 真实模板文件。
"""
import os
import unittest

from ._common import load_script, TEMPLATES_DIR

tm = load_script("template_manager")


class TestTemplateManager(unittest.TestCase):
    def test_list_all_templates(self):
        templates = tm.list_all_templates()
        self.assertIsInstance(templates, list)
        self.assertGreaterEqual(len(templates), 1)
        for t in templates:
            self.assertIn("name", t)
            self.assertIn("filename", t)

    def test_select_report_template(self):
        name, path = tm.select_report_template("某品牌发生危机事件")
        self.assertIsInstance(name, str)
        self.assertTrue(os.path.exists(path))

    def test_parse_template_structure(self):
        # 取第一个真实存在的模板解析
        templates = tm.list_all_templates()
        path = next(
            (t["filename"] for t in templates
             if os.path.exists(os.path.join(TEMPLATES_DIR, t["filename"]))),
            None,
        )
        self.assertIsNotNone(path, "未找到可解析的模板")
        structure = tm.parse_template_structure(os.path.join(TEMPLATES_DIR, path))
        self.assertIn("chapters", structure)

    def test_validate_template(self):
        templates = tm.list_all_templates()
        path = os.path.join(TEMPLATES_DIR, templates[0]["filename"])
        self.assertTrue(tm.validate_template(path))

    def test_validate_missing_template(self):
        self.assertFalse(tm.validate_template("/nonexistent/template.md"))

    def test_get_section_content_guidance(self):
        guidance = tm.get_section_content_guidance("执行摘要")
        self.assertIsInstance(guidance, str)

    def test_load_template_for_query(self):
        result = tm.load_template_for_query("日常舆情监测")
        self.assertIsInstance(result, dict)
        self.assertIn("template_name", result)


if __name__ == "__main__":
    unittest.main()

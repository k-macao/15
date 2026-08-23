#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""评测用例 (evals.json) 结构校验

保证 evals/ 下的测试用例定义始终合法, 供 GitHub Actions 在每次提交时校验。
"""
import unittest

from ._common import load_json, EVALS_FILE

REQUIRED_KEYS = {"id", "eval_name", "prompt", "expected_output"}


class TestEvals(unittest.TestCase):
    def setUp(self):
        self.data = load_json(EVALS_FILE)

    def test_file_exists_and_valid_json(self):
        self.assertIsInstance(self.data, dict)

    def test_has_skill_name(self):
        self.assertIn("skill_name", self.data)
        self.assertIsInstance(self.data["skill_name"], str)

    def test_has_evals_list(self):
        self.assertIn("evals", self.data)
        self.assertIsInstance(self.data["evals"], list)
        self.assertGreaterEqual(len(self.data["evals"]), 1)

    def test_each_eval_has_required_keys(self):
        for ev in self.data["evals"]:
            missing = REQUIRED_KEYS - set(ev.keys())
            self.assertEqual(missing, set(), f"评测 {ev.get('id')} 缺少字段: {missing}")
            self.assertIsInstance(ev["prompt"], str)
            self.assertGreater(len(ev["prompt"].strip()), 0)

    def test_eval_names_unique(self):
        names = [ev["eval_name"] for ev in self.data["evals"]]
        self.assertEqual(len(names), len(set(names)), "eval_name 必须唯一")

    def test_eval_ids_unique_and_present(self):
        ids = [ev["id"] for ev in self.data["evals"]]
        self.assertEqual(len(ids), len(set(ids)), "eval id 必须唯一")
        for i in ids:
            self.assertIsNotNone(i)


if __name__ == "__main__":
    unittest.main()

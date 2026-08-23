#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""知识图谱生成器测试 (graph_generator)"""
import unittest

from ._common import load_script

gm = load_script("graph_generator")


class TestGraphGenerator(unittest.TestCase):
    def setUp(self):
        self.builder = gm.KnowledgeGraphBuilder("测试主题")

    def test_add_topic_node(self):
        tid = self.builder.add_topic_node({"note": "root"})
        self.assertTrue(tid.startswith("topic_"))
        self.assertEqual(len(self.builder.to_dict()["nodes"]), 1)

    def test_add_engine_node(self):
        self.builder.add_topic_node()
        eid = self.builder.add_engine_node("QueryAgent", {"role": "query"})
        self.assertTrue(eid.startswith("engine_"))

    def test_add_section_node(self):
        self.builder.add_topic_node()
        sid = self.builder.add_section_node("执行摘要", 1)
        self.assertTrue(sid.startswith("section_"))

    def test_add_link(self):
        t = self.builder.add_topic_node()
        e = self.builder.add_engine_node("MediaAgent")
        self.builder.add_link(t, e, "analyzed_by")
        self.assertGreaterEqual(len(self.builder.to_dict()["links"]), 1)

    def test_to_json_roundtrip(self):
        self.builder.add_topic_node()
        data = self.builder.to_dict()
        text = self.builder.to_json()
        self.assertIsInstance(text, str)
        self.assertIn("nodes", data)

    def test_generate_graph_html(self):
        self.builder.add_topic_node()
        self.builder.add_engine_node("InsightAgent")
        html = gm.generate_graph_html(self.builder.to_dict())
        self.assertIsInstance(html, str)
        self.assertIn("knowledge-graph-container", html)

    def test_get_subgraph(self):
        t = self.builder.add_topic_node()
        e = self.builder.add_engine_node("QueryAgent")
        self.builder.add_link(t, e)
        sub = self.builder.get_subgraph(t, depth=1)
        node_ids = [n["id"] for n in sub["nodes"]]
        self.assertIn(t, node_ids)

    def test_build_from_analysis_result(self):
        # build_knowledge_graph(topic, query_results, media_results, insight_results)
        # 返回图谱数据字典 (含 nodes / links)
        data = gm.build_knowledge_graph(
            "某品牌",
            query_results=[{"title": "品牌口碑搜索", "source": "weibo"}],
            media_results=[{"title": "相关视频", "platform": "douyin"}],
            insight_results={"summary": "洞察结论"},
        )
        self.assertIsInstance(data, dict)
        # 至少包含: 主题节点 + 3 引擎节点 + 若干章节/查询节点
        self.assertGreaterEqual(len(data["nodes"]), 4)


if __name__ == "__main__":
    unittest.main()

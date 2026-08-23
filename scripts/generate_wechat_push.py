#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Generate a WeChat-ready HTML briefing and optionally send it via PushPlus.

Used by `.github/workflows/ci.yml` (workflow_dispatch):

    python scripts/generate_wechat_push.py \\
        --topic "$TOPIC" \\
        --output dist/wechat_push.html \\
        --push
"""

from __future__ import annotations

import argparse
import html
import json
import os
import sys
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

# Allow `python scripts/generate_wechat_push.py` from repo root.
_SCRIPTS_DIR = Path(__file__).resolve().parent
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))

from data_collector import DataCollector  # noqa: E402
from report_generator import prepare_report_data  # noqa: E402
from sentiment_analyzer import SentimentAnalyzer  # noqa: E402
from template_manager import select_report_template  # noqa: E402

PUSHPLUS_URL = "https://www.pushplus.plus/send"


def _escape(text: Any) -> str:
    return html.escape(str(text), quote=True)


def _sample_search_results(topic: str) -> List[Dict[str, str]]:
    """Lightweight structured stubs so the pipeline is offline-safe in CI.

    Production agents should replace this with real WebSearch results.
    """
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return [
        {
            "title": f"{topic}：行业观察与舆情摘要",
            "snippet": f"围绕「{topic}」的公开讨论持续升温，关注产品、监管与用户体验。",
            "url": "https://weibo.com/example/topic",
            "source": "weibo",
            "date": today,
        },
        {
            "title": f"{topic} 社区热议",
            "snippet": "用户反馈整体偏中性偏正，部分对价格与落地节奏存疑。",
            "url": "https://www.zhihu.com/question/example",
            "source": "zhihu",
            "date": today,
        },
        {
            "title": f"{topic} 短视频传播",
            "snippet": "短视频平台出现解读与评测内容，互动以好奇与期待为主。",
            "url": "https://www.douyin.com/video/example",
            "source": "douyin",
            "date": today,
        },
    ]


def _posts_from_results(results: List[Dict]) -> List[Dict]:
    analyzer = SentimentAnalyzer()
    posts: List[Dict] = []
    for i, item in enumerate(results):
        text = f"{item.get('title', '')} {item.get('snippet', '')}"
        sent = analyzer.analyze(text)
        posts.append(
            {
                "id": f"p{i}",
                "platform": item.get("platform") or item.get("source") or "other",
                "nickname": item.get("source") or "source",
                "content": item.get("snippet") or item.get("title") or "",
                "sentiment": sent.label,
                "likes": 10 + i * 3,
                "comments": 2 + i,
                "shares": 1 + i,
                "timestamp": item.get("date") or "",
                "url": item.get("url") or "",
                "topic": "",
                "engagement_score": float(12 + i * 4),
            }
        )
    return posts


def build_insight(topic: str) -> Dict[str, Any]:
    collector = DataCollector()
    raw = _sample_search_results(topic)
    parsed = [collector.parse_search_result(r) for r in raw]
    posts = _posts_from_results(parsed)
    platform_stats = collector.aggregate_platform_stats(posts)
    heat = collector.calculate_heat_index([p["content"] for p in posts])

    labels = [p["sentiment"] for p in posts]
    sentiment = {
        "positive": labels.count("positive"),
        "negative": labels.count("negative"),
        "neutral": labels.count("neutral"),
        "dominant": max(
            ("positive", "negative", "neutral"),
            key=lambda k: labels.count(k) if labels else 0,
        ),
    }
    return {
        "parsed": parsed,
        "posts": posts,
        "platform_stats": platform_stats,
        "heat": heat,
        "sentiment": sentiment,
        "keywords": [topic, "舆情", "口碑"],
        "risks": [],
        "trend_data": [],
    }


def render_html(topic: str, data: Dict[str, Any], template_name: str) -> str:
    sentiment = data.get("sentiment") or {}
    heat = data.get("heat") or {}
    platforms = data.get("platform_stats") or {}
    findings = (data.get("query_summary") or {}).get("key_findings") or []
    generated = data.get("analysis_time") or datetime.now().strftime("%Y-%m-%d %H:%M")

    rows = []
    for name, stats in platforms.items():
        rows.append(
            "<tr>"
            f"<td>{_escape(name)}</td>"
            f"<td>{_escape(stats.get('count', 0))}</td>"
            f"<td>{_escape(stats.get('percentage', 0))}%</td>"
            "</tr>"
        )
    table_body = "\n".join(rows) or "<tr><td colspan='3'>暂无平台数据</td></tr>"
    finding_items = "".join(f"<li>{_escape(f)}</li>" for f in findings) or "<li>暂无条目</li>"

    return f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>{_escape(topic)} · 微信舆情简报</title>
  <style>
    body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
           margin: 0; padding: 24px; background: #0a192f; color: #e6f1ff; }}
    h1 {{ color: #ffd700; font-size: 1.6rem; }}
    .meta {{ color: #8892b0; font-size: 0.9rem; }}
    .card {{ background: #112240; border: 1px solid #233554; border-radius: 8px;
             padding: 16px; margin: 16px 0; }}
    table {{ width: 100%; border-collapse: collapse; }}
    th, td {{ text-align: left; padding: 8px; border-bottom: 1px solid #233554; }}
    a {{ color: #64ffda; }}
  </style>
</head>
<body>
  <h1>{_escape(topic)}</h1>
  <p class="meta">模板：{_escape(template_name)} · 生成时间：{_escape(generated)}</p>
  <div class="card">
    <h2>热度</h2>
    <p>指数 {_escape(heat.get('heat_score', '—'))}（{_escape(heat.get('heat_level', '—'))}），
       提及 {_escape(heat.get('total_mentions', 0))} 条。</p>
  </div>
  <div class="card">
    <h2>情感</h2>
    <p>正面 {_escape(sentiment.get('positive', 0))} ·
       中性 {_escape(sentiment.get('neutral', 0))} ·
       负面 {_escape(sentiment.get('negative', 0))}
       （主导：{_escape(sentiment.get('dominant', 'neutral'))}）</p>
  </div>
  <div class="card">
    <h2>渠道分布</h2>
    <table>
      <thead><tr><th>平台</th><th>条数</th><th>占比</th></tr></thead>
      <tbody>{table_body}</tbody>
    </table>
  </div>
  <div class="card">
    <h2>要点</h2>
    <ul>{finding_items}</ul>
  </div>
</body>
</html>
"""


def push_via_pushplus(title: str, content: str, token: str) -> Dict[str, Any]:
    payload = json.dumps(
        {
            "token": token,
            "title": title[:100],
            "content": content,
            "template": "html",
        }
    ).encode("utf-8")
    req = urllib.request.Request(
        PUSHPLUS_URL,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            body = resp.read().decode("utf-8", errors="replace")
            try:
                return json.loads(body)
            except json.JSONDecodeError:
                return {"code": resp.status, "msg": body}
    except urllib.error.HTTPError as exc:
        return {"code": exc.code, "msg": str(exc)}
    except urllib.error.URLError as exc:
        return {"code": -1, "msg": str(exc.reason)}


def generate(topic: str) -> Dict[str, Any]:
    template_name, _path = select_report_template(topic)
    insight = build_insight(topic)
    report = prepare_report_data(
        topic=topic,
        query_results=insight["parsed"],
        media_results=[],
        insight_results={
            "sentiment": insight["sentiment"],
            "platform_stats": insight["platform_stats"],
            "keywords": insight["keywords"],
            "risks": insight["risks"],
            "trend_data": insight["trend_data"],
        },
        forum_discussion=[],
        knowledge_graph={},
    )
    report["heat"] = insight["heat"]
    html_doc = render_html(topic, report, template_name)
    return {"html": html_doc, "report": report, "template": template_name}


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate WeChat push HTML via library tools.")
    parser.add_argument("--topic", default=os.environ.get("TOPIC", "AI 行业本周舆情"))
    parser.add_argument("--output", default="dist/wechat_push.html")
    parser.add_argument("--push", action="store_true", help="Send via PushPlus (PUSHPLUS_TOKEN).")
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    result = generate(args.topic)
    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(result["html"], encoding="utf-8")
    print(f"Wrote {out} (template={result['template']})")

    if args.push:
        token = os.environ.get("PUSHPLUS_TOKEN", "").strip()
        if not token:
            print("PUSHPLUS_TOKEN is empty; skip push.", file=sys.stderr)
            return 0
        resp = push_via_pushplus(args.topic, result["html"], token)
        print(f"PushPlus response: {resp}")
        code = resp.get("code")
        if code not in (200, "200", 0, "0"):
            return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

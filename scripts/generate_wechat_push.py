#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Generate a WeChat-ready HTML briefing and optionally send it via PushPlus.

Data pipeline
-------------
1. Fetch real news items about the topic from public RSS feeds
   (Google News / Bing News, no API key required).
2. Run the library analyzers: platform stats, heat index, sentiment
   distribution, keyword extraction, and risk identification.
3. Render a rich multi-card HTML briefing (heat / sentiment bar /
   keywords / channels / trend / risks / findings / linked sources).

Offline safety: when the network is unavailable, fetching fails, or
``--offline`` / ``WECHAT_PUSH_OFFLINE=1`` is set, the pipeline falls back
to lightweight structured stubs so CI keeps working without network.

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
import re
import sys
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

# Allow `python scripts/generate_wechat_push.py` from repo root.
_SCRIPTS_DIR = Path(__file__).resolve().parent
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))

from data_collector import DataCollector  # noqa: E402
from report_generator import prepare_report_data  # noqa: E402
from sentiment_analyzer import (  # noqa: E402
    SentimentAnalyzer,
    extract_keywords,
    identify_risk_points,
)
from template_manager import select_report_template  # noqa: E402

PUSHPLUS_URL = "https://www.pushplus.plus/send"
FETCH_TIMEOUT = 12
DEFAULT_LIMIT = 12

SENTIMENT_ZH = {"positive": "正面", "negative": "负面", "neutral": "中性"}
RISK_ZH = {"high": "高", "medium": "中", "low": "低"}
RISK_COLOR = {"high": "#ff6b6b", "medium": "#ffb347", "low": "#64ffda"}


def _escape(text: Any) -> str:
    return html.escape(str(text), quote=True)


def _strip_tags(text: str) -> str:
    text = re.sub(r"<[^>]+>", " ", text or "")
    return re.sub(r"\s+", " ", html.unescape(text)).strip()


# ---------------------------------------------------------------------------
# Real data fetching (RSS, no API key required)
# ---------------------------------------------------------------------------

def _http_get(url: str, timeout: int = FETCH_TIMEOUT) -> bytes:
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
            )
        },
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read()


def _parse_rss(xml_bytes: bytes, default_source: str = "news") -> List[Dict[str, str]]:
    """Parse an RSS 2.0 feed into search-result-shaped dicts."""
    results: List[Dict[str, str]] = []
    try:
        root = ET.fromstring(xml_bytes)
    except ET.ParseError:
        return results

    for item in root.findall(".//item"):
        title = _strip_tags(item.findtext("title") or "")
        link = (item.findtext("link") or "").strip()
        snippet = _strip_tags(item.findtext("description") or "")
        source = _strip_tags(item.findtext("source") or "") or default_source
        date = ""
        pub = item.findtext("pubDate") or ""
        if pub:
            try:
                date = parsedate_to_datetime(pub).strftime("%Y-%m-%d")
            except (TypeError, ValueError):
                date = ""
        if not title or not link:
            continue
        # Google News duplicates the title inside description; drop the echo.
        if snippet == title or not snippet:
            snippet = title
        results.append(
            {
                "title": title,
                "snippet": snippet[:300],
                "url": link,
                "source": source,
                "date": date or datetime.now(timezone.utc).strftime("%Y-%m-%d"),
            }
        )
    return results


def fetch_search_results(topic: str, limit: int = DEFAULT_LIMIT) -> List[Dict[str, str]]:
    """Fetch real news/search items about the topic from public RSS feeds.

    Tries Google News first, then Bing News; merges and dedupes by title.
    Returns an empty list when everything fails (caller falls back to stubs).
    """
    q = urllib.parse.quote(topic)
    feeds = [
        (
            "google_news",
            f"https://news.google.com/rss/search?q={q}&hl=zh-CN&gl=CN&ceid=CN:zh-Hans",
        ),
        ("bing_news", f"https://www.bing.com/news/search?q={q}&format=rss"),
    ]

    merged: List[Dict[str, str]] = []
    seen_titles = set()
    for name, url in feeds:
        try:
            items = _parse_rss(_http_get(url), default_source=name)
        except (urllib.error.URLError, OSError, ValueError) as exc:
            print(f"[fetch] {name} failed: {exc}", file=sys.stderr)
            continue
        for it in items:
            key = it["title"][:60]
            if key in seen_titles:
                continue
            seen_titles.add(key)
            merged.append(it)
        if len(merged) >= limit:
            break
    return merged[:limit]


def _sample_search_results(topic: str) -> List[Dict[str, str]]:
    """Lightweight structured stubs so the pipeline is offline-safe in CI."""
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


def _offline_mode_requested() -> bool:
    return os.environ.get("WECHAT_PUSH_OFFLINE", "").strip().lower() in {"1", "true", "yes"}


# ---------------------------------------------------------------------------
# Analysis
# ---------------------------------------------------------------------------

def _posts_from_results(results: List[Dict]) -> List[Dict]:
    analyzer = SentimentAnalyzer()
    posts: List[Dict] = []
    for i, item in enumerate(results):
        text = f"{item.get('title', '')} {item.get('snippet', '')}"
        sent = analyzer.analyze(text)
        platform = item.get("platform") or "other"
        if platform in ("other", "unknown"):
            platform = item.get("source") or "other"
        posts.append(
            {
                "id": f"p{i}",
                "platform": platform,
                "nickname": item.get("source") or "source",
                "content": item.get("snippet") or item.get("title") or "",
                "title": item.get("title") or "",
                "sentiment": sent.label,
                "confidence": sent.confidence,
                "negative_score": sent.negative_score,
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


def build_insight(topic: str, offline: bool = False, limit: int = DEFAULT_LIMIT) -> Dict[str, Any]:
    collector = DataCollector()

    data_source = "offline_stub"
    raw: List[Dict[str, str]] = []
    if not offline:
        raw = fetch_search_results(topic, limit=limit)
        if raw:
            data_source = "rss_news"
    if not raw:
        raw = _sample_search_results(topic)
        data_source = "offline_stub"

    parsed = [collector.parse_search_result(r) for r in raw]
    posts = _posts_from_results(parsed)
    platform_stats = collector.aggregate_platform_stats(posts)
    texts = [p["content"] for p in posts]
    heat = collector.calculate_heat_index(texts)

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

    keyword_pairs = extract_keywords([f"{p['title']} {p['content']}" for p in posts], top_k=12)
    keywords = [w for w, _cnt in keyword_pairs if w and w != topic][:10] or [topic]

    sentiment_dicts = [
        {
            "label": p["sentiment"],
            "confidence": p["confidence"],
            "negative_score": p["negative_score"],
        }
        for p in posts
    ]
    risks = identify_risk_points(texts, sentiment_dicts)

    trend_counter: Dict[str, int] = {}
    for p in posts:
        day = (p.get("timestamp") or "")[:10]
        if day:
            trend_counter[day] = trend_counter.get(day, 0) + 1
    trend_data = [{"date": d, "count": c} for d, c in sorted(trend_counter.items())]

    return {
        "parsed": parsed,
        "posts": posts,
        "platform_stats": platform_stats,
        "heat": heat,
        "sentiment": sentiment,
        "keywords": keywords,
        "risks": risks,
        "trend_data": trend_data,
        "data_source": data_source,
    }


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------

def _sentiment_badge(label: str) -> str:
    color = {"positive": "#64ffda", "negative": "#ff6b6b", "neutral": "#8892b0"}.get(label, "#8892b0")
    return (
        f'<span style="color:{color};border:1px solid {color};border-radius:4px;'
        f'padding:1px 6px;font-size:0.75rem;">{SENTIMENT_ZH.get(label, label)}</span>'
    )


def render_html(topic: str, data: Dict[str, Any], template_name: str) -> str:
    sentiment = data.get("sentiment") or {}
    heat = data.get("heat") or {}
    platforms = data.get("platform_stats") or {}
    keywords = data.get("keywords") or []
    risks = data.get("risks") or []
    posts = data.get("posts") or []
    trend = data.get("trend_data") or []
    findings = (data.get("query_summary") or {}).get("key_findings") or []
    generated = data.get("analysis_time") or datetime.now().strftime("%Y-%m-%d %H:%M")
    data_source = data.get("data_source") or "unknown"
    source_label = "实时 RSS 抓取" if data_source == "rss_news" else "离线示例数据"

    # --- sentiment stacked bar ---
    pos, neg, neu = (
        sentiment.get("positive", 0),
        sentiment.get("negative", 0),
        sentiment.get("neutral", 0),
    )
    total = max(pos + neg + neu, 1)
    pos_pct, neg_pct = round(pos / total * 100), round(neg / total * 100)
    neu_pct = 100 - pos_pct - neg_pct
    sentiment_bar = (
        '<div style="display:flex;height:14px;border-radius:7px;overflow:hidden;margin:10px 0;">'
        f'<div style="width:{pos_pct}%;background:#64ffda;"></div>'
        f'<div style="width:{neu_pct}%;background:#8892b0;"></div>'
        f'<div style="width:{neg_pct}%;background:#ff6b6b;"></div>'
        "</div>"
    )

    # --- keyword chips ---
    chips = "".join(
        f'<span class="chip">{_escape(k)}</span>' for k in keywords[:10]
    ) or '<span class="chip">暂无关键词</span>'

    # --- platform table ---
    rows = []
    for name, stats in list(platforms.items())[:8]:
        rows.append(
            "<tr>"
            f"<td>{_escape(name)}</td>"
            f"<td>{_escape(stats.get('count', 0))}</td>"
            f"<td>{_escape(stats.get('percentage', 0))}%</td>"
            "</tr>"
        )
    table_body = "\n".join(rows) or "<tr><td colspan='3'>暂无平台数据</td></tr>"

    # --- trend mini bars ---
    trend_html = ""
    if len(trend) > 1:
        max_count = max(t["count"] for t in trend) or 1
        bars = "".join(
            '<div style="display:flex;align-items:center;gap:8px;margin:4px 0;">'
            f'<span style="color:#8892b0;font-size:0.8rem;min-width:82px;">{_escape(t["date"])}</span>'
            f'<div style="height:10px;border-radius:5px;background:#64ffda;'
            f'width:{max(int(t["count"] / max_count * 100), 6)}%;max-width:70%;"></div>'
            f'<span style="font-size:0.8rem;">{t["count"]}</span>'
            "</div>"
            for t in trend[-7:]
        )
        trend_html = f'<div class="card"><h2>📈 声量趋势</h2>{bars}</div>'

    # --- risks ---
    if risks:
        risk_items = "".join(
            "<li>"
            f'<span style="color:{RISK_COLOR.get(r.get("risk_level"), "#8892b0")};font-weight:bold;">'
            f'[{RISK_ZH.get(r.get("risk_level"), r.get("risk_level"))}]</span> '
            f'{_escape(r.get("text_preview", ""))}'
            + (
                f' <span class="meta">（命中：{_escape("、".join(r.get("matched_keywords", [])))}）</span>'
                if r.get("matched_keywords")
                else ""
            )
            + "</li>"
            for r in risks[:5]
        )
        risk_html = f"<ul>{risk_items}</ul>"
    else:
        risk_html = '<p class="meta">未识别到明显风险信号。</p>'

    # --- findings ---
    finding_items = "".join(f"<li>{_escape(f)}</li>" for f in findings[:6]) or "<li>暂无条目</li>"

    # --- linked sources ---
    source_items = []
    for p in posts[:10]:
        title = p.get("title") or p.get("content") or ""
        url = p.get("url") or "#"
        source_items.append(
            '<li style="margin:8px 0;">'
            f'<a href="{_escape(url)}" target="_blank">{_escape(title[:70])}</a><br>'
            f'<span class="meta">{_escape(p.get("nickname", ""))} · {_escape(p.get("timestamp", ""))}</span> '
            f'{_sentiment_badge(p.get("sentiment", "neutral"))}'
            "</li>"
        )
    sources_html = "".join(source_items) or "<li>暂无信源</li>"

    return f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>{_escape(topic)} · 微信舆情简报</title>
  <style>
    body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
           margin: 0; padding: 24px; background: #0a192f; color: #e6f1ff; }}
    h1 {{ color: #ffd700; font-size: 1.6rem; margin-bottom: 4px; }}
    h2 {{ font-size: 1.05rem; margin: 0 0 8px; }}
    .meta {{ color: #8892b0; font-size: 0.85rem; }}
    .badge {{ display:inline-block; border:1px solid #64ffda; color:#64ffda;
              border-radius:4px; padding:1px 8px; font-size:0.78rem; margin-left:6px; }}
    .card {{ background: #112240; border: 1px solid #233554; border-radius: 8px;
             padding: 16px; margin: 16px 0; }}
    .chip {{ display:inline-block; background:#233554; color:#64ffda; border-radius:12px;
             padding:3px 10px; margin:3px 4px 3px 0; font-size:0.82rem; }}
    table {{ width: 100%; border-collapse: collapse; }}
    th, td {{ text-align: left; padding: 8px; border-bottom: 1px solid #233554; }}
    ul {{ padding-left: 20px; margin: 6px 0; }}
    li {{ margin: 5px 0; line-height: 1.5; }}
    a {{ color: #64ffda; text-decoration: none; }}
    .grid {{ display:flex; gap:16px; flex-wrap:wrap; }}
    .grid .card {{ flex:1; min-width:240px; }}
  </style>
</head>
<body>
  <h1>{_escape(topic)}</h1>
  <p class="meta">模板：{_escape(template_name)} · 生成时间：{_escape(generated)}
     <span class="badge">数据源：{_escape(source_label)} · 样本 {len(posts)} 条</span></p>

  <div class="grid">
    <div class="card">
      <h2>🔥 热度</h2>
      <p>指数 <strong>{_escape(heat.get('heat_score', '—'))}</strong>（{_escape(heat.get('heat_level', '—'))}），
         提及 {_escape(heat.get('total_mentions', 0))} 条。</p>
    </div>
    <div class="card">
      <h2>💬 情感</h2>
      {sentiment_bar}
      <p class="meta">正面 {pos}（{pos_pct}%） · 中性 {neu}（{neu_pct}%） · 负面 {neg}（{neg_pct}%）
         · 主导：{_escape(SENTIMENT_ZH.get(sentiment.get('dominant', 'neutral'), '中性'))}</p>
    </div>
  </div>

  <div class="card">
    <h2>🏷️ 热门关键词</h2>
    {chips}
  </div>

  <div class="card">
    <h2>📊 渠道分布</h2>
    <table>
      <thead><tr><th>平台/来源</th><th>条数</th><th>占比</th></tr></thead>
      <tbody>{table_body}</tbody>
    </table>
  </div>

  {trend_html}

  <div class="card">
    <h2>⚠️ 风险提示</h2>
    {risk_html}
  </div>

  <div class="card">
    <h2>📌 要点</h2>
    <ul>{finding_items}</ul>
  </div>

  <div class="card">
    <h2>🔗 信源列表</h2>
    <ul style="list-style:none;padding-left:0;">{sources_html}</ul>
  </div>

  <p class="meta" style="text-align:center;margin-top:24px;">
    由 BettaFish-skill 自动生成 · QueryAgent + MediaAgent + InsightAgent
  </p>
</body>
</html>
"""


# ---------------------------------------------------------------------------
# Push & CLI
# ---------------------------------------------------------------------------

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


def generate(topic: str, offline: Optional[bool] = None, limit: int = DEFAULT_LIMIT) -> Dict[str, Any]:
    if offline is None:
        offline = _offline_mode_requested()
    template_name, _path = select_report_template(topic)
    insight = build_insight(topic, offline=offline, limit=limit)
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
    report["posts"] = insight["posts"]
    report["keywords"] = insight["keywords"]
    report["risks"] = insight["risks"]
    report["trend_data"] = insight["trend_data"]
    report["data_source"] = insight["data_source"]
    html_doc = render_html(topic, report, template_name)
    return {
        "html": html_doc,
        "report": report,
        "template": template_name,
        "data_source": insight["data_source"],
    }


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate WeChat push HTML via library tools.")
    parser.add_argument("--topic", default=os.environ.get("TOPIC", "AI 行业本周舆情"))
    parser.add_argument("--output", default="dist/wechat_push.html")
    parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT, help="Max fetched items.")
    parser.add_argument(
        "--offline",
        action="store_true",
        help="Skip network fetching and use offline stubs (also: WECHAT_PUSH_OFFLINE=1).",
    )
    parser.add_argument("--push", action="store_true", help="Send via PushPlus (PUSHPLUS_TOKEN).")
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    offline = args.offline or _offline_mode_requested()
    result = generate(args.topic, offline=offline, limit=args.limit)
    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(result["html"], encoding="utf-8")
    print(
        f"Wrote {out} (template={result['template']}, "
        f"data_source={result['data_source']})"
    )

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

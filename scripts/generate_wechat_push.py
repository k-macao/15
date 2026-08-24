#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Generate a WeChat-ready HTML briefing and optionally send it via PushPlus.

Data pipeline
-------------
1. Fetch real news items about the topic from a bilingual RSS/Atom roster
   (50 English + 20 Chinese publisher feeds, plus Google News / Bing News,
   no API key required).
2. Run the library analyzers: platform stats, heat index, sentiment
   distribution, keyword extraction, and risk identification.
3. Render a vertical long-form HTML briefing in Guizang PPT Skill "Style A"
   (电子杂志 × 电子墨水, WeChat-reading friendly: coverage / sentiment /
   keywords / channels / trend / risks / findings / linked source archive).

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

import hashlib
import argparse
import html
import json
import os
import re
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
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
DEFAULT_ENGLISH_LIMIT = 50
DEFAULT_CHINESE_LIMIT = 20
DEFAULT_LIMIT = DEFAULT_ENGLISH_LIMIT + DEFAULT_CHINESE_LIMIT
RSS_FETCH_WORKERS = 12

# A broad source roster keeps the briefing from reflecting a single publisher's
# editorial bias.  Entries are real public RSS/Atom endpoints; a failed or
# retired feed is skipped without taking down the whole report.  The language
# quotas are deliberately explicit because the push page promises a bilingual
# source sample (50 English + 20 Chinese items by default).
ENGLISH_RSS_FEEDS = [
    ("BBC News / World", "https://feeds.bbci.co.uk/news/world/rss.xml"),
    ("BBC News / Business", "https://feeds.bbci.co.uk/news/business/rss.xml"),
    ("BBC News / Technology", "https://feeds.bbci.co.uk/news/technology/rss.xml"),
    ("BBC News / Science", "https://feeds.bbci.co.uk/news/science_and_environment/rss.xml"),
    ("BBC News / Health", "https://feeds.bbci.co.uk/news/health/rss.xml"),
    ("BBC News / UK", "https://feeds.bbci.co.uk/news/uk/rss.xml"),
    ("NPR / News", "https://feeds.npr.org/1001/rss.xml"),
    ("NPR / Business", "https://feeds.npr.org/1006/rss.xml"),
    ("NPR / Technology", "https://feeds.npr.org/1019/rss.xml"),
    ("NPR / Science", "https://feeds.npr.org/1007/rss.xml"),
    ("NPR / Politics", "https://feeds.npr.org/1014/rss.xml"),
    ("The Guardian / World", "https://www.theguardian.com/world/rss"),
    ("The Guardian / Business", "https://www.theguardian.com/business/rss"),
    ("The Guardian / Technology", "https://www.theguardian.com/technology/rss"),
    ("The Guardian / Science", "https://www.theguardian.com/science/rss"),
    ("The New York Times / World", "https://rss.nytimes.com/services/xml/rss/nyt/World.xml"),
    ("The New York Times / Business", "https://rss.nytimes.com/services/xml/rss/nyt/Business.xml"),
    ("The New York Times / Technology", "https://rss.nytimes.com/services/xml/rss/nyt/Technology.xml"),
    ("The New York Times / Science", "https://rss.nytimes.com/services/xml/rss/nyt/Science.xml"),
    ("The New York Times / U.S.", "https://rss.nytimes.com/services/xml/rss/nyt/US.xml"),
    ("CNN / Top Stories", "http://rss.cnn.com/rss/edition.rss"),
    ("CNN / World", "http://rss.cnn.com/rss/edition_world.rss"),
    ("CNN / Business", "http://rss.cnn.com/rss/money_latest.rss"),
    ("CNN / Technology", "http://rss.cnn.com/rss/edition_technology.rss"),
    ("AP News / Top Stories", "https://feeds.apnews.com/apnews/topnews"),
    ("AP News / Business", "https://feeds.apnews.com/apnews/business"),
    ("Al Jazeera / All", "https://www.aljazeera.com/xml/rss/all.xml"),
    ("The Atlantic", "https://www.theatlantic.com/feed/all/"),
    ("DW / All", "https://rss.dw.com/rdf/rss-en-all"),
    ("DW / Business", "https://rss.dw.com/rdf/rss-en-bus"),
    ("DW / Science", "https://rss.dw.com/rdf/rss-en-sci"),
    ("France 24 / English", "https://www.france24.com/en/rss"),
    ("Euronews / News", "https://www.euronews.com/rss?level=theme&name=news"),
    ("CNBC / Top News", "https://www.cnbc.com/id/100003114/device/rss/rss.html"),
    ("CNBC / Technology", "https://www.cnbc.com/id/19854910/device/rss/rss.html"),
    ("MarketWatch / Top Stories", "https://feeds.marketwatch.com/marketwatch/topstories/"),
    ("TechCrunch", "https://techcrunch.com/feed/"),
    ("The Verge", "https://www.theverge.com/rss/index.xml"),
    ("Ars Technica", "https://feeds.arstechnica.com/arstechnica/index"),
    ("WIRED", "https://www.wired.com/feed/rss"),
    ("Engadget", "https://www.engadget.com/rss.xml"),
    ("CNET / News", "https://www.cnet.com/rss/news/"),
    ("MIT Technology Review", "https://www.technologyreview.com/feed/"),
    ("Nature", "https://www.nature.com/nature.rss"),
    ("ScienceDaily", "https://www.sciencedaily.com/rss/top/sciencedaily.xml"),
    ("Phys.org", "https://phys.org/rss-feed/"),
    ("NASA / Breaking News", "https://www.nasa.gov/rss/dyn/breaking_news.rss"),
    ("Space.com", "https://www.space.com/feeds/all"),
    ("The Conversation", "https://theconversation.com/us/articles.atom"),
    ("Popular Mechanics", "https://www.popularmechanics.com/rss/all.xml"),
]

CHINESE_RSS_FEEDS = [
    ("中国政府网", "https://www.gov.cn/rss/gov.xml"),
    ("新华网", "http://www.news.cn/rss/news.xml"),
    ("人民网 / 时政", "http://www.people.com.cn/rss/politics.xml"),
    ("中国新闻网 / 滚动", "https://www.chinanews.com.cn/rss/scroll-news.xml"),
    ("环球网", "https://www.huanqiu.com/rss.xml"),
    ("央视网", "https://news.cctv.com/rss/"),
    ("中国经济网", "http://www.ce.cn/rss/index.xml"),
    ("澎湃新闻", "https://www.thepaper.cn/rss_news.jsp"),
    ("36氪", "https://36kr.com/feed"),
    ("虎嗅", "https://www.huxiu.com/rss/0.xml"),
    ("爱范儿", "https://www.ifanr.com/feed"),
    ("少数派", "https://sspai.com/feed"),
    ("IT之家", "https://www.ithome.com/rss/"),
    ("Solidot", "https://www.solidot.org/index.rss"),
    ("联合早报", "https://www.zaobao.com.sg/rss/realtime"),
    ("新浪新闻", "https://rss.sina.com.cn/news/marquee/ddt.xml"),
    ("凤凰网", "https://i.ifeng.com/rss/news.xml"),
    ("观察者网", "https://www.guancha.cn/rss"),
    ("南方周末", "https://www.infzm.com/rss"),
    ("财联社", "https://www.cls.cn/rss"),
]

# Query feeds are retained as high-relevance candidates and the publisher
# roster fills the bilingual quota with broader context when the search feed
# does not return enough items.
QUERY_FEEDS = (
    ("Google News / English query", "https://news.google.com/rss/search?q={query}&hl=en-US&gl=US&ceid=US:en", "en"),
    ("Google News / 中文检索", "https://news.google.com/rss/search?q={query}&hl=zh-CN&gl=CN&ceid=CN:zh-Hans", "zh"),
    ("Bing News / English query", "https://www.bing.com/news/search?q={query}&format=rss&setlang=en-US", "en"),
    ("Bing News / 中文检索", "https://www.bing.com/news/search?q={query}&format=rss&setlang=zh-CN", "zh"),
)

LAST_FETCH_STATS: Dict[str, Any] = {}

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


def _local_name(tag: Any) -> str:
    """Return an XML tag name without a namespace prefix."""
    return str(tag).rsplit("}", 1)[-1].lower()


def _child_text(element: ET.Element, *names: str) -> str:
    wanted = {name.lower() for name in names}
    for child in list(element):
        if _local_name(child.tag) in wanted:
            return "".join(child.itertext()).strip()
    return ""


def _child_link(element: ET.Element) -> str:
    for child in list(element):
        if _local_name(child.tag) != "link":
            continue
        href = (child.attrib.get("href") or "").strip()
        if href:
            return href
        text = "".join(child.itertext()).strip()
        if text:
            return text
    return ""


def _normalise_feed_date(value: str) -> str:
    value = (value or "").strip()
    if not value:
        return ""
    try:
        return parsedate_to_datetime(value).strftime("%Y-%m-%d")
    except (TypeError, ValueError, OverflowError):
        pass
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).strftime("%Y-%m-%d")
    except (TypeError, ValueError, OverflowError):
        return ""


def _parse_feed_document(xml_bytes: bytes, default_source: str = "news") -> List[Dict[str, str]]:
    """Parse RSS 2.0 and Atom documents into search-result-shaped dicts."""
    try:
        root = ET.fromstring(xml_bytes)
    except ET.ParseError:
        return []

    item_tag = "entry" if _local_name(root.tag) == "feed" else "item"
    results: List[Dict[str, str]] = []
    for item in (node for node in root.iter() if _local_name(node.tag) == item_tag):
        title = _strip_tags(_child_text(item, "title"))
        link = _child_link(item)
        snippet = _strip_tags(_child_text(item, "description", "summary", "content"))
        source = _strip_tags(_child_text(item, "source", "author")) or default_source
        date = _normalise_feed_date(_child_text(item, "pubDate", "published", "updated"))
        if not title or not link:
            continue
        # Google News often duplicates the title inside description.
        if snippet == title or not snippet:
            snippet = title
        results.append(
            {
                "title": title,
                "snippet": snippet[:500],
                "url": link,
                "source": source,
                "date": date or datetime.now(timezone.utc).strftime("%Y-%m-%d"),
            }
        )
    return results


def _parse_rss(xml_bytes: bytes, default_source: str = "news") -> List[Dict[str, str]]:
    """Backward-compatible RSS parser, now also accepting Atom feeds."""
    return _parse_feed_document(xml_bytes, default_source=default_source)


def _feed_result(
    feed_name: str,
    feed_url: str,
    language: str,
    xml_bytes: bytes,
) -> Dict[str, Any]:
    items = _parse_feed_document(xml_bytes, default_source=feed_name)
    for item in items:
        item.update({"feed_name": feed_name, "feed_url": feed_url, "language": language})
    return {"name": feed_name, "url": feed_url, "language": language, "items": items, "error": ""}


def _fetch_one_feed(feed_name: str, feed_url: str, language: str) -> Dict[str, Any]:
    try:
        return _feed_result(feed_name, feed_url, language, _http_get(feed_url))
    except (urllib.error.URLError, OSError, ValueError, ET.ParseError) as exc:
        return {"name": feed_name, "url": feed_url, "language": language, "items": [], "error": str(exc)}


def _topic_relevance(item: Dict[str, str], topic: str) -> int:
    """Rank search-feed items before broad publisher-feed context."""
    haystack = f'{item.get("title", "")} {item.get("snippet", "")}'.lower()
    terms = re.findall(r"[a-z0-9]{3,}|[\u4e00-\u9fff]{2,}", topic.lower())
    return sum(1 for term in set(terms) if term in haystack)


def _language_targets(limit: int) -> Dict[str, int]:
    requested = max(int(limit or 0), 0)
    if requested == 0:
        return {"en": 0, "zh": 0}
    english = min(DEFAULT_ENGLISH_LIMIT, round(requested * DEFAULT_ENGLISH_LIMIT / DEFAULT_LIMIT))
    english = max(0, english)
    chinese = min(DEFAULT_CHINESE_LIMIT, requested - english)
    if english + chinese < requested:
        english = min(DEFAULT_ENGLISH_LIMIT, english + requested - english - chinese)
    return {"en": english, "zh": chinese}


def fetch_search_results(topic: str, limit: int = DEFAULT_LIMIT) -> List[Dict[str, str]]:
    """Fetch a bilingual RSS/Atom sample, defaulting to 50 English + 20 Chinese.

    Four query feeds provide high-relevance results.  The 50 English and 20
    Chinese publisher feeds then add source diversity and background context.
    All feeds are fetched concurrently; one unavailable publisher never blocks
    the remaining sources.  The returned list remains compatible with the
    original search-result contract.
    """
    global LAST_FETCH_STATS
    targets = _language_targets(limit)
    query = urllib.parse.quote(topic)
    feed_jobs = [
        (name, template.format(query=query), language)
        for name, template, language in QUERY_FEEDS
    ]
    feed_jobs.extend((name, url, "en") for name, url in ENGLISH_RSS_FEEDS)
    feed_jobs.extend((name, url, "zh") for name, url in CHINESE_RSS_FEEDS)

    completed: List[Dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=RSS_FETCH_WORKERS) as executor:
        futures = [executor.submit(_fetch_one_feed, *job) for job in feed_jobs]
        for future in as_completed(futures):
            completed.append(future.result())

    query_names = {name for name, _template, _language in QUERY_FEEDS}
    LAST_FETCH_STATS = {
        "configured_english_feeds": len(ENGLISH_RSS_FEEDS),
        "configured_chinese_feeds": len(CHINESE_RSS_FEEDS),
        "configured_total_feeds": len(ENGLISH_RSS_FEEDS) + len(CHINESE_RSS_FEEDS),
        "successful_english_feeds": sum(1 for feed in completed if feed["language"] == "en" and feed["name"] not in query_names and not feed["error"]),
        "successful_chinese_feeds": sum(1 for feed in completed if feed["language"] == "zh" and feed["name"] not in query_names and not feed["error"]),
        "english_items": 0,
        "chinese_items": 0,
        "requested_english_items": targets["en"],
        "requested_chinese_items": targets["zh"],
    }

    # Query feeds are more relevant than broad publisher feeds.  Preserve that
    # priority while using relevance and source order to make output stable.
    candidates: Dict[str, List[Dict[str, str]]] = {"en": [], "zh": []}
    seen_titles: set[str] = set()
    for feed in sorted(completed, key=lambda value: value["name"] not in query_names):
        for item in feed["items"]:
            key = re.sub(r"\W+", "", item["title"].lower())[:100]
            if not key or key in seen_titles:
                continue
            seen_titles.add(key)
            item["relevance"] = _topic_relevance(item, topic)
            candidates[feed["language"]].append(item)

    selected: List[Dict[str, str]] = []
    for language in ("en", "zh"):
        language_items = sorted(
            candidates[language],
            key=lambda item: (-int(item.get("relevance", 0)), item.get("date", "")),
        )
        chosen = language_items[:targets[language]]
        LAST_FETCH_STATS[f"{language}_items"] = len(chosen)
        selected.extend(chosen)
    return selected


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
                "feed_name": item.get("feed_name") or item.get("source") or "source",
                "feed_url": item.get("feed_url") or "",
                "language": item.get("language") or "unknown",
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

    parsed = []
    for result in raw:
        parsed_result = collector.parse_search_result(result)
        for field in ("feed_name", "feed_url", "language", "relevance"):
            if field in result:
                parsed_result[field] = result[field]
        parsed.append(parsed_result)
    posts = _posts_from_results(parsed)
    coverage = dict(LAST_FETCH_STATS) if data_source == "rss_news" else {
        "configured_english_feeds": len(ENGLISH_RSS_FEEDS),
        "configured_chinese_feeds": len(CHINESE_RSS_FEEDS),
        "configured_total_feeds": len(ENGLISH_RSS_FEEDS) + len(CHINESE_RSS_FEEDS),
        "successful_english_feeds": 0,
        "successful_chinese_feeds": 0,
        "english_items": sum(1 for item in raw if item.get("language") == "en"),
        "chinese_items": sum(1 for item in raw if item.get("language") == "zh"),
        "requested_english_items": min(DEFAULT_ENGLISH_LIMIT, limit),
        "requested_chinese_items": min(DEFAULT_CHINESE_LIMIT, limit),
    }
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
        "rss_coverage": coverage,
    }


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------

def _sentiment_badge(label: str) -> str:
    color = {"positive": "#3c7a00", "negative": "#0c0c0d", "neutral": "#8a8a83"}.get(label, "#8a8a83")
    return (
        f'<span class="meta-badge" style="color:{color};">'
        f'{SENTIMENT_ZH.get(label, label)}</span>'
    )


def render_html(topic: str, data: Dict[str, Any], template_name: str) -> str:
    """Render a vertical long-form editorial briefing (Guizang Style A).

    电子杂志 × 电子墨水: light-gray paper background, black body text, neon-green
    on black for headings and emphasis, hairline rules, small type. The page is
    a single scrolling column sized for WeChat reading — self-contained CSS,
    no JavaScript, system font stacks only.
    """
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
    coverage = data.get("rss_coverage") or {}
    configured_english = int(coverage.get("configured_english_feeds", len(ENGLISH_RSS_FEEDS)) or 0)
    configured_chinese = int(coverage.get("configured_chinese_feeds", len(CHINESE_RSS_FEEDS)) or 0)
    successful_english = int(coverage.get("successful_english_feeds", 0) or 0)
    successful_chinese = int(coverage.get("successful_chinese_feeds", 0) or 0)
    english_items = int(coverage.get("english_items", 0) or 0)
    chinese_items = int(coverage.get("chinese_items", 0) or 0)
    configured_total = configured_english + configured_chinese
    successful_total = successful_english + successful_chinese
    coverage_note = (
        f"本轮配置 {configured_english} 个英文与 {configured_chinese} 个中文 RSS/Atom 源，"
        f"成功连通 {successful_total} 个，最终纳入 {english_items} 条英文、{chinese_items} 条中文内容。"
    )

    # --- shared values ---
    pos = int(sentiment.get("positive", 0) or 0)
    neg = int(sentiment.get("negative", 0) or 0)
    neu = int(sentiment.get("neutral", 0) or 0)
    total = max(pos + neg + neu, 1)
    pos_pct = round(pos / total * 100)
    neg_pct = round(neg / total * 100)
    neu_pct = 100 - pos_pct - neg_pct
    pos_deg = round(pos / total * 360, 2)
    neu_end_deg = round((pos + neu) / total * 360, 2)
    dominant = SENTIMENT_ZH.get(sentiment.get("dominant", "neutral"), "中性")
    risk_count = len(risks)
    mention_count = int(heat.get("total_mentions", len(posts)) or 0)

    # --- channel bars and table ---
    platform_items = list(platforms.items())[:8]
    max_platform_count = max(
        (int(stats.get("count", 0) or 0) for _name, stats in platform_items),
        default=1,
    )
    channel_bars = "".join(
        f'''<div class="channel-row">
          <div class="channel-label"><span>{_escape(name)}</span><b>{_escape(stats.get("count", 0))}</b></div>
          <div class="channel-track"><i style="width:{max(int(stats.get("count", 0) or 0) / max_platform_count * 100, 8)}%"></i></div>
          <span class="channel-share">{_escape(stats.get("percentage", 0))}%</span>
        </div>'''
        for name, stats in platform_items
    ) or '<p class="muted">暂无平台数据</p>'
    channel_rows = "".join(
        f'''<tr><td>{_escape(name)}</td><td>{_escape(stats.get("count", 0))}</td>
        <td>{_escape(stats.get("percentage", 0))}%</td>
        <td>{_escape(stats.get("avg_engagement", 0))}</td></tr>'''
        for name, stats in platform_items
    ) or '<tr><td colspan="4">暂无平台数据</td></tr>'

    # --- trend bars ---
    trend_items = trend[-7:]
    max_trend_count = max((int(item.get("count", 0) or 0) for item in trend_items), default=1)
    trend_bars = "".join(
        f'''<div class="trend-row">
          <time>{_escape(item.get("date", ""))}</time>
          <div class="trend-track"><i style="width:{max(int(item.get("count", 0) or 0) / max_trend_count * 100, 7)}%"></i></div>
          <b>{_escape(item.get("count", 0))}</b>
        </div>'''
        for item in trend_items
    ) or '<p class="muted">当前样本暂无可用趋势数据。</p>'

    # --- keyword chips ---
    chips = "".join(
        f'<span class="keyword">{_escape(keyword)}</span>' for keyword in keywords[:10]
    ) or '<span class="muted">暂无关键词</span>'

    # --- risks ---
    if risks:
        risk_items = "".join(
            f'''<li class="risk-item risk-{_escape(risk.get("risk_level", "low"))}">
              <span class="risk-level">{_escape(RISK_ZH.get(risk.get("risk_level"), risk.get("risk_level", "低")))}</span>
              <div><strong>{_escape(risk.get("text_preview", ""))}</strong>
              {f'<small>命中：{_escape("、".join(risk.get("matched_keywords", [])))}</small>' if risk.get("matched_keywords") else ""}</div>
            </li>'''
            for risk in risks[:5]
        )
    else:
        risk_items = '<li class="empty-state">未识别到明显风险信号。</li>'

    # --- findings ---
    finding_items = "".join(
        f'<li><span>{index:02d}</span><p>{_escape(finding)}</p></li>'
        for index, finding in enumerate(findings[:6], 1)
    ) or '<li class="empty-state">暂无要点</li>'

    # --- linked sources ---
    def render_source_items(source_posts: List[Dict], start_index: int = 1) -> str:
        return "".join(
            f'''<li class="source-item">
              <span class="source-index">{index:02d}</span>
              <div><a href="{_escape(post.get("url") or "#")}" target="_blank" rel="noopener">{_escape((post.get("title") or post.get("content") or "")[:110])}</a>
              <small>{_escape("EN" if post.get("language") == "en" else "中文" if post.get("language") == "zh" else "来源")} <em>·</em> {_escape(post.get("feed_name") or post.get("nickname", ""))} <em>·</em> {_escape(post.get("timestamp", ""))}</small></div>
              {_sentiment_badge(post.get("sentiment", "neutral"))}
            </li>'''
            for index, post in enumerate(source_posts, start_index)
        )

    source_items = render_source_items(posts[:12]) or '<li class="empty-state">暂无信源</li>'
    archive_items = render_source_items(posts[12:70], 13)
    source_archive = (
        f'<details class="source-archive"><summary>展开其余 {len(posts[12:70])} 条信源</summary><ul class="source-list">{archive_items}</ul></details>'
        if archive_items
        else ""
    )

    css = r'''
    :root {
      --paper: #e9e9e5;        /* 整体浅灰纸底 */
      --paper-tint: #dfdfda;
      --ink: #0c0c0d;          /* 正文黑 */
      --ink-rgb: 12, 12, 13;
      --neon: #b8ff2e;         /* 荧光绿 */
      --neon-soft: rgba(184, 255, 46, .32);
      --mist: #8a8a83;
      --serif: 'Noto Serif SC', 'Songti SC', 'SimSun', Georgia, serif;
      --sans: -apple-system, BlinkMacSystemFont, 'PingFang SC', 'Noto Sans SC', 'Microsoft YaHei', sans-serif;
      --mono: 'JetBrains Mono', 'SFMono-Regular', Menlo, Consolas, monospace;
    }
    * { box-sizing: border-box; }
    html { background: var(--paper); }
    body { min-width: 300px; margin: 0; color: var(--ink); background: var(--paper);
      background-image: radial-gradient(rgba(var(--ink-rgb), .045) 1px, transparent 1px); background-size: 22px 22px;
      font-family: var(--sans); font-size: 13.5px; line-height: 1.8; -webkit-font-smoothing: antialiased; }
    a { color: var(--ink); text-decoration: none; }
    h1, h2, h3, p { margin-top: 0; }
    .muted { color: var(--mist); }
    .sheet { max-width: 640px; margin: 0 auto; padding: 34px 20px 46px; }

    /* --- masthead --- */
    .masthead { border-top: 3px solid var(--ink); padding-top: 8px; }
    .masthead-rule { height: 1px; background: var(--ink); margin-bottom: 22px; }
    .kicker { display: flex; justify-content: space-between; gap: 10px; margin-bottom: 20px;
      font-family: var(--mono); font-size: 9px; letter-spacing: .28em; text-transform: uppercase; color: var(--ink); opacity: .72; }
    .masthead h1 { display: inline-block; margin: 0 0 12px; padding: 8px 16px 9px; background: var(--ink); color: var(--neon);
      font-family: var(--serif); font-size: 27px; font-weight: 700; line-height: 1.25; letter-spacing: .01em; }
    .subtitle { margin: 4px 0 0; font-family: var(--serif); font-size: 14.5px; line-height: 1.75; color: var(--ink); }
    .subtitle em { font-style: normal; border-bottom: 2px solid var(--neon); }
    .meta-line { display: flex; flex-wrap: wrap; gap: 6px 16px; margin-top: 18px; padding: 10px 0;
      border-top: 1px solid var(--ink); border-bottom: 1px solid var(--ink);
      font-family: var(--mono); font-size: 9.5px; letter-spacing: .1em; color: var(--ink); }
    .meta-line b { font-weight: 700; background: var(--ink); color: var(--neon); padding: 0 5px; }

    /* --- section head --- */
    .sec { margin-top: 40px; }
    .sec-head { display: flex; align-items: baseline; gap: 10px; margin-bottom: 16px; border-bottom: 1px solid var(--ink); padding-bottom: 10px; }
    .sec-head .no { font-family: var(--mono); font-size: 10px; letter-spacing: .12em; color: var(--ink); opacity: .55; }
    .sec-head h2 { display: inline-block; margin: 0; padding: 3px 10px 4px; background: var(--ink); color: var(--neon);
      font-family: var(--serif); font-size: 15.5px; font-weight: 700; letter-spacing: .04em; }
    .sec-head .en { margin-left: auto; font-family: var(--mono); font-size: 9px; letter-spacing: .2em; text-transform: uppercase; color: var(--mist); }

    /* --- editor note --- */
    .note-text { margin: 0; font-family: var(--serif); font-size: 14px; line-height: 1.95; color: var(--ink); }
    .note-text strong, .hl { font-weight: 600; background: var(--ink); color: var(--neon); padding: 1px 6px 2px; margin: 0 1px; }

    /* --- stats --- */
    .stat-hero { display: flex; align-items: baseline; gap: 14px; padding: 16px 0 6px; }
    .stat-hero .n { font-family: var(--serif); font-size: 54px; font-weight: 800; line-height: .9; letter-spacing: -.02em; font-feature-settings: 'tnum'; }
    .stat-hero .l { font-family: var(--mono); font-size: 9.5px; letter-spacing: .18em; text-transform: uppercase; color: var(--mist); line-height: 1.9; }
    .stat-hero .l i { display: block; font-style: normal; color: var(--ink); }
    .stat-grid { display: grid; grid-template-columns: repeat(2, 1fr); border-top: 1px solid var(--ink); border-left: 1px solid var(--ink); margin-top: 14px; }
    .stat-cell { padding: 12px 14px; border-right: 1px solid var(--ink); border-bottom: 1px solid var(--ink); }
    .stat-cell .n { font-family: var(--serif); font-size: 24px; font-weight: 700; line-height: 1.05; font-feature-settings: 'tnum'; }
    .stat-cell .n.neon { display: inline-block; padding: 0 7px; background: var(--ink); color: var(--neon); }
    .stat-cell .l { margin-top: 4px; font-family: var(--mono); font-size: 8.5px; letter-spacing: .14em; text-transform: uppercase; color: var(--mist); }
    .cov-grid { display: grid; grid-template-columns: repeat(4, 1fr); border-top: 1px solid var(--ink); border-left: 1px solid var(--ink); }
    .cov-cell { padding: 10px 10px 9px; border-right: 1px solid var(--ink); border-bottom: 1px solid var(--ink); }
    .cov-cell b { display: block; font-family: var(--serif); font-size: 20px; font-weight: 700; line-height: 1; }
    .cov-cell span { display: block; margin-top: 5px; font-family: var(--mono); font-size: 8px; letter-spacing: .06em; text-transform: uppercase; color: var(--mist); }
    .cov-note { margin: 10px 0 0; font-size: 12px; color: var(--mist); }

    /* --- sentiment --- */
    .sentiment-layout { display: grid; grid-template-columns: 120px 1fr; align-items: center; gap: 20px; }
    .donut { width: 120px; height: 120px; display: grid; place-items: center; border-radius: 50%;
      background: conic-gradient(var(--neon) 0deg var(--pos-deg), #c8c8c0 var(--pos-deg) var(--neu-deg), var(--ink) var(--neu-deg) 360deg);
      box-shadow: 0 0 0 1px var(--ink); }
    .donut::after { content: ''; width: 76px; height: 76px; border-radius: 50%; background: var(--paper); box-shadow: 0 0 0 1px var(--ink); }
    .legend { display: grid; gap: 8px; }
    .legend-row { display: flex; align-items: center; justify-content: space-between; gap: 10px; font-size: 12.5px; color: var(--ink); }
    .legend-row b { font-family: var(--mono); font-size: 11px; font-weight: 500; }
    .legend-row i { width: 9px; height: 9px; margin-right: 7px; display: inline-block; background: var(--neon); box-shadow: 0 0 0 1px var(--ink); }
    .legend-row i.neutral { background: #c8c8c0; } .legend-row i.negative { background: var(--ink); }

    /* --- bars / table --- */
    .channel-list { display: grid; gap: 12px; }
    .channel-label { display: flex; justify-content: space-between; margin-bottom: 4px; font-size: 12.5px; color: var(--ink); }
    .channel-label b, .channel-share { font-family: var(--mono); font-size: 10px; font-weight: 700; }
    .channel-row { display: grid; grid-template-columns: 1fr 40px; align-items: center; column-gap: 8px; }
    .channel-row .channel-label { grid-column: 1 / -1; }
    .channel-track, .trend-track { height: 8px; background: transparent; box-shadow: inset 0 0 0 1px var(--ink); }
    .channel-track i, .trend-track i { display: block; height: 100%; background: var(--ink);
      background-image: linear-gradient(90deg, var(--neon), var(--neon) 3px, transparent 3px); background-repeat: no-repeat; background-position: right; }
    .channel-share { text-align: right; color: var(--ink); }
    .data-table { width: 100%; margin-top: 2px; border-collapse: collapse; font-size: 12px; }
    .data-table th { font-family: var(--mono); font-size: 8.5px; font-weight: 700; letter-spacing: .12em; text-align: left; text-transform: uppercase;
      background: var(--ink); color: var(--neon); }
    .data-table th, .data-table td { padding: 8px 9px; border-bottom: 1px solid rgba(var(--ink-rgb), .35); }
    .data-table td { color: var(--ink); font-feature-settings: 'tnum'; } .data-table td:first-child { font-weight: 600; }
    .trend-list { display: grid; gap: 11px; }
    .trend-row { display: grid; grid-template-columns: 78px 1fr 26px; align-items: center; gap: 10px; }
    .trend-row time, .trend-row b { font-family: var(--mono); font-size: 9.5px; font-weight: 500; color: var(--ink); }
    .trend-row b { text-align: right; font-weight: 700; }
    .pull-quote { margin: 18px 0 0; padding: 10px 14px; border-left: 3px solid var(--neon); background: rgba(var(--ink-rgb), .05);
      font-family: var(--serif); font-size: 12.5px; color: var(--ink); }

    /* --- keywords / risks --- */
    .keyword-cloud { display: flex; flex-wrap: wrap; gap: 7px; }
    .keyword { padding: 4px 11px; border: 1px solid var(--ink); font-size: 12px; color: var(--ink); background: transparent; }
    .keyword:nth-child(3n+1) { background: var(--ink); color: var(--neon); border-color: var(--ink); }
    .risk-list { display: grid; gap: 10px; margin: 0; padding: 0; list-style: none; }
    .risk-item { display: flex; gap: 10px; align-items: flex-start; padding: 10px 12px; border: 1px solid var(--ink); background: rgba(255,255,255,.35); }
    .risk-item strong { display: block; font-size: 12.5px; font-weight: 600; line-height: 1.6; color: var(--ink); }
    .risk-item small { display: block; margin-top: 3px; font-size: 11px; color: var(--mist); }
    .risk-level { flex: none; margin-top: 1px; padding: 2px 8px; font-family: var(--mono); font-size: 9px; letter-spacing: .1em; }
    .risk-high .risk-level { background: var(--ink); color: var(--neon); }
    .risk-medium .risk-level { border: 1px solid var(--ink); color: var(--ink); }
    .risk-low .risk-level { border: 1px solid rgba(var(--ink-rgb), .4); color: var(--mist); }
    .empty-state { padding: 8px 2px; color: var(--mist); font-size: 12px; list-style: none; }

    /* --- findings / sources --- */
    .finding-list { display: grid; gap: 10px; margin: 0; padding: 0; list-style: none; }
    .finding-list li { display: flex; gap: 12px; align-items: baseline; }
    .finding-list li span { flex: none; padding: 0 6px; background: var(--ink); color: var(--neon); font-family: var(--mono); font-size: 10px; font-weight: 700; }
    .finding-list li p { margin: 0; font-family: var(--serif); font-size: 13px; color: var(--ink); }
    .source-list { display: grid; margin: 0; padding: 0; list-style: none; }
    .source-item { display: grid; grid-template-columns: 26px 1fr auto; gap: 10px; align-items: start; padding: 10px 2px; border-bottom: 1px solid rgba(var(--ink-rgb), .3); }
    .source-index { font-family: var(--mono); font-size: 9.5px; color: var(--mist); padding-top: 3px; }
    .source-item a { display: block; font-family: var(--serif); font-size: 13px; font-weight: 600; line-height: 1.55; color: var(--ink);
      border-bottom: 1px solid transparent; }
    .source-item a:hover { background: var(--neon-soft); }
    .source-item small { display: block; margin-top: 2px; font-size: 10.5px; color: var(--mist); }
    .source-item small em { font-style: normal; opacity: .55; }
    .meta-badge { flex: none; padding: 2px 8px; font-family: var(--mono); font-size: 9px; letter-spacing: .06em; border: 1px solid rgba(var(--ink-rgb), .4); }
    .source-archive { margin-top: 12px; }
    .source-archive summary { cursor: pointer; padding: 8px 12px; border: 1px dashed var(--ink); font-family: var(--mono); font-size: 10.5px; color: var(--ink); }
    .source-archive summary:hover { background: var(--neon-soft); }
    .source-archive[open] summary { margin-bottom: 8px; }

    /* --- closing --- */
    .closing { margin-top: 40px; border-top: 3px solid var(--ink); padding-top: 4px; }
    .closing-rule { height: 1px; background: var(--ink); margin-bottom: 16px; }
    .closing p { font-family: var(--serif); font-size: 13px; line-height: 1.95; color: var(--ink); }
    .byline { display: flex; flex-wrap: wrap; justify-content: space-between; gap: 8px 18px; margin-top: 18px; padding: 10px 12px; background: var(--ink); color: var(--neon);
      font-family: var(--serif); font-size: 12.5px; }
    .colophon { margin-top: 14px; text-align: center; font-family: var(--mono); font-size: 8.5px; letter-spacing: .22em; text-transform: uppercase; color: var(--mist); }

    @media (max-width: 480px) {
      .sheet { padding: 26px 14px 40px; }
      .masthead h1 { font-size: 22px; padding: 7px 12px 8px; }
      .stat-hero .n { font-size: 44px; }
      .cov-grid { grid-template-columns: repeat(2, 1fr); }
      .sentiment-layout { grid-template-columns: 104px 1fr; gap: 14px; }
      .donut { width: 104px; height: 104px; } .donut::after { width: 64px; height: 64px; }
      .source-item { grid-template-columns: 22px 1fr; } .source-item > .meta-badge { grid-column: 2; justify-self: start; }
    }
    '''

    return f'''<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <meta name="theme-color" content="#e9e9e5">
  <title>章鱼 AI 全景分析</title>
  <style>{css}</style>
</head>
<body>
  <div class="sheet">

    <header class="masthead">
      <div class="masthead-rule"></div>
      <div class="kicker"><span>OCTOPUS AI</span><span>PANORAMA REPORT</span></div>
      <h1>章鱼 AI 全景分析</h1>
      <p class="subtitle">全网 AI 调研<em>境内境外</em>数据，由多个大模型混合部署。</p>
      <div class="meta-line"><span>本期主题 <b>{_escape(topic)}</b></span><span>样本 <b>{mention_count:02d} 条</b></span><span>来源 <b>{_escape(source_label)}</b></span></div>
    </header>

    <section class="sec">
      <div class="sec-head"><span class="no">01</span><h2>编辑按语</h2><span class="en">Editor's Note</span></div>
      <p class="note-text">本期围绕「{_escape(topic)}」的公开信息共整理 <strong>{mention_count} 条</strong>。整体讨论主导情感为 <strong>{_escape(dominant)}</strong>，热度处于 <strong>{_escape(heat.get("heat_level", "—"))}</strong> 区间。建议先关注声量最大的渠道，再回到原始信源核验判断。</p>
    </section>

    <section class="sec">
      <div class="sec-head"><span class="no">02</span><h2>核心指标</h2><span class="en">Metrics</span></div>
      <div class="stat-hero"><span class="n">{_escape(heat.get("heat_score", "—"))}</span><span class="l">Heat Index · 热度指数<i>{_escape(heat.get("heat_level", "—"))} 热度 · 综合声量与互动</i></span></div>
      <div class="stat-grid">
        <div class="stat-cell"><span class="n">{mention_count}</span><div class="l">Mentions · 公开提及总量</div></div>
        <div class="stat-cell"><span class="n neon">{_escape(dominant)}</span><div class="l">Dominant · 主导情感</div></div>
        <div class="stat-cell"><span class="n">{neu_pct}%</span><div class="l">Neutral Share · 中性占比</div></div>
        <div class="stat-cell"><span class="n neon">{risk_count:02d}</span><div class="l">Watchlist · 风险信号</div></div>
      </div>
    </section>

    <section class="sec">
      <div class="sec-head"><span class="no">03</span><h2>调研覆盖</h2><span class="en">Coverage</span></div>
      <div class="cov-grid">
        <div class="cov-cell"><b>{configured_english}</b><span>English RSS feeds</span></div>
        <div class="cov-cell"><b>{configured_chinese}</b><span>中文 RSS feeds</span></div>
        <div class="cov-cell"><b>{successful_total}</b><span>Feeds connected</span></div>
        <div class="cov-cell"><b>{configured_total}</b><span>Feeds configured</span></div>
      </div>
      <p class="cov-note">{_escape(coverage_note)}</p>
    </section>

    <section class="sec">
      <div class="sec-head"><span class="no">04</span><h2>情感光谱</h2><span class="en">Sentiment · n = {total}</span></div>
      <div class="sentiment-layout">
        <div class="donut" style="--pos-deg:{pos_deg}deg;--neu-deg:{neu_end_deg}deg" role="img" aria-label="正面 {pos_pct}%，中性 {neu_pct}%，负面 {neg_pct}%"></div>
        <div class="legend">
          <div class="legend-row"><span><i></i>正面</span><b>{pos} / {pos_pct}%</b></div>
          <div class="legend-row"><span><i class="neutral"></i>中性</span><b>{neu} / {neu_pct}%</b></div>
          <div class="legend-row"><span><i class="negative"></i>负面</span><b>{neg} / {neg_pct}%</b></div>
          <p class="muted" style="margin:4px 0 0;font-size:11.5px;">主导情绪：<span class="hl">{_escape(dominant)}</span></p>
        </div>
      </div>
    </section>

    <section class="sec">
      <div class="sec-head"><span class="no">05</span><h2>渠道分布</h2><span class="en">Channels</span></div>
      <div class="channel-list">{channel_bars}</div>
      <table class="data-table" style="margin-top:18px;"><thead><tr><th>平台 / 来源</th><th>提及</th><th>占比</th><th>平均互动</th></tr></thead><tbody>{channel_rows}</tbody></table>
    </section>

    <section class="sec">
      <div class="sec-head"><span class="no">06</span><h2>声量趋势</h2><span class="en">Trend · Last 7</span></div>
      <div class="trend-list">{trend_bars}</div>
      <blockquote class="pull-quote">趋势是线索，不是结论；回到信源，才是判断的开始。</blockquote>
    </section>

    <section class="sec">
      <div class="sec-head"><span class="no">07</span><h2>热门关键词</h2><span class="en">Top Signals</span></div>
      <div class="keyword-cloud">{chips}</div>
    </section>

    <section class="sec">
      <div class="sec-head"><span class="no">08</span><h2>风险提示</h2><span class="en">Watchlist · {risk_count:02d}</span></div>
      <ul class="risk-list">{risk_items}</ul>
    </section>

    <section class="sec">
      <div class="sec-head"><span class="no">09</span><h2>编辑要点</h2><span class="en">Key Findings</span></div>
      <ol class="finding-list">{finding_items}</ol>
    </section>

    <section class="sec">
      <div class="sec-head"><span class="no">10</span><h2>信源列表</h2><span class="en">Verify · {mention_count} items</span></div>
      <ul class="source-list">{source_items}</ul>
      {source_archive}
    </section>

    <footer class="closing">
      <div class="closing-rule"></div>
      <p>全网境内外为你寻找蛛丝马迹——提供<span class="hl">全景视野分析</span>，由多模型协同推理决策。底层所使用的大语言模型（LLM）多模式背后结合使用了多种不同的先进模型，包括但不限于 <span class="hl">Claude</span>、<span class="hl">ChatGPT</span>、<span class="hl">Gemini</span>、<span class="hl">Grok</span>、<span class="hl">Qwen</span> 以及 <span class="hl">Kimi</span>。根据不同的资产管理任务需求，更好地发挥各个模型的优势来提供数据支持！加油 💪</p>
      <div class="byline"><span>作者：章鱼 ai</span><span>仅供参考，分析研究</span></div>
      <div class="colophon">OCTOPUS AI · PANORAMA REPORT · END</div>
    </footer>

  </div>
</body>
</html>
'''

# ---------------------------------------------------------------------------
# Push & CLI



def render_push_content(topic: str, data: Dict[str, Any]) -> str:
    """Render a WeChat-message-safe briefing in the same e-magazine x e-ink style.

    The WeChat message page (PushPlus ``template: html``) strips ``<style>``
    blocks, ``<script>`` and external fonts, so this variant is a compact
    fragment where *every* rule is inlined on the element.  Layout relies on
    plain block elements and tables only, which WeChat renders faithfully.
    Palette mirrors render_html(): light-gray paper #e9e9e5, ink #0c0c0d,
    neon #b8ff2e; small type throughout.
    """
    sentiment = data.get("sentiment") or {}
    heat = data.get("heat") or {}
    platforms = data.get("platform_stats") or {}
    keywords = data.get("keywords") or []
    risks = data.get("risks") or []
    posts = data.get("posts") or []
    findings = (data.get("query_summary") or {}).get("key_findings") or []
    source_label = "实时 RSS 抓取" if data.get("data_source") == "rss_news" else "离线示例数据"

    pos = int(sentiment.get("positive", 0) or 0)
    neg = int(sentiment.get("negative", 0) or 0)
    neu = int(sentiment.get("neutral", 0) or 0)
    total = max(pos + neg + neu, 1)
    pos_pct = round(pos / total * 100)
    neg_pct = round(neg / total * 100)
    neu_pct = 100 - pos_pct - neg_pct
    dominant = SENTIMENT_ZH.get(sentiment.get("dominant", "neutral"), "中性")
    mention_count = int(heat.get("total_mentions", len(posts)) or 0)
    risk_count = len(risks)

    # --- shared inline-style shorthands (e-ink Style A palette) ---
    INK = "#0c0c0d"
    NEON = "#b8ff2e"
    PAPER = "#e9e9e5"
    CARD = "rgba(255,255,255,.35)"
    MIST = "#8a8a83"
    HAIR = "1px solid rgba(12,12,13,.35)"
    MONO = "font-family:'JetBrains Mono','SFMono-Regular',Menlo,Consolas,monospace;"
    SERIF = "font-family:'Noto Serif SC','Songti SC','SimSun',Georgia,serif;"
    SOFT = "rgba(12,12,13,.78)"
    hl = f"background:{INK};color:{NEON};padding:1px 6px;font-weight:600;"

    def sec_head(num: str, title: str) -> str:
        return (
            f'<div style="margin:22px 0 8px;">'
            f'<span style="{MONO}background:{INK};color:{NEON};font-size:9px;letter-spacing:2px;padding:3px 8px;">{num}</span>'
            f'<span style="{SERIF}background:{INK};color:{NEON};font-size:14px;font-weight:700;padding:3px 10px;margin-left:6px;">{title}</span>'
            f'</div>'
        )

    def bar(pct: int, label: str, value: str) -> str:
        width = max(int(pct), 4)
        return (
            f'<div style="margin:0 0 9px;">'
            f'<div style="font-size:11px;color:{SOFT};margin-bottom:3px;">{label}'
            f' <span style="{MONO}{hl}font-size:9px;">{value}</span></div>'
            f'<div style="height:8px;border:1px solid {INK};">'
            f'<div style="background:{INK};border-right:3px solid {NEON};height:8px;width:{width}%;"></div></div>'
            f'</div>'
        )

    # metrics 2x2 table
    def metric_cell(label: str, value: str, note: str, accent: bool = False) -> str:
        value_style = f"{SERIF}font-size:20px;font-weight:700;line-height:1.2;" + (
            f"background:{INK};color:{NEON};padding:0 6px;display:inline-block;" if accent else f"color:{INK};"
        )
        return (
            f'<td style="width:50%;background:{CARD};border:{HAIR};border-top:3px solid {NEON if accent else INK};padding:10px 12px;vertical-align:top;">'
            f'<div style="{MONO}font-size:8px;letter-spacing:1.5px;color:{MIST};text-transform:uppercase;">{label}</div>'
            f'<div style="margin:6px 0 1px;"><span style="{value_style}">{value}</span></div>'
            f'<div style="font-size:10px;color:{MIST};">{note}</div></td>'
        )

    heat_score = _escape(heat.get("heat_score", "—"))
    heat_level = _escape(heat.get("heat_level", "—"))
    metrics_table = (
        f'<table style="width:100%;border-collapse:separate;border-spacing:5px 5px;margin:8px -5px 0;"><tbody>'
        f'<tr>{metric_cell("Heat index", heat_score, heat_level + " 热度", True)}'
        f'{metric_cell("Mentions", str(mention_count), "公开提及总量")}</tr>'
        f'<tr>{metric_cell("Neutral share", f"{neu_pct}%", "中性讨论占比")}'
        f'{metric_cell("Watchlist", f"{risk_count:02d}", "待跟进风险信号", True)}</tr>'
        f'</tbody></table>'
    )

    # channels: top 5 bars
    platform_items = list(platforms.items())[:5]
    max_count = max((int(s.get("count", 0) or 0) for _n, s in platform_items), default=1)
    channel_bars = "".join(
        bar(
            int(int(s.get("count", 0) or 0) / max_count * 100),
            _escape(name),
            f"{_escape(s.get('count', 0))} · {_escape(s.get('percentage', 0))}%",
        )
        for name, s in platform_items
    ) or f'<p style="font-size:11px;color:{MIST};">暂无平台数据</p>'

    # keywords chips
    chips = "".join(
        f'<span style="display:inline-block;background:{INK};color:{NEON};font-size:11px;padding:3px 10px;margin:0 6px 6px 0;">{_escape(k)}</span>'
        for k in keywords[:10]
    ) or f'<span style="font-size:11px;color:{MIST};">暂无关键词</span>'

    # risks top 3
    risk_rows = "".join(
        f'<div style="border-bottom:{HAIR};padding:8px 0;">'
        f'<span style="{MONO}{hl}font-size:8px;">{_escape(RISK_ZH.get(r.get("risk_level"), r.get("risk_level", "低")))}</span>'
        f'<span style="font-size:11.5px;color:{SOFT};margin-left:8px;">{_escape((r.get("text_preview") or "")[:80])}</span></div>'
        for r in risks[:3]
    ) or f'<p style="font-size:11px;color:{MIST};">未识别到明显风险信号。</p>'

    # findings
    finding_rows = "".join(
        f'<div style="border-bottom:{HAIR};padding:8px 0;font-size:12px;color:{SOFT};">'
        f'<span style="{MONO}{hl}font-size:8.5px;margin-right:8px;">{i:02d}</span>{_escape(f)}</div>'
        for i, f in enumerate(findings[:6], 1)
    ) or f'<p style="font-size:11px;color:{MIST};">暂无要点</p>'

    # sources top 10 (linked)
    source_rows = "".join(
        f'<div style="border-bottom:{HAIR};padding:8px 0;">'
        f'<a href="{_escape(p.get("url") or "#")}" style="font-size:12px;color:{INK};font-weight:500;text-decoration:none;">'
        f'{i:02d}. {_escape((p.get("title") or p.get("content") or "")[:60])}</a>'
        f'<div style="{MONO}font-size:8.5px;color:{MIST};margin-top:2px;">'
        f'{_escape("EN" if p.get("language") == "en" else "中文" if p.get("language") == "zh" else "来源")}'
        f' · {_escape(p.get("feed_name") or p.get("nickname", ""))}'
        f' · {_escape(SENTIMENT_ZH.get(p.get("sentiment", "neutral"), "中性"))}</div></div>'
        for i, p in enumerate(posts[:10], 1)
    ) or f'<p style="font-size:11px;color:{MIST};">暂无信源</p>'

    card = f'background:{CARD};border:{HAIR};padding:14px 16px;margin:0 0 10px;'

    return (
        f'<div style="background:{PAPER};padding:14px 12px;color:{INK};'
        f"font-family:-apple-system,BlinkMacSystemFont,'PingFang SC','Noto Sans SC','Microsoft YaHei',sans-serif;"
        f'font-size:13px;line-height:1.75;">'

        # --- hero (black block) ---
        f'<div style="background:{INK};padding:24px 20px 18px;margin-bottom:14px;">'
        f'<div style="{MONO}color:{NEON};font-size:9px;letter-spacing:3px;text-transform:uppercase;">OCTOPUS AI INTELLIGENCE</div>'
        f'<div style="{SERIF}color:{NEON};font-size:24px;font-weight:700;line-height:1.25;margin:10px 0 6px;">章鱼 AI 全景分析</div>'
        f'<div style="{SERIF}color:rgba(232,234,237,.78);font-size:12.5px;">全网 AI 调研境内境外数据，由多个大模型混合部署。</div>'
        f'<div style="margin-top:12px;">'
        f'<span style="{MONO}border:1px solid rgba(184,255,46,.4);color:{NEON};font-size:9px;padding:3px 8px;margin-right:6px;">主题 {_escape(topic)}</span>'
        f'<span style="{MONO}border:1px solid rgba(184,255,46,.4);color:{NEON};font-size:9px;padding:3px 8px;">样本 {mention_count:02d} 条</span></div>'
        f'<div style="border-top:1px solid rgba(184,255,46,.22);margin-top:14px;padding-top:10px;">'
        f'<span style="{SERIF}color:{NEON};font-size:22px;font-weight:700;">{_escape(heat.get("heat_score", "—"))}</span>'
        f'<span style="{MONO}color:rgba(232,234,237,.5);font-size:8px;letter-spacing:2px;margin-left:8px;">HEAT INDEX</span></div>'
        f'</div>'

        # --- 01 结论 ---
        f'{sec_head("01", "先看结论")}'
        f'<div style="{card}border-left:4px solid {NEON};">'
        f'<div style="{SERIF}font-size:13px;font-weight:700;margin-bottom:6px;">编辑按语</div>'
        f'<div style="font-size:12.5px;color:{SOFT};">本期围绕「{_escape(topic)}」的公开信息共整理'
        f' <span style="{hl}">{mention_count}</span> 条，主导情感为 <span style="{hl}">{_escape(dominant)}</span>，'
        f'热度处于 <span style="{hl}">{_escape(heat.get("heat_level", "—"))}</span> 区间。'
        f'建议先关注声量最大的渠道，再回到原始信源核验判断。</div></div>'
        f'{metrics_table}'

        # --- 02 分布 ---
        f'{sec_head("02", "情绪与声量分布")}'
        f'<div style="{card}">'
        f'<div style="{SERIF}font-size:13px;font-weight:700;margin-bottom:10px;">情感光谱 <span style="{MONO}font-size:8.5px;color:{MIST};font-weight:400;">n = {total}</span></div>'
        f'{bar(pos_pct, "正面", f"{pos} / {pos_pct}%")}'
        f'{bar(neu_pct, "中性", f"{neu} / {neu_pct}%")}'
        f'{bar(neg_pct, "负面", f"{neg} / {neg_pct}%")}</div>'
        f'<div style="{card}">'
        f'<div style="{SERIF}font-size:13px;font-weight:700;margin-bottom:10px;">渠道分布</div>{channel_bars}</div>'

        # --- 03 叙事 ---
        f'{sec_head("03", "讨论正在说什么")}'
        f'<div style="{card}">'
        f'<div style="{SERIF}font-size:13px;font-weight:700;margin-bottom:8px;">热门关键词</div>{chips}</div>'
        f'<div style="{card}">'
        f'<div style="{SERIF}font-size:13px;font-weight:700;margin-bottom:4px;">风险提示 <span style="{MONO}font-size:8.5px;color:{MIST};font-weight:400;">watchlist / {risk_count:02d}</span></div>{risk_rows}</div>'

        # --- 04 要点与信源 ---
        f'{sec_head("04", "要点与信源")}'
        f'<div style="{card}">'
        f'<div style="{SERIF}font-size:13px;font-weight:700;margin-bottom:4px;">编辑要点</div>{finding_rows}</div>'
        f'<div style="{card}">'
        f'<div style="{SERIF}font-size:13px;font-weight:700;margin-bottom:4px;">信源列表 <span style="{MONO}font-size:8.5px;color:{MIST};font-weight:400;">top 10 / {mention_count} items</span></div>{source_rows}</div>'

        # --- colophon (black block) ---
        f'<div style="background:{INK};padding:18px 18px 14px;margin-top:18px;color:rgba(232,234,237,.82);">'
        f'<div style="border-bottom:1px solid rgba(184,255,46,.25);padding-bottom:9px;margin-bottom:9px;">'
        f'<span style="{SERIF}color:{NEON};font-size:13px;font-weight:700;">作者：章鱼 ai</span>'
        f'<span style="{MONO}color:rgba(232,234,237,.55);font-size:8.5px;letter-spacing:1.5px;margin-left:10px;">仅供参考 · 分析研究</span></div>'
        f'<div style="font-size:11px;line-height:1.8;">全网境内外为你寻找蛛丝马迹，提供全景视野分析，由多模型协同推理决策。'
        f'底层所使用的大语言模型（LLM）多模式背后结合使用了多种不同的先进模型，包括但不限于 Claude、ChatGPT、Gemini、Grok、Qwen 以及 Kimi。'
        f'根据不同的资产管理任务需求，更好地发挥各个模型的优势来提供数据支持！'
        f'<span style="background:{NEON};color:{INK};padding:0 4px;font-weight:600;">[加油]</span></div>'
        f'<div style="{MONO}border-top:1px solid rgba(232,234,237,.14);margin-top:10px;padding-top:8px;'
        f'font-size:7.5px;letter-spacing:2px;color:rgba(232,234,237,.4);text-transform:uppercase;">'
        f'OCTOPUS AI · PANORAMA&nbsp;&nbsp;|&nbsp;&nbsp;{_escape(source_label)}&nbsp;&nbsp;|&nbsp;&nbsp;END OF BRIEF</div>'
        f'</div>'
        f'</div>'
    )


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
    report["rss_coverage"] = insight["rss_coverage"]
    html_doc = render_html(topic, report, template_name)
    push_content = render_push_content(topic, report)
    return {
        "html": html_doc,
        "push_content": push_content,
        "report": report,
        "template": template_name,
        "data_source": insight["data_source"],
    }


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate WeChat push HTML via library tools.")
    parser.add_argument("--topic", default=os.environ.get("TOPIC", "AI 行业本周舆情"))
    parser.add_argument("--output", default="dist/wechat_push.html")
    parser.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_LIMIT,
        help="Max fetched items (default: 70 = 50 English + 20 Chinese).",
    )
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
    push_preview = out.with_name(out.stem + ".push" + out.suffix)
    push_preview.write_text(result["push_content"], encoding="utf-8")
    print(
        f"Wrote {out} (template={result['template']}, "
        f"data_source={result['data_source']}); "
        f"push preview: {push_preview}"
    )

    if args.push:
        token = os.environ.get("PUSHPLUS_TOKEN", "").strip()
        if not token:
            print("PUSHPLUS_TOKEN is empty; skip push.", file=sys.stderr)
            return 0
        resp = push_via_pushplus("章鱼 AI 全景分析", result["push_content"], token)
        print(f"PushPlus response: {resp}")
        code = resp.get("code")
        if code not in (200, "200", 0, "0"):
            return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

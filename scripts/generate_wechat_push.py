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
3. Render a deep, paginated HTML briefing (coverage / sentiment / keywords /
   channels / trend / risks / findings / linked source archive).

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
    color = {"positive": "#a3e635", "negative": "#fb7185", "neutral": "#8a8f9e"}.get(label, "#8a8f9e")
    return (
        f'<span class="meta-badge" style="color:{color};">'
        f'{SENTIMENT_ZH.get(label, label)}</span>'
    )


def render_html(topic: str, data: Dict[str, Any], template_name: str) -> str:
    """Render a compact but editorial, paginated HTML briefing.

    The WeChat page is intentionally self-contained: CSS carries the visual
    system and a small amount of vanilla JS handles paging and keyboard
    navigation, so the generated artifact works when opened as a local file.
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
      --bg: #050507;
      --bg-soft: #0b0b12;
      --card: linear-gradient(180deg, rgba(255,255,255,.05), rgba(255,255,255,.02));
      --line: rgba(255,255,255,.08);
      --line-hover: rgba(168,85,247,.55);
      --text: #f5f6fa;
      --muted: #8a8f9e;
      --indigo: #6366f1;
      --violet: #a855f7;
      --pink: #ec4899;
      --cyan: #22d3ee;
      --lime: #a3e635;
      --coral: #fb7185;
      --grad: linear-gradient(115deg, #6366f1, #a855f7 55%, #ec4899);
      --sans: 'Inter', 'Noto Sans SC', 'PingFang SC', 'Microsoft YaHei', sans-serif;
      --mono: 'JetBrains Mono', 'SFMono-Regular', Consolas, monospace;
      --radius: 24px;
    }
    * { box-sizing: border-box; }
    html { background: var(--bg); scroll-behavior: smooth; }
    body {
      min-width: 320px; margin: 0; color: var(--text); background: var(--bg);
      font-family: var(--sans); font-size: 16px; line-height: 1.7;
      -webkit-font-smoothing: antialiased;
    }
    body::before { content: ''; position: fixed; inset: 0; z-index: 0; pointer-events: none;
      background:
        radial-gradient(620px 420px at 82% -8%, rgba(99,102,241,.22), transparent 65%),
        radial-gradient(560px 420px at 6% 28%, rgba(168,85,247,.14), transparent 65%),
        radial-gradient(680px 480px at 60% 108%, rgba(236,72,153,.10), transparent 65%); }
    a { color: var(--cyan); text-decoration: none; }
    a:hover { color: var(--violet); }
    button { font: inherit; }
    h1, h2, h3, p { margin-top: 0; }
    .muted { color: var(--muted); }
    .grad-text { background: var(--grad); -webkit-background-clip: text; background-clip: text; color: transparent; }

    /* --- top bar --- */
    .topbar { position: sticky; top: 0; z-index: 30; display: flex; align-items: center; gap: 22px;
      padding: 14px clamp(18px, 4vw, 44px); border-bottom: 1px solid var(--line);
      background: rgba(5,5,7,.72); backdrop-filter: blur(18px); }
    .brand { display: flex; align-items: center; gap: 10px; font-weight: 700; font-size: 16px; letter-spacing: .01em; white-space: nowrap; }
    .brand-mark { width: 22px; height: 22px; border-radius: 7px; background: var(--grad); box-shadow: 0 0 18px rgba(168,85,247,.55); }
    .nav-menu { display: flex; gap: 6px; margin: 0 auto; padding: 0; overflow-x: auto; scrollbar-width: none; }
    .nav-menu::-webkit-scrollbar { display: none; }
    .nav-menu a { display: flex; align-items: baseline; gap: 8px; padding: 7px 15px; border: 1px solid transparent; border-radius: 999px;
      color: var(--muted); font-size: 13px; white-space: nowrap; transition: color .2s, background .2s, border-color .2s; }
    .nav-menu a span { font-family: var(--mono); font-size: 10px; opacity: .6; }
    .nav-menu a:hover { color: var(--text); background: rgba(255,255,255,.05); }
    .nav-menu a.active { color: var(--text); border-color: rgba(168,85,247,.45); background: rgba(168,85,247,.12); box-shadow: 0 0 22px rgba(168,85,247,.18); }
    .top-meta { color: var(--muted); font-family: var(--mono); font-size: 10px; letter-spacing: .06em; white-space: nowrap; }

    /* --- pages --- */
    .main-content { position: relative; z-index: 1; }
    .page { display: none; min-height: 100vh; padding: 46px clamp(18px, 4vw, 44px) 80px; }
    .page.active { display: block; animation: page-in .6s cubic-bezier(.16,1,.3,1) both; }
    .page-inner { max-width: 1160px; margin: 0 auto; }
    .page-top { display: flex; align-items: center; justify-content: space-between; gap: 14px; margin-bottom: 40px;
      color: var(--muted); font-family: var(--mono); font-size: 10px; letter-spacing: .14em; text-transform: uppercase; }
    .page-top .rule { flex: 1; max-width: 120px; height: 1px; background: linear-gradient(90deg, rgba(168,85,247,.65), transparent); }
    .eyebrow { display: inline-flex; align-items: center; gap: 10px; padding: 6px 14px; border: 1px solid rgba(168,85,247,.4);
      border-radius: 999px; color: #c4b5fd; font-family: var(--mono); font-size: 11px; letter-spacing: .16em; text-transform: uppercase;
      background: rgba(168,85,247,.08); }
    .eyebrow::before { content: ''; width: 6px; height: 6px; border-radius: 50%; background: var(--grad); box-shadow: 0 0 10px rgba(236,72,153,.8); }

    /* --- hero --- */
    .hero { padding: 30px 0 44px; }
    .hero h1 { margin: 26px 0 18px; font-size: clamp(42px, 7vw, 96px); font-weight: 800; line-height: 1.04; letter-spacing: -.035em; }
    .hero h1 em { font-style: normal; background: var(--grad); -webkit-background-clip: text; background-clip: text; color: transparent; }
    .hero-deck { max-width: 620px; color: #b9bece; font-size: 18px; }
    .hero-meta { display: flex; flex-wrap: wrap; gap: 20px; margin-top: 28px; color: var(--muted); font-family: var(--mono); font-size: 11px; }
    .hero-meta b { color: var(--cyan); font-weight: 500; }

    /* --- section heading --- */
    .section-intro { display: flex; align-items: end; justify-content: space-between; gap: 30px; margin-bottom: 26px; }
    .section-intro h2 { margin: 14px 0 0; font-size: clamp(30px, 4.4vw, 54px); font-weight: 800; line-height: 1.1; letter-spacing: -.03em; }
    .section-intro > p { max-width: 330px; margin-bottom: 6px; color: var(--muted); font-size: 14px; }

    /* --- bento grid --- */
    .bento { display: grid; grid-template-columns: repeat(12, 1fr); gap: 16px; }
    .bento + .bento { margin-top: 16px; }
    .card { position: relative; overflow: hidden; display: flex; flex-direction: column; padding: 26px 28px;
      border: 1px solid var(--line); border-radius: var(--radius); background: var(--card);
      transition: transform .25s, border-color .25s, box-shadow .25s; }
    .card:hover { transform: translateY(-4px); border-color: var(--line-hover); box-shadow: 0 12px 44px rgba(99,102,241,.16); }
    .span-3 { grid-column: span 3; } .span-4 { grid-column: span 4; } .span-5 { grid-column: span 5; }
    .span-6 { grid-column: span 6; } .span-7 { grid-column: span 7; } .span-8 { grid-column: span 8; } .span-12 { grid-column: span 12; }
    .card-label { margin-bottom: 14px; color: var(--muted); font-family: var(--mono); font-size: 10px; letter-spacing: .14em; text-transform: uppercase; }
    .card-title { display: flex; align-items: baseline; justify-content: space-between; gap: 14px; margin-bottom: 20px; }
    .card-title h3 { margin: 0; font-size: 19px; font-weight: 700; }
    .card-title span { color: var(--muted); font-family: var(--mono); font-size: 10px; letter-spacing: .06em; }
    .card-note { color: var(--muted); font-size: 12.5px; }
    .note-text { margin: 0; color: #c6cad8; font-size: 16.5px; line-height: 1.85; }
    .note-text strong { color: var(--text); }
    .card.glow::after { content: ''; position: absolute; right: -70px; top: -70px; width: 210px; height: 210px; border-radius: 50%;
      background: radial-gradient(circle, rgba(168,85,247,.35), transparent 70%); pointer-events: none; }
    .big-num { margin: 2px 0 10px; font-size: clamp(58px, 6.5vw, 92px); font-weight: 800; line-height: 1; letter-spacing: -.04em;
      background: var(--grad); -webkit-background-clip: text; background-clip: text; color: transparent; }
    .card.stat .stat-num { display: block; margin: 4px 0 8px; font-size: 38px; font-weight: 800; line-height: 1.1; letter-spacing: -.02em; }
    .card.stat.alert .stat-num { color: var(--coral); }
    .card.mini { align-items: flex-start; padding: 20px 22px; }
    .card.mini b { font-size: 30px; font-weight: 800; line-height: 1; background: var(--grad); -webkit-background-clip: text; background-clip: text; color: transparent; }
    .card.mini span { margin-top: 8px; color: var(--muted); font-family: var(--mono); font-size: 10px; letter-spacing: .08em; text-transform: uppercase; }
    .card.quiet { padding: 18px 24px; border-style: dashed; background: transparent; }
    .coverage-note { margin: 0; color: var(--muted); font-size: 13px; }

    /* --- sentiment donut --- */
    .sentiment-layout { display: grid; grid-template-columns: 160px 1fr; align-items: center; gap: 24px; }
    .donut { width: 160px; height: 160px; display: grid; place-items: center; border-radius: 50%;
      background: conic-gradient(var(--lime) 0deg var(--pos-deg), #3c4155 var(--pos-deg) var(--neu-deg), var(--coral) var(--neu-deg) 360deg);
      box-shadow: 0 0 40px rgba(163,230,53,.08); }
    .donut::after { content: ''; width: 104px; height: 104px; border-radius: 50%; background: #0b0b12; box-shadow: inset 0 0 0 1px var(--line); }
    .legend { display: grid; gap: 11px; }
    .legend-row { display: flex; align-items: center; justify-content: space-between; gap: 12px; color: var(--muted); font-size: 13px; }
    .legend-row b { color: var(--text); font-family: var(--mono); font-size: 12px; font-weight: 500; }
    .legend-row i { width: 8px; height: 8px; margin-right: 8px; display: inline-block; border-radius: 3px; background: var(--lime); }
    .legend-row i.neutral { background: #3c4155; } .legend-row i.negative { background: var(--coral); }

    /* --- channel + trend bars --- */
    .channel-list { display: grid; gap: 16px; }
    .channel-label { display: flex; justify-content: space-between; margin-bottom: 6px; color: #cdd2e0; font-size: 13px; }
    .channel-label b, .channel-share { color: #c4b5fd; font-family: var(--mono); font-size: 11px; font-weight: 500; }
    .channel-row { display: grid; grid-template-columns: minmax(70px, .4fr) 1fr 44px; align-items: center; column-gap: 10px; }
    .channel-row .channel-label { grid-column: 1 / -1; }
    .channel-track, .trend-track { height: 8px; overflow: hidden; border-radius: 999px; background: rgba(255,255,255,.07); }
    .channel-track i, .trend-track i { display: block; height: 100%; border-radius: 999px; background: var(--grad); transform-origin: left; animation: grow .9s cubic-bezier(.16,1,.3,1) both; }
    .channel-share { text-align: right; }
    .data-table { width: 100%; margin-top: 4px; border-collapse: collapse; font-size: 13px; }
    .data-table th { color: #c4b5fd; font-family: var(--mono); font-size: 10px; font-weight: 500; letter-spacing: .1em; text-align: left; text-transform: uppercase; }
    .data-table th, .data-table td { padding: 13px 12px; border-bottom: 1px solid var(--line); }
    .data-table td { color: #c0c5d4; } .data-table td:first-child { color: var(--text); font-weight: 600; }
    .trend-list { display: grid; gap: 16px; }
    .trend-row { display: grid; grid-template-columns: 92px 1fr 30px; align-items: center; gap: 12px; }
    .trend-row time, .trend-row b { color: var(--muted); font-family: var(--mono); font-size: 10px; font-weight: 500; }
    .trend-row b { color: #c4b5fd; text-align: right; }
    .pull-quote { margin: 26px 0 0; padding: 16px 20px; border-left: 3px solid; border-image: var(--grad) 1;
      color: #b9bece; font-size: 14.5px; background: rgba(255,255,255,.03); border-radius: 0 14px 14px 0; }

    /* --- keywords / risks --- */
    .keyword-cloud { display: flex; flex-wrap: wrap; gap: 9px; align-content: start; }
    .keyword { padding: 7px 15px; border: 1px solid rgba(168,85,247,.4); border-radius: 999px; color: #c4b5fd; font-size: 13px;
      background: rgba(168,85,247,.07); transition: background .2s, color .2s, border-color .2s; }
    .keyword:hover { color: #fff; border-color: var(--violet); background: rgba(168,85,247,.22); }
    .risk-list { display: grid; gap: 12px; margin: 0; padding: 0; list-style: none; }
    .risk-item { display: flex; gap: 14px; align-items: flex-start; padding: 15px 18px; border: 1px solid var(--line); border-radius: 16px; background: rgba(255,255,255,.03); }
    .risk-item strong { display: block; color: #dde1ec; font-size: 14px; font-weight: 600; line-height: 1.55; }
    .risk-item small { display: block; margin-top: 5px; color: var(--muted); font-size: 12px; }
    .risk-level { flex: none; margin-top: 2px; padding: 3px 11px; border-radius: 999px; font-family: var(--mono); font-size: 10px; letter-spacing: .08em; }
    .risk-high .risk-level { color: #ffd7dd; background: rgba(251,113,133,.18); box-shadow: inset 0 0 0 1px rgba(251,113,133,.5); }
    .risk-medium .risk-level { color: #fde8c8; background: rgba(251,191,36,.14); box-shadow: inset 0 0 0 1px rgba(251,191,36,.45); }
    .risk-low .risk-level { color: #cffafe; background: rgba(34,211,238,.12); box-shadow: inset 0 0 0 1px rgba(34,211,238,.4); }
    .empty-state { padding: 14px 4px; color: var(--muted); font-size: 13px; list-style: none; }

    /* --- findings / sources --- */
    .finding-list { display: grid; gap: 14px; margin: 0; padding: 0; list-style: none; counter-reset: findings; }
    .finding-list li { display: flex; gap: 16px; align-items: baseline; }
    .finding-list li span { flex: none; font-family: var(--mono); font-size: 13px; font-weight: 700;
      background: var(--grad); -webkit-background-clip: text; background-clip: text; color: transparent; }
    .finding-list li p { margin: 0; color: #ccd1de; font-size: 15px; }
    .source-list { display: grid; gap: 4px; margin: 0; padding: 0; list-style: none; }
    .source-item { display: grid; grid-template-columns: 34px 1fr auto; gap: 14px; align-items: start; padding: 13px 10px; border-radius: 14px; transition: background .2s; }
    .source-item:hover { background: rgba(255,255,255,.04); }
    .source-item + .source-item { border-top: 1px solid rgba(255,255,255,.05); }
    .source-index { color: var(--muted); font-family: var(--mono); font-size: 11px; padding-top: 3px; }
    .source-item a { display: block; color: #e6e9f2; font-size: 14.5px; font-weight: 600; line-height: 1.5; }
    .source-item a:hover { background: var(--grad); -webkit-background-clip: text; background-clip: text; color: transparent; }
    .source-item small { display: block; margin-top: 4px; color: var(--muted); font-size: 12px; }
    .source-item small em { font-style: normal; opacity: .5; }
    .meta-badge { flex: none; padding: 3px 11px; border-radius: 999px; font-family: var(--mono); font-size: 10px; letter-spacing: .06em;
      background: rgba(255,255,255,.05); box-shadow: inset 0 0 0 1px rgba(255,255,255,.12); }
    .source-archive { margin-top: 18px; }
    .source-archive summary { cursor: pointer; padding: 12px 16px; border: 1px dashed rgba(168,85,247,.4); border-radius: 14px;
      color: #c4b5fd; font-family: var(--mono); font-size: 12px; letter-spacing: .04em; transition: background .2s; }
    .source-archive summary:hover { background: rgba(168,85,247,.1); }
    .source-archive[open] summary { margin-bottom: 12px; }

    /* --- pager --- */
    .pager { display: flex; align-items: center; justify-content: space-between; max-width: 1160px; margin: 56px auto 0; padding-top: 22px; border-top: 1px solid var(--line); }
    .pager button { border: 1px solid var(--line); border-radius: 999px; padding: 9px 20px; color: var(--muted); background: transparent; cursor: pointer;
      font-family: var(--mono); font-size: 11px; letter-spacing: .1em; text-transform: uppercase; transition: color .2s, border-color .2s, background .2s; }
    .pager button:hover:not(:disabled) { color: #fff; border-color: rgba(168,85,247,.55); background: rgba(168,85,247,.12); }
    .pager button:disabled { visibility: hidden; }
    .pager-center { color: var(--muted); font-family: var(--mono); font-size: 10px; }
    .pager-center b { font-weight: 700; background: var(--grad); -webkit-background-clip: text; background-clip: text; color: transparent; }

    @keyframes page-in { from { opacity: 0; transform: translateY(20px); } to { opacity: 1; transform: translateY(0); } }
    @keyframes grow { from { transform: scaleX(0); } to { transform: scaleX(1); } }
    @media (prefers-reduced-motion: reduce) { *, *::before, *::after { animation-duration: .01ms !important; transition-duration: .01ms !important; } }
    @media (max-width: 980px) {
      .top-meta { display: none; }
      .span-3, .span-4, .span-5, .span-6, .span-7, .span-8 { grid-column: span 6; }
      .section-intro { display: block; } .section-intro > p { margin-top: 12px; }
    }
    @media (max-width: 620px) {
      .topbar { flex-wrap: wrap; gap: 10px; padding: 12px 16px; }
      .nav-menu { width: 100%; margin: 0; }
      .page { padding: 32px 16px 56px; }
      .span-3, .span-4, .span-5, .span-6, .span-7, .span-8 { grid-column: span 12; }
      .card { padding: 22px 20px; }
      .sentiment-layout { grid-template-columns: 1fr; justify-items: center; } .legend { width: 100%; }
      .source-item { grid-template-columns: 26px 1fr; } .source-item > .meta-badge { grid-column: 2; justify-self: start; }
      .trend-row { grid-template-columns: 75px 1fr 22px; gap: 8px; } .pager { margin-top: 40px; }
    }
    '''

    return f'''<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <meta name="theme-color" content="#050507">
  <title>{_escape(topic)} · 微信舆情简报</title>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;600;800&family=Noto+Sans+SC:wght@400;500;700;900&display=swap" rel="stylesheet">
  <style>{css}</style>
</head>
<body>
  <header class="topbar">
    <div class="brand"><span class="brand-mark" aria-hidden="true"></span><span>BettaFish · 微舆</span></div>
    <nav class="nav-menu" aria-label="报告章节">
      <a href="#page-1" data-page="page-1" class="active"><span>01</span>编辑摘要</a>
      <a href="#page-2" data-page="page-2"><span>02</span>信号分布</a>
      <a href="#page-3" data-page="page-3"><span>03</span>叙事与风险</a>
      <a href="#page-4" data-page="page-4"><span>04</span>要点与信源</a>
    </nav>
    <div class="top-meta">{_escape(source_label)} · {_escape(generated)}</div>
  </header>

  <main class="main-content">
    <section id="page-1" class="page active" aria-labelledby="page-1-title">
      <div class="page-inner">
        <div class="page-top"><span>BettaFish Intelligence / 01</span><span class="rule"></span><span>Bento Briefing</span></div>
        <header class="hero">
          <div class="eyebrow">Public Opinion Radar · 舆情雷达</div>
          <h1 id="page-1-title">读懂 <em>{_escape(topic)}</em><br>的公共叙事</h1>
          <p class="hero-deck">把分散的公开讨论，整理成一份可以快速阅读、判断和转发的舆情简报。</p>
          <div class="hero-meta"><span>生成 <b>{_escape(generated)}</b></span><span>样本 <b>{mention_count:02d} 条</b></span><span>来源 <b>{_escape(source_label)}</b></span></div>
        </header>
        <div class="bento">
          <article class="card span-7"><span class="card-label">Editor's Note · 编辑按语</span><p class="note-text">本期围绕「{_escape(topic)}」的公开信息共整理 <strong>{mention_count}</strong> 条。整体讨论主导情感为 <strong>{_escape(dominant)}</strong>，热度处于「{_escape(heat.get("heat_level", "—"))}」区间。建议先关注声量最大的渠道，再回到原始信源核验判断。</p></article>
          <article class="card span-5 glow"><span class="card-label">Heat Index · 热度指数</span><div class="big-num">{_escape(heat.get("heat_score", "—"))}</div><p class="card-note">{_escape(heat.get("heat_level", "—"))} 热度 · 综合声量与互动强度</p></article>
          <article class="card span-3 stat"><span class="card-label">Mentions</span><b class="stat-num">{mention_count}</b><span class="card-note">公开提及总量</span></article>
          <article class="card span-3 stat"><span class="card-label">Dominant · 主导情感</span><b class="stat-num grad-text">{_escape(dominant)}</b><span class="card-note">当前样本主导情绪</span></article>
          <article class="card span-3 stat"><span class="card-label">Neutral Share</span><b class="stat-num">{neu_pct}%</b><span class="card-note">中性讨论占比</span></article>
          <article class="card span-3 stat alert"><span class="card-label">Watchlist</span><b class="stat-num">{risk_count:02d}</b><span class="card-note">待跟进风险信号</span></article>
          <article class="card span-3 mini"><b>{configured_english}</b><span>English RSS feeds</span></article>
          <article class="card span-3 mini"><b>{configured_chinese}</b><span>中文 RSS feeds</span></article>
          <article class="card span-3 mini"><b>{successful_total}</b><span>Feeds connected</span></article>
          <article class="card span-3 mini"><b>{configured_total}</b><span>Feeds configured</span></article>
          <article class="card span-12 quiet"><p class="coverage-note">{_escape(coverage_note)}</p></article>
        </div>
        <div class="pager"><button type="button" data-next="page-2">下一页&nbsp;&nbsp;→</button><span class="pager-center"><b>01</b> / 04</span><button type="button" data-next="page-2">Explore signals&nbsp;&nbsp;→</button></div>
      </div>
    </section>

    <section id="page-2" class="page" aria-labelledby="page-2-title">
      <div class="page-inner">
        <div class="page-top"><span>BettaFish Intelligence / 02</span><span class="rule"></span><span>Distribution</span></div>
        <div class="section-intro"><div><div class="eyebrow">02 · Signal Map</div><h2 id="page-2-title">情绪与声量，<br><span class="grad-text">在哪里发生</span></h2></div><p>分布比单一数字更重要：它揭示讨论的温度，也揭示讨论的来源。</p></div>
        <div class="bento">
          <article class="card span-5"><div class="card-title"><h3>情感光谱</h3><span>n = {total}</span></div><div class="sentiment-layout"><div class="donut" style="--pos-deg:{pos_deg}deg;--neu-deg:{neu_end_deg}deg" role="img" aria-label="正面 {pos_pct}%，中性 {neu_pct}%，负面 {neg_pct}%"></div><div class="legend"><div class="legend-row"><span><i></i>正面</span><b>{pos} / {pos_pct}%</b></div><div class="legend-row"><span><i class="neutral"></i>中性</span><b>{neu} / {neu_pct}%</b></div><div class="legend-row"><span><i class="negative"></i>负面</span><b>{neg} / {neg_pct}%</b></div><p class="muted" style="margin:8px 0 0;font-size:12px;">主导：{_escape(dominant)}</p></div></div></article>
          <article class="card span-7"><div class="card-title"><h3>渠道分布</h3><span>Share of mentions</span></div><div class="channel-list">{channel_bars}</div></article>
          <article class="card span-12"><div class="card-title"><h3>渠道明细</h3><span>Engagement overview</span></div><table class="data-table"><thead><tr><th>平台 / 来源</th><th>提及</th><th>占比</th><th>平均互动</th></tr></thead><tbody>{channel_rows}</tbody></table></article>
        </div>
        <div class="pager"><button type="button" data-next="page-1">←&nbsp;&nbsp;上一页</button><span class="pager-center"><b>02</b> / 04</span><button type="button" data-next="page-3">下一页&nbsp;&nbsp;→</button></div>
      </div>
    </section>

    <section id="page-3" class="page" aria-labelledby="page-3-title">
      <div class="page-inner">
        <div class="page-top"><span>BettaFish Intelligence / 03</span><span class="rule"></span><span>Narrative &amp; Risk</span></div>
        <div class="section-intro"><div><div class="eyebrow">03 · Narrative</div><h2 id="page-3-title">讨论正在<br><span class="grad-text">说什么</span></h2></div><p>从趋势、关键词和风险命中词中，寻找值得进一步验证的叙事线索。</p></div>
        <div class="bento">
          <article class="card span-7"><div class="card-title"><h3>声量趋势</h3><span>Last 7 observations</span></div><div class="trend-list">{trend_bars}</div><blockquote class="pull-quote">趋势是线索，不是结论；回到信源，才是判断的开始。</blockquote></article>
          <article class="card span-5"><div class="card-title"><h3>热门关键词</h3><span>Top signals</span></div><div class="keyword-cloud">{chips}</div></article>
          <article class="card span-12"><div class="card-title"><h3>风险提示</h3><span>Watchlist · {risk_count:02d}</span></div><ul class="risk-list">{risk_items}</ul></article>
        </div>
        <div class="pager"><button type="button" data-next="page-2">←&nbsp;&nbsp;上一页</button><span class="pager-center"><b>03</b> / 04</span><button type="button" data-next="page-4">下一页&nbsp;&nbsp;→</button></div>
      </div>
    </section>

    <section id="page-4" class="page" aria-labelledby="page-4-title">
      <div class="page-inner">
        <div class="page-top"><span>BettaFish Intelligence / 04</span><span class="rule"></span><span>Evidence</span></div>
        <div class="section-intro"><div><div class="eyebrow">04 · Evidence Desk</div><h2 id="page-4-title">要点与信源，<br><span class="grad-text">留给下一步</span></h2></div><p>每一条摘要都应当可以被追溯。点击标题打开原始来源，继续完成核验。</p></div>
        <div class="bento">
          <article class="card span-12"><div class="card-title"><h3>编辑要点</h3><span>Key findings</span></div><ol class="finding-list">{finding_items}</ol></article>
          <article class="card span-12"><div class="card-title"><h3>信源列表</h3><span>Click to verify · {mention_count} items</span></div><ul class="source-list">{source_items}</ul>{source_archive}</article>
        </div>
        <footer style="display:flex;justify-content:space-between;gap:20px;margin-top:48px;color:var(--muted);font-family:var(--mono);font-size:10px;letter-spacing:.06em;"><span>BettaFish-skill / Query + Media + Insight</span><span>END OF BRIEF</span></footer>
        <div class="pager"><button type="button" data-next="page-3">←&nbsp;&nbsp;上一页</button><span class="pager-center"><b>04</b> / 04</span><button type="button" data-next="page-1">Back to top&nbsp;&nbsp;↗</button></div>
      </div>
    </section>
  </main>
  <script>
    (() => {{
      const pages = [...document.querySelectorAll('.page')];
      const links = [...document.querySelectorAll('[data-page]')];
      const order = pages.map(page => page.id);

      function goToPage(pageId, updateHash = true) {{
        const target = document.getElementById(pageId) || pages[0];
        pages.forEach(page => page.classList.toggle('active', page === target));
        links.forEach(link => {{
          const selected = link.dataset.page === target.id;
          link.classList.toggle('active', selected);
          link.setAttribute('aria-current', selected ? 'page' : 'false');
        }});
        if (updateHash) history.replaceState(null, '', '#' + target.id);
        window.scrollTo({{ top: 0, behavior: 'smooth' }});
      }}

      links.forEach(link => link.addEventListener('click', event => {{
        event.preventDefault();
        goToPage(link.dataset.page);
      }}));
      document.querySelectorAll('[data-next]').forEach(button => button.addEventListener('click', () => goToPage(button.dataset.next)));
      document.addEventListener('keydown', event => {{
        if (event.key !== 'ArrowLeft' && event.key !== 'ArrowRight') return;
        const current = order.findIndex(id => document.getElementById(id).classList.contains('active'));
        const next = event.key === 'ArrowRight' ? Math.min(current + 1, order.length - 1) : Math.max(current - 1, 0);
        if (next !== current) goToPage(order[next]);
      }});
      const initial = location.hash.slice(1);
      if (order.includes(initial)) goToPage(initial, false);
      window.goToPage = goToPage;
    }})();
  </script>
</body>
</html>
'''

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

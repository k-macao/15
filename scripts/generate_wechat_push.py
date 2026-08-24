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
    color = {"positive": "#64ffda", "negative": "#ff6b6b", "neutral": "#8892b0"}.get(label, "#8892b0")
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
      --paper: #e9ebed;           /* 整体浅灰背景 */
      --card: #f7f8f9;            /* 卡片纸面 */
      --ink: #0b0c0d;             /* 正文黑 */
      --ink-soft: #33373b;
      --muted: #6d7378;
      --hair: rgba(11, 12, 13, .16);   /* 发丝线 */
      --hair-strong: rgba(11, 12, 13, .32);
      --neon: #c6ff00;            /* 荧光绿 */
      --neon-dim: #a8d900;
      --neg: #0b0c0d;
      --neu: #b3b8bc;
      --serif: 'Noto Serif SC', source-han-serif-sc, Georgia, serif;
      --sans: 'Noto Sans SC', source-han-sans-sc, -apple-system, 'PingFang SC', 'Microsoft YaHei', sans-serif;
      --mono: 'IBM Plex Mono', 'JetBrains Mono', ui-monospace, 'SFMono-Regular', Consolas, monospace;
    }
    * { box-sizing: border-box; }
    html { scroll-behavior: smooth; background: var(--paper); }
    body {
      min-width: 320px; margin: 0; color: var(--ink); background: var(--paper);
      font-family: var(--sans); font-size: 13px; line-height: 1.75;
      -webkit-font-smoothing: antialiased; text-rendering: optimizeLegibility;
    }
    a { color: var(--ink); text-decoration: none; border-bottom: 1px solid var(--hair-strong); }
    a:hover { background: var(--ink); color: var(--neon); border-bottom-color: var(--ink); }
    h1, h2, h3, p { margin-top: 0; }
    .hl { padding: 1px 6px; background: var(--ink); color: var(--neon); font-weight: 600; }

    /* ---- 顶部黑条 ---- */
    .chrome { position: sticky; top: 0; z-index: 30; background: var(--ink); color: var(--neon); }
    .chrome-inner { display: flex; align-items: center; justify-content: space-between; gap: 14px;
      max-width: 680px; margin: 0 auto; padding: 9px 18px;
      font-family: var(--mono); font-size: 9px; letter-spacing: .24em; text-transform: uppercase; }
    .chrome b { font-weight: 500; }
    .chrome i { font-style: normal; opacity: .55; }

    .wrap { max-width: 680px; margin: 0 auto; padding: 22px 18px 60px; }
    section { scroll-margin-top: 52px; }

    /* ---- 封面头版（黑底） ---- */
    .hero { position: relative; margin-bottom: 26px; padding: 34px 26px 28px; background: var(--ink); color: #e8eaed; overflow: hidden; }
    .hero::before { content: ''; position: absolute; inset: 10px; border: 1px solid rgba(198,255,0,.25); pointer-events: none; }
    .hero > * { position: relative; }
    .kicker { display: flex; align-items: center; gap: 10px; margin-bottom: 18px;
      color: var(--neon); font-family: var(--mono); font-size: 9px; letter-spacing: .3em; text-transform: uppercase; }
    .kicker::after { content: ''; flex: 1; height: 1px; background: rgba(198,255,0,.35); }
    .hero h1 { margin: 0 0 10px; color: var(--neon); font-family: var(--serif); font-size: 27px; font-weight: 700; line-height: 1.2; letter-spacing: .01em; }
    .hero-sub { margin: 0 0 18px; color: rgba(232,234,237,.78); font-family: var(--serif); font-size: 13px; line-height: 1.7; }
    .hero-tags { display: flex; flex-wrap: wrap; gap: 8px; }
    .hero-tags span { padding: 4px 10px; border: 1px solid rgba(198,255,0,.4); color: var(--neon);
      font-family: var(--mono); font-size: 9px; letter-spacing: .14em; text-transform: uppercase; }
    .hero-tags b { font-weight: 500; }
    .hero-foot { display: flex; justify-content: space-between; align-items: flex-end; gap: 14px; margin-top: 22px; padding-top: 14px; border-top: 1px solid rgba(198,255,0,.22); }
    .hero-foot small { color: rgba(232,234,237,.55); font-family: var(--mono); font-size: 8.5px; letter-spacing: .18em; text-transform: uppercase; }
    .heat-block { text-align: right; }
    .heat-block b { display: block; color: var(--neon); font-family: var(--serif); font-size: 30px; font-weight: 700; line-height: 1; }
    .heat-block span { display: block; margin-top: 4px; color: rgba(232,234,237,.5); font-family: var(--mono); font-size: 8px; letter-spacing: .24em; }

    /* ---- 章节标题：黑底荧光绿 ---- */
    .section-head { display: flex; align-items: center; gap: 10px; margin: 34px 0 6px; }
    .section-num { flex: none; padding: 3px 8px; background: var(--ink); color: var(--neon);
      font-family: var(--mono); font-size: 9px; letter-spacing: .18em; }
    .section-head h2 { margin: 0; display: inline-block; padding: 3px 10px; background: var(--ink); color: var(--neon);
      font-family: var(--serif); font-size: 15px; font-weight: 700; letter-spacing: .04em; }
    .section-head::after { content: ''; flex: 1; height: 1px; background: var(--hair-strong); }
    .section-lede { margin: 0 0 14px; color: var(--muted); font-size: 11.5px; }

    /* ---- 卡片：纸面 + 发丝线 ---- */
    .panel { margin-bottom: 14px; padding: 18px 20px; border: 1px solid var(--hair); background: var(--card); }
    .panel-title { display: flex; align-items: baseline; justify-content: space-between; gap: 12px; margin-bottom: 14px; padding-bottom: 9px; border-bottom: 1px solid var(--hair); }
    .panel-title h3 { margin: 0; font-family: var(--serif); font-size: 13.5px; font-weight: 700; }
    .panel-title span { color: var(--muted); font-family: var(--mono); font-size: 8.5px; letter-spacing: .12em; text-transform: uppercase; }

    .note { position: relative; margin-bottom: 14px; padding: 18px 20px 18px 24px; border: 1px solid var(--hair); background: var(--card); }
    .note::before { content: ''; position: absolute; inset: 0 auto 0 0; width: 4px; background: var(--neon); }
    .note h3 { margin-bottom: 8px; font-family: var(--serif); font-size: 13.5px; font-weight: 700; }
    .note p { margin-bottom: 0; color: var(--ink-soft); font-size: 12.5px; }
    .note strong { padding: 1px 6px; background: var(--ink); color: var(--neon); font-weight: 600; }
    .snapshot { display: flex; flex-direction: column; justify-content: center; margin-bottom: 14px; padding: 16px 20px; background: var(--ink); color: var(--neon); }
    .snapshot-label { color: rgba(198,255,0,.6); font-family: var(--mono); font-size: 8.5px; letter-spacing: .2em; text-transform: uppercase; }
    .snapshot strong { display: block; margin: 8px 0 2px; font-family: var(--serif); font-size: 24px; font-weight: 700; line-height: 1; }
    .snapshot small { color: rgba(232,234,237,.6); font-size: 10.5px; }

    .coverage-strip { display: grid; grid-template-columns: repeat(4, 1fr); gap: 1px; margin-top: 14px; border: 1px solid var(--hair-strong); background: var(--hair-strong); }
    .coverage-item { padding: 12px 12px; background: var(--card); }
    .coverage-item strong { display: block; font-family: var(--serif); font-size: 18px; font-weight: 700; line-height: 1; }
    .coverage-item span { display: block; margin-top: 6px; color: var(--muted); font-family: var(--mono); font-size: 7.5px; letter-spacing: .06em; text-transform: uppercase; }
    .coverage-note { margin: 10px 2px 0; color: var(--muted); font-size: 11px; }

    .metric-grid { display: grid; grid-template-columns: repeat(2, 1fr); gap: 10px; margin-top: 16px; }
    .metric { position: relative; min-height: 86px; padding: 14px 16px; border: 1px solid var(--hair); background: var(--card); }
    .metric::before { content: ''; position: absolute; inset: 0 0 auto 0; height: 3px; background: var(--ink); }
    .metric.accent::before, .metric.alert::before { background: var(--neon); }
    .metric-label { color: var(--muted); font-family: var(--mono); font-size: 8px; letter-spacing: .14em; text-transform: uppercase; }
    .metric-value { display: block; margin: 8px 0 1px; font-family: var(--serif); font-size: 22px; font-weight: 700; line-height: 1; color: var(--ink); }
    .metric-note { color: var(--muted); font-size: 10.5px; }
    .metric.accent .metric-value { padding: 0 6px; display: inline-block; background: var(--ink); color: var(--neon); }
    .metric.alert .metric-value { padding: 0 6px; display: inline-block; background: var(--ink); color: var(--neon); }

    /* ---- 情感 / 渠道 ---- */
    .sentiment-layout { display: grid; grid-template-columns: 128px 1fr; align-items: center; gap: 18px; }
    .donut { width: 128px; height: 128px; display: grid; place-items: center; border-radius: 50%;
      background: conic-gradient(var(--neon) 0deg var(--pos-deg), var(--neu) var(--pos-deg) var(--neu-deg), var(--ink) var(--neu-deg) 360deg);
      box-shadow: 0 0 0 1px var(--hair); }
    .donut::after { content: ''; width: 82px; height: 82px; border-radius: 50%; background: var(--card); box-shadow: inset 0 0 0 1px var(--hair); }
    .legend { display: grid; gap: 8px; }
    .legend-row { display: flex; align-items: center; justify-content: space-between; gap: 10px; color: var(--ink-soft); font-size: 11.5px; }
    .legend-row b { color: var(--ink); font-family: var(--mono); font-size: 10px; font-weight: 600; }
    .legend-row i { width: 9px; height: 9px; margin-right: 7px; display: inline-block; background: var(--neon); box-shadow: 0 0 0 1px var(--hair-strong); }
    .legend-row i.neutral { background: var(--neu); } .legend-row i.negative { background: var(--ink); }

    .channel-list { display: grid; gap: 12px; }
    .channel-label { display: flex; justify-content: space-between; margin-bottom: 5px; color: var(--ink-soft); font-size: 11.5px; font-weight: 500; }
    .channel-label b, .channel-share { font-family: var(--mono); font-size: 9.5px; font-weight: 600; }
    .channel-label b { padding: 0 5px; background: var(--ink); color: var(--neon); }
    .channel-row { display: grid; grid-template-columns: minmax(60px, .4fr) 1fr 38px; align-items: center; column-gap: 8px; }
    .channel-row .channel-label { grid-column: 1 / -1; }
    .channel-row .channel-track { grid-column: 1 / 3; }
    .channel-row .channel-share { grid-column: 3; }
    .channel-track, .trend-track { height: 8px; overflow: hidden; background: #dcdfe2; box-shadow: inset 0 0 0 1px var(--hair); }
    .channel-track i, .trend-track i { display: block; height: 100%; background: var(--neon); box-shadow: inset 0 0 0 1px rgba(11,12,13,.4);
      transform-origin: left; animation: grow .8s cubic-bezier(.16,1,.3,1) both; }
    .channel-share { text-align: right; color: var(--muted); }

    .data-table { width: 100%; margin-top: 4px; border-collapse: collapse; font-size: 11.5px; }
    .data-table th { color: var(--muted); font-family: var(--mono); font-size: 8px; font-weight: 600; letter-spacing: .12em; text-align: left; text-transform: uppercase; }
    .data-table th, .data-table td { padding: 9px 8px; border-bottom: 1px solid var(--hair); }
    .data-table td { color: var(--ink-soft); } .data-table td:first-child { color: var(--ink); font-weight: 600; }

    /* ---- 叙事 ---- */
    .trend-list { display: grid; gap: 11px; }
    .trend-row { display: grid; grid-template-columns: 74px 1fr 26px; align-items: center; gap: 10px; }
    .trend-row time, .trend-row b { color: var(--muted); font-family: var(--mono); font-size: 9px; font-weight: 500; }
    .trend-row b { padding: 0 4px; background: var(--ink); color: var(--neon); font-weight: 600; text-align: center; }
    .pull-quote { margin: 16px 0 0; padding: 12px 16px; border-left: 3px solid var(--neon); background: rgba(198,255,0,.12);
      color: var(--ink-soft); font-family: var(--serif); font-size: 12px; line-height: 1.65; }
    .keyword-cloud { display: flex; flex-wrap: wrap; gap: 7px; }
    .keyword { padding: 4px 10px; background: var(--ink); color: var(--neon); font-size: 11px; font-weight: 500; letter-spacing: .02em; }
    .keyword:nth-child(3n) { background: transparent; color: var(--ink); box-shadow: inset 0 0 0 1px var(--hair-strong); }

    .risk-list, .finding-list, .source-list { margin: 0; padding: 0; list-style: none; }
    .risk-list { display: grid; }
    .risk-item { display: grid; grid-template-columns: 38px 1fr; gap: 11px; padding: 11px 0; border-bottom: 1px solid var(--hair); }
    .risk-item:last-child { border-bottom: 0; }
    .risk-level { align-self: start; padding: 2px 0; font-family: var(--mono); font-size: 8px; font-weight: 700; text-align: center; }
    .risk-high .risk-level { background: var(--ink); color: var(--neon); }
    .risk-medium .risk-level { background: transparent; color: var(--ink); box-shadow: inset 0 0 0 1px var(--hair-strong); }
    .risk-low .risk-level { color: var(--muted); box-shadow: inset 0 0 0 1px var(--hair); }
    .risk-item strong { display: block; color: var(--ink-soft); font-size: 11.5px; font-weight: 500; line-height: 1.6; }
    .risk-item small { display: block; margin-top: 4px; color: var(--muted); font-family: var(--mono); font-size: 8.5px; }

    /* ---- 要点 / 信源 ---- */
    .finding-list li { display: grid; grid-template-columns: 26px 1fr; gap: 12px; align-items: start; padding: 10px 0; border-bottom: 1px solid var(--hair); }
    .finding-list li:last-child { border-bottom: 0; }
    .finding-list li > span { padding: 1px 0; background: var(--ink); color: var(--neon); font-family: var(--mono); font-size: 8.5px; font-weight: 700; text-align: center; }
    .finding-list p { margin: 0; color: var(--ink-soft); font-size: 12px; }
    .source-index { color: var(--muted); font-family: var(--mono); font-size: 9px; font-weight: 600; }
    .source-item { display: grid; grid-template-columns: 26px 1fr auto; gap: 10px; align-items: center; padding: 10px 0; border-bottom: 1px solid var(--hair); }
    .source-item:last-child { border-bottom: 0; }
    .source-item a { display: block; color: var(--ink); font-size: 12px; font-weight: 500; line-height: 1.5; border-bottom: 0; }
    .source-item a:hover { background: var(--ink); color: var(--neon); }
    .source-item small { display: block; margin-top: 3px; color: var(--muted); font-family: var(--mono); font-size: 8.5px; }
    .source-item small em { color: var(--neon-dim); font-style: normal; font-weight: 700; }
    .source-item > span:last-child { white-space: nowrap; }
    .source-archive { margin-top: 14px; border-top: 1px solid var(--hair); }
    .source-archive summary { padding: 12px 0 4px; color: var(--ink); cursor: pointer; font-family: var(--mono); font-size: 9.5px; font-weight: 600; letter-spacing: .08em; list-style: none; }
    .source-archive summary::-webkit-details-marker { display: none; }
    .source-archive summary::before { content: '+ '; color: var(--neon-dim); font-weight: 700; }
    .source-archive[open] summary::before { content: '− '; }
    .meta-badge { display: inline-block; padding: 2px 7px; font-family: var(--mono); font-size: 8px; font-weight: 600; border: 1px solid currentColor; }
    .muted, .empty-state { color: var(--muted); }
    .empty-state { padding: 10px 0; list-style: none; }

    /* ---- 文末落款（黑底） ---- */
    .colophon { margin-top: 30px; padding: 22px 22px 18px; background: var(--ink); color: rgba(232,234,237,.82); }
    .colophon-author { display: flex; align-items: baseline; justify-content: space-between; gap: 14px; padding-bottom: 12px; margin-bottom: 12px; border-bottom: 1px solid rgba(198,255,0,.25); }
    .colophon-author b { color: var(--neon); font-family: var(--serif); font-size: 14px; font-weight: 700; }
    .colophon-author span { color: rgba(232,234,237,.55); font-family: var(--mono); font-size: 8.5px; letter-spacing: .16em; text-transform: uppercase; }
    .colophon p { margin: 0; font-size: 11px; line-height: 1.8; }
    .colophon p em { font-style: normal; padding: 0 4px; background: var(--neon); color: var(--ink); font-weight: 600; }
    .colophon-foot { display: flex; justify-content: space-between; gap: 12px; margin-top: 14px; padding-top: 10px; border-top: 1px solid rgba(232,234,237,.14);
      color: rgba(232,234,237,.4); font-family: var(--mono); font-size: 7.5px; letter-spacing: .18em; text-transform: uppercase; }

    .to-top { position: fixed; right: 16px; bottom: 16px; z-index: 40; width: 38px; height: 38px; border: 1px solid var(--neon); background: var(--ink); color: var(--neon);
      font-size: 15px; cursor: pointer; opacity: 0; pointer-events: none; transform: translateY(8px); transition: opacity .25s, transform .25s; }
    .to-top.show { opacity: 1; pointer-events: auto; transform: translateY(0); }

    @keyframes grow { from { transform: scaleX(0); } to { transform: scaleX(1); } }
    @media (prefers-reduced-motion: reduce) { *, *::before, *::after { animation-duration: .01ms !important; transition-duration: .01ms !important; } html { scroll-behavior: auto; } }
    @media (max-width: 560px) {
      .wrap { padding: 16px 12px 48px; }
      .hero { padding: 26px 18px 22px; }
      .hero h1 { font-size: 23px; }
      .coverage-strip { grid-template-columns: repeat(2, 1fr); }
      .sentiment-layout { grid-template-columns: 1fr; justify-items: center; } .legend { width: 100%; }
      .source-item { grid-template-columns: 22px 1fr; } .source-item > span:last-child { grid-column: 2; justify-self: start; }
      .trend-row { grid-template-columns: 64px 1fr 22px; gap: 7px; }
      .colophon-author { flex-direction: column; gap: 4px; }
    }
    '''

    return f'''<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <meta name="theme-color" content="#0b0c0d">
  <title>章鱼 AI 全景分析</title>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Noto+Serif+SC:wght@500;700&family=Noto+Sans+SC:wght@400;500;700&family=IBM+Plex+Mono:wght@400;500;700&display=swap" rel="stylesheet">
  <style>{css}</style>
</head>
<body>
  <header class="chrome">
    <div class="chrome-inner"><b>OCTOPUS AI</b><i>PANORAMA REPORT</i></div>
  </header>

  <main class="wrap">
    <header class="hero">
      <div class="kicker">Octopus AI Intelligence</div>
      <h1>章鱼 AI 全景分析</h1>
      <p class="hero-sub">全网 AI 调研境内境外数据，由多个大模型混合部署。</p>
      <div class="hero-tags"><span>主题 <b>{_escape(topic)}</b></span><span>样本 <b>{mention_count:02d} 条</b></span><span>来源 <b>{_escape(source_label)}</b></span></div>
      <div class="hero-foot">
        <small>Multi-model · Cross-border · Panorama</small>
        <div class="heat-block"><b>{_escape(heat.get("heat_score", "—"))}</b><span>HEAT INDEX</span></div>
      </div>
    </header>

    <section id="sec-overview" aria-labelledby="sec-overview-title">
      <div class="section-head"><span class="section-num">01</span><h2 id="sec-overview-title">先看结论</h2></div>
      <p class="section-lede">一页掌握本次抓取的规模、主导情感与需要继续观察的信号。</p>
      <article class="note"><h3>编辑按语</h3><p>本期围绕「{_escape(topic)}」的公开信息共整理 <strong>{mention_count}</strong> 条。整体讨论主导情感为 <strong>{_escape(dominant)}</strong>，热度处于 <strong>{_escape(heat.get("heat_level", "—"))}</strong> 区间。建议先关注声量最大的渠道，再回到原始信源核验判断。</p></article>
      <aside class="snapshot"><span class="snapshot-label">Dominant sentiment</span><strong>{_escape(dominant)}</strong><small>当前样本中的主导情绪</small></aside>
      <div class="coverage-strip" aria-label="RSS 来源覆盖范围">
        <div class="coverage-item"><strong>{configured_english}</strong><span>English RSS feeds</span></div>
        <div class="coverage-item"><strong>{configured_chinese}</strong><span>中文 RSS feeds</span></div>
        <div class="coverage-item"><strong>{successful_total}</strong><span>feeds connected</span></div>
        <div class="coverage-item"><strong>{configured_total}</strong><span>feeds configured</span></div>
      </div>
      <p class="coverage-note">{_escape(coverage_note)}</p>
      <div class="metric-grid">
        <div class="metric accent"><span class="metric-label">Heat index</span><strong class="metric-value">{_escape(heat.get("heat_score", "—"))}</strong><span class="metric-note">{_escape(heat.get("heat_level", "—"))} 热度</span></div>
        <div class="metric"><span class="metric-label">Mentions</span><strong class="metric-value">{mention_count}</strong><span class="metric-note">公开提及总量</span></div>
        <div class="metric"><span class="metric-label">Neutral share</span><strong class="metric-value">{neu_pct}%</strong><span class="metric-note">中性讨论占比</span></div>
        <div class="metric alert"><span class="metric-label">Watchlist</span><strong class="metric-value">{risk_count:02d}</strong><span class="metric-note">待跟进风险信号</span></div>
      </div>
    </section>

    <section id="sec-signal" aria-labelledby="sec-signal-title">
      <div class="section-head"><span class="section-num">02</span><h2 id="sec-signal-title">情绪与声量分布</h2></div>
      <p class="section-lede">分布比单一数字更重要：它揭示讨论的温度，也揭示讨论的来源。</p>
      <article class="panel"><div class="panel-title"><h3>情感光谱</h3><span>n = {total}</span></div><div class="sentiment-layout"><div class="donut" style="--pos-deg:{pos_deg}deg;--neu-deg:{neu_end_deg}deg" role="img" aria-label="正面 {pos_pct}%，中性 {neu_pct}%，负面 {neg_pct}%"></div><div class="legend"><div class="legend-row"><span><i></i>正面</span><b>{pos} / {pos_pct}%</b></div><div class="legend-row"><span><i class="neutral"></i>中性</span><b>{neu} / {neu_pct}%</b></div><div class="legend-row"><span><i class="negative"></i>负面</span><b>{neg} / {neg_pct}%</b></div><p class="muted" style="margin:6px 0 0;font-size:10.5px;">主导：{_escape(dominant)}</p></div></div></article>
      <article class="panel"><div class="panel-title"><h3>渠道分布</h3><span>share of mentions</span></div><div class="channel-list">{channel_bars}</div></article>
      <article class="panel"><div class="panel-title"><h3>渠道明细</h3><span>engagement overview</span></div><table class="data-table"><thead><tr><th>平台 / 来源</th><th>提及</th><th>占比</th><th>平均互动</th></tr></thead><tbody>{channel_rows}</tbody></table></article>
    </section>

    <section id="sec-narrative" aria-labelledby="sec-narrative-title">
      <div class="section-head"><span class="section-num">03</span><h2 id="sec-narrative-title">讨论正在说什么</h2></div>
      <p class="section-lede">从趋势、关键词和风险命中词中，寻找值得进一步验证的叙事线索。</p>
      <article class="panel"><div class="panel-title"><h3>声量趋势</h3><span>last 7 observations</span></div><div class="trend-list">{trend_bars}</div><blockquote class="pull-quote">趋势是线索，不是结论；回到信源，才是判断的开始。</blockquote></article>
      <article class="panel"><div class="panel-title"><h3>热门关键词</h3><span>top signals</span></div><div class="keyword-cloud">{chips}</div></article>
      <article class="panel"><div class="panel-title"><h3>风险提示</h3><span>watchlist / {risk_count:02d}</span></div><ul class="risk-list">{risk_items}</ul></article>
    </section>

    <section id="sec-evidence" aria-labelledby="sec-evidence-title">
      <div class="section-head"><span class="section-num">04</span><h2 id="sec-evidence-title">要点与信源</h2></div>
      <p class="section-lede">每一条摘要都应当可以被追溯。点击标题打开原始来源，继续完成核验。</p>
      <article class="panel"><div class="panel-title"><h3>编辑要点</h3><span>key findings</span></div><ol class="finding-list">{finding_items}</ol></article>
      <article class="panel"><div class="panel-title"><h3>信源列表</h3><span>click to verify / {mention_count} items</span></div><ul class="source-list">{source_items}</ul>{source_archive}</article>

      <footer class="colophon">
        <div class="colophon-author"><b>作者：章鱼 ai</b><span>仅供参考 · 分析研究</span></div>
        <p>全网境内外为你寻找蛛丝马迹，提供全景视野分析，由多模型协同推理决策。底层所使用的大语言模型（LLM）多模式背后结合使用了多种不同的先进模型，包括但不限于 Claude、ChatGPT、Gemini、Grok、Qwen 以及 Kimi。根据不同的资产管理任务需求，更好地发挥各个模型的优势来提供数据支持！<em>[加油]</em></p>
        <div class="colophon-foot"><span>Octopus AI · Panorama</span><span>{_escape(source_label)} · {_escape(generated)}</span><span>END OF BRIEF</span></div>
      </footer>
    </section>
  </main>

  <button type="button" class="to-top" aria-label="回到顶部">↑</button>
  <script>
    (() => {{
      const toTop = document.querySelector('.to-top');
      if (toTop) {{
        const onScroll = () => toTop.classList.toggle('show', window.scrollY > 480);
        window.addEventListener('scroll', onScroll, {{ passive: true }});
        onScroll();
        toTop.addEventListener('click', () => window.scrollTo({{ top: 0, behavior: 'smooth' }}));
      }}
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
        resp = push_via_pushplus("章鱼 AI 全景分析", result["html"], token)
        print(f"PushPlus response: {resp}")
        code = resp.get("code")
        if code not in (200, "200", 0, "0"):
            return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

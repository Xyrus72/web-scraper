import asyncio
import json
import os
import sys
from typing import List, Dict, Any
from urllib.parse import urlparse

from crawl4ai import (
    AsyncWebCrawler,
    CrawlerRunConfig,
    CacheMode,
    JsonCssExtractionStrategy,
    RateLimiter,
)
from crawl4ai.async_dispatcher import MemoryAdaptiveDispatcher

# On Windows consoles with cp1252 encoding, rich logging may emit Unicode arrows
# and other characters that cause UnicodeEncodeError. Reconfigure stdout/stderr
# to UTF-8 where possible so Crawl4AI's logger can write safely.
if os.name == "nt":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        # If reconfigure is not available, silently ignore and let logging fallback.
        pass


def _is_same_domain(base_url: str, candidate: str) -> bool:
    """
    Keep only links that are on the same domain as the base URL.
    """
    try:
        base = urlparse(base_url)
        link = urlparse(candidate)
    except Exception:
        return False

    if not link.scheme:
        return True  # relative URL, we'll handle later

    return base.netloc == link.netloc


def _normalize_url(base_url: str, link: str) -> str:
    """
    Convert relative URLs to absolute based on base_url.
    """
    from urllib.parse import urljoin

    return urljoin(base_url, link)


def build_product_schema() -> Dict[str, Any]:
    """
    Schema tuned for Shwapno.com style product cards using CSS selectors.

    Notes for shwapno.com:
    - Product tiles consistently include an image ("Picture of ..."), a name line,
      a price line with the BDT symbol, and an "Add to Bag" button.
    - Product containers tend to have CSS classes containing "product".
    - Product name is almost always present in the image alt attribute as well.
    """
    schema: Dict[str, Any] = {
        "name": "Shwapno Products",
        # Match any div/li whose class contains "product" (case-insensitive variants)
        "baseSelector": "div[class*='product'], li[class*='product'], div[class*='Product'], li[class*='Product']",
        "baseFields": [
            {
                "name": "product_url",
                "type": "attribute",
                "attribute": "data-url",
                "default": None,
            }
        ],
        "fields": [
            {
                "name": "name",
                # Prefer the image alt text, which usually contains full product name
                "selector": "img[alt], .product-title, .product-name, h2, h3, a[title]",
                "type": "attribute",
                "attribute": "alt",
                "default": "",
            },
            {
                "name": "price",
                # BDT prices like "৳375 Per Piece"
                "selector": ".price, .product-price, [itemprop='price'], span[class*='price'], div[class*='price']",
                "type": "text",
                "default": "",
            },
            {
                "name": "image_url",
                "selector": "img",
                "type": "attribute",
                "attribute": "src",
                "default": "",
            },
            {
                "name": "product_href",
                "selector": "a[href]",
                "type": "attribute",
                "attribute": "href",
                "default": "",
            },
            {
                "name": "description",
                "selector": ".description, .product-description, p",
                "type": "text",
                "default": "",
            },
        ],
    }
    return schema


async def _discover_links(crawler: AsyncWebCrawler, home_url: str, max_pages: int) -> List[str]:
    """
    Crawl the homepage once to discover internal links, then return up to max_pages URLs
    (including the homepage itself).
    """
    config = CrawlerRunConfig(
        cache_mode=CacheMode.BYPASS,
        # We only need links and lightweight processing for discovery
        prefetch=True,
    )

    result = await crawler.arun(url=home_url, config=config)

    urls: List[str] = []
    seen: set[str] = set()

    def add_url(u: str) -> None:
        normalized = _normalize_url(home_url, u)
        if normalized not in seen and _is_same_domain(home_url, normalized):
            seen.add(normalized)
            urls.append(normalized)

    # Always include the homepage first
    add_url(home_url)

    if result.success:
        # result.links is a dict like {"internal": [...], "external": [...]}
        for category, links in (result.links or {}).items():
            if category != "internal":
                continue
            for link in links:
                href = link.get("href")
                if not href:
                    continue
                add_url(href)
                if len(urls) >= max_pages:
                    break
            if len(urls) >= max_pages:
                break

    # Fallback: if we didn't find enough URLs, just return whatever we have
    return urls[:max_pages]


async def crawl_site_async(home_url: str, max_pages: int = 30) -> List[Dict[str, Any]]:
    """
    Multi-URL crawl using Crawl4AI with JSON CSS extraction (no LLM).

    - Discovers up to `max_pages` internal URLs from the homepage.
    - Uses MemoryAdaptiveDispatcher + RateLimiter for efficient crawling.
    - Extracts products with JsonCssExtractionStrategy.
    """
    schema = build_product_schema()
    extraction_strategy = JsonCssExtractionStrategy(schema, verbose=False)

    run_config = CrawlerRunConfig(
        cache_mode=CacheMode.BYPASS,
        extraction_strategy=extraction_strategy,
        check_robots_txt=True,
        word_count_threshold=0,
    )

    dispatcher = MemoryAdaptiveDispatcher(
        memory_threshold_percent=80.0,
        check_interval=1.0,
        max_session_permit=8,
        rate_limiter=RateLimiter(
            base_delay=(0.5, 1.5),
            max_delay=20.0,
            max_retries=2,
        ),
        monitor=None,
    )

    products: List[Dict[str, Any]] = []

    async with AsyncWebCrawler(verbose=False) as crawler:
        urls = await _discover_links(crawler, home_url, max_pages=max_pages)

        if not urls:
            return []

        # Multi-URL crawl using arun_many + dispatcher
        results = await crawler.arun_many(
            urls=urls,
            config=run_config,
            dispatcher=dispatcher,
        )

        for result in results:
            if not result.success or not result.extracted_content:
                continue

            try:
                extracted = json.loads(result.extracted_content)
            except json.JSONDecodeError:
                continue

            if not isinstance(extracted, list):
                continue

            for item in extracted:
                if not isinstance(item, dict):
                    continue
                item = dict(item)
                # Attach source page for traceability
                item.setdefault("source_page", result.url)
                products.append(item)

    return products


def crawl_site(home_url: str, max_pages: int = 30) -> List[Dict[str, Any]]:
    """
    Synchronous wrapper for Flask or other WSGI frameworks.
    """
    return asyncio.run(crawl_site_async(home_url, max_pages=max_pages))


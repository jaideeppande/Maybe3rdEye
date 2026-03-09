#!/usr/bin/env python3
import asyncio
import json
import re
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import urlparse, urlunparse

import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.linkextractors import LinkExtractor

try:
    import tldextract
except Exception:
    tldextract = None

EMAIL_PATTERN = re.compile(
    r"(?<![A-Z0-9._%+-])[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,63}(?![A-Z0-9._%+-])",
    re.IGNORECASE,
)

HOST_PATTERN = re.compile(r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?(?:\.[a-z0-9-]{1,63})+$", re.IGNORECASE)

NON_HTML_EXTENSIONS = {
    "jpg",
    "jpeg",
    "png",
    "gif",
    "bmp",
    "svg",
    "webp",
    "ico",
    "pdf",
    "zip",
    "rar",
    "7z",
    "tar",
    "gz",
    "mp3",
    "wav",
    "mp4",
    "avi",
    "mov",
    "wmv",
    "mkv",
    "css",
    "js",
    "json",
    "xml",
    "csv",
    "doc",
    "docx",
    "xls",
    "xlsx",
    "ppt",
    "pptx",
    "exe",
    "msi",
    "dmg",
    "woff",
    "woff2",
    "ttf",
    "eot",
    "otf",
}


def emit(event: dict[str, Any]) -> None:
    sys.stdout.write(json.dumps(event, ensure_ascii=True) + "\n")
    sys.stdout.flush()


def normalize_email(candidate: str) -> str | None:
    email = candidate.strip().rstrip(".,;:)]>").lower()
    if not email or len(email) > 254 or ".." in email:
        return None

    parts = email.split("@")
    if len(parts) != 2:
        return None

    local_part, domain_part = parts
    if not local_part or not domain_part:
        return None
    if domain_part.startswith(".") or domain_part.endswith("."):
        return None
    if "." not in domain_part:
        return None

    return email


def normalize_url(raw_url: str) -> str | None:
    value = (raw_url or "").strip()
    if not value:
        return None

    if not value.startswith(("http://", "https://")):
        value = "https://" + value

    parsed = urlparse(value)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return None

    cleaned = parsed._replace(fragment="")
    return urlunparse(cleaned)


def derive_target_domain(seed_urls: list[str]) -> str:
    for seed_url in seed_urls:
        parsed = urlparse(seed_url)
        host = (parsed.hostname or "").strip().lower()
        if not host:
            continue

        if tldextract is not None:
            extracted = tldextract.extract(host)
            if extracted.domain and extracted.suffix:
                return f"{extracted.domain}.{extracted.suffix}".lower()

        parts = host.split(".")
        if len(parts) >= 2:
            return ".".join(parts[-2:]).lower()

        return host

    return ""


def normalize_host(value: str, target_domain: str) -> str | None:
    candidate = (value or "").strip().lower()
    if not candidate:
        return None

    if "//" in candidate:
        candidate = (urlparse(candidate).hostname or "").lower()
    else:
        if candidate.startswith("*."):
            candidate = candidate[2:]
        candidate = candidate.split("/")[0]

        if ":" in candidate:
            prefix, suffix = candidate.rsplit(":", 1)
            if suffix.isdigit():
                candidate = prefix

    if not candidate or not HOST_PATTERN.match(candidate):
        return None

    if target_domain:
        if candidate != target_domain and not candidate.endswith(f".{target_domain}"):
            return None

    return candidate


def dedupe_seed_urls(raw_urls: list[str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []

    for raw in raw_urls:
        normalized = normalize_url(raw)
        if not normalized:
            continue

        if normalized in seen:
            continue

        seen.add(normalized)
        ordered.append(normalized)

    return ordered


@dataclass
class HarvesterResult:
    emails: set[str] = field(default_factory=set)
    hosts: set[str] = field(default_factory=set)


class TheHarvesterAdapter:
    DEFAULT_SOURCES = (
        "crtsh",
        "rapiddns",
        "waybackarchive",
        "commoncrawl",
        "thc",
        "duckduckgo",
    )

    def __init__(
        self,
        target_domain: str,
        requested_sources: list[str],
        limit: int,
        proxy: bool,
        source_timeout_seconds: int,
    ) -> None:
        self.target_domain = target_domain.strip().lower()
        self.requested_sources = [source.strip().lower() for source in requested_sources if source.strip()]
        self.limit = max(1, int(limit))
        self.proxy = proxy
        self.source_timeout_seconds = max(5, int(source_timeout_seconds))

    @staticmethod
    def _ensure_repo_path() -> None:
        this_file = Path(__file__).resolve()
        candidates: list[Path] = []

        for parent in [this_file.parent, *this_file.parents]:
            candidates.append(parent / "third_party" / "theHarvester")
            candidates.append(parent / "external" / "theHarvester")

        for candidate in candidates:
            package_init = candidate / "theHarvester" / "__init__.py"
            if package_init.exists():
                sys.path.insert(0, str(candidate))
                return

    @staticmethod
    async def _maybe_await(value: Any) -> Any:
        if asyncio.iscoroutine(value):
            return await value
        return value

    async def _collect_sequence(self, engine: Any, method_name: str) -> list[Any]:
        if not hasattr(engine, method_name):
            return []

        method = getattr(engine, method_name)
        result = await self._maybe_await(method())
        if not result:
            return []

        if isinstance(result, (list, tuple, set)):
            return list(result)

        return []

    async def run(self) -> HarvesterResult:
        if not self.target_domain:
            emit({"type": "log", "message": "theHarvester skipped: target domain could not be derived."})
            return HarvesterResult()

        try:
            from theHarvester.discovery import commoncrawl, crtsh, duckduckgosearch, rapiddns, thc, waybackarchive
        except ModuleNotFoundError:
            self._ensure_repo_path()
            try:
                from theHarvester.discovery import commoncrawl, crtsh, duckduckgosearch, rapiddns, thc, waybackarchive
            except Exception as exc:
                emit({"type": "log", "message": f"theHarvester import failed: {exc}"})
                return HarvesterResult()
        except Exception as exc:
            emit({"type": "log", "message": f"theHarvester import failed: {exc}"})
            return HarvesterResult()

        builders = {
            "crtsh": lambda: crtsh.SearchCrtsh(self.target_domain),
            "rapiddns": lambda: rapiddns.SearchRapidDns(self.target_domain),
            "waybackarchive": lambda: waybackarchive.SearchWaybackarchive(self.target_domain),
            "commoncrawl": lambda: commoncrawl.SearchCommoncrawl(self.target_domain),
            "thc": lambda: thc.SearchThc(self.target_domain),
            "duckduckgo": lambda: duckduckgosearch.SearchDuckDuckGo(f"site:{self.target_domain}", self.limit),
        }

        if self.requested_sources:
            sources = [source for source in self.requested_sources if source in builders]
        else:
            sources = list(self.DEFAULT_SOURCES)

        if not sources:
            emit({"type": "log", "message": "theHarvester skipped: no valid sources selected."})
            return HarvesterResult()

        result = HarvesterResult()

        for source in sources:
            emit({"type": "log", "message": f"theHarvester source start: {source}"})

            engine = builders[source]()
            process_method = getattr(engine, "process", None)
            if process_method is None:
                emit({"type": "log", "message": f"theHarvester source has no process() method: {source}"})
                continue

            try:
                await asyncio.wait_for(self._maybe_await(process_method(self.proxy)), timeout=self.source_timeout_seconds)
            except asyncio.TimeoutError:
                emit({"type": "log", "message": f"theHarvester source timeout: {source}"})
                continue
            except Exception as exc:
                emit({"type": "log", "message": f"theHarvester source error ({source}): {exc}"})
                continue

            emails = await self._collect_sequence(engine, "get_emails")
            hosts = await self._collect_sequence(engine, "get_hostnames")

            email_count = 0
            for value in emails:
                normalized = normalize_email(str(value))
                if not normalized:
                    continue
                result.emails.add(normalized)
                email_count += 1

            host_count = 0
            for value in hosts:
                normalized_host = normalize_host(str(value), self.target_domain)
                if not normalized_host:
                    continue
                result.hosts.add(normalized_host)
                host_count += 1

            emit(
                {
                    "type": "log",
                    "message": (
                        f"theHarvester source done: {source} "
                        f"(emails={email_count}, hosts={host_count})"
                    ),
                }
            )

        return result


class EmailSpider(scrapy.Spider):
    name = "email_scrape_worker"

    def __init__(self, config: dict[str, Any], *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)

        seed_urls = [str(url).strip() for url in config.get("seed_urls", []) if str(url).strip()]
        if not seed_urls:
            raise ValueError("At least one seed URL is required.")

        self.start_urls = seed_urls
        self.same_host_only = bool(config.get("same_host_only", True))
        self.max_links_per_page = int(config.get("max_links_per_page", 200))

        self.allowed_hosts = {
            parsed.hostname.lower()
            for parsed in (urlparse(url) for url in self.start_urls)
            if parsed.hostname
        }

        self.extractor = LinkExtractor(
            unique=True,
            deny_extensions=list(NON_HTML_EXTENSIONS),
        )

        self.started_at = time.time()
        self.processed_pages = 0
        self.failed_pages = 0
        self.total_email_hits = 0
        self.unique_emails: set[str] = set()

    def parse(self, response: scrapy.http.Response):
        self.processed_pages += 1

        content_type_raw = response.headers.get(b"Content-Type", b"").decode("latin1").lower()
        if "html" not in content_type_raw:
            self.failed_pages += 1
            emit(
                {
                    "type": "log",
                    "message": f"Skipped non-HTML response {response.url} ({content_type_raw or 'unknown'})",
                }
            )
            self.emit_progress(response.url)
            return

        page_unique: set[str] = set()
        body_text = response.text or ""
        for match in EMAIL_PATTERN.finditer(body_text):
            email = normalize_email(match.group(0))
            if not email or email in page_unique:
                continue

            page_unique.add(email)
            self.total_email_hits += 1
            self.unique_emails.add(email)
            emit({"type": "email", "email": email, "source_url": response.url})

        queued = 0
        for link in self.extractor.extract_links(response):
            if queued >= self.max_links_per_page:
                break

            parsed = urlparse(link.url)
            if parsed.scheme not in {"http", "https"}:
                continue

            hostname = parsed.hostname
            if self.same_host_only:
                if not hostname or hostname.lower() not in self.allowed_hosts:
                    continue

            queued += 1
            yield response.follow(link.url, callback=self.parse, errback=self.on_error)

        self.emit_progress(response.url)

    def on_error(self, failure):
        self.failed_pages += 1

        request_url = ""
        request = getattr(failure, "request", None)
        if request is not None and getattr(request, "url", None):
            request_url = request.url

        error_text = str(failure.value) if getattr(failure, "value", None) else str(failure)
        emit({"type": "log", "message": f"Request failed {request_url}: {error_text}"})
        self.emit_progress(request_url)

    def closed(self, reason: str):
        duration_seconds = max(0.0, time.time() - self.started_at)
        emit(
            {
                "type": "summary",
                "reason": reason,
                "processed_pages": self.processed_pages,
                "failed_pages": self.failed_pages,
                "unique_email_count": len(self.unique_emails),
                "total_email_hits": self.total_email_hits,
                "duration_seconds": duration_seconds,
            }
        )

    def emit_progress(self, current_url: str):
        stats = self.crawler.stats.get_stats() if self.crawler and self.crawler.stats else {}
        enqueued = int(stats.get("scheduler/enqueued", 0) or 0)
        dequeued = int(stats.get("scheduler/dequeued", 0) or 0)
        queue_length = max(0, enqueued - dequeued)

        emit(
            {
                "type": "progress",
                "processed_pages": self.processed_pages,
                "failed_pages": self.failed_pages,
                "queue_length": queue_length,
                "unique_email_count": len(self.unique_emails),
                "total_email_hits": self.total_email_hits,
                "current_url": current_url,
            }
        )


def main() -> int:
    try:
        raw_input = sys.stdin.read()
        config = json.loads(raw_input) if raw_input.strip() else {}
    except json.JSONDecodeError as exc:
        emit({"type": "log", "message": f"Invalid config JSON: {exc}"})
        return 2

    seed_urls = dedupe_seed_urls([str(url).strip() for url in config.get("seed_urls", []) if str(url).strip()])
    if not seed_urls:
        emit({"type": "log", "message": "No seed URLs were provided."})
        return 2

    max_pages = max(1, int(config.get("max_pages", 250)))
    max_concurrency = max(1, int(config.get("max_concurrency", 6)))
    timeout_seconds = max(1, int(config.get("timeout_seconds", 15)))
    max_depth = max(1, int(config.get("max_depth", 4)))
    download_delay_ms = max(0, int(config.get("download_delay_ms", 0)))
    same_host_only = bool(config.get("same_host_only", True))
    max_links_per_page = max(1, int(config.get("max_links_per_page", 200)))

    enable_theharvester = bool(config.get("enable_theharvester", False))
    theharvester_sources = [str(source).strip().lower() for source in config.get("theharvester_sources", [])]
    theharvester_source_timeout = max(5, int(config.get("theharvester_source_timeout_seconds", 30)))
    theharvester_seed_cap = max(1, int(config.get("theharvester_seed_cap", 250)))

    target_domain = derive_target_domain(seed_urls)

    if enable_theharvester:
        adapter = TheHarvesterAdapter(
            target_domain=target_domain,
            requested_sources=theharvester_sources,
            limit=max_pages,
            proxy=False,
            source_timeout_seconds=theharvester_source_timeout,
        )

        harvest_result = asyncio.run(adapter.run())

        for email in sorted(harvest_result.emails):
            emit({"type": "email", "email": email, "source_url": "theharvester://enrichment"})

        host_seed_urls = [f"https://{host}" for host in sorted(harvest_result.hosts)]
        if len(host_seed_urls) > theharvester_seed_cap:
            host_seed_urls = host_seed_urls[:theharvester_seed_cap]

        seed_urls = dedupe_seed_urls(seed_urls + host_seed_urls)

        emit(
            {
                "type": "log",
                "message": (
                    f"theHarvester enrichment added {len(harvest_result.emails)} email(s) "
                    f"and {len(host_seed_urls)} host seed(s)."
                ),
            }
        )

    settings: dict[str, Any] = {
        "LOG_ENABLED": False,
        "ROBOTSTXT_OBEY": False,
        "TELNETCONSOLE_ENABLED": False,
        "COOKIES_ENABLED": False,
        "RETRY_ENABLED": True,
        "RETRY_TIMES": 2,
        "CONCURRENT_REQUESTS": max_concurrency,
        "CONCURRENT_REQUESTS_PER_DOMAIN": max_concurrency,
        "DOWNLOAD_TIMEOUT": timeout_seconds,
        "DOWNLOAD_DELAY": download_delay_ms / 1000.0,
        "RANDOMIZE_DOWNLOAD_DELAY": True,
        "DEPTH_LIMIT": max_depth,
        "DEPTH_PRIORITY": 1,
        "CLOSESPIDER_PAGECOUNT": max_pages,
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": max(0.05, download_delay_ms / 1000.0),
        "AUTOTHROTTLE_MAX_DELAY": 10.0,
        "AUTOTHROTTLE_TARGET_CONCURRENCY": max(1.0, min(8.0, float(max_concurrency) / 2.0)),
        "SCHEDULER_MEMORY_QUEUE": "scrapy.squeues.FifoMemoryQueue",
        "SCHEDULER_DISK_QUEUE": "scrapy.squeues.PickleFifoDiskQueue",
        "DUPEFILTER_CLASS": "scrapy.dupefilters.RFPDupeFilter",
    }

    emit(
        {
            "type": "log",
            "message": (
                f"Scrapy worker started. Seeds={len(seed_urls)}, MaxPages={max_pages}, "
                f"Concurrency={max_concurrency}, Timeout={timeout_seconds}s, "
                f"Depth={max_depth}, Delay={download_delay_ms}ms, theHarvester={'on' if enable_theharvester else 'off'}."
            ),
        }
    )

    spider_config = {
        "seed_urls": seed_urls,
        "same_host_only": same_host_only,
        "max_links_per_page": max_links_per_page,
    }

    try:
        process = CrawlerProcess(settings=settings)
        process.crawl(EmailSpider, config=spider_config)
        process.start(stop_after_crawl=True)
    except Exception as exc:
        emit({"type": "log", "message": f"Scrapy runtime error: {exc}"})
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

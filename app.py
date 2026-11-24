"""Siteora Lead Finder.

A Streamlit lead generator that uses the TinyFish Search API to discover local
businesses, verifies whether they have an official website, extracts contact
details per business, scores leads and manages them in a SQLite-backed pipeline.

TinyFish Search API (verified against https://docs.tinyfish.ai/search-api/reference):
  GET https://api.search.tinyfish.ai
  Auth header: X-API-Key
  Params: query (required), location (ISO country code, NOT a city), language,
          include_domains, exclude_domains (comma separated), page (0-10), purpose
  Response: {"query", "results": [{position, site_name, title, snippet, url}],
             "total_results", "page"}
"""
from __future__ import annotations

import inspect
import io
import ipaddress
import json
import os
import re
import socket
import sqlite3
import tempfile
import threading
import time
import unicodedata
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from difflib import SequenceMatcher
from html import unescape
from pathlib import Path
from typing import Callable, Dict, List, Optional, Tuple
from urllib.parse import quote, unquote, urljoin, urlparse

import pandas as pd
import requests
import streamlit as st

try:  # optional: enables MX-record checks
    import dns.resolver  # type: ignore

    HAS_DNS = True
except Exception:  # pragma: no cover - depends on environment
    HAS_DNS = False

# ----------------------------------------------------------------------------
# Constants
# ----------------------------------------------------------------------------
APP_VERSION = "3.1"
TINYFISH_ENDPOINT = "https://api.search.tinyfish.ai"
DEFAULT_DB_NAME = "siteora_leads.db"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; SiteoraLeadFinder/3.0)",
    "Accept-Language": "en",
}

WS_VERIFIED = "Verified official website"
WS_NONE = "No official website found"
WS_DIRECTORY = "Directory listing only"
WS_SOCIAL = "Social media only"
WS_REVIEW = "Website needs review"
WS_UNREACHABLE = "Website unreachable"
WEBSITE_STATUSES = [WS_VERIFIED, WS_NONE, WS_DIRECTORY, WS_SOCIAL, WS_REVIEW, WS_UNREACHABLE]

PIPELINE = ["New", "Contacted", "Replied", "Meeting booked", "Won", "Not interested", "Do not contact"]
VERIFICATION_STATES = ["pending", "verified", "rejected"]

SOCIAL_PLATFORMS = {
    "facebook.com": "Facebook",
    "fb.com": "Facebook",
    "fb.me": "Facebook",
    "instagram.com": "Instagram",
    "tiktok.com": "TikTok",
    "linkedin.com": "LinkedIn",
    "x.com": "X",
    "twitter.com": "X",
    "youtube.com": "YouTube",
    "youtu.be": "YouTube",
    "pinterest.com": "Pinterest",
    "threads.net": "Threads",
    "snapchat.com": "Snapchat",
    "linktr.ee": "Link-in-bio",
}
DIRECTORY_DOMAINS = {
    "yelp.com", "yelp.com.au", "yelp.co.uk", "yellowpages.com", "yellowpages.com.au",
    "truelocal.com.au", "hotfrog.com", "hotfrog.com.au", "tripadvisor.com", "tripadvisor.com.au",
    "tripadvisor.co.uk", "foursquare.com", "google.com", "booksy.com", "fresha.com",
    "treatwell.com", "wordofmouth.com.au", "localsearch.com.au", "startlocal.com.au",
    "mapquest.com", "bing.com", "gumtree.com.au", "oneflare.com.au", "productreview.com.au",
    "bark.com", "nextdoor.com", "groupon.com", "yell.com", "thomsonlocal.com", "cylex.com.au",
    "womo.com.au", "hipages.com.au", "serviceseeking.com.au", "brownbook.net", "manta.com",
    "bbb.org", "angi.com", "thumbtack.com", "indeed.com", "glassdoor.com", "justdial.com",
}
THIRDPARTY_DOMAINS = {
    "wikipedia.org", "reddit.com", "quora.com", "medium.com", "wikihow.com",
    "news.com.au", "abc.net.au", "theguardian.com", "forbes.com", "pinterest.com",
}
FREE_SUBDOMAIN_HOSTS = (
    "wixsite.com", "weebly.com", "business.site", "blogspot.com", "wordpress.com",
    "godaddysites.com", "sites.google.com", "square.site", "carrd.co", "webnode.com",
    "myshopify.com", "webflow.io", "netlify.app", "github.io",
)
BUILDER_SIGNS = {
    "Wix": ["wix.com", "wixstatic.com"],
    "Squarespace": ["squarespace.com", "sqsp.net"],
    "Weebly": ["weebly.com"],
    "GoDaddy Builder": ["godaddysites.com", "secureserver.net"],
    "WordPress": ["wp-content", "wp-includes"],
    "Shopify": ["cdn.shopify.com"],
}
# Directory sites queried by the "listings" search angle (editable in the UI).
DEFAULT_LISTING_DOMAINS = [
    "yelp.com", "yellowpages.com.au", "truelocal.com.au", "hotfrog.com.au",
    "localsearch.com.au", "tripadvisor.com", "foursquare.com", "yellowpages.com",
]
SOCIAL_SEARCH_DOMAINS = ["facebook.com", "instagram.com", "tiktok.com", "linkedin.com"]

BLOCKED_EMAIL_DOMAINS = {
    "wix.com", "wixpress.com", "squarespace.com", "godaddy.com", "sentry.io", "sentry-next.wixpress.com",
    "yelp.com", "facebook.com", "google.com", "gmail.google.com", "example.com", "domain.com",
    "email.com", "yourdomain.com", "w3.org", "schema.org", "wordpress.com", "shopify.com",
}
JUNK_EMAIL_PARTS = ("sentry", "wixpress", "yourname", "youremail", "your@", "name@", "user@",
                    "test@", "email@", "noreply", "no-reply", "donotreply")
JUNK_EMAIL_EXT = (".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg", ".css", ".js", ".woff", ".woff2")
FREE_MAIL = {"gmail.com", "yahoo.com", "yahoo.com.au", "hotmail.com", "outlook.com", "icloud.com",
             "live.com", "bigpond.com", "optusnet.com.au", "protonmail.com", "me.com", "msn.com"}
ROLE_PREFIXES = ("info", "hello", "contact", "admin", "enquiries", "enquiry", "office", "sales",
                 "support", "bookings", "booking", "team", "mail", "reception")

CONF_RANK = {"manual": 4, "high": 3, "medium": 2, "low": 1}
TRUSTED_CONF = {"manual", "high", "medium"}

EMAIL_RE = re.compile(r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9\-]+(?:\.[A-Za-z0-9\-]+)*\.[A-Za-z]{2,}")
PHONE_RE = re.compile(r"(?:\+\d{1,3}[\s.\-]?)?(?:\(0?\d{1,3}\)|0\d{1,3})[\s.\-]?\d{3,4}[\s.\-]?\d{3,4}")

GENERIC_TITLE_PARTS = {
    "home", "homepage", "welcome", "index", "official site", "official website", "contact",
    "contact us", "about", "about us", "menu", "services", "our services", "main page",
}
LEGAL_WORDS = {"pty", "ltd", "llc", "inc", "limited", "co", "corp", "the"}
LIST_TITLE_RE = re.compile(
    r"\b(top|best)\s+\d+\b|\b\d+\s+(best|top)\b|\bbest\b.{0,40}\b(in|near|around)\b|"
    r"\btop\b.{0,40}\b(in|near)\b|\blist of\b|\bdirectory\b|\bnear me\b|\bcompare\b|\b\d+\s+results\b",
    re.I,
)
LIST_URL_RE = re.compile(r"/(search|find|categor(y|ies)|list|results|browse|directory)(/|\?|$)|[?&](find_desc|q|query|search)=", re.I)


def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


# ----------------------------------------------------------------------------
# URL / domain helpers
# ----------------------------------------------------------------------------
SECOND_LEVEL = {"co", "com", "net", "org", "gov", "edu", "ac", "or", "ne"}


def host_of(url: str) -> str:
    if not url:
        return ""
    try:
        parsed = urlparse(url if "//" in url else "//" + url)
        host = (parsed.hostname or "").lower()
    except ValueError:
        return ""
    return host[4:] if host.startswith("www.") else host


def domain_in(host: str, domains) -> bool:
    host = (host or "").lower()
    return any(host == d or host.endswith("." + d) for d in domains)


def registrable_domain(host: str) -> str:
    host = (host or "").lower().strip(".")
    if host.startswith("www."):
        host = host[4:]
    labels = host.split(".")
    if len(labels) <= 2:
        return host
    if len(labels[-1]) == 2 and labels[-2] in SECOND_LEVEL:
        return ".".join(labels[-3:])
    return ".".join(labels[-2:])


def site_domain(host: str) -> str:
    """Identity of a website. Free builder subdomains keep the full host so that
    two businesses on *.wixsite.com are never treated as the same site."""
    host = (host or "").lower()
    if host.startswith("www."):
        host = host[4:]
    if domain_in(host, FREE_SUBDOMAIN_HOSTS):
        return host
    return registrable_domain(host)


def classify_url(url: str) -> Tuple[str, str]:
    """Return (type, platform). type is website | social | directory | thirdparty | invalid.

    Social pages and directory listings are NEVER returned as "website"."""
    host = host_of(url)
    if not host or "." not in host:
        return "invalid", ""
    if domain_in(host, FREE_SUBDOMAIN_HOSTS):
        return "website", ""
    for dom, platform in SOCIAL_PLATFORMS.items():
        if domain_in(host, {dom}):
            return "social", platform
    if domain_in(host, DIRECTORY_DOMAINS):
        return "directory", ""
    if domain_in(host, THIRDPARTY_DOMAINS):
        return "thirdparty", ""
    return "website", ""


_BAD_SOCIAL_SEGMENTS = {
    "events", "event", "groups", "group", "posts", "post", "photo", "photos", "watch", "marketplace",
    "hashtag", "explore", "p", "reel", "reels", "tv", "stories", "story", "share", "sharer", "sharer.php",
    "login", "help", "policies", "privacy", "dialog", "plugins", "tr", "intent", "search", "video",
    "videos", "directory", "pages",
}


def is_social_profile(url: str, platform: str) -> bool:
    """True for something that looks like a business/profile page, not a post or share link."""
    try:
        parsed = urlparse(url)
    except ValueError:
        return False
    segs = [s for s in parsed.path.split("/") if s]
    if not segs:
        return False
    first = segs[0].lower()
    if platform == "LinkedIn":
        return first == "company" and len(segs) >= 2
    if platform == "Facebook" and first == "pages" and len(segs) >= 2:
        return True
    if first in _BAD_SOCIAL_SEGMENTS:
        return False
    if "sharer" in url.lower() or "intent/" in url.lower():
        return False
    return True


def root_url(url: str) -> str:
    p = urlparse(url if "//" in url else "https://" + url)
    scheme = p.scheme if p.scheme in ("http", "https") else "https"
    return f"{scheme}://{p.netloc.lower()}/"


# ----------------------------------------------------------------------------
# Name helpers
# ----------------------------------------------------------------------------
_SPLIT_RE = re.compile(r"\s+[|\-\u2013\u2014\u00b7\u2022]\s+|\s*[|\u2022\u00b7]\s*|\s+:\s+")


def clean_business_name(title: str, fallback: str = "") -> str:
    parts = [p.strip() for p in _SPLIT_RE.split(title or "") if p.strip()]
    for part in parts:
        if part.lower().strip(" .") in GENERIC_TITLE_PARTS or len(part) < 2:
            continue
        return re.sub(r"\s+", " ", part)[:120]
    return fallback


def normalize_name(name: str) -> str:
    s = unicodedata.normalize("NFKD", name or "").encode("ascii", "ignore").decode()
    s = s.lower().replace("&", " and ").replace("'", "").replace("\u2019", "")
    s = re.sub(r"[^a-z0-9]+", " ", s)
    return " ".join(t for t in s.split() if t not in LEGAL_WORDS)


def is_list_page(title: str, url: str) -> bool:
    return bool(LIST_TITLE_RE.search(title or "")) or bool(LIST_URL_RE.search(url or ""))


def domain_matches_name(host: str, name: str) -> bool:
    stem = re.sub(r"[^a-z0-9]", "", site_domain(host).split(".")[0])
    tokens = [t for t in normalize_name(name).split() if t != "and"]
    if not stem or not tokens:
        return False
    joined = "".join(tokens)
    if joined in stem or (len(stem) >= 5 and stem in joined):
        return True
    significant = [t for t in tokens if len(t) >= 3]
    if significant and all(t in stem for t in significant):
        return True
    return SequenceMatcher(None, joined, stem).ratio() >= 0.8


def has_phrase(text_lower: str, phrase: str) -> bool:
    phrase = (phrase or "").strip().lower()
    return bool(phrase) and bool(re.search(r"(?<![a-z0-9])" + re.escape(phrase) + r"(?![a-z0-9])", text_lower))


# ----------------------------------------------------------------------------
# Contact helpers
# ----------------------------------------------------------------------------
def clean_email(raw: str) -> str:
    e = (raw or "").strip().strip(".,;:()<>[]\"'").lower()
    if not EMAIL_RE.fullmatch(e):
        return ""
    if e.endswith(JUNK_EMAIL_EXT) or any(p in e for p in JUNK_EMAIL_PARTS):
        return ""
    domain = e.split("@", 1)[1]
    if domain_in(domain, BLOCKED_EMAIL_DOMAINS) or domain_in(domain, DIRECTORY_DOMAINS):
        return ""
    if domain_in(domain, set(SOCIAL_PLATFORMS)):
        return ""
    return e


def extract_emails(text: str) -> List[str]:
    out: List[str] = []
    for m in EMAIL_RE.findall(text or ""):
        e = clean_email(m)
        if e and e not in out:
            out.append(e)
    return out


def normalize_phone(raw: str) -> str:
    raw = (raw or "").strip()
    digits = re.sub(r"\D", "", raw)
    if not 8 <= len(digits) <= 15:
        return ""
    return ("+" if raw.startswith("+") else "") + digits


def extract_phones(text: str) -> List[str]:
    out: List[str] = []
    for m in PHONE_RE.findall(text or ""):
        p = normalize_phone(m)
        if p and p not in out:
            out.append(p)
    return out


def phone_key(phone: str) -> str:
    """Comparable form of a phone number (last 9 digits)."""
    return re.sub(r"\D", "", phone or "")[-9:]


def email_is_free(email: str) -> bool:
    return email.split("@")[-1] in FREE_MAIL


def email_matches_site(email: str, site_doms) -> bool:
    dom = registrable_domain(email.split("@")[-1])
    return any(dom == registrable_domain(d) for d in site_doms)


def email_confidence(kind: str, site_verified: bool, domain_match: bool, is_free: bool) -> str:
    """How much a contact can be trusted to belong to THIS business.

    site_page     - found on the business's own fetched website pages
    snippet_site  - found in a search snippet of the business's own site
    snippet_other - found in a snippet of a directory / social / third-party page
    """
    if kind == "site_page":
        if not site_verified:
            return "low"
        if domain_match:
            return "high"
        return "medium" if is_free else "low"
    if site_verified and domain_match:
        return "medium"
    return "low"


def check_email(email: str, use_dns: bool = True) -> str:
    if not email:
        return ""
    if not re.fullmatch(r"[a-z0-9._%+\-]+@[a-z0-9\-]+(?:\.[a-z0-9\-]+)*\.[a-z]{2,}", email):
        return "Invalid syntax"
    local, domain = email.split("@", 1)
    label = "role-based" if local in ROLE_PREFIXES else "named"
    if domain in FREE_MAIL:
        label += ", free provider"
    if use_dns and HAS_DNS:
        try:
            dns.resolver.resolve(domain, "MX", lifetime=4)
            return f"OK - MX found ({label})"
        except Exception:
            return "No MX record - likely undeliverable"
    return f"Syntax OK, MX not checked ({label})"


# ----------------------------------------------------------------------------
# TinyFish Search API client
# ----------------------------------------------------------------------------
class TinyFishError(Exception):
    def __init__(self, status: int, code: str, message: str, fatal: bool = False):
        super().__init__(f"TinyFish {status} {code}: {message}")
        self.status = status
        self.code = code
        self.message = message
        self.fatal = fatal  # fatal = stop the whole run (bad key, no credit, daily limit)


@dataclass
class SearchResult:
    position: int
    title: str
    url: str
    snippet: str
    site_name: str = ""
    date: str = ""


def parse_results(payload: dict) -> List[SearchResult]:
    """Parse the documented response shape: {"results": [{position, title, url, snippet, site_name}]}."""
    items = payload.get("results") if isinstance(payload, dict) else None
    if not isinstance(items, list):
        raise TinyFishError(200, "BAD_RESPONSE", "Response has no 'results' array")
    out: List[SearchResult] = []
    for i, it in enumerate(items, 1):
        if not isinstance(it, dict):
            continue
        url = str(it.get("url") or "").strip()
        if not url.lower().startswith(("http://", "https://")):
            continue
        out.append(SearchResult(
            position=int(it.get("position") or i),
            title=str(it.get("title") or "").strip(),
            url=url,
            snippet=str(it.get("snippet") or "").strip(),
            site_name=str(it.get("site_name") or "").strip(),
            date=str(it.get("date") or "").strip(),
        ))
    return out


class TinyFishClient:
    """Minimal client for GET https://api.search.tinyfish.ai (X-API-Key auth)."""

    def __init__(self, api_key: str, session=None, endpoint: str = TINYFISH_ENDPOINT,
                 requests_per_minute: int = 30, timeout: int = 45, max_retries: int = 2,
                 clock: Callable[[], float] = time.monotonic, sleeper: Callable[[float], None] = time.sleep):
        self.api_key = (api_key or "").strip()
        self.session = session or requests.Session()
        self.endpoint = endpoint
        self.min_interval = 60.0 / max(1, requests_per_minute) * 1.05
        self.timeout = timeout
        self.max_retries = max_retries
        self._clock = clock
        self._sleep = sleeper
        self._last = None
        self._lock = threading.Lock()
        self.calls = 0

    def _throttle(self) -> None:
        with self._lock:
            now = self._clock()
            if self._last is not None:
                wait = self.min_interval - (now - self._last)
                if wait > 0:
                    self._sleep(wait)
                    now = self._clock()
            self._last = now

    def build_params(self, query: str, location: Optional[str] = None, language: Optional[str] = "en",
                     include_domains: Optional[List[str]] = None, exclude_domains: Optional[List[str]] = None,
                     page: int = 0, purpose: Optional[str] = None) -> dict:
        query = (query or "").strip()
        if not query:
            raise ValueError("query is required")
        params: dict = {"query": query[:2000]}
        if location:
            loc = location.strip().upper()
            if not re.fullmatch(r"[A-Z]{2}", loc):
                raise ValueError("location must be a 2-letter country code such as AU, US or GB")
            params["location"] = loc
        if language:
            params["language"] = language.strip().lower()
        if include_domains:
            params["include_domains"] = ",".join(include_domains)
        if exclude_domains:
            params["exclude_domains"] = ",".join(exclude_domains)
        if page:
            if not 0 <= int(page) <= 10:
                raise ValueError("page must be between 0 and 10")
            params["page"] = int(page)
        if purpose:
            params["purpose"] = purpose[:2000]
        return params

    @staticmethod
    def _error_parts(resp) -> Tuple[str, str, dict]:
        try:
            body = resp.json()
        except ValueError:
            return "", (getattr(resp, "text", "") or "")[:300], {}
        err = body.get("error") if isinstance(body, dict) else None
        if isinstance(err, dict):
            details = err.get("details") if isinstance(err.get("details"), dict) else {}
            return str(err.get("code") or ""), str(err.get("message") or "")[:300], details
        return "", str(body)[:300], {}

    def search(self, query: str, **kwargs) -> dict:
        params = self.build_params(query, **kwargs)
        attempt = 0
        while True:
            self._throttle()
            self.calls += 1
            try:
                resp = self.session.get(
                    self.endpoint, params=params,
                    headers={"X-API-Key": self.api_key, "Accept": "application/json"},
                    timeout=self.timeout,
                )
            except requests.RequestException as exc:
                if attempt < self.max_retries:
                    attempt += 1
                    self._sleep(2 * attempt)
                    continue
                raise TinyFishError(0, "NETWORK_ERROR", str(exc)[:200])
            status = resp.status_code
            if status == 200:
                try:
                    data = resp.json()
                except ValueError:
                    raise TinyFishError(200, "BAD_RESPONSE", "Response was not valid JSON")
                if not isinstance(data, dict) or not isinstance(data.get("results"), list):
                    raise TinyFishError(200, "BAD_RESPONSE", "Response has no 'results' array")
                return data
            code, message, details = self._error_parts(resp)
            if status == 401:
                raise TinyFishError(status, code or "INVALID_API_KEY", message or "Missing or invalid API key", fatal=True)
            if status == 402:
                raise TinyFishError(status, code or "INSUFFICIENT_CREDITS", message or "Payment required", fatal=True)
            if status == 404:  # documented: "TinySearch is not available" - retrying every query is pointless
                raise TinyFishError(status, code or "NOT_AVAILABLE", message or "TinySearch is not available", fatal=True)
            exhausted = status == 429 and details.get("reason") == "free_allowance_exhausted"
            if exhausted:
                raise TinyFishError(status, code or "RATE_LIMIT_EXCEEDED", message or "Daily free allowance exhausted", fatal=True)
            if status in (429, 500, 503) and attempt < self.max_retries:
                retry_after = resp.headers.get("Retry-After", "") if getattr(resp, "headers", None) else ""
                delay = float(retry_after) if str(retry_after).replace(".", "", 1).isdigit() else (5, 20)[min(attempt, 1)]
                attempt += 1
                self._sleep(delay)
                continue
            raise TinyFishError(status, code or f"HTTP_{status}", message or "Request failed")


# ----------------------------------------------------------------------------
# Safe website fetching
# ----------------------------------------------------------------------------
class FetchBlocked(Exception):
    """The URL must not be fetched (private address, robots.txt, etc.)."""


class FetchUnreachable(Exception):
    """The host could not be reached (DNS failure, timeout, too many redirects)."""


def is_public_url(url: str) -> Tuple[bool, str]:
    """Refuse localhost / private / link-local targets so the app cannot be used for SSRF.

    Known limit: the name is resolved here and again by `requests`, so a hostile DNS
    server could answer differently the second time (DNS rebinding)."""
    try:
        parsed = urlparse(url)
    except ValueError:
        return False, "invalid"
    host = parsed.hostname
    if parsed.scheme not in ("http", "https") or not host:
        return False, "invalid"
    if host.lower() == "localhost" or host.lower().endswith((".local", ".internal", ".localhost")):
        return False, "non-public"
    try:
        infos = socket.getaddrinfo(host, parsed.port or (443 if parsed.scheme == "https" else 80),
                                   type=socket.SOCK_STREAM)
    except (socket.gaierror, UnicodeError):
        return False, "dns"
    for info in infos:
        ip = ipaddress.ip_address(info[4][0])
        if ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_reserved or ip.is_multicast or ip.is_unspecified:
            return False, "non-public"
    return True, ""


def safe_get(url: str, timeout: int = 8, max_bytes: int = 500_000, max_redirects: int = 3, session=None):
    """GET with per-hop public-address checks and a response size cap.
    Returns (status_code, final_url, text)."""
    sess = session or requests
    current = url
    for _ in range(max_redirects + 1):
        ok, why = is_public_url(current)
        if not ok:
            if why == "dns":
                raise FetchUnreachable("DNS lookup failed")
            raise FetchBlocked(f"refused to fetch {why} address")
        resp = sess.get(current, headers=HEADERS, timeout=timeout, allow_redirects=False, stream=True)
        try:
            location = resp.headers.get("Location")
            if resp.status_code in (301, 302, 303, 307, 308) and location:
                current = urljoin(current, location)
                continue
            body = b""
            for chunk in resp.iter_content(16384):
                body += chunk
                if len(body) >= max_bytes:
                    break
            text = body.decode(resp.encoding or "utf-8", errors="replace")
            return resp.status_code, current, text
        finally:
            resp.close()
    raise FetchUnreachable("too many redirects")


@dataclass
class PageInfo:
    url: str
    final_url: str = ""
    reachable: bool = False
    blocked: bool = False  # robots.txt / 401 / 403 / 429 / private address: cannot judge automatically
    status_code: int = 0
    error: str = ""
    https: bool = False
    viewport: Optional[bool] = None
    builder: str = ""
    copyright_year: Optional[int] = None
    title: str = ""
    site_name: str = ""
    h1: str = ""
    text: str = ""
    emails: Dict[str, str] = field(default_factory=dict)   # email -> page url it was found on
    phones: Dict[str, str] = field(default_factory=dict)   # phone -> page url it was found on
    socials: Dict[str, Tuple[str, str]] = field(default_factory=dict)  # url -> (platform, page url)
    pages: List[str] = field(default_factory=list)


def _strip_html(html_text: str) -> str:
    no_script = re.sub(r"(?is)<(script|style|noscript|svg)\b.*?</\1>", " ", html_text)
    no_comments = re.sub(r"(?s)<!--.*?-->", " ", no_script)
    text = re.sub(r"(?s)<[^>]+>", " ", no_comments)
    return re.sub(r"\s+", " ", unescape(text)).strip()


def _meta_content(html_text: str, attr: str, value: str) -> str:
    pat1 = rf'<meta[^>]+{attr}=["\']{re.escape(value)}["\'][^>]*content=["\']([^"\']*)["\']'
    pat2 = rf'<meta[^>]+content=["\']([^"\']*)["\'][^>]*{attr}=["\']{re.escape(value)}["\']'
    m = re.search(pat1, html_text, re.I) or re.search(pat2, html_text, re.I)
    return unescape(m.group(1)).strip() if m else ""


def analyze_html(url: str, html_text: str, collect_social: bool = False) -> dict:
    """Pure function: extract identity, quality signals and contact details from one page."""
    lower = html_text.lower()
    title_m = re.search(r"(?is)<title[^>]*>(.*?)</title>", html_text)
    h1_m = re.search(r"(?is)<h1[^>]*>(.*?)</h1>", html_text)
    text = _strip_html(html_text)
    out = {
        "title": _strip_html(title_m.group(1)) if title_m else "",
        "site_name": _meta_content(html_text, "property", "og:site_name"),
        "h1": _strip_html(h1_m.group(1)) if h1_m else "",
        "text": text[:30000],
        "viewport": bool(re.search(r'<meta[^>]+name=["\']viewport["\']', html_text, re.I)),
        "builder": next((n for n, signs in BUILDER_SIGNS.items() if any(s in lower for s in signs)), ""),
        "emails": {}, "phones": {}, "socials": {},
    }
    years = [int(y) for y in re.findall(r"(?:\u00a9|&copy;|copyright)[^0-9]{0,30}((?:19|20)\d{2})", html_text, re.I)]
    out["copyright_year"] = max(years) if years else None

    for raw in re.findall(r'mailto:([^"\'?>\s]+)', html_text, re.I):
        e = clean_email(unquote(raw))
        if e:
            out["emails"][e] = url
    for e in extract_emails(text):
        out["emails"].setdefault(e, url)

    tel_numbers = [normalize_phone(unquote(t)) for t in re.findall(r'tel:([^"\'>\s]+)', html_text, re.I)]
    for p in tel_numbers:
        if p:
            out["phones"][p] = url
    if not out["phones"]:
        for p in extract_phones(text)[:2]:
            out["phones"][p] = url

    if collect_social:
        for link in re.findall(r'href=["\'](https?://[^"\'>\s]+)["\']', html_text, re.I):
            rtype, platform = classify_url(link)
            if rtype == "social" and is_social_profile(link, platform):
                out["socials"][link.split("#")[0].rstrip("/")] = (platform, url)
    return out


def find_contact_link(base_url: str, html_text: str) -> str:
    for href in re.findall(r'href=["\']([^"\'#]+)["\']', html_text, re.I):
        if re.search(r"contact|find-us|get-in-touch|enquir", href, re.I):
            target = urljoin(base_url, href)
            if site_domain(host_of(target)) == site_domain(host_of(base_url)) and target.startswith("http"):
                return target
    return ""


class SiteFetcher:
    """Fetches a business homepage (and one contact page) and returns a PageInfo."""

    def __init__(self, timeout: int = 8, respect_robots: bool = True, collect_social: bool = False, session=None):
        self.timeout = timeout
        self.respect_robots = respect_robots
        self.collect_social = collect_social
        self.session = session
        self._robots: Dict[str, Optional[object]] = {}
        self._lock = threading.Lock()

    def _robots_allows(self, url: str) -> bool:
        if not self.respect_robots:
            return True
        from urllib import robotparser

        base = root_url(url)
        with self._lock:
            cached = self._robots.get(base, "unset")
        if cached == "unset":
            parser = None
            try:
                status, _, text = safe_get(base + "robots.txt", timeout=self.timeout, max_bytes=100_000, session=self.session)
                if status == 200:
                    parser = robotparser.RobotFileParser()
                    parser.parse(text.splitlines())
            except (FetchBlocked, FetchUnreachable, requests.RequestException):
                parser = None
            with self._lock:
                self._robots[base] = parser
            cached = parser
        return True if cached is None else cached.can_fetch(HEADERS["User-Agent"], url)

    def fetch_site(self, url: str) -> PageInfo:
        info = PageInfo(url=url)
        if not self._robots_allows(url):
            info.blocked = True
            info.error = "robots.txt disallows automated access"
            return info
        try:
            status, final_url, html_text = safe_get(url, timeout=self.timeout, session=self.session)
        except FetchBlocked as exc:
            info.blocked, info.error = True, str(exc)
            return info
        except FetchUnreachable as exc:
            info.error = str(exc)
            return info
        except requests.RequestException as exc:
            info.error = f"{type(exc).__name__}"
            return info
        info.status_code, info.final_url = status, final_url
        if status in (401, 403, 429, 999):
            info.blocked = True
            info.error = f"HTTP {status} (site blocks automated access)"
            return info
        if status >= 400:
            info.error = f"HTTP {status}"
            return info
        info.reachable = True
        info.https = final_url.startswith("https")
        pages = [(final_url, html_text)]
        contact = find_contact_link(final_url, html_text)
        if contact and contact != final_url and self._robots_allows(contact):
            try:
                c_status, c_final, c_html = safe_get(contact, timeout=self.timeout, session=self.session)
                if c_status < 400 and site_domain(host_of(c_final)) == site_domain(host_of(final_url)):
                    pages.append((c_final, c_html))
            except (FetchBlocked, FetchUnreachable, requests.RequestException):
                pass
        for i, (page_url, page_html) in enumerate(pages):
            data = analyze_html(page_url, page_html, self.collect_social)
            info.pages.append(page_url)
            if i == 0:
                info.title, info.site_name, info.h1 = data["title"], data["site_name"], data["h1"]
                info.viewport, info.builder, info.copyright_year = data["viewport"], data["builder"], data["copyright_year"]
            info.text += " " + data["text"]
            for e, src in data["emails"].items():
                info.emails.setdefault(e, src)
            for p, src in data["phones"].items():
                info.phones.setdefault(p, src)
            for link, val in data["socials"].items():
                info.socials.setdefault(link, val)
        info.text = info.text[:40000]
        return info


# ----------------------------------------------------------------------------
# Official-website evaluation
# ----------------------------------------------------------------------------
@dataclass
class WebsiteEval:
    state: str  # verified | needs_review | unreachable | unchecked
    points: int = 0
    reasons: List[str] = field(default_factory=list)


def category_in_text(category: str, text_lower: str) -> bool:
    words = re.findall(r"[a-z]{3,}", (category or "").lower())
    stems = {w[:-1] if w.endswith("s") and len(w) > 4 else w for w in words}
    return any(s in text_lower for s in stems)


def evaluate_website(name: str, city: str, category: str, page: Optional[PageInfo]) -> WebsiteEval:
    """Decide whether a candidate site really is THIS business's official website.

    A site is only verified when it was fetched successfully AND shows independent
    evidence (city mention, name in domain, category, own contact details). Being
    returned by a search engine is never enough."""
    if page is None:
        return WebsiteEval("unchecked", 0, ["Site was not fetched (enrichment off)"])
    if page.blocked:
        return WebsiteEval("needs_review", 0, [f"Could not be checked automatically: {page.error}"])
    if not page.reachable:
        return WebsiteEval("unreachable", 0, [f"Unreachable: {page.error or 'no response'}"])

    text_lower = " ".join([page.title, page.site_name, page.h1, page.text]).lower()
    host = host_of(page.final_url or page.url)
    points, reasons = 0, []
    city_ok = has_phrase(text_lower, city)
    if domain_matches_name(host, name):
        points += 2
        reasons.append("domain resembles business name (+2)")
    heading = " ".join([page.title, page.site_name, page.h1])
    n_norm = normalize_name(name)
    if n_norm and n_norm in normalize_name(heading):
        points += 1
        reasons.append("name appears in page title/heading (+1)")
    if city_ok:
        points += 1
        reasons.append(f"mentions {city} (+1)")
    if category_in_text(category, text_lower):
        points += 1
        reasons.append("mentions the business category (+1)")
    if page.emails or page.phones:
        points += 1
        reasons.append("shows its own contact details (+1)")
    if points >= 4 and city_ok:
        return WebsiteEval("verified", points, reasons)
    if not city_ok:
        reasons.append(f"does not mention {city}")
    return WebsiteEval("needs_review", points, reasons)


def determine_website_status(states: List[str], has_social: bool, has_directory: bool) -> str:
    if "verified" in states:
        return WS_VERIFIED
    if any(s in ("needs_review", "unchecked") for s in states):
        return WS_REVIEW
    if "unreachable" in states:
        return WS_UNREACHABLE
    if has_social:
        return WS_SOCIAL
    if has_directory:
        return WS_DIRECTORY
    return WS_NONE


# ----------------------------------------------------------------------------
# Lead scoring
# ----------------------------------------------------------------------------
STATUS_BASE_SCORE = {
    WS_NONE: 50, WS_SOCIAL: 45, WS_DIRECTORY: 40, WS_UNREACHABLE: 40, WS_REVIEW: 15, WS_VERIFIED: 0,
}


def score_business(status: str, signals: dict, email_confs: List[str], has_phone: bool, verified: bool) -> Tuple[int, str]:
    """0-100: how attractive this business is as a web-design prospect."""
    score = STATUS_BASE_SCORE.get(status, 0)
    reasons = [f"{status} (+{score})"] if score else []
    if status == WS_VERIFIED:
        extra = 0
        if signals.get("https") is False:
            extra += 15
            reasons.append("no HTTPS (+15)")
        if signals.get("viewport") is False:
            extra += 20
            reasons.append("not mobile-friendly (+20)")
        cy = signals.get("copyright_year")
        if cy and cy < datetime.now().year - 2:
            extra += 10
            reasons.append(f"footer says {cy} (+10)")
        if signals.get("builder") in ("Wix", "Weebly", "GoDaddy Builder"):
            extra += 5
            reasons.append(f"built on {signals['builder']} (+5)")
        score += min(extra, 40)
    best = max(email_confs, key=lambda c: CONF_RANK.get(c, 0)) if email_confs else ""
    bonus = {"manual": 20, "high": 20, "medium": 15, "low": 5}.get(best, 0)
    if bonus:
        score += bonus
        reasons.append(f"{best}-confidence email (+{bonus})")
    if has_phone:
        score += 10
        reasons.append("phone (+10)")
    if verified:
        score += 5
        reasons.append("manually verified (+5)")
    return min(score, 100), "; ".join(reasons)


# ----------------------------------------------------------------------------
# Draft (a business assembled from one search run, before it is saved)
# ----------------------------------------------------------------------------
@dataclass
class Draft:
    name: str
    norm: str
    city: str
    category: str
    country: str
    first_search_id: int = 0
    sources: List[dict] = field(default_factory=list)          # url, source_type, platform, title, snippet, search_id, domain
    site_domains: Dict[str, str] = field(default_factory=dict)  # site domain -> root url
    pages: Dict[str, PageInfo] = field(default_factory=dict)
    states: Dict[str, WebsiteEval] = field(default_factory=dict)
    emails: List[dict] = field(default_factory=list)            # email, kind, domain_match, is_free, confidence, source_url
    phones: List[dict] = field(default_factory=list)            # phone, confidence, source_url
    socials: List[dict] = field(default_factory=list)           # platform, url, source_url
    lookup_done: bool = False
    signals: dict = field(default_factory=dict)
    # Identity domain: ONLY set for a website the search engine returned for this business.
    # A website found by a follow-up "official website" lookup is a candidate, never an identity,
    # so two businesses that match the same aggregator site can never be merged by accident.
    key_domain: str = ""

    @property
    def key(self) -> str:
        if self.key_domain:
            return "site:" + self.key_domain
        return f"name:{self.norm}|{self.city.strip().lower()}"


# ----------------------------------------------------------------------------
# SQLite storage
# ----------------------------------------------------------------------------
SCHEMA = """
CREATE TABLE IF NOT EXISTS businesses(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    key TEXT UNIQUE NOT NULL,
    name TEXT NOT NULL,
    norm_name TEXT NOT NULL,
    category TEXT DEFAULT '',
    city TEXT DEFAULT '',
    country TEXT DEFAULT '',
    official_website TEXT DEFAULT '',
    candidate_website TEXT DEFAULT '',
    website_status TEXT NOT NULL DEFAULT 'No official website found',
    website_locked INTEGER NOT NULL DEFAULT 0,
    website_lookup_done INTEGER NOT NULL DEFAULT 0,
    site_signals TEXT NOT NULL DEFAULT '{}',
    score INTEGER NOT NULL DEFAULT 0,
    score_reasons TEXT DEFAULT '',
    verification TEXT NOT NULL DEFAULT 'pending',
    pipeline TEXT NOT NULL DEFAULT 'New',
    notes TEXT DEFAULT '',
    do_not_contact INTEGER NOT NULL DEFAULT 0,
    first_search_id INTEGER,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS emails(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    business_id INTEGER NOT NULL REFERENCES businesses(id) ON DELETE CASCADE,
    email TEXT NOT NULL,
    kind TEXT NOT NULL DEFAULT 'snippet_other',
    domain_match INTEGER NOT NULL DEFAULT 0,
    is_free INTEGER NOT NULL DEFAULT 0,
    confidence TEXT NOT NULL DEFAULT 'low',
    source_url TEXT NOT NULL DEFAULT '',
    check_result TEXT DEFAULT '',
    confirmed INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    UNIQUE(business_id, email)
);
CREATE TABLE IF NOT EXISTS phones(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    business_id INTEGER NOT NULL REFERENCES businesses(id) ON DELETE CASCADE,
    phone TEXT NOT NULL,
    confidence TEXT NOT NULL DEFAULT 'low',
    source_url TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL,
    UNIQUE(business_id, phone)
);
CREATE TABLE IF NOT EXISTS sources(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    business_id INTEGER NOT NULL REFERENCES businesses(id) ON DELETE CASCADE,
    url TEXT NOT NULL,
    source_type TEXT NOT NULL,
    platform TEXT DEFAULT '',
    title TEXT DEFAULT '',
    snippet TEXT DEFAULT '',
    search_id INTEGER,
    domain TEXT DEFAULT '',
    site_state TEXT DEFAULT '',
    state_locked INTEGER NOT NULL DEFAULT 0,
    eval_reasons TEXT DEFAULT '',
    created_at TEXT NOT NULL,
    UNIQUE(business_id, url)
);
CREATE TABLE IF NOT EXISTS socials(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    business_id INTEGER NOT NULL REFERENCES businesses(id) ON DELETE CASCADE,
    platform TEXT NOT NULL,
    url TEXT NOT NULL,
    source_url TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL,
    UNIQUE(business_id, url)
);
CREATE TABLE IF NOT EXISTS searches(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at TEXT NOT NULL,
    angle TEXT DEFAULT '',
    query TEXT NOT NULL,
    country TEXT DEFAULT '',
    city TEXT DEFAULT '',
    category TEXT DEFAULT '',
    page INTEGER DEFAULT 0,
    include_domains TEXT DEFAULT '',
    exclude_domains TEXT DEFAULT '',
    results INTEGER DEFAULT 0,
    new_leads INTEGER DEFAULT 0,
    status TEXT DEFAULT 'started',
    error TEXT DEFAULT '',
    settings TEXT DEFAULT '{}'
);
CREATE TABLE IF NOT EXISTS verification_log(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    business_id INTEGER NOT NULL REFERENCES businesses(id) ON DELETE CASCADE,
    created_at TEXT NOT NULL,
    check_type TEXT NOT NULL,
    result TEXT NOT NULL,
    detail TEXT DEFAULT ''
);
CREATE TABLE IF NOT EXISTS outreach_log(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    business_id INTEGER NOT NULL REFERENCES businesses(id) ON DELETE CASCADE,
    created_at TEXT NOT NULL,
    from_status TEXT DEFAULT '',
    to_status TEXT NOT NULL,
    note TEXT DEFAULT ''
);
CREATE TABLE IF NOT EXISTS dup_dismissed(
    a INTEGER NOT NULL,
    b INTEGER NOT NULL,
    PRIMARY KEY(a, b)
);
CREATE TABLE IF NOT EXISTS settings(
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_emails_biz ON emails(business_id);
CREATE INDEX IF NOT EXISTS idx_phones_biz ON phones(business_id);
CREATE INDEX IF NOT EXISTS idx_sources_biz ON sources(business_id);
CREATE INDEX IF NOT EXISTS idx_biz_city ON businesses(city);
"""

REQUIRED_TABLES = {"businesses", "emails", "phones", "sources", "socials", "searches",
                   "verification_log", "outreach_log", "dup_dismissed", "settings"}


class Database:
    """All persistence. Every contact row (email/phone/social/source) belongs to exactly
    one business_id and records the URL it was found on."""

    def __init__(self, path: str):
        self.path = str(path)
        Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()

    @contextmanager
    def conn(self):
        c = sqlite3.connect(self.path, timeout=30)
        c.row_factory = sqlite3.Row
        c.execute("PRAGMA foreign_keys=ON")
        try:
            yield c
            c.commit()
        except Exception:
            c.rollback()
            raise
        finally:
            c.close()

    def _init_schema(self) -> None:
        with self.conn() as c:
            try:
                c.execute("PRAGMA journal_mode=WAL")
            except sqlite3.DatabaseError:
                pass
            c.executescript(SCHEMA)

    # ---- settings ---------------------------------------------------------
    def get_setting(self, key: str, default: Optional[str] = None) -> Optional[str]:
        with self.conn() as c:
            row = c.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
        return row["value"] if row else default

    def set_setting(self, key: str, value) -> None:
        with self.conn() as c:
            c.execute("INSERT INTO settings(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                      (key, str(value)))

    def get_bool(self, key: str, default: bool) -> bool:
        raw = self.get_setting(key)
        return default if raw is None else raw == "True"

    # ---- search history -----------------------------------------------------
    def log_search_start(self, job: dict, settings: dict) -> int:
        with self.conn() as c:
            cur = c.execute(
                "INSERT INTO searches(created_at,angle,query,country,city,category,page,include_domains,exclude_domains,settings)"
                " VALUES(?,?,?,?,?,?,?,?,?,?)",
                (now_iso(), job.get("angle", ""), job["query"], job.get("country", ""), job.get("city", ""),
                 job.get("category", ""), job.get("page", 0), ",".join(job.get("include") or []),
                 ",".join(job.get("exclude") or []), json.dumps(settings)),
            )
            return int(cur.lastrowid)

    def log_search_finish(self, sid: int, results: int, new_leads: int, status: str, error: str = "") -> None:
        with self.conn() as c:
            c.execute("UPDATE searches SET results=?, new_leads=?, status=?, error=? WHERE id=?",
                      (results, new_leads, status, error[:500], sid))

    def add_search_new_leads(self, sid: int, n: int) -> None:
        with self.conn() as c:
            c.execute("UPDATE searches SET new_leads=new_leads+? WHERE id=?", (n, sid))

    def searches_df(self) -> pd.DataFrame:
        with self.conn() as c:
            rows = c.execute("SELECT id,created_at,angle,query,country,city,category,page,include_domains,"
                             "exclude_domains,results,new_leads,status,error FROM searches ORDER BY id DESC").fetchall()
        cols = ["id", "created_at", "angle", "query", "country", "city", "category", "page", "include_domains",
                "exclude_domains", "results", "new_leads", "status", "error"]
        return pd.DataFrame([dict(r) for r in rows], columns=cols)

    # ---- saving drafts --------------------------------------------------------
    def _match(self, c, d: Draft):
        """Find the existing business this draft is the same as, or None.

        Matching is deliberately strict so contact data from different businesses
        is never mixed: same website domain, or an exact (normalised) name + city
        that identifies exactly one existing business."""
        row = c.execute("SELECT * FROM businesses WHERE key=?", (d.key,)).fetchone()
        if row:
            return row
        city = d.city.strip().lower()
        if d.key_domain:
            rows = c.execute("SELECT * FROM businesses WHERE key LIKE 'name:%' AND norm_name=? AND lower(city)=?",
                             (d.norm, city)).fetchall()
        else:
            rows = c.execute("SELECT * FROM businesses WHERE norm_name=? AND lower(city)=?", (d.norm, city)).fetchall()
        return rows[0] if len(rows) == 1 else None

    def exists_for(self, d: Draft) -> bool:
        with self.conn() as c:
            return self._match(c, d) is not None

    def save_draft(self, d: Draft) -> Tuple[int, bool]:
        now = now_iso()
        with self.conn() as c:
            row = self._match(c, d)
            created = row is None
            if created:
                cur = c.execute(
                    "INSERT INTO businesses(key,name,norm_name,category,city,country,first_search_id,"
                    "website_lookup_done,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?)",
                    (d.key, d.name, d.norm, d.category, d.city, d.country, d.first_search_id,
                     int(d.lookup_done), now, now))
                bid = int(cur.lastrowid)
            else:
                bid = int(row["id"])
                if d.key_domain and row["key"].startswith("name:"):
                    taken = c.execute("SELECT 1 FROM businesses WHERE key=? AND id<>?", (d.key, bid)).fetchone()
                    if not taken:
                        c.execute("UPDATE businesses SET key=? WHERE id=?", (d.key, bid))
                if d.lookup_done:
                    c.execute("UPDATE businesses SET website_lookup_done=1 WHERE id=?", (bid,))
            for s in d.sources:
                ev = d.states.get(s.get("domain", "")) if s["source_type"] == "website" else None
                c.execute(
                    "INSERT OR IGNORE INTO sources(business_id,url,source_type,platform,title,snippet,search_id,"
                    "domain,site_state,eval_reasons,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                    (bid, s["url"], s["source_type"], s.get("platform", ""), s.get("title", "")[:300],
                     s.get("snippet", "")[:600], s.get("search_id"), s.get("domain", ""),
                     ev.state if ev else "", "; ".join(ev.reasons) if ev else "", now))
            for dom, ev in d.states.items():
                c.execute("UPDATE sources SET site_state=?, eval_reasons=? WHERE business_id=? AND "
                          "source_type='website' AND domain=? AND state_locked=0",
                          (ev.state, "; ".join(ev.reasons), bid, dom))
            self._add_contacts(c, bid, d.emails, d.phones, d.socials)
            if d.signals:
                c.execute("UPDATE businesses SET site_signals=? WHERE id=?", (json.dumps(d.signals), bid))
        self.recompute(bid)
        return bid, created

    def _add_contacts(self, c, bid: int, emails: List[dict], phones: List[dict], socials: List[dict]) -> None:
        now = now_iso()
        for e in emails:
            ex = c.execute("SELECT id,confidence,confirmed FROM emails WHERE business_id=? AND email=?",
                           (bid, e["email"])).fetchone()
            if ex is None:
                c.execute(
                    "INSERT INTO emails(business_id,email,kind,domain_match,is_free,confidence,source_url,check_result,created_at)"
                    " VALUES(?,?,?,?,?,?,?,?,?)",
                    (bid, e["email"], e["kind"], int(e["domain_match"]), int(e["is_free"]), e["confidence"],
                     e["source_url"], e.get("check_result", ""), now))
            elif not ex["confirmed"] and CONF_RANK.get(e["confidence"], 0) > CONF_RANK.get(ex["confidence"], 0):
                c.execute("UPDATE emails SET confidence=?, kind=?, source_url=?, domain_match=? WHERE id=?",
                          (e["confidence"], e["kind"], e["source_url"], int(e["domain_match"]), ex["id"]))
        for p in phones:
            ex = c.execute("SELECT id,confidence FROM phones WHERE business_id=? AND phone=?", (bid, p["phone"])).fetchone()
            if ex is None:
                c.execute("INSERT INTO phones(business_id,phone,confidence,source_url,created_at) VALUES(?,?,?,?,?)",
                          (bid, p["phone"], p["confidence"], p["source_url"], now))
            elif CONF_RANK.get(p["confidence"], 0) > CONF_RANK.get(ex["confidence"], 0):
                c.execute("UPDATE phones SET confidence=?, source_url=? WHERE id=?", (p["confidence"], p["source_url"], ex["id"]))
        for s in socials:
            c.execute("INSERT OR IGNORE INTO socials(business_id,platform,url,source_url,created_at) VALUES(?,?,?,?,?)",
                      (bid, s["platform"], s["url"], s["source_url"], now))

    def add_contacts(self, bid: int, emails: List[dict], phones: List[dict], socials: List[dict]) -> None:
        with self.conn() as c:
            self._add_contacts(c, bid, emails, phones, socials)
        self.recompute(bid)

    # ---- derived fields ----------------------------------------------------------
    def recompute(self, bid: int) -> None:
        with self.conn() as c:
            b = c.execute("SELECT * FROM businesses WHERE id=?", (bid,)).fetchone()
            if b is None:
                return
            srcs = c.execute("SELECT * FROM sources WHERE business_id=?", (bid,)).fetchall()
            site_rows = [s for s in srcs if s["source_type"] == "website"]
            states = [s["site_state"] or "unchecked" for s in site_rows]
            has_social = any(s["source_type"] == "social" for s in srcs)
            has_dir = any(s["source_type"] == "directory" for s in srcs)
            status = b["website_status"] if b["website_locked"] else determine_website_status(states, has_social, has_dir)
            official = next((root_url(s["url"]) for s in site_rows if s["site_state"] == "verified"), "")
            candidate = next((root_url(s["url"]) for s in site_rows if s["site_state"] != "verified"), "")
            confs = [r["confidence"] for r in c.execute("SELECT confidence FROM emails WHERE business_id=?", (bid,))]
            has_phone = c.execute("SELECT 1 FROM phones WHERE business_id=? LIMIT 1", (bid,)).fetchone() is not None
            try:
                signals = json.loads(b["site_signals"] or "{}")
            except ValueError:
                signals = {}
            score, reasons = score_business(status, signals, confs, has_phone, b["verification"] == "verified")
            c.execute("UPDATE businesses SET website_status=?, official_website=?, candidate_website=?, score=?, "
                      "score_reasons=?, updated_at=? WHERE id=?",
                      (status, official, candidate, score, reasons, now_iso(), bid))

    def recompute_all(self) -> None:
        with self.conn() as c:
            ids = [r["id"] for r in c.execute("SELECT id FROM businesses")]
        for i in ids:
            self.recompute(i)

    # ---- reading -----------------------------------------------------------------
    def snapshot(self) -> dict:
        with self.conn() as c:
            data = {t: [dict(r) for r in c.execute(f"SELECT * FROM {t}")]
                    for t in ("businesses", "emails", "phones", "sources", "socials")}
            data["dismissed"] = {(r["a"], r["b"]) for r in c.execute("SELECT a,b FROM dup_dismissed")}
        return data

    def get_business(self, bid: int) -> Optional[dict]:
        with self.conn() as c:
            row = c.execute("SELECT * FROM businesses WHERE id=?", (bid,)).fetchone()
        return dict(row) if row else None

    def cities(self) -> List[str]:
        with self.conn() as c:
            return [r["city"] for r in c.execute("SELECT DISTINCT city FROM businesses WHERE city<>'' ORDER BY city")]

    def verification_log(self, bid: int) -> pd.DataFrame:
        with self.conn() as c:
            rows = c.execute("SELECT created_at,check_type,result,detail FROM verification_log WHERE business_id=? "
                             "ORDER BY id DESC", (bid,)).fetchall()
        return pd.DataFrame([dict(r) for r in rows], columns=["created_at", "check_type", "result", "detail"])

    def outreach_log(self, bid: int) -> pd.DataFrame:
        with self.conn() as c:
            rows = c.execute("SELECT created_at,from_status,to_status,note FROM outreach_log WHERE business_id=? "
                             "ORDER BY id DESC", (bid,)).fetchall()
        return pd.DataFrame([dict(r) for r in rows], columns=["created_at", "from_status", "to_status", "note"])

    # ---- verification / pipeline actions ------------------------------------------
    @staticmethod
    def _log_verification(c, bid: int, check_type: str, result: str, detail: str = "") -> None:
        c.execute("INSERT INTO verification_log(business_id,created_at,check_type,result,detail) VALUES(?,?,?,?,?)",
                  (bid, now_iso(), check_type, result, detail[:500]))

    def log_verification(self, bid: int, check_type: str, result: str, detail: str = "") -> None:
        with self.conn() as c:
            self._log_verification(c, bid, check_type, result, detail)

    def set_verification(self, bid: int, state: str, note: str = "") -> None:
        if state not in VERIFICATION_STATES:
            raise ValueError(f"unknown verification state: {state}")
        with self.conn() as c:
            c.execute("UPDATE businesses SET verification=?, updated_at=? WHERE id=?", (state, now_iso(), bid))
            self._log_verification(c, bid, "lead", state, note)
        self.recompute(bid)

    def set_pipeline(self, bid: int, stage: str, note: str = "") -> None:
        if stage not in PIPELINE:
            raise ValueError(f"unknown pipeline stage: {stage}")
        with self.conn() as c:
            row = c.execute("SELECT pipeline FROM businesses WHERE id=?", (bid,)).fetchone()
            if row is None:
                return
            c.execute("UPDATE businesses SET pipeline=?, do_not_contact=?, updated_at=? WHERE id=?",
                      (stage, int(stage == "Do not contact"), now_iso(), bid))
            if row["pipeline"] != stage or note:
                c.execute("INSERT INTO outreach_log(business_id,created_at,from_status,to_status,note) VALUES(?,?,?,?,?)",
                          (bid, now_iso(), row["pipeline"], stage, note[:500]))

    def set_notes(self, bid: int, notes: str) -> None:
        with self.conn() as c:
            c.execute("UPDATE businesses SET notes=?, updated_at=? WHERE id=?", (notes, now_iso(), bid))

    def confirm_website(self, bid: int, domain: str) -> None:
        """Confirm only a saved website candidate belonging to this business."""
        raw_domain = (domain or "").strip()
        if not raw_domain:
            raise ValueError("A website domain is required.")

        host = host_of(
            raw_domain if "://" in raw_domain else "https://" + raw_domain
        )
        normalized_domain = site_domain(host)

        if not host or "." not in host or not normalized_domain:
            raise ValueError("That is not a valid website domain.")

        with self.conn() as c:
            business = c.execute(
                "SELECT id FROM businesses WHERE id=?", (bid,)
            ).fetchone()

            if business is None:
                raise ValueError("Business not found.")

            website_sources = c.execute(
                """SELECT id, domain, url FROM sources
                   WHERE business_id=? AND source_type='website'""",
                (bid,)
            ).fetchall()

            matching_ids = []
            for source in website_sources:
                saved_domain = site_domain(
                    host_of(source["url"] or "")
                )
                stored_domain = site_domain(
                    host_of(
                        source["domain"]
                        if "://" in (source["domain"] or "")
                        else "https://" + (source["domain"] or "")
                    )
                )

                if normalized_domain in (saved_domain, stored_domain):
                    matching_ids.append(source["id"])

            if not matching_ids:
                raise ValueError(
                    "This domain is not a saved website candidate for this business."
                )

            placeholders = ",".join("?" for _ in matching_ids)
            c.execute(
                f"""UPDATE sources
                    SET site_state='verified', state_locked=1,
                        eval_reasons='Confirmed manually'
                    WHERE business_id=? AND source_type='website'
                    AND id IN ({placeholders})""",
                (bid, *matching_ids)
            )

            # Recalculate the business website status from the verified source.
            c.execute(
                "UPDATE businesses SET website_locked=0 WHERE id=?",
                (bid,)
            )

            for email in c.execute(
                "SELECT * FROM emails WHERE business_id=? AND confirmed=0",
                (bid,)
            ).fetchall():
                if site_domain(host_of(email["source_url"])) == normalized_domain:
                    confidence = email_confidence(
                        email["kind"], True,
                        bool(email["domain_match"]),
                        bool(email["is_free"])
                    )
                    if CONF_RANK.get(confidence, 0) > CONF_RANK.get(
                        email["confidence"], 0
                    ):
                        c.execute(
                            "UPDATE emails SET confidence=? WHERE id=?",
                            (confidence, email["id"])
                        )

            for phone in c.execute(
                "SELECT * FROM phones WHERE business_id=?", (bid,)
            ).fetchall():
                if site_domain(host_of(phone["source_url"])) == normalized_domain:
                    c.execute(
                        "UPDATE phones SET confidence='high' WHERE id=?",
                        (phone["id"],)
                    )

            self._log_verification(
                c, bid, "website", "confirmed official", normalized_domain
            )

        self.recompute(bid)

    def set_website_status_manual(self, bid: int, status: str, note: str = "") -> None:
        if status not in WEBSITE_STATUSES or status == WS_VERIFIED:
            raise ValueError("use confirm_website to mark a website as verified")
        with self.conn() as c:
            c.execute("UPDATE businesses SET website_status=?, website_locked=1, website_lookup_done=1 WHERE id=?",
                      (status, bid))
            self._log_verification(c, bid, "website", f"set manually: {status}", note)
        self.recompute(bid)

    def unlock_website(self, bid: int) -> None:
        with self.conn() as c:
            c.execute("UPDATE businesses SET website_locked=0 WHERE id=?", (bid,))
        self.recompute(bid)

    def add_website(self, bid: int, url: str) -> str:
        """Attach a user-supplied website URL as an unchecked candidate. Returns its domain."""
        rtype, _ = classify_url(url if "//" in url else "https://" + url)
        if rtype != "website":
            raise ValueError(f"That URL is a {rtype} page, not a business website.")
        root = root_url(url)
        dom = site_domain(host_of(root))
        with self.conn() as c:
            c.execute("INSERT OR IGNORE INTO sources(business_id,url,source_type,domain,site_state,title,created_at) "
                      "VALUES(?,?,?,?,?,?,?)", (bid, root, "website", dom, "unchecked", "Added manually", now_iso()))
            self._log_verification(c, bid, "website", "candidate added manually", root)
        self.recompute(bid)
        return dom

    def apply_recheck(self, bid: int, domain: str, ev: WebsiteEval, signals: dict,
                      emails: List[dict], phones: List[dict], socials: List[dict]) -> None:
        with self.conn() as c:
            c.execute("UPDATE sources SET site_state=?, eval_reasons=? WHERE business_id=? AND source_type='website' "
                      "AND domain=? AND state_locked=0", (ev.state, "; ".join(ev.reasons), bid, domain))
            if signals:
                c.execute("UPDATE businesses SET site_signals=? WHERE id=?", (json.dumps(signals), bid))
            self._add_contacts(c, bid, emails, phones, socials)
            self._log_verification(c, bid, "website", f"re-checked: {ev.state}", "; ".join(ev.reasons))
        self.recompute(bid)

    def confirm_email(self, email_id: int) -> None:
        with self.conn() as c:
            row = c.execute("SELECT business_id,email FROM emails WHERE id=?", (email_id,)).fetchone()
            if row is None:
                return
            c.execute("UPDATE emails SET confirmed=1, confidence='manual' WHERE id=?", (email_id,))
            self._log_verification(c, row["business_id"], "email", "confirmed manually", row["email"])
            bid = row["business_id"]
        self.recompute(bid)

    def remove_email(self, email_id: int) -> None:
        with self.conn() as c:
            row = c.execute("SELECT business_id,email FROM emails WHERE id=?", (email_id,)).fetchone()
            if row is None:
                return
            c.execute("DELETE FROM emails WHERE id=?", (email_id,))
            self._log_verification(c, row["business_id"], "email", "removed (wrong business / invalid)", row["email"])
            bid = row["business_id"]
        self.recompute(bid)

    def remove_phone(self, phone_id: int) -> None:
        with self.conn() as c:
            row = c.execute("SELECT business_id,phone FROM phones WHERE id=?", (phone_id,)).fetchone()
            if row is None:
                return
            c.execute("DELETE FROM phones WHERE id=?", (phone_id,))
            self._log_verification(c, row["business_id"], "phone", "removed (wrong business / invalid)", row["phone"])
            bid = row["business_id"]
        self.recompute(bid)

    def save_email_check(self, email_id: int, result: str) -> None:
        with self.conn() as c:
            row = c.execute("SELECT business_id,email FROM emails WHERE id=?", (email_id,)).fetchone()
            if row is None:
                return
            c.execute("UPDATE emails SET check_result=? WHERE id=?", (result, email_id))
            self._log_verification(c, row["business_id"], "email check", result, row["email"])

    def remove_website(self, bid: int, domain: str) -> None:
        """Reviewer says `domain` is NOT this business's website: drop it and every unconfirmed
        contact that was taken from it, so another business's details cannot linger here."""
        with self.conn() as c:
            c.execute("DELETE FROM sources WHERE business_id=? AND source_type='website' AND domain=?", (bid, domain))
            for e in c.execute("SELECT id,source_url FROM emails WHERE business_id=? AND confirmed=0", (bid,)).fetchall():
                if site_domain(host_of(e["source_url"])) == domain:
                    c.execute("DELETE FROM emails WHERE id=?", (e["id"],))
            for p in c.execute("SELECT id,source_url FROM phones WHERE business_id=?", (bid,)).fetchall():
                if site_domain(host_of(p["source_url"])) == domain:
                    c.execute("DELETE FROM phones WHERE id=?", (p["id"],))
            for s in c.execute("SELECT id,source_url FROM socials WHERE business_id=?", (bid,)).fetchall():
                if site_domain(host_of(s["source_url"])) == domain:
                    c.execute("DELETE FROM socials WHERE id=?", (s["id"],))
            self._log_verification(c, bid, "website", "rejected as not this business's website", domain)
        self.recompute(bid)

    # ---- duplicates ----------------------------------------------------------------
    def dismiss_duplicate(self, a: int, b: int) -> None:
        lo, hi = sorted((a, b))
        with self.conn() as c:
            c.execute("INSERT OR IGNORE INTO dup_dismissed(a,b) VALUES(?,?)", (lo, hi))

    def merge(self, primary: int, secondary: int) -> None:
        """Safely merge a user-confirmed duplicate, preserving conflicting records."""
        if primary == secondary:
            raise ValueError("cannot merge a business into itself")

        def best_conf(a, b):
            return max((a or "low", b or "low"), key=lambda x: CONF_RANK.get(x, 0))

        with self.conn() as c:
            p = c.execute("SELECT * FROM businesses WHERE id=?", (primary,)).fetchone()
            s = c.execute("SELECT * FROM businesses WHERE id=?", (secondary,)).fetchone()

            if p is None or s is None:
                raise ValueError("business not found")

            # Emails: combine matching records instead of silently discarding one.
            for old in c.execute(
                "SELECT * FROM emails WHERE business_id=?", (secondary,)
            ).fetchall():
                existing = c.execute(
                    "SELECT * FROM emails WHERE business_id=? AND email=?",
                    (primary, old["email"])
                ).fetchone()

                if existing is None:
                    c.execute(
                        "UPDATE emails SET business_id=? WHERE id=?",
                        (primary, old["id"])
                    )
                else:
                    confidence = best_conf(existing["confidence"], old["confidence"])
                    preferred = old if CONF_RANK.get(old["confidence"], 0) > CONF_RANK.get(existing["confidence"], 0) else existing
                    source_url = preferred["source_url"] or existing["source_url"] or old["source_url"]
                    check_result = existing["check_result"] or old["check_result"] or ""
                    c.execute(
                        """UPDATE emails SET confidence=?, confirmed=?, domain_match=?,
                           is_free=?, kind=?, source_url=?, check_result=?
                           WHERE id=?""",
                        (
                            confidence,
                            int(bool(existing["confirmed"] or old["confirmed"])),
                            int(bool(existing["domain_match"] or old["domain_match"])),
                            int(bool(existing["is_free"] or old["is_free"])),
                            preferred["kind"] or existing["kind"] or old["kind"],
                            source_url,
                            check_result,
                            existing["id"]
                        )
                    )
                    c.execute("DELETE FROM emails WHERE id=?", (old["id"],))

            # Phones: retain the strongest confidence and a useful source.
            for old in c.execute(
                "SELECT * FROM phones WHERE business_id=?", (secondary,)
            ).fetchall():
                existing = c.execute(
                    "SELECT * FROM phones WHERE business_id=? AND phone=?",
                    (primary, old["phone"])
                ).fetchone()

                if existing is None:
                    c.execute(
                        "UPDATE phones SET business_id=? WHERE id=?",
                        (primary, old["id"])
                    )
                else:
                    confidence = best_conf(existing["confidence"], old["confidence"])
                    preferred = old if CONF_RANK.get(old["confidence"], 0) > CONF_RANK.get(existing["confidence"], 0) else existing
                    c.execute(
                        "UPDATE phones SET confidence=?, source_url=? WHERE id=?",
                        (
                            confidence,
                            preferred["source_url"] or existing["source_url"] or old["source_url"],
                            existing["id"]
                        )
                    )
                    c.execute("DELETE FROM phones WHERE id=?", (old["id"],))

            # Sources: preserve each URL and combine useful metadata for duplicates.
            for old in c.execute(
                "SELECT * FROM sources WHERE business_id=?", (secondary,)
            ).fetchall():
                existing = c.execute(
                    "SELECT * FROM sources WHERE business_id=? AND url=?",
                    (primary, old["url"])
                ).fetchone()

                if existing is None:
                    c.execute(
                        "UPDATE sources SET business_id=? WHERE id=?",
                        (primary, old["id"])
                    )
                else:
                    locked = bool(existing["state_locked"] or old["state_locked"])
                    if old["state_locked"] and not existing["state_locked"]:
                        site_state = old["site_state"]
                    else:
                        site_state = existing["site_state"] or old["site_state"]

                    reasons = list(dict.fromkeys(
                        x for x in (existing["eval_reasons"], old["eval_reasons"]) if x
                    ))
                    c.execute(
                        """UPDATE sources SET source_type=?, platform=?, title=?, snippet=?,
                           search_id=?, domain=?, site_state=?, state_locked=?,
                           eval_reasons=?, created_at=? WHERE id=?""",
                        (
                            existing["source_type"] or old["source_type"],
                            existing["platform"] or old["platform"],
                            existing["title"] or old["title"],
                            existing["snippet"] or old["snippet"],
                            existing["search_id"] if existing["search_id"] is not None else old["search_id"],
                            existing["domain"] or old["domain"],
                            site_state,
                            int(locked),
                            "\n".join(reasons),
                            min(existing["created_at"], old["created_at"]),
                            existing["id"]
                        )
                    )
                    c.execute("DELETE FROM sources WHERE id=?", (old["id"],))

            # Social links: keep each distinct URL and combine duplicate metadata.
            for old in c.execute(
                "SELECT * FROM socials WHERE business_id=?", (secondary,)
            ).fetchall():
                existing = c.execute(
                    "SELECT * FROM socials WHERE business_id=? AND url=?",
                    (primary, old["url"])
                ).fetchone()

                if existing is None:
                    c.execute(
                        "UPDATE socials SET business_id=? WHERE id=?",
                        (primary, old["id"])
                    )
                else:
                    c.execute(
                        "UPDATE socials SET platform=?, source_url=? WHERE id=?",
                        (
                            existing["platform"] or old["platform"],
                            existing["source_url"] or old["source_url"],
                            existing["id"]
                        )
                    )
                    c.execute("DELETE FROM socials WHERE id=?", (old["id"],))

            # Preserve both businesses' history.
            for table in ("verification_log", "outreach_log"):
                c.execute(
                    f"UPDATE {table} SET business_id=? WHERE business_id=?",
                    (primary, secondary)
                )

            notes = "\n".join(x for x in (p["notes"], s["notes"]) if x)
            dnc = int(bool(p["do_not_contact"] or s["do_not_contact"]))
            pipeline = "Do not contact" if dnc else (
                p["pipeline"] if p["pipeline"] != "New" else s["pipeline"]
            )

            key = p["key"]
            if key.startswith("name:") and s["key"].startswith("site:"):
                key = s["key"]

            signals = p["site_signals"] if p["site_signals"] not in ("", "{}") else s["site_signals"]
            verification = p["verification"]
            if verification == "pending" and s["verification"] == "verified":
                verification = "verified"

            # Remove duplicate-dismissal references before deleting the secondary row.
            c.execute(
                "DELETE FROM dup_dismissed WHERE a IN (?,?) OR b IN (?,?)",
                (primary, secondary, primary, secondary)
            )

            # Delete only after every child record has been transferred or consolidated.
            c.execute("DELETE FROM businesses WHERE id=?", (secondary,))

            c.execute(
                """UPDATE businesses SET key=?, notes=?, do_not_contact=?, pipeline=?,
                   site_signals=?, verification=?, website_lookup_done=?, updated_at=?
                   WHERE id=?""",
                (
                    key, notes, dnc, pipeline, signals, verification,
                    int(bool(p["website_lookup_done"] or s["website_lookup_done"])),
                    now_iso(), primary
                )
            )

            self._log_verification(
                c, primary, "duplicate", "merged",
                f"merged #{secondary} ({s['name']})"
            )

        self.recompute(primary)
    # ---- backup / restore --------------------------------------------------------------
    def backup_bytes(self) -> bytes:
        with tempfile.TemporaryDirectory() as td:
            dst_path = Path(td) / "backup.db"
            src = sqlite3.connect(self.path)
            dst = sqlite3.connect(dst_path)
            try:
                src.backup(dst)
            finally:
                dst.close()
                src.close()
            return dst_path.read_bytes()

    def restore_bytes(self, data: bytes) -> None:
        """Validate a backup completely before replacing the active database."""
        if not isinstance(data, bytes) or not data.startswith(b"SQLite format 3\x00"):
            raise ValueError("That file is not a SQLite database.")

        expected_columns = {
            "businesses": {
                "id", "key", "name", "norm_name", "category", "city", "country",
                "official_website", "candidate_website", "website_status",
                "website_locked", "website_lookup_done", "site_signals", "score",
                "score_reasons", "verification", "pipeline", "notes",
                "do_not_contact", "first_search_id", "created_at", "updated_at"
            },
            "emails": {
                "id", "business_id", "email", "kind", "domain_match", "is_free",
                "confidence", "source_url", "check_result", "confirmed", "created_at"
            },
            "phones": {
                "id", "business_id", "phone", "confidence", "source_url", "created_at"
            },
            "sources": {
                "id", "business_id", "url", "source_type", "platform", "title",
                "snippet", "search_id", "domain", "site_state", "state_locked",
                "eval_reasons", "created_at"
            },
            "socials": {
                "id", "business_id", "platform", "url", "source_url", "created_at"
            },
            "searches": {
                "id", "created_at", "angle", "query", "country", "city",
                "category", "page", "include_domains", "exclude_domains",
                "results", "new_leads", "status", "error", "settings"
            },
            "verification_log": {
                "id", "business_id", "created_at", "check_type", "result", "detail"
            },
            "outreach_log": {
                "id", "business_id", "created_at", "from_status", "to_status", "note"
            },
            "dup_dismissed": {"a", "b"},
            "settings": {"key", "value"}
        }

        target = Path(self.path).resolve()
        target.parent.mkdir(parents=True, exist_ok=True)

        def validate_database(connection):
            integrity = connection.execute("PRAGMA integrity_check").fetchone()
            if not integrity or integrity[0] != "ok":
                raise ValueError("The backup database failed SQLite integrity checks.")

            tables = {
                row[0] for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                )
            }

            if not set(expected_columns).issubset(tables):
                raise ValueError(
                    "That database is missing required Siteora tables."
                )

            for table, required in expected_columns.items():
                actual = {
                    row[1] for row in connection.execute(
                        f'PRAGMA table_info("{table}")'
                    )
                }
                missing = required - actual
                if missing:
                    raise ValueError(
                        f"Backup table '{table}' is missing columns: "
                        + ", ".join(sorted(missing))
                    )

            foreign_key_errors = connection.execute(
                "PRAGMA foreign_key_check"
            ).fetchall()
            if foreign_key_errors:
                raise ValueError(
                    "The backup contains broken foreign-key references."
                )

        # Use the destination directory so the final replacement stays
        # on the same filesystem.
        with tempfile.TemporaryDirectory(
            prefix="siteora_restore_", dir=str(target.parent)
        ) as td:
            incoming = Path(td) / "incoming.db"
            staged = Path(td) / "staged.db"
            incoming.write_bytes(data)

            probe = sqlite3.connect(str(incoming), timeout=30)
            try:
                validate_database(probe)

                staged_connection = sqlite3.connect(str(staged), timeout=30)
                try:
                    probe.backup(staged_connection)
                    validate_database(staged_connection)
                finally:
                    staged_connection.close()
            finally:
                probe.close()

            # Checkpoint the existing database before replacing its file.
            if target.exists():
                current = sqlite3.connect(str(target), timeout=30)
                try:
                    result = current.execute(
                        "PRAGMA wal_checkpoint(TRUNCATE)"
                    ).fetchone()
                    if result and result[0] != 0:
                        raise RuntimeError(
                            "The current database is busy. Close other app "
                            "sessions and try restoring again."
                        )
                finally:
                    current.close()

            # Remove stale WAL sidecars only after a successful checkpoint.
            for suffix in ("-wal", "-shm"):
                sidecar = Path(str(target) + suffix)
                if sidecar.exists():
                    sidecar.unlink()

            os.replace(str(staged), str(target))

        self._init_schema()


# ----------------------------------------------------------------------------
# Search pipeline
# ----------------------------------------------------------------------------
WEBSITE_EXCLUDES = [
    "facebook.com", "instagram.com", "tiktok.com", "linkedin.com", "x.com", "youtube.com", "yelp.com",
    "yellowpages.com.au", "truelocal.com.au", "tripadvisor.com", "foursquare.com", "hotfrog.com.au",
    "localsearch.com.au", "google.com", "reddit.com", "wikipedia.org",
]


@dataclass
class SearchParams:
    country: str = "AU"
    cities: List[str] = field(default_factory=list)
    categories: List[str] = field(default_factory=list)
    extra: str = ""
    angles: Tuple[str, ...] = ("websites", "listings")
    pages: int = 1
    require_email: bool = True
    collect_social: bool = False
    enrich: bool = True
    lookup_websites: bool = True
    lookup_cap: int = 15
    skip_lists: bool = True
    language: str = "en"
    listing_domains: List[str] = field(default_factory=lambda: list(DEFAULT_LISTING_DOMAINS))
    workers: int = 6
    check_mx: bool = False
    max_jobs: int = 80

    def snapshot(self) -> dict:
        return {"require_email": self.require_email, "collect_social": self.collect_social,
                "enrich": self.enrich, "lookup_websites": self.lookup_websites, "pages": self.pages}


@dataclass
class Job:
    angle: str
    city: str
    category: str
    query: str
    include: List[str]
    exclude: List[str]
    page: int
    country: str


@dataclass
class Record:
    search_id: int
    city: str
    category: str
    url: str
    title: str
    snippet: str
    rtype: str
    platform: str
    domain: str
    name: str
    norm: str


@dataclass
class RunStats:
    api_calls: int = 0
    results: int = 0
    skipped_lists: int = 0
    skipped_other: int = 0
    businesses_found: int = 0
    new_leads: int = 0
    merged: int = 0
    excluded_no_email: int = 0
    lookups: int = 0
    jobs_planned: int = 0
    errors: List[str] = field(default_factory=list)
    aborted: bool = False


def build_jobs(params: SearchParams) -> List[Job]:
    jobs: List[Job] = []
    for city in params.cities:
        for category in params.categories:
            base = f"{category} {city} {params.extra}".strip()
            for angle in params.angles:
                if angle == "social" and not params.collect_social:
                    continue  # social results are only requested when collection is ON
                if angle == "websites":
                    q, inc, exc = base, [], WEBSITE_EXCLUDES
                elif angle == "contact":
                    q, inc, exc = f"{base} contact email phone", [], WEBSITE_EXCLUDES
                elif angle == "listings":
                    q, inc, exc = base, params.listing_domains[:8], []
                elif angle == "social":
                    q, inc, exc = base, SOCIAL_SEARCH_DOMAINS, []
                else:
                    continue
                for page in range(max(1, params.pages)):
                    jobs.append(Job(angle, city, category, q, list(inc), list(exc), page, params.country))
    return jobs[: params.max_jobs]


def make_record(r: SearchResult, job: Job, sid: int, params: SearchParams, stats: RunStats) -> Optional[Record]:
    rtype, platform = classify_url(r.url)
    if rtype in ("invalid", "thirdparty"):
        stats.skipped_other += 1
        return None
    if rtype == "social" and (not params.collect_social or not is_social_profile(r.url, platform)):
        stats.skipped_other += 1
        return None
    if params.skip_lists and is_list_page(r.title, r.url):
        stats.skipped_lists += 1
        return None
    host = host_of(r.url)
    fallback = site_domain(host).split(".")[0].replace("-", " ").title()
    name = clean_business_name(r.title, fallback=r.site_name or fallback)
    norm = normalize_name(name)
    if not norm:
        stats.skipped_other += 1
        return None
    domain = site_domain(host) if rtype == "website" else ""
    return Record(sid, job.city, job.category, r.url, r.title, r.snippet, rtype, platform, domain, name, norm)


def _add_source(d: Draft, rec: Record) -> None:
    if any(s["url"] == rec.url for s in d.sources):
        return
    d.sources.append({"url": rec.url, "source_type": rec.rtype, "platform": rec.platform, "title": rec.title,
                      "snippet": rec.snippet, "search_id": rec.search_id, "domain": rec.domain})


def group_records(records: List[Record], country: str) -> List[Draft]:
    """Group search results into businesses.

    * results on the same website domain are one business;
    * a directory/social result joins a website business ONLY when the normalised
      name is identical, the city is identical and exactly one such business exists;
    * everything else stays a separate business (never fuzzily merged)."""
    drafts: Dict[str, Draft] = {}
    by_name_city: Dict[Tuple[str, str], List[str]] = {}
    for rec in records:
        if rec.rtype != "website":
            continue
        key = "site:" + rec.domain
        d = drafts.get(key)
        if d is None:
            d = Draft(rec.name, rec.norm, rec.city, rec.category, country, rec.search_id)
            d.key_domain = rec.domain
            d.site_domains[rec.domain] = root_url(rec.url)
            drafts[key] = d
            by_name_city.setdefault((rec.norm, rec.city.lower()), []).append(key)
        _add_source(d, rec)
    for rec in records:
        if rec.rtype == "website":
            continue
        candidates = by_name_city.get((rec.norm, rec.city.lower()), [])
        if len(candidates) == 1:
            _add_source(drafts[candidates[0]], rec)
            continue
        key = f"name:{rec.norm}|{rec.city.lower()}"
        d = drafts.get(key)
        if d is None:
            d = Draft(rec.name, rec.norm, rec.city, rec.category, country, rec.search_id)
            drafts[key] = d
        _add_source(d, rec)
    return list(drafts.values())


def _safe_fetch(fetcher, url: str) -> PageInfo:
    try:
        return fetcher.fetch_site(url)
    except Exception as exc:  # a broken site must never crash the run
        return PageInfo(url=url, error=f"fetch error: {type(exc).__name__}")


def verify_sites(drafts: List[Draft], params: SearchParams, fetcher) -> None:
    """Fetch (optionally) and evaluate every candidate website that has not been evaluated yet."""
    tasks = [(d, dom, root) for d in drafts for dom, root in d.site_domains.items() if dom not in d.states]
    if params.enrich and fetcher is not None and tasks:
        with ThreadPoolExecutor(max_workers=max(1, params.workers)) as pool:
            futures = {pool.submit(_safe_fetch, fetcher, root): (d, dom) for d, dom, root in tasks}
            for fut in as_completed(futures):
                d, dom = futures[fut]
                d.pages[dom] = fut.result()
    for d, dom, _root in tasks:
        d.states[dom] = evaluate_website(d.name, d.city, d.category, d.pages.get(dom))
    # A domain claimed by several different businesses cannot be trusted as any one's website.
    claims: Dict[str, int] = {}
    for d in drafts:
        for dom in d.site_domains:
            claims[dom] = claims.get(dom, 0) + 1
    for d in drafts:
        for dom in d.site_domains:
            if claims[dom] > 1 and d.states[dom].state == "verified":
                d.states[dom] = WebsiteEval("needs_review", d.states[dom].points,
                                            d.states[dom].reasons + ["same domain matched several businesses"])


def lookup_official_websites(drafts: List[Draft], params: SearchParams, client, db: Database, stats: RunStats) -> None:
    """For businesses seen only on directories/social pages, search for their own website.
    Candidates become sources with a state; they never change the business's identity key."""
    targets = [d for d in drafts if not d.site_domains][: max(0, params.lookup_cap)]
    for d in targets:
        job = Job("website_lookup", d.city, d.category, f"{d.name} {d.city} official website", [],
                  WEBSITE_EXCLUDES, 0, params.country)
        sid = db.log_search_start(job.__dict__, params.snapshot())
        try:
            payload = client.search(job.query, location=params.country, language=params.language,
                                    exclude_domains=WEBSITE_EXCLUDES)
            results = parse_results(payload)
        except (TinyFishError, ValueError) as exc:
            db.log_search_finish(sid, 0, 0, "error", str(exc))
            stats.errors.append(f"Website lookup for {d.name}: {exc}")
            if isinstance(exc, TinyFishError) and exc.fatal:
                stats.aborted = True
                break
            continue
        db.log_search_finish(sid, len(results), 0, "ok")
        stats.api_calls += 1
        stats.lookups += 1
        d.lookup_done = True
        added = 0
        for r in results[:6]:
            rtype, _ = classify_url(r.url)
            if rtype != "website":
                continue
            host = host_of(r.url)
            plausible = domain_matches_name(host, d.name) or normalize_name(d.name) in normalize_name(r.title)
            if not plausible:
                continue
            dom = site_domain(host)
            d.site_domains.setdefault(dom, root_url(r.url))
            d.sources.append({"url": r.url, "source_type": "website", "platform": "", "title": r.title,
                              "snippet": r.snippet, "search_id": sid, "domain": dom})
            added += 1
            if added >= 2:
                break


def _keep_best(store: Dict[str, dict], key: str, item: dict) -> None:
    cur = store.get(key)
    if cur is None or CONF_RANK.get(item["confidence"], 0) > CONF_RANK.get(cur["confidence"], 0):
        store[key] = item


def collect_contacts(d: Draft, params: SearchParams) -> None:
    """Attach emails / phones / social links to this business from ITS OWN pages and snippets only."""
    emails: Dict[str, dict] = {}
    phones: Dict[str, dict] = {}
    socials: Dict[str, dict] = {}
    verified_doms = {dom for dom, ev in d.states.items() if ev.state == "verified"}
    site_doms = list(d.site_domains)

    for dom, page in d.pages.items():
        if page is None or not page.reachable:
            continue
        verified = dom in verified_doms
        page_emails = page.emails
        if len(page_emails) > 5:  # looks like a directory/staff dump: keep only same-domain emails
            page_emails = {e: s for e, s in page_emails.items() if email_matches_site(e, [dom])}
        for e, src in page_emails.items():
            dm, free = email_matches_site(e, [dom]), email_is_free(e)
            _keep_best(emails, e, {"email": e, "kind": "site_page", "domain_match": dm, "is_free": free,
                                   "confidence": email_confidence("site_page", verified, dm, free), "source_url": src,
                                   "check_result": check_email(e, params.check_mx)})
        for p, src in page.phones.items():
            _keep_best(phones, p, {"phone": p, "confidence": "high" if verified else "low", "source_url": src})
        if params.collect_social:
            for link, (platform, src) in page.socials.items():
                socials[link] = {"platform": platform, "url": link, "source_url": src}

    for s in d.sources:
        snippet = s.get("snippet", "")
        found_emails, found_phones = extract_emails(snippet), extract_phones(snippet)
        if len(found_emails) > 2 or len(found_phones) > 2:
            continue  # a snippet naming many contacts describes several businesses: ignore it
        kind = "snippet_site" if s["source_type"] == "website" else "snippet_other"
        for e in found_emails:
            dm, free = email_matches_site(e, site_doms), email_is_free(e)
            _keep_best(emails, e, {"email": e, "kind": kind, "domain_match": dm, "is_free": free,
                                   "confidence": email_confidence(kind, bool(verified_doms), dm, free),
                                   "source_url": s["url"], "check_result": check_email(e, params.check_mx)})
        for p in found_phones:
            _keep_best(phones, p, {"phone": p, "confidence": "low", "source_url": s["url"]})
        if params.collect_social and s["source_type"] == "social":
            socials[s["url"]] = {"platform": s["platform"], "url": s["url"], "source_url": s["url"]}

    d.emails, d.phones, d.socials = list(emails.values()), list(phones.values()), list(socials.values())
    page = next((d.pages[dom] for dom in verified_doms if d.pages.get(dom)), None) or \
        next((p for p in d.pages.values() if p and p.reachable), None)
    if page is not None:
        d.signals = {"https": page.https, "viewport": page.viewport, "builder": page.builder,
                     "copyright_year": page.copyright_year}


def run_search(db: Database, client, fetcher, params: SearchParams,
               progress: Optional[Callable[[float, str], None]] = None) -> RunStats:
    stats = RunStats()
    jobs = build_jobs(params)
    stats.jobs_planned = len(jobs)

    def tick(frac: float, text: str) -> None:
        if progress:
            progress(min(max(frac, 0.0), 1.0), text)

    records: List[Record] = []
    for i, job in enumerate(jobs):
        tick(0.45 * i / max(len(jobs), 1), f"Searching {i + 1}/{len(jobs)}: {job.query}")
        sid = db.log_search_start(job.__dict__, params.snapshot())
        try:
            payload = client.search(job.query, location=job.country, language=params.language,
                                    include_domains=job.include or None, exclude_domains=job.exclude or None,
                                    page=job.page)
            results = parse_results(payload)
        except (TinyFishError, ValueError) as exc:
            db.log_search_finish(sid, 0, 0, "error", str(exc))
            stats.errors.append(str(exc))
            if isinstance(exc, TinyFishError) and exc.fatal:
                stats.aborted = True
                break
            continue
        db.log_search_finish(sid, len(results), 0, "ok")
        stats.api_calls += 1
        stats.results += len(results)
        for r in results:
            rec = make_record(r, job, sid, params, stats)
            if rec:
                records.append(rec)

    drafts = group_records(records, params.country)
    stats.businesses_found = len(drafts)
    tick(0.5, f"Verifying {len(drafts)} businesses' websites")
    verify_sites(drafts, params, fetcher)
    if params.lookup_websites and not stats.aborted:
        tick(0.7, "Looking up official websites")
        lookup_official_websites(drafts, params, client, db, stats)
        verify_sites(drafts, params, fetcher)
    tick(0.9, "Saving leads")
    for d in drafts:
        collect_contacts(d, params)
        if params.require_email and not d.emails and not db.exists_for(d):
            stats.excluded_no_email += 1
            continue
        _bid, created = db.save_draft(d)
        if created:
            stats.new_leads += 1
            db.add_search_new_leads(d.first_search_id, 1)
        else:
            stats.merged += 1
    tick(1.0, "Done")
    return stats


def recheck_business(db: Database, fetcher, bid: int, domain: str, collect_social: bool = False) -> WebsiteEval:
    """Re-fetch one candidate website for an existing business and store the verification result."""
    b = db.get_business(bid)
    if b is None:
        raise ValueError("business not found")
    root = next((root_url(s["url"]) for s in db.snapshot()["sources"]
                 if s["business_id"] == bid and s["source_type"] == "website" and s["domain"] == domain), "")
    if not root:
        raise ValueError("website not found for this business")
    page = _safe_fetch(fetcher, root)
    ev = evaluate_website(b["name"], b["city"], b["category"], page)
    draft = Draft(b["name"], b["norm_name"], b["city"], b["category"], b["country"])
    draft.site_domains[domain] = root
    draft.pages[domain] = page
    draft.states[domain] = ev
    collect_contacts(draft, SearchParams(collect_social=collect_social))
    db.apply_recheck(bid, domain, ev, draft.signals, draft.emails, draft.phones, draft.socials)
    return ev


# ----------------------------------------------------------------------------
# Duplicate detection
# ----------------------------------------------------------------------------
def find_duplicates(snapshot: dict) -> List[dict]:
    """Return candidate duplicate pairs [{a, b, reasons}]. Never merges anything automatically."""
    from collections import defaultdict
    from itertools import combinations

    biz = {b["id"]: b for b in snapshot["businesses"] if b["verification"] != "rejected"}
    dismissed = snapshot.get("dismissed", set())
    pairs: Dict[Tuple[int, int], List[str]] = {}

    def add(a: int, b: int, reason: str) -> None:
        lo, hi = sorted((a, b))
        if lo != hi and (lo, hi) not in dismissed:
            pairs.setdefault((lo, hi), []).append(reason)

    def bucket_pairs(groups: Dict[str, set], label: str) -> None:
        for value, ids in groups.items():
            if 1 < len(ids) <= 25:
                for a, b in combinations(sorted(ids), 2):
                    add(a, b, f"{label}: {value}")

    by_domain, by_email, by_phone = defaultdict(set), defaultdict(set), defaultdict(set)
    for s in snapshot["sources"]:
        if s["business_id"] in biz and s["source_type"] == "website" and s["domain"]:
            by_domain[s["domain"]].add(s["business_id"])
    for e in snapshot["emails"]:
        if e["business_id"] in biz:
            by_email[e["email"]].add(e["business_id"])
    for p in snapshot["phones"]:
        if p["business_id"] in biz and phone_key(p["phone"]):
            by_phone[phone_key(p["phone"])].add(p["business_id"])
    bucket_pairs(by_domain, "same website")
    bucket_pairs(by_email, "same email")
    bucket_pairs(by_phone, "same phone")

    by_name, by_city = defaultdict(set), defaultdict(list)
    for bid, b in biz.items():
        city = (b["city"] or "").lower()
        by_name[f"{b['norm_name']} / {city}"].add(bid)
        by_city[city].append((bid, b["norm_name"]))
    bucket_pairs(by_name, "same name and city")
    for items in by_city.values():
        if len(items) > 400:
            continue
        for (a, na), (b, nb) in combinations(items, 2):
            if na != nb and SequenceMatcher(None, na, nb).ratio() >= 0.9:
                add(a, b, "very similar name")
    return [{"a": a, "b": b, "reasons": r} for (a, b), r in sorted(pairs.items())]


# ----------------------------------------------------------------------------
# Lead table, flags and qualification
# ----------------------------------------------------------------------------
def compute_flags(b: dict, emails: List[dict], is_duplicate: bool) -> List[str]:
    flags: List[str] = []
    status = b["website_status"]
    if not b["website_locked"]:
        if status == WS_REVIEW:
            flags.append("Website needs review")
        if status in (WS_NONE, WS_SOCIAL, WS_DIRECTORY) and not b["website_lookup_done"]:
            flags.append("Official-website lookup not run")
    if status == WS_UNREACHABLE and not b["website_locked"]:
        flags.append("Website unreachable - confirm it is really down")
    if emails and not any(e["confidence"] in TRUSTED_CONF for e in emails):
        flags.append("Email low confidence")
    if is_duplicate:
        flags.append("Possible duplicate")
    return flags


FRAME_COLUMNS = [
    "id", "name", "category", "city", "country", "website_status", "official_website", "candidate_website",
    "primary_email", "email_confidence", "email_source", "all_emails", "has_email", "has_trusted_email",
    "phones", "socials", "source_urls", "score", "score_reasons", "verification", "pipeline", "notes",
    "flags", "qualified", "dnc", "created_at", "updated_at",
]


def build_frame(snapshot: dict, require_email: bool = True, min_score: int = 40, show_social: bool = False) -> pd.DataFrame:
    from collections import defaultdict

    emails_by, phones_by, srcs_by, socials_by = defaultdict(list), defaultdict(list), defaultdict(list), defaultdict(list)
    for e in snapshot["emails"]:
        emails_by[e["business_id"]].append(e)
    for p in snapshot["phones"]:
        phones_by[p["business_id"]].append(p)
    for s in snapshot["sources"]:
        srcs_by[s["business_id"]].append(s)
    for s in snapshot["socials"]:
        socials_by[s["business_id"]].append(s)
    dup_ids = set()
    for pair in find_duplicates(snapshot):
        dup_ids.update((pair["a"], pair["b"]))

    rows = []
    for b in snapshot["businesses"]:
        bid = b["id"]
        es = sorted(emails_by[bid], key=lambda e: -CONF_RANK.get(e["confidence"], 0))
        ps = sorted(phones_by[bid], key=lambda p: -CONF_RANK.get(p["confidence"], 0))
        flags = compute_flags(b, es, bid in dup_ids)
        trusted = any(e["confidence"] in TRUSTED_CONF for e in es)
        qualified = (
            b["verification"] != "rejected" and not b["do_not_contact"]
            and "Possible duplicate" not in flags
            and (b["verification"] == "verified" or not flags)
            and b["score"] >= min_score and (trusted or not require_email)
        )
        social_text = "; ".join(f"{s['platform']}: {s['url']}" for s in socials_by[bid]) if show_social else ""
        rows.append({
            "id": bid, "name": b["name"], "category": b["category"], "city": b["city"], "country": b["country"],
            "website_status": b["website_status"], "official_website": b["official_website"],
            "candidate_website": b["candidate_website"],
            "primary_email": es[0]["email"] if es else "",
            "email_confidence": es[0]["confidence"] if es else "",
            "email_source": es[0]["source_url"] if es else "",
            "all_emails": "; ".join(f"{e['email']} ({e['confidence']})" for e in es),
            "has_email": bool(es), "has_trusted_email": trusted,
            "phones": "; ".join(p["phone"] for p in ps), "socials": social_text,
            "source_urls": "\n".join(dict.fromkeys(s["url"] for s in srcs_by[bid])),
            "score": b["score"], "score_reasons": b["score_reasons"], "verification": b["verification"],
            "pipeline": b["pipeline"], "notes": b["notes"], "flags": "; ".join(flags), "qualified": qualified,
            "dnc": bool(b["do_not_contact"]), "created_at": b["created_at"], "updated_at": b["updated_at"],
        })
    return pd.DataFrame(rows, columns=FRAME_COLUMNS)


def apply_view_filters(df: pd.DataFrame, cities: List[str], require_email: bool) -> Tuple[pd.DataFrame, int]:
    """City filter + email rule. Preserve all columns, including for empty dataframes."""
    out = df

    if cities:
        out = out.loc[out["city"].isin(cities)]

    hidden = 0

    if require_email:
        email_mask = out["has_email"].fillna(False).astype(bool)
        hidden = int((~email_mask).sum())
        out = out.loc[email_mask].copy()

    return out, hidden


# ----------------------------------------------------------------------------
# Excel / CSV export
# ----------------------------------------------------------------------------
_PHONE_LIKE_RE = re.compile(r"^[+\-]?\d[\d\s().;+\-]{6,}$")


def _safe_cell(value):
    """Neutralise spreadsheet formula injection from scraped text.

    Phone numbers such as '+61412345678' start with '+' but are plain data, so they are left alone."""
    if isinstance(value, str) and value[:1] in ("=", "+", "-", "@", "\t", "\r"):
        if _PHONE_LIKE_RE.match(value):
            return value
        return "'" + value
    return value


def sanitize_frame(df: pd.DataFrame) -> pd.DataFrame:
    return df.apply(lambda col: col.map(_safe_cell) if col.dtype == object else col)


def _qualified_sheet(f: pd.DataFrame, show_social: bool) -> pd.DataFrame:
    cols = {"name": "Business", "category": "Category", "city": "City", "country": "Country",
            "website_status": "Website status", "official_website": "Official website",
            "primary_email": "Primary email", "email_confidence": "Email confidence", "phones": "Phone(s)",
            "score": "Lead score", "score_reasons": "Score reasons", "pipeline": "Pipeline",
            "verification": "Verification", "source_urls": "Source URLs", "notes": "Notes"}
    if show_social:
        cols["socials"] = "Social links"
    return f.rename(columns=cols)[list(cols.values())]


def _email_sheet(f: pd.DataFrame) -> pd.DataFrame:
    out = f.assign(qualified_text=f["qualified"].map({True: "Yes", False: "No"}))
    cols = {"name": "Business", "city": "City", "primary_email": "Email", "email_confidence": "Confidence",
            "email_source": "Email found on", "all_emails": "All emails", "website_status": "Website status",
            "score": "Lead score", "qualified_text": "Qualified", "pipeline": "Pipeline", "source_urls": "Source URLs"}
    return out.rename(columns=cols)[list(cols.values())]


def _review_sheet(f: pd.DataFrame) -> pd.DataFrame:
    cols = {"name": "Business", "city": "City", "website_status": "Website status", "flags": "Why it needs review",
            "candidate_website": "Candidate website", "all_emails": "Emails (confidence)", "phones": "Phone(s)",
            "score": "Lead score", "verification": "Verification", "source_urls": "Source URLs"}
    return f.rename(columns=cols)[list(cols.values())]


def build_workbook(frame: pd.DataFrame, history: pd.DataFrame, show_social: bool = False) -> bytes:
    """Excel file with four sheets: Qualified leads, Email leads, Review leads, Search history.
    Rejected and do-not-contact businesses are never exported as leads."""
    from openpyxl.styles import Alignment, Font, PatternFill
    from openpyxl.utils import get_column_letter

    active = frame[(frame["verification"] != "rejected") & (~frame["dnc"].astype(bool))]
    sheets = {
        "Qualified leads": _qualified_sheet(active[active["qualified"].astype(bool)], show_social),
        "Email leads": _email_sheet(active[active["has_trusted_email"].astype(bool)]),
        "Review leads": _review_sheet(active[active["flags"] != ""]),
        "Search history": history.copy(),
    }
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        for name, df in sheets.items():
            df = sanitize_frame(df.reset_index(drop=True))
            df.to_excel(writer, index=False, sheet_name=name)
            ws = writer.sheets[name]
            ws.freeze_panes = "A2"
            if ws.max_row >= 1 and ws.max_column >= 1:
                ws.auto_filter.ref = ws.dimensions
            for cell in ws[1]:
                cell.font = Font(bold=True, color="FFFFFF")
                cell.fill = PatternFill("solid", fgColor="1F4E78")
                cell.alignment = Alignment(vertical="center", wrap_text=True)
            for idx, col in enumerate(df.columns, 1):
                longest = max([len(str(col))] + [min(len(str(v)), 60) for v in df[col].head(200)])
                ws.column_dimensions[get_column_letter(idx)].width = max(12, min(longest + 2, 62))
    return buf.getvalue()


# ----------------------------------------------------------------------------
# Streamlit UI
# ----------------------------------------------------------------------------
def _secret(name: str) -> str:
    try:
        return str(st.secrets[name])
    except Exception:
        return ""


def get_api_key() -> str:
    """Existing deployment secret first (TINYFISH_API_KEY), then env var, then sidebar input."""
    return _secret("TINYFISH_API_KEY") or os.environ.get("TINYFISH_API_KEY", "") or st.session_state.get("manual_api_key", "")


def resolve_db_path() -> str:
    path = os.environ.get("LEAD_DB_PATH") or _secret("LEAD_DB_PATH")
    return path or str(Path(__file__).resolve().parent / DEFAULT_DB_NAME)


@st.cache_resource
def get_db(path: str) -> Database:
    return Database(path)


def _wide(func) -> dict:
    """Full-width kwargs that work on both old (use_container_width) and new (width) Streamlit."""
    try:
        params = set(inspect.signature(func).parameters)
    except (TypeError, ValueError):
        params = set()
    return {"width": "stretch"} if "width" in params else {"use_container_width": True}


def md_link(url: str, label: str = "") -> str:
    safe = url.replace("(", "%28").replace(")", "%29").replace(" ", "%20")
    return f"[{label or url}]({safe})"


def persistent_toggle(db: Database, label: str, key: str, default: bool, help_text: str) -> bool:
    stored = db.get_bool(key, default)
    value = st.toggle(label, value=stored, key=f"toggle_{key}", help=help_text)
    if value != stored:
        db.set_setting(key, value)
    return value


@dataclass
class UiOptions:
    enrich: bool = True
    respect_robots: bool = True
    lookup_websites: bool = True
    lookup_cap: int = 15
    timeout: int = 8
    workers: int = 6
    rpm: int = 30
    skip_lists: bool = True
    check_mx: bool = False


def render_find(db: Database, require_email: bool, collect_social: bool, opts: UiOptions) -> None:
    st.subheader("Find businesses")
    c1, c2 = st.columns(2)
    country = c1.text_input("Country code (2 letters, e.g. AU, US, GB)", value=db.get_setting("country", "AU"),
                            max_chars=2, help="TinyFish's `location` parameter is a country code, not a city.").strip().upper()
    cities_raw = c1.text_area("Cities / areas (one per line)", value="Brisbane")
    categories_raw = c2.text_area("Business categories (one per line)", value="barbershops")
    extra = c2.text_input("Optional search detail", placeholder="e.g. independent, family owned")

    st.markdown("**Search angles**")
    a1, a2, a3, a4 = st.columns(4)
    ang_web = a1.checkbox("Business websites", value=True, help="Web results excluding directories and social sites.")
    ang_list = a2.checkbox("Directory listings", value=True, help="Only Yelp, Yellow Pages, TrueLocal etc.")
    ang_contact = a3.checkbox("Contact pages", value=False)
    ang_social = a4.checkbox("Social pages", value=False, disabled=not collect_social,
                             help="Requires the 'Collect social media links' toggle." if not collect_social else None)
    listing_text = st.text_input("Directory domains for the listings angle (comma separated, max 8)",
                                 value=", ".join(DEFAULT_LISTING_DOMAINS))
    p1, p2 = st.columns(2)
    pages = p1.slider("Result pages per search (about 10 results each)", 1, 3, 1)
    lookup_cap = p2.number_input("Max official-website lookups per run", 0, 60, opts.lookup_cap)

    cities = [x.strip() for x in cities_raw.splitlines() if x.strip()]
    categories = [x.strip() for x in categories_raw.splitlines() if x.strip()]
    angles = tuple(a for a, on in (("websites", ang_web), ("listings", ang_list), ("contact", ang_contact),
                                   ("social", ang_social and collect_social)) if on)
    params = SearchParams(
        country=country, cities=cities, categories=categories, extra=extra.strip(), angles=angles, pages=pages,
        require_email=require_email, collect_social=collect_social, enrich=opts.enrich,
        lookup_websites=opts.lookup_websites, lookup_cap=int(lookup_cap), skip_lists=opts.skip_lists,
        listing_domains=[d.strip() for d in listing_text.split(",") if d.strip()], workers=opts.workers,
        check_mx=opts.check_mx,
    )
    planned = len(build_jobs(params))
    extra_calls = int(lookup_cap) if opts.lookup_websites else 0
    minutes = (planned + extra_calls) * 60.0 / max(1, opts.rpm) / 60.0 * 1.05
    st.caption(f"Up to **{planned + extra_calls}** TinyFish calls ({planned} searches + up to {extra_calls} website lookups), "
               f"about {minutes:.1f} min at {opts.rpm} requests/min.")
    if require_email and not opts.enrich:
        st.warning("'Require public business email' is ON but website visits are OFF: most businesses will be excluded "
                   "because emails are normally found on the business's own website.")

    problems = []
    if not re.fullmatch(r"[A-Z]{2}", country):
        problems.append("Country must be a 2-letter code such as AU.")
    if not cities or not categories or not angles:
        problems.append("Enter at least one city, one category and one search angle.")
    for p in problems:
        st.error(p)

    if st.button("Run search", type="primary", disabled=bool(problems)):
        api_key = get_api_key()
        if not api_key:
            st.error("TinyFish API key is missing. Add TINYFISH_API_KEY in Streamlit Secrets.")
        else:
            db.set_setting("country", country)
            client = TinyFishClient(api_key, requests_per_minute=opts.rpm)
            fetcher = SiteFetcher(opts.timeout, opts.respect_robots, collect_social)
            bar = st.progress(0.0, text="Starting...")
            stats = run_search(db, client, fetcher, params, progress=lambda f, t: bar.progress(f, text=t[:90]))
            bar.empty()
            st.session_state["last_run"] = stats.__dict__
            st.rerun()

    last = st.session_state.get("last_run")
    if last:
        st.success(f"Last run: {last['api_calls']} API calls, {last['results']} results, {last['businesses_found']} businesses "
                   f"-> **{last['new_leads']} new**, {last['merged']} merged into existing leads.")
        m = st.columns(4)
        m[0].metric("Excluded (no public email)", last["excluded_no_email"])
        m[1].metric("Skipped list/article pages", last["skipped_lists"])
        m[2].metric("Other results skipped", last["skipped_other"])
        m[3].metric("Website lookups", last["lookups"])
        if last["aborted"]:
            st.error("The run was stopped early (invalid key, no credit, or daily allowance used).")
        for err in last["errors"][:8]:
            st.error(err)


def _business_summary(frame: pd.DataFrame, bid: int) -> Optional[pd.Series]:
    rows = frame[frame["id"] == bid]
    return rows.iloc[0] if len(rows) else None


def render_review_card(db: Database, row: pd.Series, snapshot: dict, opts: UiOptions, collect_social: bool) -> None:
    bid = int(row["id"])
    b = db.get_business(bid)
    emails = [e for e in snapshot["emails"] if e["business_id"] == bid]
    phones = [p for p in snapshot["phones"] if p["business_id"] == bid]
    sources = [s for s in snapshot["sources"] if s["business_id"] == bid]
    st.markdown(f"**Why it is in the queue:** {row['flags'] or 'not reviewed yet'}")
    st.markdown(f"**Website status:** {row['website_status']}" + ("  (set manually)" if b["website_locked"] else ""))
    st.markdown("**Sources** (each contact below came from the URL shown next to it)")
    for s in sources:
        state = f" - state: `{s['site_state']}` {s['eval_reasons']}" if s["source_type"] == "website" else ""
        st.markdown(f"- `{s['source_type']}` {md_link(s['url'])}{state}")

    st.markdown("**Emails**")
    if not emails:
        st.caption("None found.")
    for e in sorted(emails, key=lambda x: -CONF_RANK.get(x["confidence"], 0)):
        cols = st.columns([3, 1.2, 3, 1, 1])
        cols[0].markdown(f"`{e['email']}`")
        cols[1].markdown(f"**{e['confidence']}**")
        cols[2].markdown(f"{md_link(e['source_url'], 'source') if e['source_url'] else ''} {e['check_result'] or ''}")
        if cols[3].button("Confirm", key=f"ce_{e['id']}", disabled=bool(e["confirmed"])):
            db.confirm_email(e["id"])
            st.rerun()
        if cols[4].button("Remove", key=f"re_{e['id']}"):
            db.remove_email(e["id"])
            st.rerun()
    st.markdown("**Phones**")
    if not phones:
        st.caption("None found.")
    for p in phones:
        cols = st.columns([3, 1.2, 3, 1])
        cols[0].markdown(f"`{p['phone']}`")
        cols[1].markdown(f"**{p['confidence']}**")
        cols[2].markdown(md_link(p["source_url"], "source") if p["source_url"] else "")
        if cols[3].button("Remove", key=f"rp_{p['id']}"):
            db.remove_phone(p["id"])
            st.rerun()

    st.markdown("**Website decision**")
    domains = sorted({s["domain"] for s in sources if s["source_type"] == "website" and s["domain"]})
    if domains:
        dom = st.selectbox("Candidate website", domains, key=f"dom_{bid}")
        w1, w2, w3 = st.columns(3)
        if w1.button("Confirm as official website", key=f"cw_{bid}"):
            db.confirm_website(bid, dom)
            st.rerun()
        if w2.button("Re-check website now", key=f"rw_{bid}"):
            ev = recheck_business(db, SiteFetcher(opts.timeout, opts.respect_robots, collect_social), bid, dom, collect_social)
            st.toast(f"Re-check result: {ev.state}")
            st.rerun()
        if w3.button("Not this business's website", key=f"nw_{bid}"):
            db.remove_website(bid, dom)
            st.rerun()
    new_url = st.text_input("Add a website you found yourself", key=f"nu_{bid}", placeholder="https://example.com.au")
    if st.button("Add website", key=f"aw_{bid}") and new_url.strip():
        try:
            db.add_website(bid, new_url.strip())
            st.rerun()
        except ValueError as exc:
            st.error(str(exc))
    manual_options = [s for s in WEBSITE_STATUSES if s != WS_VERIFIED]
    m1, m2 = st.columns([3, 1])
    chosen = m1.selectbox("Or set the status manually", manual_options, key=f"ms_{bid}")
    if m2.button("Set status", key=f"ss_{bid}"):
        db.set_website_status_manual(bid, chosen, "set in Verification Center")
        st.rerun()
    if b["website_locked"] and st.button("Unlock and re-detect automatically", key=f"ul_{bid}"):
        db.unlock_website(bid)
        st.rerun()

    st.markdown("**Lead decision**")
    d1, d2, d3 = st.columns(3)
    if d1.button("Mark lead verified", key=f"vv_{bid}", type="primary"):
        db.set_verification(bid, "verified", "reviewed in Verification Center")
        st.rerun()
    if d2.button("Reject lead", key=f"vr_{bid}"):
        db.set_verification(bid, "rejected", "rejected in Verification Center")
        st.rerun()
    if d3.button("Back to pending", key=f"vp_{bid}"):
        db.set_verification(bid, "pending")
        st.rerun()
    notes = st.text_area("Notes", value=b["notes"] or "", key=f"nt_{bid}")
    if st.button("Save notes", key=f"sn_{bid}"):
        db.set_notes(bid, notes)
        st.toast("Notes saved")
    with st.expander("Verification history"):
        st.dataframe(db.verification_log(bid), hide_index=True, **_wide(st.dataframe))


QUEUE_FILTERS = {
    "Everything needing review": "",
    "Website needs review": "Website needs review",
    "Website unreachable": "unreachable",
    "Email low confidence": "Email low confidence",
    "Official-website lookup not run": "lookup not run",
    "Possible duplicates": "Possible duplicate",
}


def render_verification(db: Database, frame: pd.DataFrame, snapshot: dict, opts: UiOptions, collect_social: bool) -> None:
    st.subheader("Lead verification center")
    st.caption("Nothing is treated as qualified until the evidence is clear or you mark the lead verified. "
               "Contacts are only ever attached to the business whose own page or listing they were found on.")
    pending = frame[(frame["verification"] == "pending") & (frame["flags"] != "")]
    m = st.columns(4)
    m[0].metric("Needs review", len(pending))
    m[1].metric("Verified", int((frame["verification"] == "verified").sum()))
    m[2].metric("Rejected", int((frame["verification"] == "rejected").sum()))
    m[3].metric("Possible duplicate pairs", len(find_duplicates(snapshot)))

    queue_tab, dup_tab = st.tabs(["Review queue", "Duplicates"])
    with queue_tab:
        choice = st.selectbox("Show", list(QUEUE_FILTERS))
        needle = QUEUE_FILTERS[choice]
        queue = pending[pending["flags"].str.contains(needle, case=False, regex=False)] if needle else pending
        queue = queue.sort_values("score", ascending=False)
        if st.button("Check email syntax and MX records for this queue (up to 40)"):
            ids = set(int(i) for i in queue["id"])
            todo = [e for e in snapshot["emails"] if e["business_id"] in ids][:40]
            with ThreadPoolExecutor(max_workers=6) as pool:
                results = list(pool.map(lambda e: (e["id"], check_email(e["email"], True)), todo))
            for eid, res in results:
                db.save_email_check(eid, res)
            st.rerun()
        if queue.empty:
            st.success("Nothing in this queue.")
        page_size = 8
        pages = max(1, -(-len(queue) // page_size))
        page = int(st.number_input("Page", 1, pages, 1)) if pages > 1 else 1
        for _, row in queue.iloc[(page - 1) * page_size: page * page_size].iterrows():
            with st.expander(f"{row['name']} - {row['city']} - {row['website_status']} - score {row['score']}"):
                render_review_card(db, row, snapshot, opts, collect_social)
    with dup_tab:
        pairs = find_duplicates(snapshot)
        if not pairs:
            st.success("No possible duplicates.")
        st.caption("Duplicates are only suggested. Contact details are merged solely when you press Merge.")
        for pair in pairs[:20]:
            a, b = pair["a"], pair["b"]
            ra, rb = _business_summary(frame, a), _business_summary(frame, b)
            if ra is None or rb is None:
                continue
            st.markdown(f"**{', '.join(pair['reasons'])}**")
            cols = st.columns(2)
            for col, r in zip(cols, (ra, rb)):
                col.markdown(f"#{r['id']} **{r['name']}** ({r['city']})  \n{r['website_status']}  \n"
                             f"{r['all_emails'] or 'no email'}  \n{r['phones'] or 'no phone'}")
            b1, b2, b3 = st.columns(3)
            if b1.button(f"Merge #{b} into #{a}", key=f"mg_{a}_{b}"):
                db.merge(a, b)
                st.rerun()
            if b2.button(f"Merge #{a} into #{b}", key=f"mh_{a}_{b}"):
                db.merge(b, a)
                st.rerun()
            if b3.button("Not a duplicate", key=f"nd_{a}_{b}"):
                db.dismiss_duplicate(a, b)
                st.rerun()
            st.divider()


def render_leads(frame: pd.DataFrame, show_social: bool) -> None:
    st.subheader("Leads")
    if frame.empty:
        st.info("No leads yet. Run a search in the Find Leads tab.")
        return
    m = st.columns(4)
    m[0].metric("Leads", len(frame))
    m[1].metric("Qualified", int(frame["qualified"].sum()))
    m[2].metric("With trusted email", int(frame["has_trusted_email"].sum()))
    m[3].metric("Need review", int(((frame["flags"] != "") & (frame["verification"] == "pending")).sum()))
    f1, f2, f3, f4 = st.columns(4)
    statuses = f1.multiselect("Website status", WEBSITE_STATUSES)
    stages = f2.multiselect("Pipeline", PIPELINE)
    verifs = f3.multiselect("Verification", VERIFICATION_STATES)
    text = f4.text_input("Search text")
    only_q = st.checkbox("Only qualified leads")
    view = frame
    if statuses:
        view = view[view["website_status"].isin(statuses)]
    if stages:
        view = view[view["pipeline"].isin(stages)]
    if verifs:
        view = view[view["verification"].isin(verifs)]
    if only_q:
        view = view[view["qualified"]]
    if text:
        blob = view[["name", "city", "all_emails", "phones", "source_urls", "notes"]].astype(str).agg(" ".join, axis=1)
        view = view[blob.str.contains(text, case=False, regex=False)]
    cols = ["name", "category", "city", "website_status", "official_website", "primary_email", "email_confidence",
            "phones", "score", "qualified", "verification", "pipeline", "flags"]
    if show_social:
        cols.insert(8, "socials")
    st.dataframe(
        view.sort_values("score", ascending=False)[cols], hide_index=True,
        column_config={
            "score": st.column_config.ProgressColumn("Score", min_value=0, max_value=100, format="%d"),
            "official_website": st.column_config.LinkColumn("Official website"),
            "name": "Business", "city": "City", "category": "Category", "website_status": "Website status",
            "primary_email": "Email", "email_confidence": "Email confidence", "phones": "Phone(s)",
            "qualified": "Qualified", "verification": "Verification", "pipeline": "Pipeline", "flags": "Review flags",
        },
        **_wide(st.dataframe),
    )
    with st.expander("Why did a lead get its score?"):
        st.dataframe(view[["name", "score", "score_reasons"]].sort_values("score", ascending=False),
                     hide_index=True, **_wide(st.dataframe))


def render_pipeline(db: Database, frame: pd.DataFrame) -> None:
    st.subheader("Pipeline")
    active = frame[frame["verification"] != "rejected"]
    if active.empty:
        st.info("No leads in the pipeline yet.")
        return
    counts = active["pipeline"].value_counts()
    cols = st.columns(len(PIPELINE))
    for col, stage in zip(cols, PIPELINE):
        col.metric(stage, int(counts.get(stage, 0)))
    stages = st.multiselect("Show stages", PIPELINE, default=[s for s in PIPELINE if s != "Do not contact"])
    view = active[active["pipeline"].isin(stages)] if stages else active
    editor_df = view[["id", "name", "city", "website_status", "primary_email", "score", "pipeline", "notes"]].reset_index(drop=True)
    edited = st.data_editor(
        editor_df, key="pipeline_editor", hide_index=True,
        disabled=["id", "name", "city", "website_status", "primary_email", "score"],
        column_config={"pipeline": st.column_config.SelectboxColumn("Pipeline", options=PIPELINE, required=True),
                       "notes": st.column_config.TextColumn("Notes")},
        **_wide(st.data_editor),
    )
    if st.button("Save pipeline changes", type="primary"):
        changed = 0
        originals = editor_df.set_index("id")
        for _, row in edited.iterrows():
            orig = originals.loc[row["id"]]
            if row["pipeline"] != orig["pipeline"]:
                db.set_pipeline(int(row["id"]), row["pipeline"], "changed in Pipeline tab")
                changed += 1
            if (row["notes"] or "") != (orig["notes"] or ""):
                db.set_notes(int(row["id"]), row["notes"] or "")
                changed += 1
        st.success(f"Saved {changed} change(s).")
        st.rerun()
    pick = st.selectbox("Outreach history for", list(active["id"]),
                        format_func=lambda i: active.loc[active["id"] == i, "name"].iloc[0])
    st.dataframe(db.outreach_log(int(pick)), hide_index=True, **_wide(st.dataframe))


def eligible_outreach_frame(frame: pd.DataFrame) -> pd.DataFrame:
    """Only verified, trusted-email leads with no DNC or unresolved duplicate flag."""
    return frame[
        frame["has_trusted_email"].fillna(False).astype(bool)
        & (~frame["dnc"].fillna(False).astype(bool))
        & (frame["verification"] == "verified")
        & (~frame["flags"].fillna("").str.contains(
            "Possible duplicate", case=False, regex=False
        ))
    ].copy()


def render_outreach(db: Database, frame: pd.DataFrame, sender_name: str, sender_company: str) -> None:
    st.subheader("Outreach")
    eligible = eligible_outreach_frame(frame)
    if eligible.empty:
        st.info("No verified leads with a trusted public email are currently eligible for outreach.")
        return
    eligible = eligible.sort_values("score", ascending=False)
    labels = {int(r["id"]): f"{r['name']} - score {r['score']} - {r['primary_email']}" for _, r in eligible.iterrows()}
    pick = st.selectbox("Choose a lead", list(labels), format_func=labels.get)
    lead = eligible[eligible["id"] == pick].iloc[0]
    subject_t = st.text_input("Subject", value="Quick idea for {business}")
    body_t = st.text_area("Message", height=240, value=(
        "Hi {business} team,\n\nI came across your {category} business in {city} and noticed {observation}.\n\n"
        "I run {company}, where we build fast, mobile-friendly websites for local businesses. I'd be happy to share a "
        "free mockup for {business} - no obligation.\n\nWould you be open to a quick chat?\n\nKind regards,\n{sender}\n"
        "{company}\n\nIf you'd rather not hear from me, reply 'no thanks' and I won't contact you again."))
    observations = {WS_NONE: "you don't seem to have your own website yet",
                    WS_SOCIAL: "your business mostly appears on social media",
                    WS_DIRECTORY: "your business mostly appears on directory listings",
                    WS_UNREACHABLE: "your website didn't load when I tried it",
                    WS_REVIEW: "a few things on your website that could help bring in more customers"}
    observation = st.text_input("Observation line", value=observations.get(lead["website_status"], "an opportunity to improve your website"))
    fill = {"business": lead["name"], "category": lead["category"], "city": lead["city"], "observation": observation,
            "company": sender_company, "sender": sender_name or "[Your name]"}
    subject, body = subject_t, body_t
    for k, v in fill.items():
        subject, body = subject.replace("{" + k + "}", str(v)), body.replace("{" + k + "}", str(v))
    st.text_area("Preview", value=body, height=240, disabled=True)
    to = lead["primary_email"]
    st.markdown(f"[Open in your email app](mailto:{to}?subject={quote(subject)}&body={quote(body)})")
    st.caption(f"To: {to} (confidence: {lead['email_confidence']}, found on {lead['email_source']})")
    if st.button("Mark as Contacted (after sending)"):
        db.set_pipeline(int(pick), "Contacted", "user marked contacted after sending email")
        st.rerun()


def render_history(db: Database) -> None:
    st.subheader("Search history")
    hist = db.searches_df()
    if hist.empty:
        st.info("No searches yet.")
        return
    m = st.columns(4)
    m[0].metric("Searches", len(hist))
    m[1].metric("Results returned", int(hist["results"].sum()))
    m[2].metric("New leads", int(hist["new_leads"].sum()))
    m[3].metric("Errors", int((hist["status"] == "error").sum()))
    st.dataframe(hist, hide_index=True, **_wide(st.dataframe))


def render_export(db: Database, frame: pd.DataFrame, show_social: bool, db_path: str) -> None:
    st.subheader("Export")
    st.caption("Exports respect the sidebar city filter and the 'Require public business email' toggle. "
               "Rejected and do-not-contact businesses are never exported as leads.")
    history = db.searches_df()
    st.download_button("Download Excel (Qualified / Email / Review / Search history)",
                       build_workbook(frame, history, show_social), "siteora_leads.xlsx",
                       "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", type="primary")
    csv_frame = frame.loc[
        (frame["verification"] != "rejected") &
        (~frame["dnc"].fillna(False).astype(bool))
    ].copy()

    st.download_button(
        "Download visible leads (excluding rejected / DNC) (CSV)",
        sanitize_frame(csv_frame).to_csv(index=False).encode("utf-8-sig"),
        "siteora_leads.csv",
        "text/csv"
    )
    st.divider()
    st.subheader("Database backup and restore")
    st.info(f"Leads, search history, verification results, notes and pipeline status are stored in SQLite at `{db_path}`. "
            "On hosts with a temporary file system (for example Streamlit Community Cloud) that file is wiped when the app "
            "is rebuilt or the container is replaced. Download a backup regularly, or point `LEAD_DB_PATH` (env var or "
            "secret) at a persistent volume.")
    st.download_button("Download database backup (.db)", db.backup_bytes(), "siteora_leads_backup.db", "application/octet-stream")
    upload = st.file_uploader("Restore from a backup (.db)", type=["db", "sqlite", "sqlite3"])
    confirm = st.checkbox("I understand this replaces all current data")
    if upload is not None and st.button("Restore backup", disabled=not confirm):
        try:
            db.restore_bytes(upload.getvalue())
            st.success("Backup restored.")
            st.rerun()
        except (ValueError, sqlite3.DatabaseError) as exc:
            st.error(f"Could not restore: {exc}")


def main() -> None:
    st.set_page_config(page_title="Siteora Lead Finder", page_icon="ðŸ”Ž", layout="wide")
    db_path = resolve_db_path()
    db = get_db(db_path)

    st.title("ðŸ”Ž Siteora Lead Finder")
    st.caption("Discover local businesses, verify their websites and contacts, score them and manage outreach.")

    with st.sidebar:
        st.header("âš™ï¸ Settings")
        require_email = persistent_toggle(
            db, "Require public business email", "require_email", True,
            "ON: businesses without a publicly listed email are excluded from results and exports. "
            "OFF: they are included.")
        collect_social = persistent_toggle(
            db, "Collect social media links", "collect_social", False,
            "ON: collect public Facebook, Instagram, TikTok, LinkedIn and other social links, and allow the "
            "'Social media only' status. OFF: no social links are requested, stored or exported.")
        if not collect_social:
            st.caption("Social collection is off, so the 'Social media only' status cannot be detected in new searches.")
        st.divider()
        opts = UiOptions()
        opts.enrich = st.toggle("Visit websites to verify them and find contacts", value=True)
        opts.respect_robots = st.toggle("Respect robots.txt", value=True)
        opts.lookup_websites = st.toggle("Look up official website for businesses that have none", value=True)
        opts.skip_lists = st.toggle("Skip 'Top 10 / Best of' list pages", value=True)
        opts.check_mx = st.toggle("Check email MX records during search", value=False, disabled=not HAS_DNS,
                                  help=None if HAS_DNS else "Install dnspython to enable.")
        opts.workers = st.slider("Parallel site checks", 1, 12, 6)
        opts.timeout = st.slider("Site timeout (seconds)", 3, 20, 8)
        opts.rpm = st.selectbox("TinyFish plan limit (requests/min)", [30, 60, 120], index=0,
                                help="Free and Pay-As-You-Go: 30. The app throttles itself to stay under this.")
        st.divider()
        st.subheader("Filters")
        cities = st.multiselect("City filter", db.cities())
        min_score = st.slider("Minimum score to count as qualified", 0, 100, 40)
        st.divider()
        st.subheader("âœ‰ï¸ Your details")
        sender_name = st.text_input("Your name")
        sender_company = st.text_input("Your company", value="Siteora")
        if not get_api_key():
            st.text_input("TinyFish API key", type="password", key="manual_api_key",
                          help="Better: add TINYFISH_API_KEY in Streamlit Secrets. This box is never saved.")
        else:
            st.caption("TinyFish API key: loaded")

    st.warning("Leads are suggestions, not proof. Verify each business before contacting it and follow your local "
               "anti-spam and privacy laws (e.g. Australia's Spam Act 2003: identify yourself and offer an opt-out).")

    snapshot = db.snapshot()
    frame_all = build_frame(snapshot, require_email, min_score, collect_social)
    frame, hidden = apply_view_filters(frame_all, cities, require_email)
    if hidden:
        st.caption(f"{hidden} business(es) without a public email are hidden because 'Require public business email' is ON.")

    tabs = st.tabs(["ðŸ” Find Leads", "âœ… Verification Center", "ðŸ“‹ Leads", "ðŸ§­ Pipeline", "âœ‰ï¸ Outreach",
                    "ðŸ“œ Search History", "â¬‡ï¸ Export & Backup"])
    with tabs[0]:
        render_find(db, require_email, collect_social, opts)
    with tabs[1]:
        render_verification(db, frame, snapshot, opts, collect_social)
    with tabs[2]:
        render_leads(frame, collect_social)
    with tabs[3]:
        render_pipeline(db, frame)
    with tabs[4]:
        render_outreach(db, frame, sender_name, sender_company)
    with tabs[5]:
        render_history(db)
    with tabs[6]:
        render_export(db, frame, collect_social, db_path)


if __name__ == "__main__":
    main()
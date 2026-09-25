#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
FURAO PROMO — Telegram público -> Discord
------------------------------------------
Scraper de canais públicos do Telegram sem usar a API do Telegram.

Fluxo:
    https://t.me/s/<canal>
            ↓
       BeautifulSoup
            ↓
  post novo encontrado
            ↓
 título / imagem / preços / cupom / links
            ↓
        SQLite
            ↓
      Discord Webhook

IMPORTANTE:
- Funciona com conteúdo PUBLICAMENTE acessível na web.
- Não faz login no Telegram.
- Não tenta acessar grupos/canais privados.
- Não contorna proteção de conteúdo, CAPTCHA ou bloqueios.
- Respeite os termos de uso e direitos sobre o conteúdo dos canais.
"""

from __future__ import annotations

import argparse
import html
import json
import logging
import os
import re
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

import psycopg2
import psycopg2.extras
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv


ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "data"
LOG_DIR = ROOT / "logs"
SOURCES_PATH = ROOT / "sources.json"

DEFAULT_INTERVAL = 60
DEFAULT_TIMEOUT = 30
MAX_POSTS_PER_SOURCE = 30

URL_RE = re.compile(r"(?i)(?:https?://|www\.)[^\s<>\"]+")
PRICE_RE = re.compile(
    r"(?<!\w)(?:R\$\s*)?(\d{1,3}(?:\.\d{3})*(?:,\d{1,2})?|\d+(?:,\d{1,2})?)"
)
DE_POR_RE = re.compile(
    r"(?is)\b(?:de|era|antes)\s*[:\-]?\s*"
    r"(?:R\$\s*)?([\d\.\,]+)"
    r".{0,120}?"
    r"\b(?:por|agora|sai\s+por|hoje)\s*[:\-]?\s*"
    r"(?:R\$\s*)?([\d\.\,]+)"
)
COUPON_RE = re.compile(
    r"(?is)\b(?:cupom|coupon)\s*[:\-]?\s*`?([A-Z0-9][A-Z0-9_\-]{2,})`?"
)

TRACKING_QUERY_KEYS = (
    "utm_source", "utm_medium", "utm_campaign", "utm_content",
    "utm_term", "gclid", "fbclid", "msclkid"
)


@dataclass
class Promo:
    source_name: str
    channel_username: str
    post_id: int
    post_url: str
    title: str
    description: str
    current_price: float | None
    original_price: float | None
    discount_percent: float | None
    coupon: str | None
    shop: str | None
    image_url: str | None
    image_bytes: bytes | None = None
    image_ext: str = "jpg"
    links: list[str] = field(default_factory=list)

    @property
    def fingerprint(self) -> str:
        key = f"{self.channel_username}|{self.post_id}|{self.post_url}"
        return key.lower()


def utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def setup_logging() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    logger = logging.getLogger()
    logger.handlers.clear()
    logger.setLevel(logging.INFO)

    stdout = logging.StreamHandler(sys.stdout)
    stdout.setFormatter(fmt)

    file_handler = logging.FileHandler(
        LOG_DIR / "scraper.log", encoding="utf-8"
    )
    file_handler.setFormatter(fmt)

    logger.addHandler(stdout)
    logger.addHandler(file_handler)


class Database:
    """PostgreSQL-backed storage (Railway Postgres or any Postgres URL).

    Keeps the exact same public interface the rest of the script already
    relies on (is_seen / save_post / was_sent / mark_sent / close), so
    nothing outside this class needed to change.
    """

    def __init__(self, dsn: str):
        self.conn = psycopg2.connect(dsn)
        self.conn.autocommit = False
        self._init_schema()

    def _init_schema(self) -> None:
        with self.conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS posts (
                    id BIGSERIAL PRIMARY KEY,
                    source_name TEXT NOT NULL,
                    channel_username TEXT NOT NULL,
                    post_id BIGINT NOT NULL,
                    post_url TEXT NOT NULL,
                    title TEXT NOT NULL,
                    current_price DOUBLE PRECISION,
                    original_price DOUBLE PRECISION,
                    discount_percent DOUBLE PRECISION,
                    coupon TEXT,
                    image_url TEXT,
                    first_seen_at TIMESTAMPTZ NOT NULL,
                    UNIQUE (channel_username, post_id)
                );

                CREATE TABLE IF NOT EXISTS sent (
                    id BIGSERIAL PRIMARY KEY,
                    fingerprint TEXT NOT NULL UNIQUE,
                    channel_username TEXT NOT NULL,
                    post_id BIGINT NOT NULL,
                    sent_at TIMESTAMPTZ NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_sent_fingerprint
                ON sent(fingerprint);

                CREATE INDEX IF NOT EXISTS idx_posts_channel_post
                ON posts(channel_username, post_id);
                """
            )
        self.conn.commit()

    def is_seen(self, channel: str, post_id: int) -> bool:
        with self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT 1 FROM posts
                WHERE channel_username = %s AND post_id = %s
                LIMIT 1
                """,
                (channel, post_id),
            )
            return cur.fetchone() is not None

    def save_post(self, promo: Promo) -> None:
        with self.conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO posts (
                    source_name, channel_username, post_id, post_url,
                    title, current_price, original_price, discount_percent,
                    coupon, image_url, first_seen_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (channel_username, post_id) DO NOTHING
                """,
                (
                    promo.source_name,
                    promo.channel_username,
                    promo.post_id,
                    promo.post_url,
                    promo.title,
                    promo.current_price,
                    promo.original_price,
                    promo.discount_percent,
                    promo.coupon,
                    promo.image_url,
                    utc_iso(),
                ),
            )
        self.conn.commit()

    def was_sent(self, fingerprint: str) -> bool:
        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM sent WHERE fingerprint = %s LIMIT 1",
                (fingerprint,),
            )
            return cur.fetchone() is not None

    def mark_sent(self, promo: Promo) -> None:
        with self.conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO sent (
                    fingerprint, channel_username, post_id, sent_at
                )
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (fingerprint) DO NOTHING
                """,
                (
                    promo.fingerprint,
                    promo.channel_username,
                    promo.post_id,
                    utc_iso(),
                ),
            )
        self.conn.commit()

    def close(self) -> None:
        self.conn.close()


class HttpClient:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/154.0 Safari/537.36 FURAO-PROMO/1.0"
                ),
                "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
                "Accept": "text/html,application/xhtml+xml,"
                          "application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            }
        )

    def get(self, url: str, timeout: int = DEFAULT_TIMEOUT) -> requests.Response:
        last = None
        for attempt in range(4):
            try:
                r = self.session.get(
                    url,
                    timeout=timeout,
                    allow_redirects=True,
                )
                if r.status_code == 429:
                    wait = min(2 ** attempt, 30)
                    logging.warning("429 em %s; aguardando %ss", url, wait)
                    time.sleep(wait)
                    continue
                if 500 <= r.status_code < 600:
                    wait = min(2 ** attempt, 20)
                    logging.warning(
                        "HTTP %s em %s; retry em %ss",
                        r.status_code, url, wait
                    )
                    time.sleep(wait)
                    continue
                r.raise_for_status()
                return r
            except requests.RequestException as exc:
                last = exc
                if attempt == 3:
                    raise
                wait = min(2 ** attempt, 15)
                logging.warning(
                    "Falha em %s: %s; retry em %ss",
                    url, exc, wait
                )
                time.sleep(wait)
        raise RuntimeError(str(last))


def clean_text(value: Any) -> str:
    if value is None:
        return ""
    value = html.unescape(str(value))
    value = BeautifulSoup(value, "html.parser").get_text(" ", strip=True)
    return re.sub(r"\s+", " ", value).strip()


def parse_brl(value: str | None) -> float | None:
    if not value:
        return None
    text = str(value)
    m = PRICE_RE.search(text)
    if not m:
        return None
    raw = m.group(1)
    try:
        if "," in raw:
            return float(raw.replace(".", "").replace(",", "."))
        if "." in raw and len(raw.rsplit(".", 1)[1]) == 2:
            return float(raw)
        return float(raw.replace(".", ""))
    except ValueError:
        return None


def parse_prices(text: str) -> tuple[float | None, float | None]:
    """Extract an advertised original/current price without reading numbers
    from URLs, post IDs, coupon codes, or unrelated metadata.

    The important rule for this bot is: if the Telegram post has only one
    current price (e.g. "Valor: R$328 no PIX"), return only that current price
    and do NOT invent an original/"De" price.
    """
    text = text or ""

    # Explicit promotion syntax: De R$999 por R$699.
    context = re.search(
        r"(?is)\b(?:de|era|antes)\s*[:\-]?\s*"
        r"R\$\s*([\d\.\,]+)"
        r".{0,120}?"
        r"\b(?:por|agora|sai\s+por|hoje)\s*[:\-]?\s*"
        r"R\$\s*([\d\.\,]+)",
        text,
    )
    if context:
        original = parse_brl(context.group(1))
        current = parse_brl(context.group(2))
        if original is not None and current is not None and original > current:
            return original, current

    # Explicit currency values are safe; numbers inside meli.la/... or other
    # URLs are deliberately ignored.
    explicit = re.findall(r"R\$\s*([\d\.\,]+)", text, flags=re.IGNORECASE)
    explicit_values: list[float] = []
    for raw in explicit:
        value = parse_brl(raw)
        if value is not None and 1 < value < 10_000_000:
            explicit_values.append(value)

    if len(explicit_values) >= 2:
        old, current = explicit_values[0], explicit_values[1]
        if old > current:
            return old, current
        # Multiple R$ amounts but no valid old/new relationship: the last
        # explicit amount is the current price; do not fabricate the old one.
        return None, current

    if len(explicit_values) == 1:
        return None, explicit_values[0]

    # Some channels omit "R$" but use an unambiguous price label.
    contextual = re.findall(
        r"(?is)\b(?:valor|preço|preco|price)\s*"
        r"(?:no|em)?\s*(?:pix|cart[aã]o)?\s*[:\-]\s*"
        r"(?:R\$\s*)?([\d\.\,]+)",
        text,
    )
    for raw in contextual:
        value = parse_brl(raw)
        if value is not None and 1 < value < 10_000_000:
            return None, value

    # Last safe fallback: words such as "por R$..." but still no arbitrary
    # numeric scanning of the message.
    fallback = re.search(
        r"(?is)\b(?:por|agora|sai\s+por|hoje)\s*[:\-]?\s*"
        r"(?:R\$\s*)?([\d\.\,]+)",
        text,
    )
    if fallback:
        value = parse_brl(fallback.group(1))
        if value is not None and 1 < value < 10_000_000:
            return None, value

    return None, None


def parse_urls(soup: BeautifulSoup, text: str, base_url: str) -> list[str]:
    urls: list[str] = []

    for raw in URL_RE.findall(text or ""):
        urls.append(raw if raw.startswith("http") else "https://" + raw)

    for anchor in soup.select("a[href]"):
        href = anchor.get("href", "").strip()
        if not href:
            continue
        absolute = urljoin(base_url, href)
        if absolute.startswith(("http://", "https://")):
            urls.append(absolute)

    result = []
    seen = set()

    for url in urls:
        url = url.strip().rstrip(".,);]!?")
        if url not in seen:
            seen.add(url)
            result.append(url)

    return result


def is_noise_link(url: str, channel_username: str) -> bool:
    low = (url or "").lower()
    blocked = (
        "beacons.ai/",
        "linktr.ee/",
        "t.me/",
        "telegram.me/",
        "wa.me/",
        "whatsapp.com/channel/",
        "discord.gg/",
        "discord.com/",
        "instagram.com/",
        "facebook.com/",
        "youtube.com/",
    )
    if f"t.me/{channel_username.lower()}/" in low:
        return True
    return any(item in low for item in blocked)


def choose_offer_links(links: list[str], channel_username: str) -> list[str]:
    clean: list[str] = []
    seen = set()

    for url in links:
        url = (url or "").strip().rstrip(".,);]!?\"")
        if not url or is_noise_link(url, channel_username):
            continue
        if url in seen:
            continue
        seen.add(url)
        clean.append(url)

    # Prefer a recognizable store/marketplace URL over auxiliary links.
    priority_domains = (
        "mercadolivre.com.br",
        "meli.la",
        "amazon.com.br",
        "amazon.com",
        "shopee.com.br",
        "s.shopee.com.br",
        "aliexpress.com",
        "magazineluiza.com.br",
        "kabum.com.br",
        "pichau.com.br",
        "terabyteshop.com.br",
        "americanas.com.br",
        "casasbahia.com.br",
        "pontofrio.com.br",
        "fastshop.com.br",
    )
    for domain in priority_domains:
        prioritized = [u for u in clean if domain in u.lower()]
        if prioritized:
            rest = [u for u in clean if u not in prioritized]
            return prioritized[:1] + rest[:2]

    return clean[:3]


def build_product_title(text: str, offer_links: list[str]) -> str:
    lines = [clean_text(x) for x in (text or "").splitlines() if clean_text(x)]
    for line in lines:
        low = line.lower().strip()
        if any(url in line for url in offer_links):
            continue
        if "grupos de ofertas" in low or "grupo de ofertas" in low:
            continue
        if re.match(r"^(?:valor|preço|preco|price|cupom|coupon|link)\b", low):
            continue
        if re.match(r"^(?:https?://|www\.)", low):
            continue
        candidate = re.sub(
            r"(?i)\b(?:de|por|agora|valor|preço|preco)\b\s*[:\-]?\s*R?\$?\s*[\d\.\,]+",
            " ",
            line,
        )
        candidate = re.sub(r"\s+", " ", candidate).strip(" -:|")
        if candidate:
            return candidate[:240]
    return "Oferta"


def extract_link_preview_title(post) -> str | None:
    """Telegram resolves a link preview (site name, title, image) for URLs
    shared in a post, and that HTML is already sitting right there in the
    public channel page. Reading it is far more reliable than fetching the
    retailer's page ourselves, since stores like Mercado Livre/Amazon
    commonly block scraping bots while Telegram's own preview fetch is
    never blocked."""
    node = post.select_one(
        ".tgme_widget_message_link_preview .link_preview_title, "
        ".link_preview_title"
    )
    if node:
        candidate = clean_text(node.get_text(" ", strip=True))
        if candidate:
            return candidate[:240]
    return None


TITLE_SUFFIX_RE = re.compile(
    r"\s*[\|\-–—:]\s*(?:Mercado Livre|Amazon\.com\.br|Amazon|Shopee|"
    r"AliExpress|Magazine Luiza|Magalu|Americanas|Casas Bahia)\s*$",
    re.IGNORECASE,
)


def fetch_title_from_link(client: "HttpClient", url: str) -> str | None:
    """Last-resort fallback, used only when Telegram itself produced no
    link preview. A single lenient attempt (no retries, no raise on
    4xx/5xx) since retailer sites frequently block non-browser requests —
    failing fast here matters more than squeezing out one extra title."""
    try:
        resp = client.session.get(url, timeout=6, allow_redirects=True)
    except Exception as exc:
        logging.debug("Não foi possível resolver título de %s: %s", url, exc)
        return None

    if not resp.ok or "text/html" not in resp.headers.get("Content-Type", ""):
        return None

    try:
        soup = BeautifulSoup(resp.text, "html.parser")
    except Exception:
        return None

    for selector in ("meta[property='og:title']", "meta[name='twitter:title']"):
        tag = soup.select_one(selector)
        if tag and tag.get("content"):
            candidate = clean_text(tag["content"])
            candidate = TITLE_SUFFIX_RE.sub("", candidate).strip()
            if candidate:
                return candidate[:240]

    if soup.title and soup.title.string:
        candidate = clean_text(soup.title.string)
        candidate = TITLE_SUFFIX_RE.sub("", candidate).strip()
        if candidate:
            return candidate[:240]

    return None


def build_product_description(
    text: str,
    title: str,
    offer_links: list[str],
) -> str:
    lines = [clean_text(x) for x in (text or "").splitlines() if clean_text(x)]
    kept: list[str] = []
    for line in lines:
        low = line.lower()
        if line.strip() == title.strip():
            continue
        if "grupos de ofertas" in low or "grupo de ofertas" in low:
            continue
        if "beacons.ai/" in low or "linktr.ee/" in low:
            continue
        if "t.me/" in low or "telegram.me/" in low:
            continue
        if any(url in line for url in offer_links):
            # Keep non-link text from the line, but the actual URL belongs in
            # the dedicated field in the embed.
            stripped = re.sub(r"https?://\S+", "", line).strip(" -:|")
            if not stripped:
                continue
            line = stripped
        if re.match(r"^(?:cupom|coupon|link)\b", low):
            continue
        if re.match(r"^(?:valor|preço|preco|price)\b", low):
            continue
        kept.append(line)

    description = re.sub(r"\s+", " ", " ".join(kept)).strip()
    return description[:900]


def discover_shop(text: str, links: list[str]) -> str | None:
    lower = text.lower()
    domains = [
        ("amazon.com.br", "Amazon"),
        ("amazon.com", "Amazon"),
        ("mercadolivre.com.br", "Mercado Livre"),
        ("meli.la", "Mercado Livre"),
        ("shopee.com.br", "Shopee"),
        ("s.shopee.com.br", "Shopee"),
        ("aliexpress.com", "AliExpress"),
        ("magazineluiza.com.br", "Magalu"),
        ("kabum.com.br", "KaBuM!"),
        ("pichau.com.br", "Pichau"),
        ("terabyteshop.com.br", "Terabyte"),
        ("americanas.com.br", "Americanas"),
        ("casasbahia.com.br", "Casas Bahia"),
        ("pontofrio.com.br", "Ponto"),
        ("fastshop.com.br", "Fast Shop"),
    ]

    for domain, name in domains:
        if domain in lower:
            return name
        if any(domain in link.lower() for link in links):
            return name

    m = re.search(
        r"(?im)^\s*(?:loja|store|🏪)\s*[:\-]\s*(.+?)\s*$",
        text,
    )
    return m.group(1).strip()[:100] if m else None


def find_image(post: BeautifulSoup, base_url: str) -> str | None:
    candidates: list[str] = []

    # Current/legacy Telegram public preview variants.
    for tag in post.select(
        ".tgme_widget_message_photo_wrap, "
        ".tgme_widget_message_photo, "
        "a.tgme_widget_message_photo_wrap"
    ):
        style = tag.get("style", "")
        m = re.search(
            r"url\((['\"]?)(.*?)\1\)",
            style,
            flags=re.IGNORECASE,
        )
        if m:
            candidates.append(urljoin(base_url, m.group(2)))

        for attr in ("data-src", "href"):
            value = tag.get(attr)
            if value:
                candidates.append(urljoin(base_url, value))

    for img in post.select("img"):
        for attr in ("src", "data-src", "data-original", "data-lazy-src"):
            value = img.get(attr)
            if value:
                candidates.append(urljoin(base_url, value))

    # og:image inside the post is occasionally present in preview markup.
    for meta in post.select("meta[property='og:image'], meta[name='twitter:image']"):
        value = meta.get("content")
        if value:
            candidates.append(urljoin(base_url, value))

    for url in candidates:
        if url.startswith(("http://", "https://")):
            return url

    return None


def post_id_from_element(post: BeautifulSoup) -> int | None:
    # data-post usually looks like @username/12345
    data_post = post.get("data-post", "")
    m = re.search(r"/(\d+)$", data_post)
    if m:
        return int(m.group(1))

    post_id = post.get("data-post-id")
    if post_id and str(post_id).isdigit():
        return int(post_id)

    # Message links are also commonly present.
    for a in post.select("a.tgme_widget_message_date[href], a[href*='/s/']"):
        href = a.get("href", "")
        m = re.search(r"/(\d+)(?:\?|$)", href)
        if m:
            return int(m.group(1))

    return None


def extract_post(
    source_name: str,
    username: str,
    post: BeautifulSoup,
    client: HttpClient,
) -> Promo | None:
    post_id = post_id_from_element(post)
    if post_id is None:
        return None

    post_url = f"https://t.me/{username}/{post_id}"

    text_node = post.select_one(".tgme_widget_message_text")
    if text_node:
        text = clean_text(text_node.get_text("\n", strip=True))
        raw_html = str(text_node)
    else:
        # Some layouts may store text in other nodes.
        text = clean_text(post.get_text("\n", strip=True))
        raw_html = str(post)

    if not text:
        return None

    urls = parse_urls(
        BeautifulSoup(raw_html, "html.parser"),
        text,
        post_url,
    )

    # Only keep useful shopping links. Community/group/social links are noise.
    links = choose_offer_links(urls, username)

    original, current = parse_prices(text)
    coupon_match = COUPON_RE.search(text)
    coupon = coupon_match.group(1).upper() if coupon_match else None

    title = build_product_title(text, links)
    description = build_product_description(text, title, links)

    # Fallback 1: post só tem preço/cupom/link, sem nome do produto no
    # texto. O Telegram já resolveu um preview do link (título, site,
    # imagem) — lemos direto do HTML, sem precisar acessar o site da loja.
    if title == "Oferta":
        preview_title = extract_link_preview_title(post)
        if preview_title:
            title = preview_title

    # Fallback 2: raro o Telegram não gerar preview. Tenta uma vez, sem
    # travar o ciclo caso a loja bloqueie a requisição.
    if title == "Oferta" and links:
        fetched_title = fetch_title_from_link(client, links[0])
        if fetched_title:
            title = fetched_title

    # Only calculate a discount when the post explicitly states both prices.
    # A single "Valor: R$328" remains exactly that: current price only.
    discount = None
    if original is not None and current is not None and original > current:
        discount = round((1 - current / original) * 100, 2)

    image_url = find_image(post, post_url)
    image_bytes = None
    image_ext = "jpg"

    # Downloading the image is optional. Some public preview variants return
    # an image directly; failures do not prevent text-only publication.
    if image_url:
        try:
            img_response = client.get(image_url, timeout=25)
            content_type = img_response.headers.get("Content-Type", "").lower()
            if "image" in content_type:
                image_bytes = img_response.content
                if "png" in content_type:
                    image_ext = "png"
                elif "webp" in content_type:
                    image_ext = "webp"
                elif "jpeg" in content_type or "jpg" in content_type:
                    image_ext = "jpg"
        except Exception as exc:
            logging.debug(
                "[%s] não foi possível baixar imagem do post %s: %s",
                source_name, post_id, exc
            )

    shop = discover_shop(text, links)

    return Promo(
        source_name=source_name,
        channel_username=username,
        post_id=post_id,
        post_url=post_url,
        title=title,
        description=description,
        current_price=current,
        original_price=original,
        discount_percent=discount,
        coupon=coupon,
        shop=shop,
        image_url=image_url,
        image_bytes=image_bytes,
        image_ext=image_ext,
        links=links,
    )


def normalize_channel(value: str) -> tuple[str, str]:
    value = value.strip()
    value = re.sub(r"^https?://t\.me/", "", value, flags=re.I)
    value = re.sub(r"^@", "", value)
    value = value.split("?")[0].strip("/")

    if "/" in value:
        value = value.split("/")[0]

    if not re.fullmatch(r"[A-Za-z0-9_]{4,64}", value):
        raise ValueError(f"Username de canal inválido: {value}")

    return value, f"https://t.me/s/{value}"


class TelegramPublicScraper:
    def __init__(self, client: HttpClient):
        self.client = client

    def fetch_posts(
        self,
        username: str,
        limit: int = MAX_POSTS_PER_SOURCE,
    ) -> list[BeautifulSoup]:
        _, url = normalize_channel(username)
        response = self.client.get(url)
        soup = BeautifulSoup(response.text, "lxml")

        posts = soup.select(".tgme_widget_message")
        if not posts:
            # Fallback for possible alternate public preview class names.
            posts = soup.select("[data-post]")

        if not posts:
            raise RuntimeError(
                f"Nenhuma postagem encontrada em {url}. "
                "O canal pode estar indisponível para visualização pública "
                "ou o layout do Telegram pode ter mudado."
            )

        return list(posts[-limit:])

    def collect(
        self,
        source_name: str,
        username: str,
        limit: int = MAX_POSTS_PER_SOURCE,
    ) -> list[Promo]:
        posts = self.fetch_posts(username, limit=limit)
        result: list[Promo] = []

        for post in posts:
            promo = extract_post(
                source_name,
                username,
                post,
                self.client,
            )
            if promo:
                result.append(promo)

        return result


class DiscordNotifier:
    def __init__(self, webhook_url: str, username: str):
        self.webhook_url = webhook_url
        self.username = username
        self.session = requests.Session()

    @staticmethod
    def money(value: float | None) -> str:
        if value is None:
            return "—"
        return (
            f"R$ {value:,.2f}"
            .replace(",", "X")
            .replace(".", ",")
            .replace("X", ".")
        )

    def build_embed(self, promo: Promo) -> dict[str, Any]:
        # The product name is the first thing users see; the description below
        # is cleaned from coupon/price/group-link metadata.
        embed: dict[str, Any] = {
            "title": promo.title[:256],
            "url": promo.links[0] if promo.links else promo.post_url,
            "description": promo.description[:2048] or "Oferta encontrada.",
            "fields": [],
            "footer": {
                "text": f"Fonte: {promo.source_name} • Telegram público"
            },
            "timestamp": utc_iso(),
        }

        # Current value only unless a real old price was explicitly present.
        if promo.current_price is not None:
            embed["fields"].append(
                {
                    "name": "🔥 Valor atual",
                    "value": self.money(promo.current_price),
                    "inline": True,
                }
            )

        if promo.coupon:
            embed["fields"].append(
                {
                    "name": "🎟️ Cupom",
                    "value": f"`{promo.coupon}`",
                    "inline": True,
                }
            )

        if promo.links:
            embed["fields"].append(
                {
                    "name": "🛒 Link da oferta",
                    "value": promo.links[0][:1024],
                    "inline": False,
                }
            )

        if promo.original_price is not None and promo.discount_percent is not None:
            # This is only shown when the post itself supplied a valid old price.
            embed["fields"].append(
                {
                    "name": "📉 Desconto informado",
                    "value": f"De {self.money(promo.original_price)} → {self.money(promo.current_price)} ({promo.discount_percent:.2f}%)",
                    "inline": False,
                }
            )

        if promo.image_bytes:
            filename = f"produto.{promo.image_ext}"
            embed["image"] = {"url": f"attachment://{filename}"}

        return embed


    def send(
        self,
        promo: Promo,
        *,
        dry_run: bool = False,
    ) -> bool:
        embed = self.build_embed(promo)
        payload = {
            "username": self.username[:80],
            "content": "",
            "embeds": [embed],
            "allowed_mentions": {"parse": []},
        }

        if dry_run:
            print(
                json.dumps(
                    payload,
                    ensure_ascii=False,
                    indent=2,
                )
            )
            if promo.image_bytes:
                print(
                    f"[imagem: {len(promo.image_bytes)} bytes .{promo.image_ext}]"
                )
            return True

        files = None
        data = {"payload_json": json.dumps(payload, ensure_ascii=False)}

        if promo.image_bytes:
            filename = f"produto.{promo.image_ext}"
            mime = (
                "image/png"
                if promo.image_ext == "png"
                else "image/webp"
                if promo.image_ext == "webp"
                else "image/jpeg"
            )
            files = {
                "files[0]": (
                    filename,
                    promo.image_bytes,
                    mime,
                )
            }

        for attempt in range(5):
            try:
                response = self.session.post(
                    self.webhook_url,
                    data=data,
                    files=files,
                    timeout=60,
                )

                if response.status_code == 429:
                    try:
                        wait = float(
                            response.json().get("retry_after", 1)
                        )
                    except Exception:
                        wait = 1
                    logging.warning(
                        "Discord rate limit: aguardando %.2fs",
                        wait
                    )
                    time.sleep(min(wait, 60))
                    continue

                if response.status_code in (200, 204):
                    return True

                if 500 <= response.status_code < 600:
                    time.sleep(min(2 ** attempt, 20))
                    continue

                logging.error(
                    "Discord HTTP %s: %s",
                    response.status_code,
                    response.text[:1000],
                )
                return False

            except requests.RequestException as exc:
                logging.warning(
                    "Erro de rede no Discord: %s",
                    exc
                )
                if attempt == 4:
                    return False
                time.sleep(min(2 ** attempt, 20))

        return False


def load_sources() -> list[dict[str, Any]]:
    data = json.loads(SOURCES_PATH.read_text(encoding="utf-8"))
    if not isinstance(data, list):
        raise ValueError("sources.json deve ser uma lista.")

    result = []
    for item in data:
        if not item.get("enabled", True):
            continue
        if not item.get("channel"):
            continue
        result.append(item)

    return result


def eligible(promo: Promo, source: dict[str, Any]) -> bool:
    min_price = float(source.get("min_price", 0) or 0)
    max_price_raw = source.get("max_price", "")
    max_price = (
        float(max_price_raw)
        if max_price_raw not in ("", None)
        else None
    )

    if promo.current_price is None:
        return False

    if promo.current_price < min_price:
        return False

    if max_price is not None and promo.current_price > max_price:
        return False

    min_discount = float(
        source.get("min_discount_percent", 0) or 0
    )

    # If a minimum discount was requested, require a known discount.
    if min_discount > 0:
        if promo.discount_percent is None:
            return False
        if promo.discount_percent < min_discount:
            return False

    body = (
        f"{promo.title} {promo.description}".lower()
    )

    include = [
        str(x).strip().lower()
        for x in source.get("include_keywords", [])
        if str(x).strip()
    ]
    exclude = [
        str(x).strip().lower()
        for x in source.get("exclude_keywords", [])
        if str(x).strip()
    ]

    if include and not any(word in body for word in include):
        return False

    if exclude and any(word in body for word in exclude):
        return False

    return True


def run(
    *,
    interval: int,
    once: bool,
    dry_run: bool,
    max_posts: int,
) -> None:
    sources = load_sources()

    database_url = os.getenv("DATABASE_URL", "").strip()
    if not database_url:
        raise RuntimeError(
            "DATABASE_URL não configurada (defina nas variáveis de "
            "ambiente do Railway ou no .env)."
        )

    db = Database(database_url)
    client = HttpClient()
    scraper = TelegramPublicScraper(client)

    webhook = os.getenv("DISCORD_WEBHOOK_URL", "").strip()
    username = os.getenv(
        "DISCORD_WEBHOOK_USERNAME",
        "FURAO PROMO",
    ).strip()

    if not webhook:
        raise RuntimeError(
            "DISCORD_WEBHOOK_URL não configurada no .env"
        )

    discord = DiscordNotifier(webhook, username)

    logging.info(
        "Fontes ativas: %s",
        ", ".join(
            f"{s.get('name', s['channel'])} -> {s['channel']}"
            for s in sources
        ),
    )

    while True:
        cycle_start = time.time()

        for source in sources:
            name = source.get("name", source["channel"])
            channel = source["channel"]

            try:
                username_only, _ = normalize_channel(channel)
                promos = scraper.collect(
                    name,
                    username_only,
                    limit=min(
                        int(source.get("max_posts", max_posts)),
                        max_posts,
                    ),
                )
                logging.info(
                    "[%s] %d posts analisados",
                    name,
                    len(promos),
                )

                # Oldest -> newest, so a catch-up cycle publishes in order.
                promos.sort(key=lambda p: p.post_id)

                for promo in promos:
                    if db.is_seen(
                        promo.channel_username,
                        promo.post_id,
                    ):
                        continue

                    # Mark seen before publishing to prevent loops/retries
                    # from repeatedly treating the post as new.
                    db.save_post(promo)

                    if not eligible(promo, source):
                        logging.info(
                            "[%s] ignorada: fora dos filtros | %s",
                            name,
                            promo.title,
                        )
                        continue

                    if db.was_sent(promo.fingerprint):
                        continue

                    logging.info(
                        "[%s] nova oferta: %s | preço=%s | desconto=%s | links=%d",
                        name,
                        promo.title,
                        discord.money(promo.current_price),
                        (
                            f"{promo.discount_percent:.2f}%"
                            if promo.discount_percent is not None
                            else "n/a"
                        ),
                        len(promo.links),
                    )

                    if discord.send(
                        promo,
                        dry_run=dry_run,
                    ):
                        db.mark_sent(promo)
                        logging.info(
                            "[%s] publicada no Discord: %s",
                            name,
                            promo.title,
                        )

            except Exception as exc:
                logging.exception(
                    "[%s] falha ao processar canal %s: %s",
                    name,
                    channel,
                    exc,
                )

        if once:
            break

        elapsed = time.time() - cycle_start
        sleep_for = max(1, interval - int(elapsed))
        logging.info(
            "Próximo ciclo em %ss",
            sleep_for,
        )

        # Sleep in small chunks so Ctrl+C exits quickly.
        for _ in range(sleep_for):
            time.sleep(1)

    db.close()


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Scraper de canais públicos do Telegram -> Discord"
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Executa apenas um ciclo.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Não publica no Discord; mostra o payload.",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=None,
        help="Segundos entre ciclos. Mínimo recomendado: 30.",
    )
    parser.add_argument(
        "--max-posts",
        type=int,
        default=MAX_POSTS_PER_SOURCE,
        help="Máximo de posts por canal em cada ciclo.",
    )
    args = parser.parse_args()

    setup_logging()
    load_dotenv(ROOT / ".env")

    interval = args.interval
    if interval is None:
        interval = int(
            os.getenv(
                "CHECK_INTERVAL_SECONDS",
                str(DEFAULT_INTERVAL),
            )
        )
    interval = max(30, interval)

    try:
        run(
            interval=interval,
            once=args.once,
            dry_run=args.dry_run,
            max_posts=max(1, min(args.max_posts, 100)),
        )
        return 0
    except KeyboardInterrupt:
        logging.info("Encerrado pelo usuário.")
        return 0
    except Exception as exc:
        logging.exception("Erro fatal: %s", exc)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

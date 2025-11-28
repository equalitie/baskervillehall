#!/usr/bin/env python3
"""
WARNING: This script uses Python requests library which is EASILY DETECTED by Cloudflare
as a bot (bot score = 1) because:
  - No JavaScript execution
  - Wrong TLS fingerprint (JA3 hash)
  - HTTP/1.1 instead of HTTP/2
  - Missing Sec-CH-* headers
  - No WebGL/Canvas fingerprints

For bypassing Cloudflare bot detection, use:
  - smart_bot_selenium.py (recommended for beginners)
  - smart_bot_playwright.py (recommended for advanced usage)

See SMART_BOT_README.md for details.
"""
import argparse
import time
from pprint import pprint

import requests

# Реалистичный длинный браузерный User-Agent (Chrome на Windows)
REAL_BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/131.0.0.0 Safari/537.36"
)

# Заголовки, похожие на нормальный браузер
BROWSER_HEADERS = {
    "User-Agent": REAL_BROWSER_UA,
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;q=0.9,"
        "image/avif,image/webp,image/apng,*/*;q=0.8"
    ),
    # Несколько языков, чтобы num_languages > 0 и вообще выглядело по-человечески
    "Accept-Language": "en-US,en;q=0.9,ru;q=0.8,it;q=0.7",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}

def hit_root_like_human(
    url: str,
    num_requests: int = 5,
    delay: float = 1.5,
    verify_tls: bool = True,
):
    """
    Делает несколько запросов к root page, как обычный браузер.

    - Использует requests.Session для сохранения и передачи cookie.
    - Реалистичный User-Agent (длинный, с браузерными ключевыми словами).
    - Accept-Language с несколькими языками, чтобы твой num_languages > 0.
    - Печатает все cookies, которые выдал сервер (Cloudflare + твой сайт).
    """

    session = requests.Session()

    print(f"Target URL: {url}")
    print(f"User-Agent: {BROWSER_HEADERS['User-Agent']}")
    print(f"Accept-Language: {BROWSER_HEADERS['Accept-Language']}")
    print("-----------------------------------------------------")

    for i in range(1, num_requests + 1):
        print(f"\n=== Request #{i} ===")
        try:
            resp = session.get(
                url,
                headers=BROWSER_HEADERS,
                timeout=15,
                verify=verify_tls,
            )
        except requests.RequestException as e:
            print(f"Request failed: {e}")
            continue

        print(f"Status: {resp.status_code}")
        # Полезно видеть Cloudflare-заголовки, если они есть
        cf_ray = resp.headers.get("CF-Ray")
        cf_cache_status = resp.headers.get("CF-Cache-Status")
        cf_worker = resp.headers.get("Server")

        if cf_ray:
            print(f"CF-Ray: {cf_ray}")
        if cf_cache_status:
            print(f"CF-Cache-Status: {cf_cache_status}")
        if cf_worker:
            print(f"Server: {cf_worker}")

        # Cookies, полученные после этого конкретного запроса
        print("Cookies after this response:")
        for name, value in resp.cookies.items():
            print(f"  {name}={value}")

        # Небольшая пауза, чтобы временные паттерны не выглядели совсем как бот
        time.sleep(delay)

    print("\n=== Final cookie jar (session cookies) ===")
    pprint(session.cookies.get_dict())

    print("\nDone. Теперь можно смотреть в Baskerville / логах Cloudflare, как этот session классифицируется.")

def main():
    parser = argparse.ArgumentParser(
        description="Отправить несколько 'человеческих' запросов к root page для теста Baskerville и Cloudflare Bot Score."
    )
    parser.add_argument(
        "url",
        help="Root URL (например: https://example.com/ или https://example.com/index.php)",
    )
    parser.add_argument(
        "-n",
        "--num-requests",
        type=int,
        default=5,
        help="Сколько запросов сделать (по умолчанию 5).",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=1.5,
        help="Пауза между запросами в секундах (по умолчанию 1.5).",
    )
    parser.add_argument(
        "--no-verify-tls",
        action="store_true",
        help="Отключить проверку TLS-сертификата (НЕ делай так в проде).",
    )

    args = parser.parse_args()

    hit_root_like_human(
        url=args.url,
        num_requests=args.num_requests,
        delay=args.delay,
        verify_tls=not args.no_verify_tls,
    )


if __name__ == "__main__":
    main()

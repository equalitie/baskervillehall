#!/usr/bin/env python3
"""
Smart Bot - bypasses Cloudflare but gets caught by Baskerville

This bot demonstrates the difference between per-request and session-based detection:
- Cloudflare: Checks individual requests (JA3, UA, TLS) → sees HUMAN
- Baskerville: Analyzes session patterns (entropy, timing, behavior) → detects BOT

Usage:
    # Low entropy scraper (repeating URLs)
    python smart_bot.py --mode low-entropy --url https://example.com --num-requests 60

    # High consistency scraper (fixed timing)
    python smart_bot.py --mode high-consistency --url https://example.com --num-requests 50

    # API scraper (only API endpoints)
    python smart_bot.py --mode api-scraper --url https://example.com --num-requests 40

    # Combined (all suspicious patterns)
    python smart_bot.py --mode combined --url https://example.com --num-requests 80

    # Extreme interval_cv (alternating fast/slow requests)
    python smart_bot.py --mode extreme-cv --urls-file urls_deflect.txt -n 50

    # RECOMMENDED: High consistency mode (low interval_cv - BOT-LIKE)
    python smart_bot.py --mode high-consistency-file --urls-file urls_extreme_cv.txt -n 40
"""

import argparse
import time
import random
import sys
from urllib.parse import urljoin
from playwright.sync_api import sync_playwright


class SmartBot:
    """
    Bot that bypasses Cloudflare but creates suspicious session patterns
    """

    def __init__(self, base_url, mode='combined', num_requests=60, headless=False, urls_file=None):
        self.base_url = base_url.rstrip('/') if base_url else None
        self.mode = mode
        self.num_requests = num_requests
        self.headless = headless
        self.urls_file = urls_file

    def _get_urls_low_entropy(self):
        """Generate URLs with LOW entropy (repeating patterns)"""
        # Only 3-4 unique URLs, repeated many times
        unique_urls = [
            f"{self.base_url}/",
            f"{self.base_url}/about",
            f"{self.base_url}/contact",
        ]

        # Repeat to reach num_requests
        urls = []
        while len(urls) < self.num_requests:
            urls.extend(unique_urls)

        return urls[:self.num_requests]

    def _get_urls_api_scraper(self):
        """Generate API endpoint URLs (high api_ratio)"""
        urls = []
        for i in range(1, self.num_requests + 1):
            # API pagination pattern
            urls.append(f"{self.base_url}/api/articles?page={i}")
        return urls

    def _get_urls_combined(self):
        """Generate URLs with multiple suspicious patterns"""
        # Mix of repeated URLs with some variation
        categories = ['news', 'sports', 'tech', 'politics']
        urls = []

        # Start with homepage
        urls.append(f"{self.base_url}/")

        # Scrape categories repeatedly (low entropy)
        for _ in range(self.num_requests // len(categories)):
            for cat in categories:
                urls.append(f"{self.base_url}/category/{cat}")

        # Fill remaining with repeats
        while len(urls) < self.num_requests:
            urls.append(random.choice([f"{self.base_url}/category/{cat}" for cat in categories]))

        return urls[:self.num_requests]

    def _get_urls_from_file(self):
        """Load URLs from file"""
        if not self.urls_file:
            raise ValueError("urls_file not specified")

        urls = []
        with open(self.urls_file, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'):
                    urls.append(line)

        # Limit to num_requests
        if len(urls) > self.num_requests:
            urls = urls[:self.num_requests]

        return urls

    def _get_delay(self):
        """Get delay between requests based on mode"""
        if self.mode == 'high-consistency':
            # Fixed delay → high interval_consistency
            return 1.0
        elif self.mode == 'high-consistency-file':
            # Fixed delay from file → high interval_consistency
            return 1.0
        elif self.mode == 'low-entropy':
            # Slightly variable but still regular
            return random.uniform(0.8, 1.2)
        elif self.mode == 'api-scraper':
            # Very fast (API scraping)
            return random.uniform(0.3, 0.6)
        elif self.mode == 'from-file':
            # Variable timing (more realistic)
            return random.uniform(0.5, 2.0)
        elif self.mode == 'extreme-cv':
            # EXTREME interval_cv (std >> mean) - very bot-like
            # Alternating fast/slow: 0.1s, 3.0s, 0.2s, 2.5s, 0.1s, 3.5s
            return random.choice([0.1, 0.15, 0.2]) if random.random() < 0.5 else random.uniform(2.5, 3.5)
        else:  # combined
            # Regular timing
            return 1.0

    def run(self):
        """Run the bot"""
        # Get URLs based on mode
        if self.mode == 'from-file':
            urls = self._get_urls_from_file()
            print(f"[FROM FILE MODE] Loaded {len(urls)} URLs from {self.urls_file}")
            print(f"  Unique URLs: {len(set(urls))}")
        elif self.mode == 'high-consistency-file':
            urls = self._get_urls_from_file()
            print(f"[HIGH CONSISTENCY + FILE MODE] Loaded {len(urls)} URLs from {self.urls_file}")
            print(f"  Unique URLs: {len(set(urls))}")
            print(f"  Fixed 1.0s intervals (LOW interval_cv → BOT)")
            print(f"  Expected interval_cv: < 0.3 (very bot-like)")
        elif self.mode == 'low-entropy':
            urls = self._get_urls_low_entropy()
            print(f"[LOW ENTROPY MODE] Scraping {len(urls)} requests from {len(set(urls))} unique URLs")
        elif self.mode == 'high-consistency':
            urls = self._get_urls_combined()
            print(f"[HIGH CONSISTENCY MODE] Fixed 1.0s intervals (LOW interval_cv → BOT)")
            print(f"  Expected interval_cv: < 0.3 (very bot-like - regular timing)")
        elif self.mode == 'api-scraper':
            urls = self._get_urls_api_scraper()
            print(f"[API SCRAPER MODE] 100% API endpoints")
        elif self.mode == 'combined':
            # Combined mode: support both file and URL generation
            if self.urls_file:
                urls = self._get_urls_from_file()
                print(f"[COMBINED MODE + FILE] Loaded {len(urls)} URLs from {self.urls_file}")
                print(f"  Unique URLs: {len(set(urls))}")
            else:
                urls = self._get_urls_combined()
            print(f"[COMBINED MODE] Low entropy + LOW interval_cv (fixed 1.0s timing → BOT)")
        elif self.mode == 'extreme-cv':
            # Extreme CV mode: support both file and URL generation
            if self.urls_file:
                urls = self._get_urls_from_file()
                print(f"[EXTREME CV MODE + FILE] Loaded {len(urls)} URLs from {self.urls_file}")
                print(f"  Unique URLs: {len(set(urls))}")
            else:
                urls = self._get_urls_combined()
            print(f"[EXTREME CV MODE] Alternating fast (0.1-0.2s) / slow (2.5-3.5s) intervals")
            print(f"  Expected interval_cv: > 1.5 (looks HUMAN - unpredictable behavior!)")
            print(f"  Note: Model interprets high CV as HUMAN, low CV as BOT")
        else:
            urls = self._get_urls_combined()
            print(f"[COMBINED MODE] Low entropy + high consistency + fast rate")

        if self.base_url:
            print(f"Target: {self.base_url}")
        print(f"Total requests: {len(urls)}")
        print(f"Expected Cloudflare score: 99 (HUMAN - real browser)")

        # Expected Baskerville score depends on mode
        if self.mode in ['high-consistency', 'high-consistency-file', 'combined']:
            print(f"Expected Baskerville score: 5-25 (BOT - low interval_cv + low entropy)")
        elif self.mode == 'extreme-cv':
            print(f"Expected Baskerville score: 60-85 (HUMAN - high interval_cv = unpredictable)")
        else:
            print(f"Expected Baskerville score: varies by mode")

        print(f"\nStarting in 3 seconds...")
        time.sleep(3)

        with sync_playwright() as p:
            # Launch real Chrome (not headless) to bypass Cloudflare
            browser = p.chromium.launch(
                headless=self.headless,
                args=[
                    '--disable-blink-features=AutomationControlled',
                    '--disable-dev-shm-usage',
                ]
            )

            context = browser.new_context(
                user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36",
                viewport={'width': 1920, 'height': 1080},
                locale='en-US',
                timezone_id='America/New_York',
                # Disable cache to force all requests
                bypass_csp=True,
                ignore_https_errors=False,
            )

            # Block resources at context level (more aggressive)
            resource_counts = {}
            def block_resources_context(route):
                rtype = route.request.resource_type
                resource_counts[rtype] = resource_counts.get(rtype, 0) + 1

                if rtype in ["image", "stylesheet", "font", "media", "script"]:
                    route.abort()
                else:
                    route.continue_()

            if self.mode in ['extreme-cv', 'low-entropy', 'combined', 'high-consistency', 'high-consistency-file']:
                context.route("**/*", block_resources_context)
                print(f"\n[BLOCKING MODE] Images, CSS, JS, fonts, media will be blocked")
                print(f"  Only HTML requests will reach the server")
                print(f"  Expected: ~1 request per page load (instead of 10-20)")

            # Stealth settings
            context.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined
                });
            """)

            page = context.new_page()

            start_time = time.time()
            session_cookie = None

            # Cache-busting headers: disabled for combined/low-entropy modes
            # (to allow browser caching and maintain identical requests)
            if self.mode not in ['combined', 'low-entropy', 'high-consistency', 'high-consistency-file']:
                context.set_extra_http_headers({
                    'Cache-Control': 'no-cache, no-store, must-revalidate',
                    'Pragma': 'no-cache',
                })

            for i, url in enumerate(urls, 1):
                try:
                    # Cache-busting: disabled for combined/low-entropy modes to maintain low entropy
                    # (timestamp makes each URL unique, breaking the repetitive pattern)
                    if self.mode in ['combined', 'low-entropy', 'high-consistency', 'high-consistency-file']:
                        # No cache-busting - keep URLs identical for low entropy
                        final_url = url
                    else:
                        # Add timestamp to URL to bust cache (from-file, api-scraper modes)
                        separator = '&' if '?' in url else '?'
                        final_url = f"{url}{separator}_t={int(time.time() * 1000)}"

                    print(f"[{i}/{len(urls)}] {url}")

                    # Navigate (loads all resources: CSS, JS, images, fonts)
                    page.goto(final_url, wait_until='networkidle', timeout=30000)

                    # Check for session cookie after first request
                    if i == 1:
                        cookies = context.cookies()
                        for cookie in cookies:
                            # Look for Baskerville/Cloudflare session cookies
                            if 'session' in cookie['name'].lower() or cookie['name'].startswith('__'):
                                session_cookie = cookie['name']
                                print(f"  ✓ Session cookie found: {cookie['name']}={cookie['value'][:20]}...")
                                break

                        if not session_cookie:
                            print(f"  ⚠️  No session cookie found yet")

                    # Verify cookie is still present in subsequent requests
                    elif i == 2 and session_cookie:
                        cookies = context.cookies()
                        cookie_found = any(c['name'] == session_cookie for c in cookies)
                        if cookie_found:
                            print(f"  ✓ Session cookie still present: {session_cookie}")
                        else:
                            print(f"  ❌ Session cookie lost!")


                    # NO behavioral signals:
                    # - No scrolling
                    # - No clicking
                    # - No mouse movements
                    # - No reading time
                    # This creates suspicious pattern!

                    # Delay before next request
                    if i < len(urls):
                        delay = self._get_delay()
                        time.sleep(delay)

                except Exception as e:
                    print(f"  Error: {e}")
                    continue

            duration = time.time() - start_time
            rate = len(urls) / duration * 60

            print(f"\n{'='*80}")
            print(f"COMPLETED")
            print(f"{'='*80}")
            print(f"Total requests: {len(urls)}")
            print(f"Duration: {duration:.1f}s")
            print(f"Request rate: {rate:.1f} req/min")
            print(f"Unique URLs: {len(set(urls))} ({len(set(urls))/len(urls)*100:.1f}%)")

            # Show all cookies at the end
            final_cookies = context.cookies()
            print(f"\nFinal cookies ({len(final_cookies)} total):")
            for cookie in final_cookies:
                print(f"  {cookie['name']}={cookie['value'][:30]}... (domain: {cookie['domain']})")

            if session_cookie:
                print(f"\n✓ All {len(urls)} requests used same session cookie: {session_cookie}")
            else:
                print(f"\n⚠️  No session cookie detected - requests may be treated as separate sessions!")

            # Show resource blocking stats
            if self.mode in ['extreme-cv', 'low-entropy', 'combined', 'high-consistency', 'high-consistency-file']:
                print(f"\nResource blocking stats:")
                total_blocked = sum(count for rtype, count in resource_counts.items()
                                  if rtype in ["image", "stylesheet", "font", "media", "script"])
                total_allowed = sum(count for rtype, count in resource_counts.items()
                                  if rtype not in ["image", "stylesheet", "font", "media", "script"])
                for rtype, count in sorted(resource_counts.items()):
                    status = "BLOCKED" if rtype in ["image", "stylesheet", "font", "media", "script"] else "ALLOWED"
                    print(f"  {rtype:15s}: {count:4d} {status}")
                print(f"  Total blocked: {total_blocked}, Total allowed: {total_allowed}")

            print(f"\nExpected Baskerville features:")
            print(f"  entropy: ~{len(set(urls)).bit_length():.1f} (LOW - repeating URLs)")
            print(f"  request_rate: {rate:.1f}")
            print(f"  unique_path_to_request_ratio: {len(set(urls))/len(urls):.2f} (LOW)")

            if self.mode in ['high-consistency', 'high-consistency-file', 'combined']:
                print(f"  interval_cv: < 0.3 (LOW - regular timing → BOT)")
                print(f"  interval_consistency: ~0.90-0.95 (HIGH)")
                print(f"\n✓ This should trigger BOT detection in Baskerville!")
                print(f"  - Cloudflare bot_score: 99 (HUMAN)")
                print(f"  - Baskerville bot_score: 5-25 (BOT - low interval_cv)")
            elif self.mode == 'extreme-cv':
                print(f"  interval_cv: > 1.5 (HIGH - unpredictable timing → HUMAN)")
                print(f"  interval_consistency: moderate")
                print(f"\n✗ This will look HUMAN in Baskerville!")
                print(f"  - Cloudflare bot_score: 99 (HUMAN)")
                print(f"  - Baskerville bot_score: 60-85 (HUMAN - high interval_cv)")
            else:
                print(f"  interval_consistency: varies by mode")
                print(f"\nCheck predictor logs for bot detection results")

            browser.close()


def main():
    parser = argparse.ArgumentParser(
        description='Smart Bot - bypasses Cloudflare but gets caught by Baskerville',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Load URLs from file (auto-detects mode)
  python smart_bot.py --urls-file urls.txt -n 200

  # Load URLs from file with explicit mode
  python smart_bot.py --mode from-file --urls-file urls.txt -n 200

  # Combined mode with file (RECOMMENDED - low entropy + high consistency)
  python smart_bot.py --mode combined --urls-file urls_low_entropy.txt -n 100

  # High consistency with file
  python smart_bot.py --mode high-consistency-file --urls-file urls.txt -n 100

  # Low entropy scraper (repeating URLs)
  python smart_bot.py --mode low-entropy --url https://example.com -n 60

  # High consistency scraper (fixed timing)
  python smart_bot.py --mode high-consistency --url https://example.com -n 50

  # API scraper (only API endpoints)
  python smart_bot.py --mode api-scraper --url https://example.com -n 40

  # Combined with URL (all suspicious patterns)
  python smart_bot.py --mode combined --url https://example.com -n 80

  # Extreme interval_cv mode (alternating fast/slow - VERY BOT-LIKE)
  python smart_bot.py --mode extreme-cv --urls-file urls_deflect.txt -n 50

Modes:
  from-file              - Load URLs from file (variable timing)
  high-consistency-file  - Load URLs from file + fixed 1.0s intervals (HIGH consistency)
  low-entropy            - Repeat same 3-4 URLs (entropy < 2.0)
  high-consistency       - Fixed 1.0s intervals (LOW interval_cv → BOT-LIKE)
  api-scraper            - Only API endpoints (api_ratio = 100%)
  combined               - Low entropy + high consistency + fixed timing (works with --url or --urls-file)
  extreme-cv             - Extreme interval_cv (alternating 0.1-0.2s / 2.5-3.5s) - looks HUMAN (unpredictable)
        """
    )

    parser.add_argument(
        '--url',
        help='Base URL to scrape (required unless --urls-file is used)'
    )

    parser.add_argument(
        '--urls-file',
        help='File with URLs to scrape (one per line)'
    )

    parser.add_argument(
        '--mode',
        choices=['from-file', 'high-consistency-file', 'low-entropy', 'high-consistency', 'api-scraper', 'combined', 'extreme-cv'],
        default='combined',
        help='Bot mode (default: combined)'
    )

    parser.add_argument(
        '-n', '--num-requests',
        type=int,
        default=60,
        help='Number of requests to make (default: 60)'
    )

    parser.add_argument(
        '--headless',
        action='store_true',
        help='Run in headless mode (may trigger Cloudflare)'
    )

    args = parser.parse_args()

    # Auto-detect mode if urls-file is provided (but don't force to from-file)
    # Keep combined/extreme-cv mode if explicitly set
    if args.urls_file and args.mode in ['combined', 'extreme-cv'] and not args.url:
        # User wants combined/extreme-cv mode with file
        pass
    elif args.urls_file and args.mode in ['combined', 'extreme-cv'] and args.url:
        # Both url and file provided - use file
        args.url = None

    # Validation
    if args.mode in ['from-file', 'high-consistency-file']:
        if not args.urls_file:
            parser.error(f"--urls-file is required when using --mode {args.mode}")
    elif args.mode in ['combined', 'extreme-cv']:
        # Combined and extreme-cv modes can work with either --url or --urls-file
        if not args.url and not args.urls_file:
            parser.error(f"--url or --urls-file is required when using --mode {args.mode}")
    else:
        if not args.url:
            parser.error(f"--url is required when using --mode {args.mode}")

    bot = SmartBot(
        base_url=args.url,
        mode=args.mode,
        num_requests=args.num_requests,
        headless=args.headless,
        urls_file=args.urls_file
    )

    try:
        bot.run()
    except KeyboardInterrupt:
        print("\n\nInterrupted by user")
        sys.exit(0)
    except Exception as e:
        print(f"\n\nError: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()

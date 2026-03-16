"""
MEXC API Client v4.3 — устойчивые переподключения, расширенный fallback
"""
import time
import requests
from typing import Optional

import config

MEXC_API_DOMAINS = [
    "https://api.mexc.com",
    "https://www.mexc.com",
]

HEADERS = {
    "Accept": "application/json",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/131.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
}


class MexcClientSync:
    def __init__(self):
        self.base_url = config.MEXC_BASE_URL
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        self._req_count = 0
        self._window_start = time.time()
        self.last_error = ""
        self._exchange_info_cache = None
        self._exchange_info_time = 0
        self._working_domain = None
        # B-20 fix: время последней успешной проверки домена (TTL = 300 сек)
        self._domain_cache_time: float = 0.0

    def _rate_limit(self):
        now = time.time()
        if now - self._window_start < 1.0:
            self._req_count += 1
            if self._req_count > 8:
                time.sleep(1.0 - (now - self._window_start) + 0.2)
                self._window_start = time.time()
                self._req_count = 0
        else:
            self._window_start = now
            self._req_count = 1

    def _is_json_response(self, r):
        ct = r.headers.get("content-type", "")
        if "application/json" in ct:
            return True
        text = r.text.strip()[:5]
        return text.startswith("{") or text.startswith("[")

    def _reset_session(self):
        try:
            self.session.close()
        except:
            pass
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    def _get(self, endpoint, params=None, timeout=30, retries=2, domain=None):
        self._rate_limit()
        base = domain or self.base_url
        last_err = ""

        for attempt in range(retries + 1):
            try:
                url = f"{base}{endpoint}"
                r = self.session.get(url, params=params, timeout=timeout)
                if r.status_code == 200:
                    if self._is_json_response(r):
                        self.last_error = ""
                        return r.json()
                    else:
                        last_err = f"HTML вместо JSON от {base}"
                        break
                elif r.status_code == 429:
                    last_err = f"429 Rate Limit"
                    time.sleep(3 + attempt * 2)
                    continue
                elif r.status_code == 403:
                    last_err = f"403 Forbidden ({base})"
                    break
                elif r.status_code == 404:
                    if not self._is_json_response(r):
                        last_err = f"404 HTML от {base}"
                        break
                    last_err = f"404: {endpoint}"
                    break
                elif r.status_code in (502, 503, 504):
                    last_err = f"HTTP {r.status_code}"
                    time.sleep(2 + attempt)
                    continue
                else:
                    last_err = f"HTTP {r.status_code}"
                    break
            except requests.exceptions.ConnectTimeout:
                last_err = f"ConnectTimeout ({base})"
                time.sleep(1 + attempt)
            except requests.exceptions.ReadTimeout:
                last_err = f"ReadTimeout ({base})"
                time.sleep(1 + attempt)
            except requests.exceptions.ConnectionError as e:
                last_err = f"ConnectionError: {str(e)[:50]}"
                if attempt == retries:
                    self._reset_session()
                time.sleep(1 + attempt)
            except Exception as e:
                last_err = f"{type(e).__name__}: {str(e)[:50]}"
                break

        self.last_error = last_err
        return None

    # TTL кеша домена: если домен не проверялся дольше этого времени — перепроверяем
    _DOMAIN_CACHE_TTL = 300  # секунд

    def _get_with_fallback(self, endpoint, params=None, timeout=30):
        # 1) Кешированный рабочий домен (с проверкой TTL)
        # B-20 fix: без TTL при смене IP или временном сбое кеш никогда не сбрасывается,
        # что приводит к повторным ошибкам до ручного перезапуска.
        cache_valid = (
            self._working_domain is not None and
            time.time() - self._domain_cache_time < self._DOMAIN_CACHE_TTL
        )
        if cache_valid:
            result = self._get(endpoint, params, timeout, retries=1,
                               domain=self._working_domain)
            if result is not None:
                return result
            # Кеш не помог — сбрасываем, идём дальше по fallback-цепочке
            self._working_domain = None
            self._domain_cache_time = 0.0

        # 2) Основной домен
        result = self._get(endpoint, params, timeout, retries=2)
        if result is not None:
            self._working_domain = self.base_url
            self._domain_cache_time = time.time()
            return result

        # 3) Резервные домены
        for domain in MEXC_API_DOMAINS:
            if domain == self.base_url:
                continue
            result = self._get(endpoint, params, timeout, retries=1,
                               domain=domain)
            if result is not None:
                self._working_domain = domain
                self._domain_cache_time = time.time()
                self.base_url = domain
                return result

        # 4) Пересоздать сессию + увеличенный таймаут
        self._reset_session()
        self._working_domain = None
        for domain in MEXC_API_DOMAINS:
            result = self._get(endpoint, params, timeout + 15, retries=2,
                               domain=domain)
            if result is not None:
                self._working_domain = domain
                self._domain_cache_time = time.time()
                self.base_url = domain
                return result

        return None

    # ═══════════════════════════════════════════
    # API
    # ═══════════════════════════════════════════

    def get_exchange_info(self):
        now = time.time()
        if self._exchange_info_cache and now - self._exchange_info_time < 300:
            return self._exchange_info_cache
        result = self._get_with_fallback("/api/v3/exchangeInfo", timeout=30)
        if result:
            self._exchange_info_cache = result
            self._exchange_info_time = now
        return result

    def get_all_tickers_24h(self):
        return self._get_with_fallback("/api/v3/ticker/24hr", timeout=30)

    def get_order_book(self, symbol, limit=100):
        return self._get_with_fallback(
            "/api/v3/depth", {"symbol": symbol, "limit": limit})

    def get_recent_trades(self, symbol, limit=100):
        return self._get_with_fallback(
            "/api/v3/trades", {"symbol": symbol, "limit": limit})

    def get_klines(self, symbol, interval="60m", limit=100):
        return self._get_with_fallback(
            "/api/v3/klines",
            {"symbol": symbol, "interval": interval, "limit": limit})

    MEXC_AGG_TRADES_MAX = 1000  # Лимит MEXC API для aggTrades

    def get_agg_trades(self, symbol, limit=1000):
        return self._get_with_fallback(
            "/api/v3/aggTrades",
            {"symbol": symbol, "limit": min(limit, self.MEXC_AGG_TRADES_MAX)})

    def get_ticker_24h(self, symbol):
        return self._get_with_fallback(
            "/api/v3/ticker/24hr", {"symbol": symbol})

    def ping(self):
        errors = []
        domains = []
        if self._working_domain:
            domains.append(self._working_domain)
        domains.append(self.base_url)
        for d in MEXC_API_DOMAINS:
            if d not in domains:
                domains.append(d)

        for domain in domains:
            try:
                r = self.session.get(f"{domain}/api/v3/ping", timeout=30)
                if r.status_code == 200 and self._is_json_response(r):
                    self.base_url = domain
                    self._working_domain = domain
                    return True, f"OK ({domain})"
                errors.append(f"{domain}: HTTP {r.status_code}")
            except requests.exceptions.ConnectTimeout:
                errors.append(f"{domain}: timeout")
            except requests.exceptions.ConnectionError:
                errors.append(f"{domain}: conn error")
            except Exception as e:
                errors.append(f"{domain}: {type(e).__name__}")

        # Пересоздаём сессию
        self._reset_session()
        for domain in MEXC_API_DOMAINS:
            try:
                r = self.session.get(f"{domain}/api/v3/ping", timeout=30)
                if r.status_code == 200 and self._is_json_response(r):
                    self.base_url = domain
                    self._working_domain = domain
                    return True, f"OK ({domain}, reconnect)"
            except:
                continue

        detail = "; ".join(errors[:3]) if errors else "нет ответа"
        return False, f"Все домены недоступны. {detail}"


# Async (опционально)
try:
    import asyncio
    import aiohttp
    class MexcClientAsync:
        def __init__(self):
            self.base_url = config.MEXC_BASE_URL
            self._session = None
            self._req_count = 0
            self._window_start = time.time()
        async def _get_session(self):
            if self._session is None or self._session.closed:
                self._session = aiohttp.ClientSession(
                    timeout=aiohttp.ClientTimeout(total=15), headers=HEADERS)
            return self._session
        async def close(self):
            if self._session and not self._session.closed:
                await self._session.close()
        async def _request(self, endpoint, params=None):
            session = await self._get_session()
            now = time.time()
            if now - self._window_start < 1.0:
                self._req_count += 1
                if self._req_count > 8:
                    await asyncio.sleep(1.1 - (now - self._window_start))
                    self._window_start = time.time(); self._req_count = 0
            else:
                self._window_start = now; self._req_count = 1
            try:
                async with session.get(
                    f"{self.base_url}{endpoint}", params=params) as resp:
                    if resp.status == 200:
                        ct = resp.headers.get("content-type", "")
                        if "json" in ct or (await resp.text())[:2] in ("{", "["):
                            return await resp.json()
                    elif resp.status == 429:
                        await asyncio.sleep(5)
                        return await self._request(endpoint, params)
                    return None
            except: return None
        async def get_exchange_info(self):
            return await self._request("/api/v3/exchangeInfo")
        async def get_all_tickers_24h(self):
            return await self._request("/api/v3/ticker/24hr")
        async def get_order_book(self, symbol, limit=100):
            return await self._request(
                "/api/v3/depth", {"symbol": symbol, "limit": limit})
        async def get_recent_trades(self, symbol, limit=100):
            return await self._request(
                "/api/v3/trades", {"symbol": symbol, "limit": limit})
except ImportError:
    pass

"""
bingx_api.py — Cliente HTTP firmado para BingX Swap (futuros perpetuos).
Implementa firma HMAC-SHA256 requerida por BingX.
"""
import asyncio
import hashlib
import hmac
import logging
import time
import urllib.parse
from typing import Any, Optional

import aiohttp

log = logging.getLogger("bingx_api")

# ── Límite de reintentos ante rate-limit ──────────────────────
MAX_RETRIES = 3
RETRY_DELAY = 1.5  # segundos


class BingXAPIError(Exception):
    def __init__(self, code: int, msg: str):
        self.code = code
        self.msg  = msg
        super().__init__(f"BingX Error {code}: {msg}")


class BingXClient:
    def __init__(self, api_key: str, api_secret: str,
                 base_url: str = "https://open-api.bingx.com"):
        self.api_key    = api_key
        self.api_secret = api_secret
        self.base_url   = base_url
        self._session: Optional[aiohttp.ClientSession] = None

    # ── Sesión aiohttp reutilizable ────────────────────────────
    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=15)
            self._session = aiohttp.ClientSession(timeout=timeout)
        return self._session

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()

    # ── Firma HMAC-SHA256 ─────────────────────────────────────
    def _sign(self, params: dict) -> str:
        query = urllib.parse.urlencode(sorted(params.items()))
        return hmac.new(
            self.api_secret.encode(), query.encode(), hashlib.sha256
        ).hexdigest()

    def _build_params(self, extra: dict) -> dict:
        params = {"timestamp": int(time.time() * 1000), **extra}
        params["signature"] = self._sign(params)
        return params

    # ── Request genérico con reintentos ───────────────────────
    async def _request(self, method: str, path: str,
                       params: dict = None, signed: bool = True) -> dict:
        session = await self._get_session()
        url     = self.base_url + path
        headers = {"X-BX-APIKEY": self.api_key}
        p       = self._build_params(params or {}) if signed else (params or {})

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                if method == "GET":
                    async with session.get(url, params=p, headers=headers) as r:
                        data = await r.json()
                else:
                    async with session.post(url, params=p, headers=headers) as r:
                        data = await r.json()

                code = data.get("code", -1)
                if code == 0:
                    return data
                # Rate limit → esperar y reintentar
                if code == 100400 and attempt < MAX_RETRIES:
                    await asyncio.sleep(RETRY_DELAY * attempt)
                    continue
                raise BingXAPIError(code, data.get("msg", ""))
            except aiohttp.ClientError as e:
                if attempt == MAX_RETRIES:
                    raise
                log.warning("Intento %d/%d falló: %s", attempt, MAX_RETRIES, e)
                await asyncio.sleep(RETRY_DELAY)

    # ══════════════════════════════════════════════════════════
    #  MERCADO
    # ══════════════════════════════════════════════════════════

    async def get_symbols(self) -> list[str]:
        """Retorna lista de símbolos activos en futuros perpetuos."""
        data = await self._request("GET",
            "/openApi/swap/v2/quote/contracts", signed=False)
        return [c["symbol"] for c in data.get("data", [])
                if c.get("status") == 1]

    async def get_klines(self, symbol: str, interval: str,
                         limit: int = 150) -> list[dict]:
        """OHLCV candles. interval: 1m,3m,5m,15m,30m,1h,4h,1d."""
        data = await self._request("GET",
            "/openApi/swap/v3/quote/klines",
            {"symbol": symbol, "interval": interval, "limit": limit},
            signed=False)
        candles = []
        for c in data.get("data", []):
            candles.append({
                "open":   float(c[1]),
                "high":   float(c[2]),
                "low":    float(c[3]),
                "close":  float(c[4]),
                "volume": float(c[5]),
            })
        return candles

    async def get_ticker(self, symbol: str) -> dict:
        """Precio actual y volumen 24h."""
        data = await self._request("GET",
            "/openApi/swap/v2/quote/ticker",
            {"symbol": symbol}, signed=False)
        return data.get("data", {})

    # ══════════════════════════════════════════════════════════
    #  CUENTA
    # ══════════════════════════════════════════════════════════

    async def get_balance(self) -> float:
        """Balance disponible en USDT."""
        data = await self._request("GET",
            "/openApi/swap/v2/user/balance")
        assets = data.get("data", {}).get("balance", {})
        return float(assets.get("availableMargin", 0))

    async def get_total_equity(self) -> float:
        """Equity total (incluye PnL no realizado)."""
        data = await self._request("GET",
            "/openApi/swap/v2/user/balance")
        assets = data.get("data", {}).get("balance", {})
        return float(assets.get("equity", 0))

    async def get_open_positions(self) -> list[dict]:
        """Posiciones abiertas con PnL."""
        data = await self._request("GET",
            "/openApi/swap/v2/user/positions")
        return [p for p in data.get("data", [])
                if float(p.get("positionAmt", 0)) != 0]

    async def get_open_orders(self, symbol: str = "") -> list[dict]:
        """Órdenes abiertas (pendientes)."""
        params = {}
        if symbol:
            params["symbol"] = symbol
        data = await self._request("GET",
            "/openApi/swap/v2/trade/openOrders", params)
        return data.get("data", {}).get("orders", [])

    # ══════════════════════════════════════════════════════════
    #  TRADING
    # ══════════════════════════════════════════════════════════

    async def set_leverage(self, symbol: str, leverage: int,
                           side: str = "LONG") -> dict:
        """Establece apalancamiento para un par."""
        return await self._request("POST",
            "/openApi/swap/v2/trade/leverage",
            {"symbol": symbol, "leverage": leverage, "side": side})

    async def set_margin_type(self, symbol: str,
                               margin_type: str = "ISOLATED") -> dict:
        """ISOLATED o CROSSED."""
        try:
            return await self._request("POST",
                "/openApi/swap/v2/trade/marginType",
                {"symbol": symbol, "marginType": margin_type})
        except BingXAPIError as e:
            # Código 200003 = ya está en ese modo, no es error
            if e.code == 200003:
                return {}
            raise

    async def place_order(self, symbol: str, side: str, qty: float,
                          stop_loss: float = 0, take_profit: float = 0,
                          order_type: str = "MARKET",
                          position_side: str = "") -> dict:
        """
        Coloca orden de mercado con SL y TP opcionales.
        side: BUY | SELL
        """
        if not position_side:
            position_side = "LONG" if side == "BUY" else "SHORT"

        params: dict[str, Any] = {
            "symbol":       symbol,
            "side":         side,
            "positionSide": position_side,
            "type":         order_type,
            "quantity":     qty,
        }
        if stop_loss:
            params["stopLoss"] = (
                f'{{"type":"STOP_MARKET","stopPrice":{stop_loss},'
                f'"workingType":"MARK_PRICE"}}'
            )
        if take_profit:
            params["takeProfit"] = (
                f'{{"type":"TAKE_PROFIT_MARKET","stopPrice":{take_profit},'
                f'"workingType":"MARK_PRICE"}}'
            )

        return await self._request("POST",
            "/openApi/swap/v2/trade/order", params)

    async def cancel_order(self, symbol: str, order_id: str) -> dict:
        return await self._request("POST",
            "/openApi/swap/v2/trade/cancel",
            {"symbol": symbol, "orderId": order_id})

    async def cancel_all_orders(self, symbol: str) -> dict:
        return await self._request("POST",
            "/openApi/swap/v2/trade/cancelAllOpenOrders",
            {"symbol": symbol})

    async def close_position(self, symbol: str,
                              position_side: str = "LONG") -> dict:
        """Cierra posición completa a mercado."""
        positions = await self.get_open_positions()
        pos = next((p for p in positions
                    if p["symbol"] == symbol
                    and p["positionSide"] == position_side), None)
        if not pos:
            return {}
        qty = abs(float(pos["positionAmt"]))
        side = "SELL" if position_side == "LONG" else "BUY"
        return await self.place_order(symbol, side, qty,
                                      position_side=position_side)

    async def set_trailing_stop(self, symbol: str,
                                 activation_price: float,
                                 callback_rate: float,
                                 position_side: str = "LONG") -> dict:
        """Trailing stop order (callback_rate en %)."""
        side = "SELL" if position_side == "LONG" else "BUY"
        params = {
            "symbol":          symbol,
            "side":            side,
            "positionSide":    position_side,
            "type":            "TRAILING_STOP_MARKET",
            "quantity":        0,           # BingX calcula la qty de la posición
            "activationPrice": activation_price,
            "callbackRate":    callback_rate,
        }
        return await self._request("POST",
            "/openApi/swap/v2/trade/order", params)

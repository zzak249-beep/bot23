"""
bingx_api.py — Cliente HTTP firmado para BingX Swap (futuros perpetuos).

FIX: Firma HMAC-SHA256 corregida según documentación oficial BingX v3.
     Todos los parámetros van como query string (GET y POST).
     La firma cubre el query string completo sin el campo signature.
"""
import asyncio
import hashlib
import hmac as hmac_lib
import logging
import time
from typing import Any, Optional

import aiohttp

log = logging.getLogger("bingx_api")

MAX_RETRIES = 3
RETRY_DELAY = 1.5


class BingXAPIError(Exception):
    def __init__(self, code: int, msg: str):
        self.code = code
        self.msg  = msg
        super().__init__(f"BingX Error {code}: {msg}")


class BingXClient:
    def __init__(self, api_key: str, api_secret: str,
                 base_url: str = "https://open-api.bingx.com"):
        self.api_key    = api_key
        self.api_secret = api_secret.encode()
        self.base_url   = base_url
        self._session: Optional[aiohttp.ClientSession] = None

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=20))
        return self._session

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()

    def _sign(self, params: dict) -> dict:
        """
        BingX firma: ordenar claves alfabéticamente → query string →
        HMAC-SHA256 → añadir signature al dict.
        """
        p  = {**params, "timestamp": int(time.time() * 1000)}
        qs = "&".join(f"{k}={p[k]}" for k in sorted(p))
        p["signature"] = hmac_lib.new(
            self.api_secret, qs.encode(), hashlib.sha256).hexdigest()
        return p

    async def _request(self, method: str, path: str,
                       params: dict = None, signed: bool = True) -> dict:
        session = await self._get_session()
        url     = self.base_url + path
        headers = {"X-BX-APIKEY": self.api_key}
        p = self._sign(params or {}) if signed else (params or {})

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                # BingX: tanto GET como POST usan query params para la firma
                if method == "GET":
                    async with session.get(url, params=p, headers=headers) as r:
                        data = await r.json(content_type=None)
                else:
                    async with session.post(url, params=p, headers=headers) as r:
                        data = await r.json(content_type=None)

                code = data.get("code", -1)
                if code == 0:
                    return data
                if code in (100400, 80012) and attempt < MAX_RETRIES:
                    await asyncio.sleep(RETRY_DELAY * attempt)
                    continue
                raise BingXAPIError(code, data.get("msg", str(data)))

            except aiohttp.ClientError as e:
                if attempt == MAX_RETRIES:
                    raise
                await asyncio.sleep(RETRY_DELAY)

    # ── Mercado ───────────────────────────────────────────────

    async def get_symbols(self) -> list[str]:
        data = await self._request("GET",
            "/openApi/swap/v2/quote/contracts", signed=False)
        return [c["symbol"] for c in data.get("data", [])
                if c.get("status") == 1]

    async def get_klines(self, symbol: str, interval: str,
                         limit: int = 150) -> list[dict]:
        data = await self._request("GET",
            "/openApi/swap/v3/quote/klines",
            {"symbol": symbol, "interval": interval, "limit": limit},
            signed=False)
        return [{"open": float(c[1]), "high": float(c[2]),
                 "low":  float(c[3]), "close": float(c[4]),
                 "volume": float(c[5])} for c in data.get("data", [])]

    async def get_ticker(self, symbol: str) -> dict:
        data = await self._request("GET",
            "/openApi/swap/v2/quote/ticker",
            {"symbol": symbol}, signed=False)
        return data.get("data", {})

    # ── Cuenta ────────────────────────────────────────────────

    async def get_balance(self) -> float:
        data = await self._request("GET", "/openApi/swap/v2/user/balance")
        b = data.get("data", {}).get("balance", {})
        return float(b.get("availableMargin", b.get("equity", 0)))

    async def get_total_equity(self) -> float:
        data = await self._request("GET", "/openApi/swap/v2/user/balance")
        b = data.get("data", {}).get("balance", {})
        return float(b.get("equity", 0))

    async def get_open_positions(self) -> list[dict]:
        data = await self._request("GET", "/openApi/swap/v2/user/positions")
        return [p for p in data.get("data", [])
                if float(p.get("positionAmt", 0)) != 0]

    # ── Trading ───────────────────────────────────────────────

    async def set_leverage(self, symbol: str, leverage: int,
                           side: str = "LONG") -> dict:
        try:
            return await self._request("POST",
                "/openApi/swap/v2/trade/leverage",
                {"symbol": symbol, "leverage": leverage, "side": side})
        except BingXAPIError as e:
            log.warning("leverage %s: %s", symbol, e)
            return {}

    async def set_margin_type(self, symbol: str,
                               margin_type: str = "ISOLATED") -> dict:
        try:
            return await self._request("POST",
                "/openApi/swap/v2/trade/marginType",
                {"symbol": symbol, "marginType": margin_type})
        except BingXAPIError as e:
            if e.code == 200003:
                return {}
            return {}

    async def place_order(self, symbol: str, side: str, qty: float,
                          stop_loss: float = 0, take_profit: float = 0,
                          order_type: str = "MARKET",
                          position_side: str = "") -> dict:
        if not position_side:
            position_side = "LONG" if side == "BUY" else "SHORT"

        params: dict[str, Any] = {
            "symbol":       symbol,
            "side":         side,
            "positionSide": position_side,
            "type":         order_type,
            "quantity":     round(qty, 4),
        }
        if stop_loss:
            params["stopLoss"] = (
                f'{{"type":"STOP_MARKET","stopPrice":{round(stop_loss,8)},'
                f'"workingType":"MARK_PRICE","closePosition":false}}'
            )
        if take_profit:
            params["takeProfit"] = (
                f'{{"type":"TAKE_PROFIT_MARKET","stopPrice":{round(take_profit,8)},'
                f'"workingType":"MARK_PRICE","closePosition":false}}'
            )
        return await self._request("POST",
            "/openApi/swap/v2/trade/order", params)

    async def cancel_all_orders(self, symbol: str) -> dict:
        try:
            return await self._request("POST",
                "/openApi/swap/v2/trade/cancelAllOpenOrders",
                {"symbol": symbol})
        except BingXAPIError:
            return {}

    async def close_position(self, symbol: str,
                              position_side: str = "LONG") -> dict:
        positions = await self.get_open_positions()
        pos = next((p for p in positions
                    if p["symbol"] == symbol
                    and p.get("positionSide") == position_side), None)
        if not pos:
            return {}
        qty  = abs(float(pos["positionAmt"]))
        side = "SELL" if position_side == "LONG" else "BUY"
        return await self.place_order(symbol, side, qty,
                                      position_side=position_side)

    async def set_trailing_stop(self, symbol: str,
                                 activation_price: float,
                                 callback_rate: float,
                                 position_side: str = "LONG") -> dict:
        side = "SELL" if position_side == "LONG" else "BUY"
        return await self._request("POST",
            "/openApi/swap/v2/trade/order", {
                "symbol":          symbol,
                "side":            side,
                "positionSide":    position_side,
                "type":            "TRAILING_STOP_MARKET",
                "quantity":        0,
                "activationPrice": round(activation_price, 8),
                "callbackRate":    callback_rate,
            })

    async def test_auth(self) -> tuple[bool, str]:
        try:
            balance = await self.get_balance()
            return True, f"OK — Balance: {balance:.2f} USDT"
        except BingXAPIError as e:
            return False, f"Error {e.code}: {e.msg}"
        except Exception as e:
            return False, str(e)

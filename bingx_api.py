"""
bingx_api.py — Cliente BingX Swap Futuros Perpetuos.

FIX 1: get_balance() — BingX retorna el balance dentro de una lista o dict
        dependiendo del endpoint. Probamos ambos formatos.
FIX 2: get_open_positions() — filtra correctamente posiciones reales.
FIX 3: Firma HMAC corregida con sorted keys exacto.
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
                log.warning("Intento %d/%d: %s", attempt, MAX_RETRIES, e)
                await asyncio.sleep(RETRY_DELAY)

    # ══════════════════════════════════════════════════════════
    #  MERCADO — sin firma
    # ══════════════════════════════════════════════════════════

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
        d = data.get("data", {})
        # BingX puede devolver lista o dict
        if isinstance(d, list):
            d = d[0] if d else {}
        return d

    # ══════════════════════════════════════════════════════════
    #  CUENTA — FIX: parseo robusto del balance
    # ══════════════════════════════════════════════════════════

    async def get_balance(self) -> float:
        """
        BingX /swap/v2/user/balance puede retornar:
          { "data": { "balance": { "equity": "...", "availableMargin": "..." } } }
        o en algunas versiones:
          { "data": [ { "asset": "USDT", "availableMargin": "..." } ] }
        Manejamos ambos formatos.
        """
        try:
            data = await self._request("GET", "/openApi/swap/v2/user/balance")
            raw  = data.get("data", {})

            # Formato 1: dict con clave "balance"
            if isinstance(raw, dict):
                b = raw.get("balance", raw)  # algunos endpoints omiten "balance"
                if isinstance(b, dict):
                    val = b.get("availableMargin", b.get("equity",
                                b.get("available", b.get("balance", 0))))
                    return float(val)

            # Formato 2: lista de assets
            if isinstance(raw, list):
                for asset in raw:
                    if asset.get("asset", "") == "USDT":
                        val = asset.get("availableMargin",
                                        asset.get("equity",
                                        asset.get("available", 0)))
                        return float(val)

            log.warning("Formato balance desconocido: %s", raw)
            return 0.0

        except BingXAPIError as e:
            log.error("get_balance error: %s", e)
            return 0.0

    async def get_total_equity(self) -> float:
        try:
            data = await self._request("GET", "/openApi/swap/v2/user/balance")
            raw  = data.get("data", {})
            if isinstance(raw, dict):
                b = raw.get("balance", raw)
                if isinstance(b, dict):
                    return float(b.get("equity", b.get("totalMarginBalance", 0)))
            return 0.0
        except BingXAPIError:
            return 0.0

    async def get_open_positions(self) -> list[dict]:
        """
        Retorna posiciones con qty != 0.
        BingX puede devolver todas las posiciones (incluso cerradas con qty=0).
        """
        try:
            data = await self._request("GET", "/openApi/swap/v2/user/positions")
            all_pos = data.get("data", [])
            if not isinstance(all_pos, list):
                all_pos = []
            return [p for p in all_pos
                    if abs(float(p.get("positionAmt", 0))) > 0]
        except BingXAPIError as e:
            log.error("get_open_positions error: %s", e)
            return []

    # ══════════════════════════════════════════════════════════
    #  TRADING
    # ══════════════════════════════════════════════════════════

    async def set_leverage(self, symbol: str, leverage: int,
                           side: str = "LONG") -> dict:
        try:
            return await self._request("POST",
                "/openApi/swap/v2/trade/leverage",
                {"symbol": symbol, "leverage": leverage, "side": side})
        except BingXAPIError as e:
            log.warning("set_leverage %s: %s", symbol, e)
            return {}

    async def set_margin_type(self, symbol: str,
                               margin_type: str = "ISOLATED") -> dict:
        try:
            return await self._request("POST",
                "/openApi/swap/v2/trade/marginType",
                {"symbol": symbol, "marginType": margin_type})
        except BingXAPIError as e:
            if e.code == 200003:   # ya en ese modo
                return {}
            log.warning("set_margin_type %s: %s", symbol, e)
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

    # ══════════════════════════════════════════════════════════
    #  DIAGNÓSTICO — muestra la respuesta raw de balance
    # ══════════════════════════════════════════════════════════
    async def debug_balance_raw(self) -> str:
        """Para diagnóstico: retorna el JSON raw del endpoint de balance."""
        try:
            data = await self._request("GET", "/openApi/swap/v2/user/balance")
            return str(data)[:500]
        except Exception as e:
            return f"ERROR: {e}"

    async def test_auth(self) -> tuple[bool, str]:
        try:
            raw = await self.debug_balance_raw()
            log.info("Balance RAW: %s", raw)
            balance = await self.get_balance()
            return True, f"OK — Balance: {balance:.2f} USDT | Raw: {raw[:100]}"
        except BingXAPIError as e:
            return False, f"Error {e.code}: {e.msg}"
        except Exception as e:
            return False, str(e)

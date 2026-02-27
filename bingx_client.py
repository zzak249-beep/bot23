"""
BingX Exchange Client
API Docs: https://bingx-api.github.io/docs/
"""

import hmac
import hashlib
import time
import asyncio
import aiohttp
import json
import logging
from urllib.parse import urlencode

logger = logging.getLogger(__name__)

BINGX_BASE_URL = "https://open-api.bingx.com"


class BingXClient:
    def __init__(self, api_key: str, api_secret: str, testnet: bool = False):
        self.api_key = api_key
        self.api_secret = api_secret
        # BingX no tiene testnet separado, usa modo demo con balance virtual
        self.demo_mode = testnet
        self.session: aiohttp.ClientSession | None = None

    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, *args):
        if self.session:
            await self.session.close()

    def _sign(self, params: dict) -> str:
        """Genera firma HMAC-SHA256 para BingX"""
        query_string = urlencode(sorted(params.items()))
        signature = hmac.new(
            self.api_secret.encode("utf-8"),
            query_string.encode("utf-8"),
            hashlib.sha256
        ).hexdigest()
        return signature

    def _get_timestamp(self) -> int:
        return int(time.time() * 1000)

    async def _request(self, method: str, endpoint: str, params: dict = None, signed: bool = False):
        """Ejecuta una request a BingX"""
        if params is None:
            params = {}

        if signed:
            params["timestamp"] = self._get_timestamp()
            params["signature"] = self._sign(params)

        headers = {
            "X-BX-APIKEY": self.api_key,
            "Content-Type": "application/json"
        }

        url = f"{BINGX_BASE_URL}{endpoint}"

        try:
            if method == "GET":
                async with self.session.get(url, params=params, headers=headers) as resp:
                    data = await resp.json()
            elif method == "POST":
                async with self.session.post(url, json=params, headers=headers) as resp:
                    data = await resp.json()
            elif method == "DELETE":
                async with self.session.delete(url, params=params, headers=headers) as resp:
                    data = await resp.json()

            if data.get("code") != 0:
                logger.error(f"BingX API Error: {data}")
                raise Exception(f"BingX API Error {data.get('code')}: {data.get('msg')}")

            return data.get("data", data)

        except aiohttp.ClientError as e:
            logger.error(f"HTTP Error: {e}")
            raise

    # ─────────────────────────────────────────
    # MARKET DATA (sin firma requerida)
    # ─────────────────────────────────────────

    async def get_klines(self, symbol: str, interval: str, limit: int = 500) -> list:
        """
        Obtiene velas OHLCV de BingX Perpetual Futures
        Intervals: 1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 12h, 1d, 1w, 1M
        """
        params = {
            "symbol": symbol,
            "interval": interval,
            "limit": limit
        }
        data = await self._request("GET", "/openApi/swap/v2/quote/klines", params)
        # BingX retorna: [timestamp, open, high, low, close, volume, ...]
        return data

    async def get_ticker(self, symbol: str) -> dict:
        """Precio actual y 24h stats"""
        params = {"symbol": symbol}
        return await self._request("GET", "/openApi/swap/v2/quote/ticker", params)

    async def get_orderbook(self, symbol: str, limit: int = 20) -> dict:
        """Order book"""
        params = {"symbol": symbol, "limit": limit}
        return await self._request("GET", "/openApi/swap/v2/quote/depth", params)

    async def get_mark_price(self, symbol: str) -> dict:
        """Precio mark y funding rate"""
        params = {"symbol": symbol}
        return await self._request("GET", "/openApi/swap/v2/quote/premiumIndex", params)

    # ─────────────────────────────────────────
    # ACCOUNT (requiere firma)
    # ─────────────────────────────────────────

    async def get_balance(self) -> dict:
        """Balance de la cuenta de futuros perpetuos"""
        return await self._request("GET", "/openApi/swap/v2/user/balance", signed=True)

    async def get_positions(self, symbol: str = None) -> list:
        """Posiciones abiertas"""
        params = {}
        if symbol:
            params["symbol"] = symbol
        return await self._request("GET", "/openApi/swap/v2/user/positions", params, signed=True)

    async def get_open_orders(self, symbol: str) -> list:
        """Órdenes abiertas"""
        params = {"symbol": symbol}
        return await self._request("GET", "/openApi/swap/v2/trade/openOrders", params, signed=True)

    # ─────────────────────────────────────────
    # TRADING (requiere firma)
    # ─────────────────────────────────────────

    async def set_leverage(self, symbol: str, leverage: int, side: str = "LONG") -> dict:
        """
        Configura apalancamiento
        side: LONG | SHORT
        """
        params = {
            "symbol": symbol,
            "leverage": leverage,
            "side": side
        }
        return await self._request("POST", "/openApi/swap/v2/trade/leverage", params, signed=True)

    async def set_margin_type(self, symbol: str, margin_type: str = "ISOLATED") -> dict:
        """
        ISOLATED | CROSSED
        """
        params = {
            "symbol": symbol,
            "marginType": margin_type
        }
        return await self._request("POST", "/openApi/swap/v2/trade/marginType", params, signed=True)

    async def place_order(
        self,
        symbol: str,
        side: str,          # BUY | SELL
        position_side: str, # LONG | SHORT
        order_type: str,    # MARKET | LIMIT | STOP_MARKET | TAKE_PROFIT_MARKET
        quantity: float,
        price: float = None,
        stop_price: float = None,
        client_order_id: str = None
    ) -> dict:
        """
        Coloca una orden en BingX Perpetual Futures

        Para LONG:  side=BUY,  positionSide=LONG
        Para SHORT: side=SELL, positionSide=SHORT
        Para cerrar LONG:  side=SELL, positionSide=LONG
        Para cerrar SHORT: side=BUY,  positionSide=SHORT
        """
        params = {
            "symbol": symbol,
            "side": side,
            "positionSide": position_side,
            "type": order_type,
            "quantity": quantity,
        }

        if price and order_type == "LIMIT":
            params["price"] = price
        if stop_price:
            params["stopPrice"] = stop_price
        if client_order_id:
            params["clientOrderID"] = client_order_id

        logger.info(f"Placing order: {params}")
        return await self._request("POST", "/openApi/swap/v2/trade/order", params, signed=True)

    async def cancel_order(self, symbol: str, order_id: str) -> dict:
        """Cancela una orden"""
        params = {"symbol": symbol, "orderId": order_id}
        return await self._request("DELETE", "/openApi/swap/v2/trade/order", params, signed=True)

    async def cancel_all_orders(self, symbol: str) -> dict:
        """Cancela todas las órdenes abiertas"""
        params = {"symbol": symbol}
        return await self._request("DELETE", "/openApi/swap/v2/trade/allOpenOrders", params, signed=True)

    async def close_position(self, symbol: str, position_side: str, quantity: float) -> dict:
        """
        Cierra una posición completa al mercado
        position_side: LONG | SHORT
        """
        side = "SELL" if position_side == "LONG" else "BUY"
        return await self.place_order(
            symbol=symbol,
            side=side,
            position_side=position_side,
            order_type="MARKET",
            quantity=quantity
        )

    async def place_oco_order(
        self,
        symbol: str,
        position_side: str,
        quantity: float,
        tp_price: float,
        sl_price: float
    ) -> tuple[dict, dict]:
        """
        Coloca TP y SL simultáneamente (cierre de posición)
        Retorna (tp_order, sl_order)
        """
        close_side = "SELL" if position_side == "LONG" else "BUY"

        tp = await self.place_order(
            symbol=symbol,
            side=close_side,
            position_side=position_side,
            order_type="TAKE_PROFIT_MARKET",
            quantity=quantity,
            stop_price=tp_price
        )

        sl = await self.place_order(
            symbol=symbol,
            side=close_side,
            position_side=position_side,
            order_type="STOP_MARKET",
            quantity=quantity,
            stop_price=sl_price
        )

        return tp, sl

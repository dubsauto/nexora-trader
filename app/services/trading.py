# app/services/trading.py
#
# Trade execution helpers for the dashboard (web service / server 1).
# Each method receives a pre-built connection from dashboard_session —
# no connection fetching or rpc_pool here.

import asyncio
from typing import Optional, Dict


class MT5Trader:
    """Execute trades on an already-open RPC connection."""

    async def get_price(self, connection, symbol: str) -> Dict:
        try:
            price = await connection.get_symbol_price(symbol)
            if not price:
                raise Exception("No price data returned")
            return {"bid": price.get("bid"), "ask": price.get("ask")}
        except Exception as e:
            raise Exception(f"get_price failed: {str(e)}")

    async def buy(
        self,
        connection,
        symbol: str,
        volume: float,
        sl: Optional[float] = None,
        tp: Optional[float] = None,
        comment: str = "",
        magic: int = 0
    ) -> Dict:
        try:
            result = await connection.create_market_buy_order(
                symbol=symbol,
                volume=volume,
                stop_loss=sl,
                take_profit=tp,
                options={"comment": comment, "magic": magic}
            )
            return {"success": True, "result": result}
        except Exception as e:
            return {"success": False, "error": str(e)}

    async def sell(
        self,
        connection,
        symbol: str,
        volume: float,
        sl: Optional[float] = None,
        tp: Optional[float] = None,
        comment: str = "",
        magic: int = 0
    ) -> Dict:
        try:
            result = await connection.create_market_sell_order(
                symbol=symbol,
                volume=volume,
                stop_loss=sl,
                take_profit=tp,
                options={"comment": comment, "magic": magic}
            )
            return {"success": True, "result": result}
        except Exception as e:
            return {"success": False, "error": str(e)}

    async def close_position(self, connection, position_id: str) -> Dict:
        try:
            result = await connection.close_position(position_id)
            return {"success": True, "result": result}
        except Exception as e:
            return {"success": False, "error": str(e)}

    async def modify_position(
        self,
        connection,
        position_id: str,
        sl: Optional[float] = None,
        tp: Optional[float] = None
    ) -> Dict:
        try:
            result = await connection.modify_position(
                position_id, stop_loss=sl, take_profit=tp
            )
            return {"success": True, "result": result}
        except Exception as e:
            return {"success": False, "error": str(e)}


trader = MT5Trader()

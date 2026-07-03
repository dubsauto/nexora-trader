# app/services/account_management.py
#
# Administrative MetaAPI operations (create, deploy, undeploy, remove, update).
# Uses a shared SDK instance — no connection pooling needed here.

import asyncio
import time
from typing import Optional, Dict

from hedgebridge.api_client import get_metaapi_client

# Shared SDK instance for admin calls only (no RPC connections)
_api = None

def _get_admin_api():
    global _api
    if _api is None:
        _api = get_metaapi_client()
    return _api


async def _get_account(account_id: str):
    api = _get_admin_api()
    return await api.metatrader_account_api.get_account(account_id)


class MT5AccountManager:
    def __init__(self):
        self._metrics_cache: Dict[str, Dict] = {}
        self._semaphore = asyncio.Semaphore(5)

    async def add_account(
        self,
        name: str,
        server: str,
        login: str,
        password: str,
        manual_trades: bool = True,
        use_dedicated_ip: bool = True,
        magic: Optional[int] = None
    ) -> Dict:
        try:
            api = _get_admin_api()
            accounts = await api.metatrader_account_api.get_accounts_with_infinite_scroll_pagination()

            for acc in accounts:
                if str(acc.login) == str(login) and acc.type.startswith('cloud'):
                    return {"success": True, "account_id": acc.id}

            account_data = {
                'name': name,
                'type': 'cloud',
                'login': login,
                'password': password,
                'server': server,
                'platform': 'mt5',
                'manualTrades': manual_trades,
                'allocateDedicatedIp': 'ipv4' if use_dedicated_ip else None,
                'magic': 0 if manual_trades else (magic or 0)
            }

            new_account = await api.metatrader_account_api.create_account(account_data)
            return {"success": True, "account_id": new_account.id}

        except Exception as e:
            return {"success": False, "message": str(e)}

    async def remove_account(self, account_id: str) -> Dict:
        try:
            account = await _get_account(account_id)
            await account.remove()
            return {"success": True}
        except Exception as e:
            if "not found" in str(e).lower():
                return {"success": True}
            return {"success": False, "message": str(e)}

    async def update_account(self, account_id: str, update_data: Dict) -> Dict:
        try:
            account = await _get_account(account_id)
            await account.update(update_data)
            return {"success": True}
        except Exception as e:
            return {"success": False, "message": str(e)}

    async def deploy(self, account_id: str) -> Dict:
        try:
            account = await _get_account(account_id)
            if account.state != "DEPLOYED":
                await account.deploy()
            return {"success": True}
        except Exception as e:
            return {"success": False, "message": str(e)}

    async def undeploy(self, account_id: str) -> Dict:
        try:
            account = await _get_account(account_id)
            if account.state != "UNDEPLOYED":
                await account.undeploy()
            return {"success": True}
        except Exception as e:
            return {"success": False, "message": str(e)}

    async def deploy_and_wait(self, account_id: str, timeout: int = 300) -> Dict:
        """Deploy the account and block until it is DEPLOYED and CONNECTED to the
        broker. This is the real gate before building an RPC connection — after
        an undeploy/redeploy the broker can take 30-90s to reconnect, so we wait
        for wait_connected() rather than racing straight into the RPC sync."""
        try:
            account = await _get_account(account_id)
            if account.state != "DEPLOYED":
                await account.deploy()
            # Block until MetaApi reports the account deployed…
            await asyncio.wait_for(account.wait_deployed(), timeout=timeout)
            # …and the broker connection is actually up. This is what was missing.
            await asyncio.wait_for(account.wait_connected(), timeout=timeout)
            return {"success": True}
        except asyncio.TimeoutError:
            return {"success": False,
                    "message": f"broker did not connect within {timeout}s "
                               f"(account may be mid-reconnect — retry shortly)"}
        except Exception as e:
            return {"success": False, "message": str(e)}

    async def get_account_metrics(self, account_id: str, connection=None) -> Dict:
        """
        Fetch balance/equity/positions for dashboard display.
        Pass the user's already-open dashboard_session connection.
        Returns {} if no connection is provided or on error.
        """
        async with self._semaphore:
            now = time.time()
            cached = self._metrics_cache.get(account_id)
            if cached and now - cached["ts"] < 30:
                print(f"[Metrics] Cache hit → {account_id}")
                return cached["data"]

            if not connection:
                print(f"[Metrics] No connection provided → {account_id}, returning empty")
                return {}

            try:
                print(f"[Metrics] Fetching account_information + positions → {account_id}")
                start = time.perf_counter()

                info, positions = await asyncio.gather(
                    asyncio.wait_for(connection.get_account_information(), timeout=5),
                    asyncio.wait_for(connection.get_positions(), timeout=5)
                )

                latency_ms = (time.perf_counter() - start) * 1000
                print(f"[Metrics] Done → {account_id} balance={info.get('balance')} equity={info.get('equity')} positions={len(positions)} latency={round(latency_ms)}ms")

                result = {
                    "balance": info.get("balance"),
                    "equity": info.get("equity"),
                    "latency_ms": round(latency_ms, 2),
                    "positions_count": len(positions),
                    "positions": positions,
                }

                self._metrics_cache[account_id] = {"ts": now, "data": result}
                return result

            except asyncio.TimeoutError:
                print(f"[Metrics] Timeout fetching info/positions → {account_id}")
                return {}
            except Exception as e:
                print(f"[Metrics] Error → {account_id}: {e}")
                return {}


account_manager = MT5AccountManager()

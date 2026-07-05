# nexora/deploy_manager.py
#
# Reference-counts MetaApi deployments. An account is deployed once and only
# undeployed when the LAST concurrent user releases it. Both the trade engine
# and the command processor run in the worker process, so this single in-memory
# manager coordinates them — one signal finishing can no longer undeploy an
# account that another signal (or a Close command) is still using.

import asyncio

from app.services.account_management import account_manager
from hedgebridge.rpc_pool import rpc_pool
from app.database import SessionLocal
from app.model import Client, ActivityLog


def _log_account(account_id: str, action: str, message: str):
    """Write an account-level event (deployed / undeployed) to the Activity log
    at the moment it actually happens (i.e. when the reference count flips)."""
    db = SessionLocal()
    try:
        client = db.query(Client).filter(
            Client.metaapi_account_id == account_id).first()
        name = client.name if client else account_id
        db.add(ActivityLog(actor="engine", category="account", action=action,
                           message=f"{name}: {message}",
                           client_id=client.id if client else None))
        db.commit()
    except Exception:
        db.rollback()
    finally:
        db.close()


class DeployManager:
    def __init__(self):
        self._refs: dict[str, int] = {}
        self._locks: dict[str, asyncio.Lock] = {}

    def _lock(self, account_id: str) -> asyncio.Lock:
        if account_id not in self._locks:
            self._locks[account_id] = asyncio.Lock()
        return self._locks[account_id]

    async def _connect_with_retry(self, account_id, attempts=3, delay=10):
        last = None
        for i in range(attempts):
            try:
                return await rpc_pool.get_connection(account_id, force=True)
            except Exception as e:
                last = e
                print(f"[Deploy] connect {account_id} attempt {i+1}/{attempts} failed: {e}")
                if i < attempts - 1:
                    await asyncio.sleep(delay)
        raise Exception(str(last) if last else "connection failed")

    async def acquire(self, account_id: str):
        """Deploy (if this is the first user) and return a live RPC connection.
        Increments the account's reference count. Raises on failure."""
        deployed_now = False
        async with self._lock(account_id):
            if self._refs.get(account_id, 0) == 0:
                dep = await account_manager.deploy_and_wait(account_id)
                if not dep.get("success"):
                    raise Exception(dep.get("message", "deploy failed"))
                deployed_now = True
            conn = await self._connect_with_retry(account_id)
            self._refs[account_id] = self._refs.get(account_id, 0) + 1
        if deployed_now:
            _log_account(account_id, "deployed", "account deployed")
        return conn

    async def release(self, account_id: str):
        """Decrement the reference count; undeploy only when it reaches zero."""
        async with self._lock(account_id):
            n = self._refs.get(account_id, 0) - 1
            if n > 0:
                self._refs[account_id] = n
                return   # still in use by another signal/command — do not undeploy
            self._refs.pop(account_id, None)
            try:
                await rpc_pool.invalidate(account_id)
            except Exception:
                pass
            await account_manager.undeploy(account_id)
        # reached only when the last reference was released
        _log_account(account_id, "undeployed", "account undeployed")

    async def reconnect(self, account_id: str):
        """Return a FRESH RPC connection for an account that is already
        acquired (reference count unchanged). Use when a held connection has
        gone stale mid-trade. Assumes the account is still deployed (it is,
        because we hold a reference), so it rebuilds the RPC connection only."""
        return await self._connect_with_retry(account_id, attempts=2, delay=5)

    def refcount(self, account_id: str) -> int:
        return self._refs.get(account_id, 0)


deploy_manager = DeployManager()

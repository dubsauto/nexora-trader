# nexora/deploy_manager.py
#
# Reference-counts MetaApi deployments. An account is deployed once and only
# undeployed when the LAST concurrent user releases it. Both the trade engine
# and the command processor run in the worker process, so this single in-memory
# manager coordinates them — one signal finishing can no longer undeploy an
# account that another signal (or a Close command) is still using.

import asyncio
import time

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
        self._fail_until: dict[str, float] = {}   # account_id -> cooldown expiry (monotonic)
        self._cooldown_seconds = 120              # skip an account this long after a connect failure

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
                    # Repeated build timeouts usually mean the SDK's websocket
                    # layer went stale (the thing only a process restart used to
                    # fix). Before the FINAL attempt, refresh the SDK so the last
                    # try runs on a brand-new client — self-healing, no restart.
                    if i == attempts - 2:
                        try:
                            if await rpc_pool.reset_sdk_after_failures():
                                print(f"[Deploy] SDK refreshed - retrying {account_id} on fresh client")
                        except Exception as re:
                            print(f"[Deploy] SDK refresh failed (continuing): {re}")
                    await asyncio.sleep(delay)
        raise Exception(str(last) if last else "connection failed")

    async def acquire(self, account_id: str):
        """Deploy (if this is the first user) and return a live RPC connection.
        Increments the account's reference count. Raises on failure."""
        deployed_now = False
        async with self._lock(account_id):
            if self._refs.get(account_id, 0) == 0:
                # Skip fast if this account failed to connect very recently, so a
                # down broker isn't re-attempted (and re-deployed) on every signal.
                until = self._fail_until.get(account_id, 0)
                if time.monotonic() < until:
                    raise Exception(
                        f"account {account_id} in connect cooldown for "
                        f"{round(until - time.monotonic())}s after a recent failure")
                dep = await account_manager.deploy_and_wait(account_id)
                if not dep.get("success"):
                    self._fail_until[account_id] = time.monotonic() + self._cooldown_seconds
                    raise Exception(dep.get("message", "deploy failed"))
                deployed_now = True
            try:
                conn = await self._connect_with_retry(account_id)
            except Exception:
                self._fail_until[account_id] = time.monotonic() + self._cooldown_seconds
                # Connection failed. If WE deployed the account this call and no
                # one else holds a reference, undeploy it so it doesn't leak as a
                # stuck DEPLOYED account (wasting MetaApi cost).
                if deployed_now and self._refs.get(account_id, 0) == 0:
                    try:
                        await rpc_pool.invalidate(account_id)
                    except Exception:
                        pass
                    try:
                        await account_manager.undeploy(account_id)
                        print(f"[Deploy] connect failed - undeployed {account_id} (no leak)")
                    except Exception as e:
                        print(f"[Deploy] cleanup undeploy failed {account_id}: {e}")
                raise
            self._refs[account_id] = self._refs.get(account_id, 0) + 1
            self._fail_until.pop(account_id, None)   # connected fine — clear any cooldown
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

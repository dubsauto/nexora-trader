# hedgebridge/rpc_pool.py

import asyncio
import time
from typing import Optional, Dict, Any
from hedgebridge.api_client import get_metaapi_client, reset_metaapi_client


class RpcConnectionPool:
    def __init__(self):
        self._api = None

        # Per-account state — all dicts are cleaned up in _purge_account()
        # so they never accumulate ghost entries for removed accounts.
        self._accounts: Dict[str, Any] = {}
        self._connections: Dict[str, Any] = {}
        self._verified_at: Dict[str, float] = {}
        self._failure_count: Dict[str, int] = {}
        self._last_used: Dict[str, float] = {}
        self._cooldown_until: Dict[str, float] = {}

        # Per-account build locks — one Lock per account ID, created on demand.
        # Locks are intentionally NOT removed because a coroutine waiting on a
        # lock holds a reference to the exact object; replacing it races.
        # asyncio.Lock is ~56 bytes, so 100 accounts = 5.6 KB — negligible.
        self._account_locks: Dict[str, asyncio.Lock] = {}

        # Serialises SDK creation/close so concurrent get_account() failures
        # don't each spin up a new MetaApi instance and leak the old ones.
        self._sdk_reset_lock: asyncio.Lock = asyncio.Lock()

        # Cooldown tuning
        self._cooldown_seconds = 60        # after watchdog hard-reset
        self._build_fail_cooldown = 20     # after build failure — short so network outages recover fast

        self._verify_ttl = 10
        self._max_failures = 3
        self._watchdog_interval = 120
        # No idle eviction — connections are kept alive via the watchdog keepalive
        # probe below.  Eviction-on-idle caused position/metric blackouts when the
        # rebuild took up to ~53 s (broker-wait + connect + sync).  Accounts are
        # only removed on explicit invalidate() or after repeated health failures.
        self._watchdog_task: Optional[asyncio.Task] = None

        self._hard_reset_times: list = []
        self._sdk_reset_window = 300
        self._sdk_reset_threshold = 5

        # Limit simultaneous connection builds to avoid overloading 0.5 CPU
        self._build_semaphore = asyncio.Semaphore(2)

        # Background build tasks — one per account.  Routes never wait for a
        # build; they fail-fast so HTTP responses are not blocked for 45s+.
        # The task stores the finished connection when done.
        self._build_tasks: Dict[str, asyncio.Task] = {}

    # =====================================
    # HELPERS
    # =====================================
    def _get_account_lock(self, account_id: str) -> asyncio.Lock:
        if account_id not in self._account_locks:
            self._account_locks[account_id] = asyncio.Lock()
        return self._account_locks[account_id]

    def _purge_account(self, account_id: str):
        """Remove all per-account dict entries so memory is reclaimed."""
        self._accounts.pop(account_id, None)
        self._connections.pop(account_id, None)
        self._verified_at.pop(account_id, None)
        self._failure_count.pop(account_id, None)
        self._last_used.pop(account_id, None)
        self._cooldown_until.pop(account_id, None)

    # =====================================
    # WATCHDOG
    # =====================================
    def start_watchdog(self):
        if self._watchdog_task is None or self._watchdog_task.done():
            self._watchdog_task = asyncio.create_task(self._watchdog_loop())
            print("[RpcPool] Watchdog started")

    async def _watchdog_loop(self):
        while True:
            await asyncio.sleep(self._watchdog_interval)
            try:
                await self._health_check_all()
            except Exception as e:
                print(f"[RpcPool] Watchdog error: {e}")

    async def _health_check_all(self):
        account_ids = list(self._connections.keys())
        now = time.monotonic()

        for account_id in account_ids:
            # ─── KEEPALIVE HEALTH PROBE ──────────────────────────────────
            # Probe every connection regardless of how long it has been idle.
            # A successful probe also stamps _last_used so the connection is
            # treated as "recently active" for verify_ttl purposes and any
            # future tooling that inspects idle time.
            # Connections are NEVER evicted for idleness — they are only
            # removed on explicit invalidate() or after _max_failures probes.
            try:
                connection = self._connections.get(account_id)
                if not connection:
                    continue
                await asyncio.wait_for(
                    connection.get_account_information(), timeout=5
                )
                self._failure_count[account_id] = 0
                self._verified_at[account_id] = now
                self._last_used[account_id] = now   # keepalive: reset idle clock
                print(f"[RpcPool] Watchdog OK → {account_id}")

            except Exception as e:
                count = self._failure_count.get(account_id, 0) + 1
                self._failure_count[account_id] = count
                print(
                    f"[RpcPool] Watchdog fail "
                    f"[{count}/{self._max_failures}] → {account_id}: {e}"
                )
                if count >= self._max_failures:
                    print(f"[RpcPool] Max failures → hard reset {account_id}")
                    async with self._get_account_lock(account_id):
                        await self._hard_reset(account_id)

    # =====================================
    # API SINGLETON + SAFE RESET
    # =====================================
    async def _get_api(self):
        if self._api is None:
            self._api = get_metaapi_client()
        return self._api

    async def _reset_sdk_safely(self, stale_instance=None):
        """
        Replace the MetaApi SDK client with a fresh one and properly close the
        old instance so its WebSocket threads and memory are freed.

        Serialised by _sdk_reset_lock so that many coroutines that all notice
        the same stale SDK at the same time only produce ONE new instance.

        stale_instance — the api object the caller saw fail.  If self._api has
                         already been swapped by another coroutine by the time
                         we acquire the lock, we skip creating another new one.
        """
        async with self._sdk_reset_lock:
            # Another coroutine beat us to it
            if stale_instance is not None and self._api is not stale_instance:
                print("[RpcPool] SDK already reset by another coroutine — skipping")
                return

            old = self._api
            print("[RpcPool] Resetting MetaApi SDK...")

            try:
                self._api = reset_metaapi_client()
                self._hard_reset_times.clear()
                print("[RpcPool] MetaApi SDK reset complete")
            except Exception as e:
                print(f"[RpcPool] SDK reset failed: {e}")
                self._api = None
                return

            # Close the old instance AFTER the new one is live so callers
            # are never left with self._api = None for longer than a moment.
            if old is not None:
                try:
                    if hasattr(old, "close"):
                        result = old.close()
                        if asyncio.iscoroutine(result):
                            await asyncio.wait_for(result, timeout=10)
                        print("[RpcPool] Old SDK instance closed cleanly")
                except Exception as e:
                    # Non-fatal — GC will eventually collect it
                    print(f"[RpcPool] Old SDK close error (non-critical): {e}")

    async def _maybe_reset_sdk(self):
        """Trigger a full SDK reset once enough hard-resets have accumulated."""
        now = time.monotonic()
        self._hard_reset_times = [
            t for t in self._hard_reset_times if now - t < self._sdk_reset_window
        ]
        self._hard_reset_times.append(now)
        if len(self._hard_reset_times) >= self._sdk_reset_threshold:
            print(
                f"[RpcPool] {self._sdk_reset_threshold} hard resets "
                f"in {self._sdk_reset_window}s → resetting SDK"
            )
            await self._reset_sdk_safely()

    # =====================================
    # GET ACCOUNT (CACHED)
    # =====================================
    async def get_account(self, account_id: str):
        if account_id in self._accounts:
            return self._accounts[account_id]

        stale_api = None
        try:
            stale_api = await self._get_api()
            account = await stale_api.metatrader_account_api.get_account(account_id)
        except Exception as e:
            # API client may have gone stale (WebSocket dropped, token expired).
            # Reset it once — serialised so parallel failures don't each create
            # a new SDK instance — then retry before giving up.
            print(
                f"[RpcPool] get_account failed ({e}), "
                f"resetting SDK and retrying → {account_id}"
            )
            await self._reset_sdk_safely(stale_instance=stale_api)
            if self._api is None:
                raise Exception(
                    f"[RpcPool] SDK unavailable after reset → {account_id}"
                )
            try:
                account = await self._api.metatrader_account_api.get_account(account_id)
                print(f"[RpcPool] SDK reset OK — account fetched → {account_id}")
            except Exception as e2:
                print(f"[RpcPool] get_account retry failed → {account_id}: {e2}")
                raise e2

        self._accounts[account_id] = account
        return account

    # =====================================
    # GET CONNECTION
    # =====================================
    async def get_connection(self, account_id: str, force: bool = False):
        """
        Returns an already-built, verified RPC connection immediately.

        force=False (default / all background/poll callers):
            Fails fast if no connection is ready.  A background build task is
            spawned automatically so the next poll will find the connection
            ready.  Routes never block waiting for a build — that was the root
            cause of the 20-second response times and the endless cancel loop.

        force=True (explicit user action like placing a trade):
            Builds synchronously in the caller's context.  No cooldown is
            imposed on failure so the user can retry immediately.
        """
        # Fast cooldown check — avoid taking the lock on the hot rejection path
        if not force:
            cooldown_until = self._cooldown_until.get(account_id, 0)
            if time.monotonic() < cooldown_until:
                remaining = round(cooldown_until - time.monotonic(), 1)
                raise Exception(
                    f"[RpcPool] Account {account_id} in cooldown "
                    f"for {remaining}s after reset"
                )

        lock = self._get_account_lock(account_id)
        async with lock:
            # Re-check cooldown inside lock (another coroutine may have reset)
            if not force:
                cooldown_until = self._cooldown_until.get(account_id, 0)
                if time.monotonic() < cooldown_until:
                    remaining = round(cooldown_until - time.monotonic(), 1)
                    raise Exception(
                        f"[RpcPool] Account {account_id} in cooldown "
                        f"for {remaining}s after reset"
                    )

            connection = self._connections.get(account_id)
            now = time.monotonic()
            last_verified = self._verified_at.get(account_id, 0)

            # Recently verified — return immediately without a probe call
            if connection and (now - last_verified) < self._verify_ttl:
                self._last_used[account_id] = now
                return connection

            # Probe existing connection
            if connection:
                try:
                    await asyncio.wait_for(
                        connection.get_account_information(), timeout=3
                    )
                    self._verified_at[account_id] = now
                    self._last_used[account_id] = now
                    self._failure_count[account_id] = 0
                    return connection
                except BaseException:
                    count = self._failure_count.get(account_id, 0) + 1
                    self._failure_count[account_id] = count
                    print(
                        f"[RpcPool] Probe fail "
                        f"[{count}/{self._max_failures}] → {account_id}"
                    )

                    if count < self._max_failures:
                        # Probe timed out but below the failure threshold.
                        # The SDK is often slow under load (subscription manager
                        # backlog, out-of-order packets) — a single timeout does
                        # NOT mean the connection is dead.  Return it optimistically
                        # and re-probe on the next get_connection() call (_verified_at
                        # is not updated so the probe fires again immediately).
                        self._last_used[account_id] = now
                        return connection

                    # Reached failure limit → close and hard reset
                    await self._close_connection_safely(connection, account_id)
                    self._connections.pop(account_id, None)
                    self._verified_at.pop(account_id, None)
                    await self._hard_reset(account_id)
                    raise Exception(
                        f"[RpcPool] {account_id} hard reset after "
                        f"{count} consecutive probe failures, retry after cooldown"
                    )

            # ── force=True: build inline (must not fail silently) ──────────
            if force:
                try:
                    connection = await self._build_connection(account_id)
                    self._connections[account_id] = connection
                    self._verified_at[account_id] = time.monotonic()
                    self._last_used[account_id] = time.monotonic()
                    self._failure_count.pop(account_id, None)
                    self._cooldown_until.pop(account_id, None)
                    return connection
                except Exception as e:
                    raise

            # ── Non-force: spawn background build, fail fast ───────────────
            # The build takes 15-45s (broker wait + connect + wait_synchronized).
            # If the route's wait_for was allowed to cancel it mid-connect, the
            # connection gets closed, a 15s cooldown fires, and the next request
            # restarts the same loop — the connection can NEVER be established.
            # Instead we return immediately with an error and let the background
            # task finish the build.  The next poll (5-6s later) will find the
            # connection ready.
            existing = self._build_tasks.get(account_id)
            if existing and not existing.done():
                raise Exception(
                    f"[RpcPool] Account {account_id} building in background — "
                    f"connection will be ready shortly"
                )

            # Previous task finished (success or failure already handled)
            if existing:
                self._build_tasks.pop(account_id, None)

            task = asyncio.create_task(self._background_build(account_id))
            self._build_tasks[account_id] = task
            raise Exception(
                f"[RpcPool] Account {account_id} build started in background — "
                f"retry in a few seconds"
            )

    # =====================================
    # BACKGROUND BUILD
    # =====================================
    async def _background_build(self, account_id: str):
        """
        Build an RPC connection in a dedicated asyncio.Task so no HTTP route
        deadline can cancel it mid-connect.  On success the connection is
        stored so the next get_connection() call returns it immediately.
        On failure a cooldown is set just like an inline build failure.
        """
        print(f"[RpcPool] Background build starting → {account_id}")
        try:
            async with self._build_semaphore:
                connection = await self._build_connection_inner(account_id)

            async with self._get_account_lock(account_id):
                self._connections[account_id] = connection
                self._verified_at[account_id] = time.monotonic()
                self._last_used[account_id] = time.monotonic()
                self._failure_count.pop(account_id, None)
                self._cooldown_until.pop(account_id, None)

            print(f"[RpcPool] Background build complete → {account_id}")

        except asyncio.CancelledError:
            # Cancelled by invalidate() or hard_reset() — not a broker failure
            print(f"[RpcPool] Background build cancelled → {account_id}")
            raise

        except Exception as e:
            print(f"[RpcPool] Background build failed → {account_id}: {e}")
            async with self._get_account_lock(account_id):
                self._cooldown_until[account_id] = (
                    time.monotonic() + self._build_fail_cooldown
                )

        finally:
            self._build_tasks.pop(account_id, None)

    # =====================================
    # CLOSE SAFELY
    # =====================================
    async def _close_connection_safely(self, connection, account_id: str):
        try:
            await asyncio.wait_for(connection.close(), timeout=10)
            print(f"[RpcPool] Closed connection → {account_id}")
        except asyncio.TimeoutError:
            print(f"[RpcPool] Close timed out → {account_id}")
        except Exception as e:
            print(f"[RpcPool] Close error → {account_id}: {e}")

    # =====================================
    # BUILD CONNECTION
    # =====================================
    async def _build_connection(self, account_id: str):
        async with self._build_semaphore:
            return await self._build_connection_inner(account_id)

    async def _build_connection_inner(self, account_id: str):
        self._accounts.pop(account_id, None)   # force fresh fetch
        account = await self.get_account(account_id)

        if account.state != "DEPLOYED":
            raise Exception(
                f"[RpcPool] Account {account_id} is not deployed "
                f"(state: {account.state}) — deploy it first"
            )

        # Wait up to 8s for broker-connected status.  Some brokers report
        # DISCONNECTED even when RPC works, so we fall through and let
        # wait_synchronized() be the real gate.  Keeping this short prevents
        # stacked tasks from holding memory when MetaApi is unreachable.
        print(f"[RpcPool] Waiting for broker connection → {account_id}")
        for i in range(8):
            try:
                await asyncio.wait_for(account.reload(), timeout=5)
            except Exception:
                pass

            state = getattr(account, "connection_status", None)
            if state == "CONNECTED":
                break

            print(
                f"[RpcPool] Waiting broker connection "
                f"[{i+1}/8] → {account_id} (status: {state})"
            )
            await asyncio.sleep(1)
        else:
            print(
                f"[RpcPool] Broker status not CONNECTED after 8s → {account_id}, "
                f"attempting RPC anyway"
            )

        connection = account.get_rpc_connection()
        try:
            await asyncio.wait_for(connection.connect(), timeout=15)
        except asyncio.TimeoutError:
            await self._close_connection_safely(connection, account_id)
            raise Exception(
                f"[RpcPool] connection.connect() timed out → {account_id}"
            )
        except BaseException:
            # CancelledError from outer deadline — connection.connect() may have
            # started a WebSocket handshake; close it so it isn't orphaned.
            await self._close_connection_safely(connection, account_id)
            raise

        try:
            await asyncio.wait_for(connection.wait_synchronized(), timeout=30)
        except asyncio.TimeoutError:
            await self._close_connection_safely(connection, account_id)
            raise Exception(
                f"[RpcPool] wait_synchronized timed out → {account_id}. "
                f"Broker may not be reachable."
            )
        except BaseException:
            # CancelledError — connected but sync never completed; close cleanly.
            await self._close_connection_safely(connection, account_id)
            raise

        print(f"[RpcPool] Connection ready → {account_id}")
        return connection

    # =====================================
    # HARD RESET
    # Must be called while holding the account lock.
    # =====================================
    async def _hard_reset(self, account_id: str, count_toward_sdk_reset: bool = True):
        print(f"[RpcPool] Hard reset → {account_id}")

        # Stop any background build so it doesn't store a connection over the reset
        task = self._build_tasks.pop(account_id, None)
        if task and not task.done():
            task.cancel()

        connection = self._connections.get(account_id)

        # Purge all per-account state first, then set cooldown
        self._purge_account(account_id)
        self._cooldown_until[account_id] = time.monotonic() + self._cooldown_seconds

        if connection:
            await self._close_connection_safely(connection, account_id)

        if count_toward_sdk_reset:
            await self._maybe_reset_sdk()

    # =====================================
    # INVALIDATE (explicit, e.g. after undeploy/redeploy)
    # =====================================
    async def invalidate(self, account_id: str):
        lock = self._get_account_lock(account_id)
        async with lock:
            # Stop background build before purging so it can't race-store a
            # connection after we've cleared the slot
            task = self._build_tasks.pop(account_id, None)
            if task and not task.done():
                task.cancel()

            connection = self._connections.get(account_id)
            self._purge_account(account_id)   # full cleanup including cooldown
            if connection:
                await self._close_connection_safely(connection, account_id)
        print(f"[RpcPool] Invalidated → {account_id}")

    async def invalidate_all(self):
        account_ids = list(self._connections.keys())
        for account_id in account_ids:
            await self.invalidate(account_id)

    def clear_cooldowns(self):
        """
        Drop all build-fail cooldowns immediately.

        Called by listener_manager after a global outage recovery so that
        the rpc_pool can rebuild connections right away instead of waiting
        up to _build_fail_cooldown seconds after the network returns.
        Accounts with active connections are unaffected.
        """
        cleared = [
            acc_id for acc_id, until in list(self._cooldown_until.items())
            if until > time.monotonic() and acc_id not in self._connections
        ]
        for acc_id in cleared:
            self._cooldown_until.pop(acc_id, None)
        if cleared:
            print(f"[RpcPool] Cleared cooldowns for {len(cleared)} account(s) after outage recovery")


rpc_pool = RpcConnectionPool()

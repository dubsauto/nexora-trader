# nexora/deployment.py
#
# 24/7 deployment reconciler (config.ALWAYS_DEPLOYED). Keeps each managed
# MetaApi account in the right state without deploy/undeploy churn per signal:
#
#   * Approved clients whose plan is live (not expired) are kept DEPLOYED.
#   * Expired clients are UNDEPLOYED so we stop paying for lapsed accounts.
#
# An account currently in use by a running signal/command (deploy_manager
# refcount > 0) is never undeployed out from under it. deploy()/undeploy() are
# idempotent (they check the account's real state), so this is safe to run
# repeatedly and after restarts.

from nexora import config
from app.database import SessionLocal
from app.model import Client, ActivityLog
from app.services.account_management import account_manager
from nexora.deploy_manager import deploy_manager
from hedgebridge.rpc_pool import rpc_pool


async def _warm_connection(account_id: str):
    """Best-effort: make sure a live RPC connection is cached and ready for this
    (already deployed) account, so the next signal opens instantly instead of
    waiting 30-50s for a fresh build. get_connection(force=False) triggers a
    background build if none exists and returns fast; the watchdog then keeps it
    alive. Any 'building/cooldown' exception is expected and ignored."""
    try:
        await rpc_pool.get_connection(account_id, force=False)
    except Exception:
        pass


def _log(action, message, client_id=None):
    db = SessionLocal()
    try:
        db.add(ActivityLog(actor="engine", category="account", action=action,
                           message=message, client_id=client_id))
        db.commit()
    except Exception:
        db.rollback()
    finally:
        db.close()


def _set_state(client_id, state):
    db = SessionLocal()
    try:
        c = db.query(Client).get(client_id)
        if c:
            c.deploy_state = state
            db.commit()
    except Exception:
        db.rollback()
    finally:
        db.close()


def _wants_deployed(approval_status, status) -> bool:
    """Deploy while the client is an approved customer with a live plan.
    Expired (lapsed trial/license) accounts are undeployed to save cost."""
    return (approval_status or "approved") == "approved" and status != "expired"


async def reconcile_deployments() -> int:
    """One reconciliation pass. Returns how many accounts changed state."""
    if not config.ALWAYS_DEPLOYED:
        return 0

    db = SessionLocal()
    try:
        rows = [(c.id, c.name, c.metaapi_account_id, c.deploy_state,
                 _wants_deployed(c.approval_status, c.status))
                for c in db.query(Client)
                .filter(Client.metaapi_account_id.isnot(None)).all()]
    finally:
        db.close()

    changed = 0
    for cid, name, acc_id, dstate, want in rows:
        try:
            if want:
                if dstate != "deployed":
                    r = await account_manager.deploy(acc_id)
                    if r.get("success"):
                        _set_state(cid, "deployed")
                        _log("deployed", f"{name}: account deployed (24/7)", cid)
                        changed += 1
                    else:
                        print(f"[Deployment] deploy failed for {name}: {r.get('message')}")
                        continue
                # keep a warm connection ready so entries are instant
                await _warm_connection(acc_id)
            elif not want and dstate != "undeployed":
                if deploy_manager.refcount(acc_id) > 0:
                    continue   # in use by a running signal/command — leave it
                r = await account_manager.undeploy(acc_id)
                if r.get("success"):
                    _set_state(cid, "undeployed")
                    _log("undeployed", f"{name}: account undeployed (plan expired)", cid)
                    changed += 1
        except Exception as e:
            print(f"[Deployment] reconcile error for {name} ({acc_id}): {e}")
    return changed

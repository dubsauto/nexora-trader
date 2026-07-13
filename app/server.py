# app/server.py
# NEXORA AI TRADER — admin dashboard web service.

import asyncio

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from dotenv import load_dotenv

from app.init_db import run_init
from app.api.auth_routes import router as auth_router
from app.api.admin_routes import router as admin_router
from app.api.client_routes import router as client_router

load_dotenv()

app = FastAPI(title="NEXORA AI TRADER", version="1.0")

app.mount("/static", StaticFiles(directory="static"), name="static")


@app.on_event("startup")
async def startup_event():
    # Run DB init in a worker thread with a hard timeout so a slow/locked
    # database can NEVER prevent the web service from binding its port.
    # (The tables/migrations are also ensured by the worker on its own boot.)
    try:
        await asyncio.wait_for(asyncio.to_thread(run_init), timeout=25)
    except Exception as e:
        print(f"[startup] DB init skipped/deferred, continuing to serve: {e}")


# ── client portal (base root — the many) ─────────────
@app.get("/")
async def serve_client_login():
    return FileResponse("static/client-login.html")


@app.get("/signup")
async def serve_client_signup():
    return FileResponse("static/client-signup.html")


@app.get("/reset")
async def serve_client_reset():
    return FileResponse("static/client-reset.html")


@app.get("/portal")
async def serve_portal():
    return FileResponse("static/portal.html")


# ── admin dashboard (prefixed — the few) ─────────────
@app.get("/admin")
async def serve_admin_login():
    return FileResponse("static/login.html")


@app.get("/admin/app")
async def serve_admin_dashboard():
    return FileResponse("static/dashboard.html")


# ── API ──────────────────────────────────────────────
app.include_router(auth_router)
app.include_router(admin_router)
app.include_router(client_router)


@app.get("/health")
async def health_check():
    return {"status": "ok", "message": "NEXORA AI TRADER API is running"}

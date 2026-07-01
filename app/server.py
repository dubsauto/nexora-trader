# app/server.py
# NEXORA AI TRADER — admin dashboard web service.

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from dotenv import load_dotenv

from app.init_db import init_database
from app.api.auth_routes import router as auth_router
from app.api.admin_routes import router as admin_router

load_dotenv()

app = FastAPI(title="NEXORA AI TRADER", version="1.0")

app.mount("/static", StaticFiles(directory="static"), name="static")


@app.on_event("startup")
async def startup_event():
    await init_database()


# ── pages ─────────────────────────────────────────────
@app.get("/")
async def serve_login():
    return FileResponse("static/login.html")


@app.get("/app")
async def serve_dashboard():
    return FileResponse("static/dashboard.html")


# ── API ──────────────────────────────────────────────
app.include_router(auth_router)
app.include_router(admin_router)


@app.get("/health")
async def health_check():
    return {"status": "ok", "message": "NEXORA AI TRADER API is running"}

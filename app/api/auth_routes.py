# app/api/auth_routes.py
# Admin login for the NEXORA dashboard.

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app.database import get_db
from app.model import AdminUser
from app.auth import verify_password, create_access_token

router = APIRouter(prefix="", tags=["Authentication"])


@router.post("/login")
async def login(data: dict, db: Session = Depends(get_db)):
    identifier = (data.get("identifier") or data.get("username") or "").strip()
    password = data.get("password") or ""

    if not identifier or not password:
        raise HTTPException(status_code=400, detail="Missing credentials")

    user = db.query(AdminUser).filter(AdminUser.username == identifier).first()
    if not user or not verify_password(password, user.password_hash):
        raise HTTPException(status_code=401, detail="Invalid credentials")

    token = create_access_token({"user_id": user.id, "role": user.role,
                                 "username": user.username})
    return {"access_token": token, "role": user.role, "username": user.username}


@router.post("/logout")
async def logout():
    return {"message": "Logged out"}

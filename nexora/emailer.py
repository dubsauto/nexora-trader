# nexora/emailer.py
#
# Minimal SMTP email sender for client notifications (approval, password
# reset, ticket replies). Configured entirely via environment variables:
#
#   SMTP_HOST, SMTP_PORT (587), SMTP_USER, SMTP_PASS, SMTP_FROM
#   PORTAL_BASE_URL  — public URL of the web service, used in links
#
# If SMTP is not configured the send is skipped with a log line — the calling
# flow (approval, reset) still succeeds, only the email is missed.

import os
import smtplib
import asyncio
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

SMTP_HOST = os.getenv("SMTP_HOST", "")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587") or 587)
SMTP_USER = os.getenv("SMTP_USER", "")
SMTP_PASS = os.getenv("SMTP_PASS", "wcqt oxru wvjb ocbz")
SMTP_FROM = os.getenv("SMTP_FROM", SMTP_USER or "noreply@nexora.local")
PORTAL_BASE_URL = os.getenv("PORTAL_BASE_URL", "").rstrip("/")
ADMIN_EMAIL = os.getenv("ADMIN_EMAIL", "")


def configured() -> bool:
    return bool(SMTP_HOST and SMTP_USER and SMTP_PASS)


def _send_sync(to: str, subject: str, html: str) -> bool:
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = SMTP_FROM
    msg["To"] = to
    msg.attach(MIMEText(html, "html"))
    # Port 465 uses implicit SSL (SMTP_SSL); 587/25 use STARTTLS. Some networks
    # block 587 but allow 465, so we pick the right method by port.
    if SMTP_PORT == 465:
        with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, timeout=20) as s:
            s.login(SMTP_USER, SMTP_PASS)
            s.sendmail(SMTP_FROM, [to], msg.as_string())
    else:
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=20) as s:
            s.starttls()
            s.login(SMTP_USER, SMTP_PASS)
            s.sendmail(SMTP_FROM, [to], msg.as_string())
    return True


async def send_email(to: str, subject: str, html: str) -> bool:
    """Send an email in a worker thread (smtplib blocks). Never raises."""
    if not to:
        return False
    if not configured():
        print(f"[Email] SMTP not configured — skipped '{subject}' to {to}")
        return False
    try:
        await asyncio.to_thread(_send_sync, to, subject, html)
        print(f"[Email] sent '{subject}' to {to}")
        return True
    except Exception as e:
        print(f"[Email] send failed to {to}: {e}")
        return False


def _wrap(title: str, body_html: str) -> str:
    return f"""
    <div style="background:#111;padding:32px;font-family:Arial,sans-serif;">
      <div style="max-width:520px;margin:0 auto;background:#1a1a1a;border:1px solid #333;
                  border-radius:12px;overflow:hidden;">
        <div style="background:linear-gradient(135deg,#f97316,#fb923c);padding:18px 24px;">
          <h2 style="margin:0;color:#111;font-size:18px;">NEXORA AI TRADER</h2>
        </div>
        <div style="padding:24px;color:#ddd;font-size:14px;line-height:1.6;">
          <h3 style="color:#fff;margin-top:0;">{title}</h3>
          {body_html}
        </div>
        <div style="padding:14px 24px;border-top:1px solid #333;color:#777;font-size:12px;">
          NEXORA AI TRADER — automated trading, managed for you.
        </div>
      </div>
    </div>"""


async def send_approval_email(to: str, first_name: str) -> bool:
    login_url = f"{PORTAL_BASE_URL}/" if PORTAL_BASE_URL else "the client portal"
    return await send_email(
        to, "Your NEXORA account has been approved 🎉",
        _wrap("Account Approved",
              f"<p>Hi {first_name},</p>"
              f"<p>Great news — your account has been <b style='color:#f97316'>approved</b>. "
              f"Your dashboard is now unlocked.</p>"
              f"<p><a href='{login_url}' style='color:#f97316'>Log in to your dashboard</a></p>"))


async def send_reset_email(to: str, first_name: str, token: str) -> bool:
    link = f"{PORTAL_BASE_URL}/reset?token={token}" if PORTAL_BASE_URL else f"(reset token: {token})"
    return await send_email(
        to, "Reset your NEXORA password",
        _wrap("Password Reset",
              f"<p>Hi {first_name},</p>"
              f"<p>Click the link below to choose a new password. "
              f"This link expires in 1 hour.</p>"
              f"<p><a href='{link}' style='color:#f97316'>Reset my password</a></p>"
              f"<p style='color:#777'>If you didn't request this, ignore this email.</p>"))


async def send_admin_signup_email(client_name: str, client_email: str) -> bool:
    if not ADMIN_EMAIL:
        return False
    admin_url = f"{PORTAL_BASE_URL}/admin" if PORTAL_BASE_URL else "the admin dashboard"
    return await send_email(
        ADMIN_EMAIL, "New client signup — approval needed",
        _wrap("New Signup",
              f"<p><b>{client_name}</b> ({client_email}) just signed up and is "
              f"<b style='color:#f97316'>pending approval</b>.</p>"
              f"<p><a href='{admin_url}' style='color:#f97316'>Review in the admin dashboard</a></p>"))


async def send_admin_ticket_email(client_name: str, subject: str) -> bool:
    if not ADMIN_EMAIL:
        return False
    admin_url = f"{PORTAL_BASE_URL}/admin" if PORTAL_BASE_URL else "the admin dashboard"
    return await send_email(
        ADMIN_EMAIL, f"New support ticket: {subject}",
        _wrap("New Support Ticket",
              f"<p><b>{client_name}</b> opened a support ticket:</p>"
              f"<p><b>{subject}</b></p>"
              f"<p><a href='{admin_url}' style='color:#f97316'>Open the Support tab</a></p>"))


async def send_expiry_email(to: str, first_name: str, kind: str) -> bool:
    login_url = f"{PORTAL_BASE_URL}/" if PORTAL_BASE_URL else "the client portal"
    word = "trial" if kind == "trial" else "license"
    return await send_email(
        to, "Your NEXORA plan has expired",
        _wrap("Plan Expired",
              f"<p>Hi {first_name},</p>"
              f"<p>Your {word} has <b style='color:#f97316'>expired</b> and automated "
              f"trading has stopped on your account.</p>"
              f"<p>To keep trading, please contact us to renew your plan.</p>"
              f"<p><a href='{login_url}' style='color:#f97316'>Open your dashboard</a></p>"))


async def send_ticket_reply_email(to: str, first_name: str, subject: str) -> bool:
    login_url = f"{PORTAL_BASE_URL}/" if PORTAL_BASE_URL else "the client portal"
    return await send_email(
        to, f"New reply on your ticket: {subject}",
        _wrap("Support replied",
              f"<p>Hi {first_name},</p>"
              f"<p>Support has replied to your ticket <b>{subject}</b>.</p>"
              f"<p><a href='{login_url}' style='color:#f97316'>View the reply</a></p>"))

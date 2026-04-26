import asyncio
import logging
import uuid
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends, HTTPException, Request, status
from fastapi.responses import StreamingResponse
from jose import JWTError, jwt
from passlib.context import CryptContext
from pydantic import BaseModel, EmailStr
from sqlalchemy.orm import Session

from app.config import settings
from app.database import SessionLocal, get_db
from app.middleware.auth import get_current_user
from app.models.models import Profile, SecurityAuditLog, UserRoleModel

logger = logging.getLogger(__name__)

router = APIRouter()

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# ── Redis session tracker ─────────────────────────────────
# Tracks whether a user has an active session so we can detect concurrent logins.
# Key: isms:session:<user_id>  |  TTL: same as JWT expiry

_SESSION_PREFIX = "isms:session:"
_SESSION_TTL_SECONDS = settings.access_token_expire_minutes * 60

try:
    import redis as _redis_sync
    _redis = _redis_sync.from_url(settings.redis_url, decode_responses=True)
    _redis.ping()
    logger.info("[auth] Redis session tracking enabled.")
except Exception as _e:
    _redis = None
    logger.warning(f"[auth] Redis unavailable — concurrent-login detection disabled. Reason: {_e}")

# Async Redis client used only for pub/sub (SSE kick channel)
try:
    import redis.asyncio as _aioredis  # type: ignore[import]
    _aredis = _aioredis.from_url(settings.redis_url, decode_responses=True)
    logger.info("[auth] Async Redis pub/sub enabled.")
except Exception as _ae:
    _aredis = None
    logger.warning(f"[auth] Async Redis unavailable — real-time kick SSE disabled. Reason: {_ae}")

_KICK_PREFIX = "isms:kick:"

def _kick_channel(user_id: str) -> str:
    return f"{_KICK_PREFIX}{user_id}"

def _publish_kick(user_id: str) -> None:
    """Publish a kick event so the existing session's SSE stream receives it immediately."""
    if _redis is None:
        return
    try:
        _redis.publish(_kick_channel(user_id), "kicked")
    except Exception:
        pass


def _session_key(user_id: str) -> str:
    return f"{_SESSION_PREFIX}{user_id}"

def _has_active_session(user_id: str) -> bool:
    if _redis is None:
        return False
    try:
        return bool(_redis.exists(_session_key(user_id)))  # type: ignore[arg-type]
    except Exception:
        return False

def _mark_session_active(user_id: str) -> None:
    if _redis is None:
        return
    try:
        _redis.setex(_session_key(user_id), _SESSION_TTL_SECONDS, "1")
    except Exception:
        pass

def _clear_session(user_id: str) -> None:
    if _redis is None:
        return
    try:
        _redis.delete(_session_key(user_id))
    except Exception:
        pass


# ── Request / Response Models ─────────────────────────────

class RegisterRequest(BaseModel):
    email: EmailStr
    password: str
    first_name: str
    last_name: str

class LoginRequest(BaseModel):
    email: str
    password: str

class LoginResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    user_id: str
    first_name: str
    last_name: str
    roles: list[str]
    profile_setup_complete: bool
    # Backward-compatible field kept for existing clients.
    session_replaced: bool = False

# ── Helpers ───────────────────────────────────────────────

def hash_password(password: str) -> str:
    return pwd_context.hash(password)

def verify_password(plain: str, hashed: str) -> bool:
    try:
        return pwd_context.verify(plain, hashed)
    except Exception as exc:
        # Prevent malformed/legacy hashes from crashing login with HTTP 500.
        logger.warning(f"[auth] Password verification failed due to invalid hash format: {exc}")
        return False

def create_access_token(user_id: str, email: str, token_version: int) -> str:
    expire = datetime.now(timezone.utc) + timedelta(minutes=settings.access_token_expire_minutes)
    payload = {
        "sub": user_id,
        "email": email,
        "tv": token_version,
        "exp": expire,
    }
    return jwt.encode(payload, settings.jwt_secret, algorithm=settings.jwt_algorithm)

def _get_ip(request: Request) -> str:
    forwarded = request.headers.get("X-Forwarded-For")
    if forwarded:
        return forwarded.split(",")[0].strip()
    return request.client.host if request.client else "unknown"

def _log_audit(db: Session, user_id: str, event_type: str, ip: str, details: dict) -> None:
    # Best-effort logging in an isolated session so auth flows never fail
    # if audit logging infrastructure is unavailable/misaligned.
    try:
        with SessionLocal() as audit_db:
            entry = SecurityAuditLog(
                user_id=user_id,
                event_type=event_type,
                ip_address=ip,
                details=details,
            )
            audit_db.add(entry)
            audit_db.commit()
    except Exception as exc:
        logger.warning(f"[auth] Failed to write security audit log ({event_type}) for {user_id}: {exc}")

# ── Routes ────────────────────────────────────────────────

@router.post("/register", status_code=status.HTTP_201_CREATED)
def register(data: RegisterRequest, db: Session = Depends(get_db)):

    existing_email = db.query(Profile).filter(Profile.email == data.email).first()
    if existing_email:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Email is already registered."
        )

    user_id = str(uuid.uuid4())

    new_profile = Profile(
        id=user_id,
        email=data.email,
        hashed_password=hash_password(data.password),
        first_name=data.first_name,
        last_name=data.last_name,
        profile_setup_complete=False,
    )
    db.add(new_profile)

    new_role = UserRoleModel(user_id=user_id, role="player")
    db.add(new_role)
    db.commit()

    return {"message": "Registration successful.", "user_id": user_id}


@router.post("/login", response_model=LoginResponse)
def login(data: LoginRequest, request: Request, db: Session = Depends(get_db)):

    profile = db.query(Profile).filter(Profile.email == data.email).first()
    if not profile or not verify_password(data.password, str(profile.hashed_password)):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid email or password."
        )

    roles = db.query(UserRoleModel).filter(UserRoleModel.user_id == str(profile.id)).all()
    role_list = [r.role.value if hasattr(r.role, "value") else str(r.role) for r in roles]

    current_version = int(getattr(profile, "token_version", 0) or 0)
    ip = _get_ip(request)
    user_id = str(profile.id)

    # ── Concurrent-session handling ────────────────────────
    # If a session key exists in Redis, another device/tab still has this account open.
    # We kick the old session (real-time SSE push) and replace it with the new one.
    # The new login is NOT blocked — the user should never be locked out of their own account.
    if _has_active_session(user_id):
        setattr(profile, "token_version", current_version + 1)
        _publish_kick(user_id)
        _clear_session(user_id)
        _log_audit(db, user_id, "ALL_SESSIONS_TERMINATED", ip, {
            "email": str(profile.email),
            "note": "Another active session was detected; all sessions were signed out.",
        })
        db.commit()
        logger.warning(f"[auth] ALL_SESSIONS_TERMINATED for user {user_id} from {ip}")
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Another active session was detected. For security, all sessions for this account were signed out. Please sign in again.",
        )

    # ── Issue new token ────────────────────────────────────
    new_version = current_version + 1
    setattr(profile, "token_version", new_version)

    _mark_session_active(user_id)

    _log_audit(db, user_id, "LOGIN", ip, {"email": str(profile.email)})
    db.commit()

    token = create_access_token(user_id, str(profile.email), new_version)

    return LoginResponse(
        access_token=token,
        user_id=user_id,
        first_name=str(profile.first_name or ""),
        last_name=str(profile.last_name or ""),
        roles=role_list,
        profile_setup_complete=bool(profile.profile_setup_complete),
    )


@router.post("/refresh")
def refresh_token(
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """
    Issue a new JWT with a fresh expiry without bumping token_version.
    Call this proactively (e.g. when < 30 min remain) so long-running
    sessions (matches, open play) never expire mid-activity.
    """
    user_id = current_user["id"]
    profile = db.query(Profile).filter(Profile.id == user_id).first()
    if not profile:
        raise HTTPException(status_code=404, detail="User not found.")
    token = create_access_token(
        user_id, str(profile.email), int(getattr(profile, "token_version", 0) or 0)
    )
    return {"access_token": token, "token_type": "bearer"}


@router.post("/logout")
def logout(
    request: Request,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Clears the server-side session marker so the next login proceeds normally."""
    user_id = current_user["id"]
    _clear_session(user_id)
    _log_audit(db, user_id, "LOGOUT", _get_ip(request), {})
    db.commit()
    return {"message": "Logged out successfully."}


@router.get("/audit-logs")
def get_audit_logs(
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
    limit: int = 100,
):
    """Returns the security audit log for the currently authenticated user."""
    logs = (
        db.query(SecurityAuditLog)
        .filter(SecurityAuditLog.user_id == current_user["id"])
        .order_by(SecurityAuditLog.created_at.desc())
        .limit(limit)
        .all()
    )
    return [
        {
            "id": str(l.id),
            "event_type": l.event_type,
            "ip_address": l.ip_address,
            "details": l.details,
            "created_at": ts.isoformat() if (ts := getattr(l, "created_at", None)) else None,
        }
        for l in logs
    ]


@router.get("/session/stream")
async def session_event_stream(token: str, db: Session = Depends(get_db)):
    """
    SSE endpoint — streams a 'kicked' event when another device logs into this account.
    Token is passed as a query param because EventSource cannot set custom headers.
    """
    # Validate token before opening the stream
    try:
        payload = jwt.decode(token, settings.jwt_secret, algorithms=[settings.jwt_algorithm])
        user_id: str | None = payload.get("sub")
        token_version: int = int(payload.get("tv", -1))
        if not user_id:
            raise HTTPException(status_code=401, detail="Invalid token")
        profile = db.query(Profile).filter(Profile.id == user_id).first()
        if not profile or int(getattr(profile, "token_version", -1)) != token_version:
            raise HTTPException(status_code=401, detail="Session expired")
    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid token")
    finally:
        # Release DB connection before entering the long-running stream
        db.close()

    if _aredis is None:
        # Redis unavailable — return an empty stream (client will fall back to polling)
        async def _empty():
            yield ": redis-unavailable\n\n"
        return StreamingResponse(_empty(), media_type="text/event-stream")

    redis_client = _aredis  # narrowed to non-None for use inside the closure

    async def _event_generator():
        pubsub = redis_client.pubsub()
        await pubsub.subscribe(_kick_channel(user_id))
        ping_ticks = 0
        try:
            while True:
                msg = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
                if msg and msg.get("data") == "kicked":
                    yield 'data: {"event":"kicked"}\n\n'
                    return
                ping_ticks += 1
                if ping_ticks % 30 == 0:
                    yield ": ping\n\n"
        except (asyncio.CancelledError, GeneratorExit):
            pass
        finally:
            try:
                await pubsub.unsubscribe(_kick_channel(user_id))
                await pubsub.aclose()
            except Exception:
                pass

    return StreamingResponse(
        _event_generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@router.get("/notifications/stream")
async def notification_event_stream(token: str, db: Session = Depends(get_db)):
    """
    SSE endpoint — streams a 'new_notification' event whenever a notification
    is created for the authenticated user.  Token passed as query param because
    EventSource cannot set custom headers.
    """
    try:
        payload = jwt.decode(token, settings.jwt_secret, algorithms=[settings.jwt_algorithm])
        user_id: str | None = payload.get("sub")
        token_version: int = int(payload.get("tv", -1))
        if not user_id:
            raise HTTPException(status_code=401, detail="Invalid token")
        profile = db.query(Profile).filter(Profile.id == user_id).first()
        if not profile or int(getattr(profile, "token_version", -1)) != token_version:
            raise HTTPException(status_code=401, detail="Session expired")
    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid token")
    finally:
        db.close()

    if _aredis is None:
        async def _empty():
            yield ": redis-unavailable\n\n"
        return StreamingResponse(_empty(), media_type="text/event-stream")

    redis_client = _aredis
    _NOTIF_PREFIX = "isms:notif:"

    async def _event_generator():
        pubsub = redis_client.pubsub()
        await pubsub.subscribe(f"{_NOTIF_PREFIX}{user_id}")
        ping_ticks = 0
        try:
            while True:
                msg = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
                if msg and msg.get("data") == "new":
                    yield 'data: {"event":"new_notification"}\n\n'
                ping_ticks += 1
                if ping_ticks % 30 == 0:
                    yield ": ping\n\n"
        except (asyncio.CancelledError, GeneratorExit):
            pass
        finally:
            try:
                await pubsub.unsubscribe(f"{_NOTIF_PREFIX}{user_id}")
                await pubsub.aclose()
            except Exception:
                pass

    return StreamingResponse(
        _event_generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@router.post("/logout-beacon")
async def logout_beacon(request: Request, db: Session = Depends(get_db)):
    """
    Called by navigator.sendBeacon() when the browser tab is closed.
    Accepts the JWT in the request body so sendBeacon can reach it without custom headers.
    """
    try:
        body = await request.json()
        token = (body.get("token") or "").strip()
    except Exception:
        return {"ok": True}

    if not token:
        return {"ok": True}

    try:
        payload = jwt.decode(token, settings.jwt_secret, algorithms=[settings.jwt_algorithm])
        user_id: str | None = payload.get("sub")
        if user_id:
            _clear_session(user_id)
            _log_audit(db, user_id, "BROWSER_CLOSED", _get_ip(request), {})
            db.commit()
            logger.info(f"[auth] BROWSER_CLOSED for user {user_id}")
    except Exception:
        pass  # Graceful — beacon fires at page unload; we can't respond meaningfully

    return {"ok": True}


@router.post("/fcm-token")
def save_fcm_token(
    body: dict,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Store the browser's FCM device token so push notifications can be sent."""
    token = (body.get("token") or "").strip()
    if not token:
        raise HTTPException(status_code=400, detail="token is required.")
    profile = db.query(Profile).filter(Profile.id == current_user["id"]).first()
    if not profile:
        raise HTTPException(status_code=404, detail="Profile not found.")
    setattr(profile, "fcm_token", token)
    db.commit()
    return {"message": "FCM token saved."}

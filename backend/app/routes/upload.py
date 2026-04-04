import os
import uuid
import shutil
import logging
from pathlib import Path
from fastapi import APIRouter, HTTPException, Depends, UploadFile, File
from fastapi.responses import FileResponse
from sqlalchemy.orm import Session

from app.database import get_db
from app.middleware.auth import get_current_user
from app.models.models import Profile, Club, Court, ClubMember

router = APIRouter()
logger = logging.getLogger(__name__)

ALLOWED_TYPES = {"image/jpeg", "image/jpg", "image/png", "image/webp"}
MAX_SIZE = 5 * 1024 * 1024  # 5 MB

# Base upload directory — mounted as Docker volume so files survive restarts
UPLOAD_DIR = Path(os.environ.get("UPLOAD_DIR", "/app/uploads"))
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)

# Public base URL prefix served by FastAPI StaticFiles (mounted in main.py)
PUBLIC_PREFIX = "/static/uploads"


def _save(folder: str, data: bytes, ext: str) -> str:
    """Save bytes to disk, return relative path under UPLOAD_DIR."""
    dest_dir = UPLOAD_DIR / folder
    dest_dir.mkdir(parents=True, exist_ok=True)
    filename = f"{uuid.uuid4()}.{ext}"
    (dest_dir / filename).write_bytes(data)
    return f"{folder}/{filename}"


def _public_url(relative_path: str) -> str:
    return f"{PUBLIC_PREFIX}/{relative_path}"


def _validate(file: UploadFile) -> None:
    if file.content_type not in ALLOWED_TYPES:
        raise HTTPException(400, "Only jpg, jpeg, png, and webp images are allowed.")


async def _read_and_check(file: UploadFile) -> bytes:
    data = await file.read()
    if len(data) > MAX_SIZE:
        raise HTTPException(400, "File exceeds 5 MB limit.")
    return data


def _ext(content_type: str) -> str:
    return content_type.split("/")[-1].replace("jpeg", "jpg")


# ── POST /upload/avatar ───────────────────────────────────────────────────────

@router.post("/avatar")
async def upload_avatar(
    file: UploadFile = File(...),
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    _validate(file)
    data = await _read_and_check(file)

    user_id = current_user["id"]
    rel_path = _save(f"avatars/{user_id}", data, _ext(file.content_type))
    url = _public_url(rel_path)

    profile = db.query(Profile).filter(Profile.id == user_id).first()
    if profile:
        setattr(profile, "avatar_url", url)
        db.commit()

    return {"url": url}


# ── POST /upload/club/{club_id}/logo ─────────────────────────────────────────

@router.post("/club/{club_id}/logo")
async def upload_club_logo(
    club_id: str,
    file: UploadFile = File(...),
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    _validate(file)
    data = await _read_and_check(file)

    club = db.query(Club).filter(Club.id == club_id).first()
    if not club:
        raise HTTPException(404, "Club not found.")
    if str(club.admin_id) != current_user["id"]:
        member = db.query(ClubMember).filter(
            ClubMember.club_id == club_id,
            ClubMember.user_id == current_user["id"],
            ClubMember.role.in_(["admin", "owner"]),
        ).first()
        if not member:
            raise HTTPException(403, "Club admin access required.")

    rel_path = _save(f"clubs/{club_id}/logo", data, _ext(file.content_type))
    url = _public_url(rel_path)
    setattr(club, "logo_url", url)
    db.commit()

    return {"url": url}


# ── POST /upload/club/{club_id}/cover ────────────────────────────────────────

@router.post("/club/{club_id}/cover")
async def upload_club_cover(
    club_id: str,
    file: UploadFile = File(...),
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    _validate(file)
    data = await _read_and_check(file)

    club = db.query(Club).filter(Club.id == club_id).first()
    if not club:
        raise HTTPException(404, "Club not found.")
    if str(club.admin_id) != current_user["id"]:
        member = db.query(ClubMember).filter(
            ClubMember.club_id == club_id,
            ClubMember.user_id == current_user["id"],
            ClubMember.role.in_(["admin", "owner"]),
        ).first()
        if not member:
            raise HTTPException(403, "Club admin access required.")

    rel_path = _save(f"clubs/{club_id}/cover", data, _ext(file.content_type))
    url = _public_url(rel_path)
    setattr(club, "cover_url", url)
    db.commit()

    return {"url": url}


# ── POST /upload/court/{court_id}/photo ──────────────────────────────────────

@router.post("/court/{court_id}/photo")
async def upload_court_photo(
    court_id: str,
    file: UploadFile = File(...),
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    _validate(file)
    data = await _read_and_check(file)

    court = db.query(Court).filter(Court.id == court_id).first()
    if not court:
        raise HTTPException(404, "Court not found.")

    user_id = current_user["id"]
    is_creator = getattr(court, "created_by", None) and str(court.created_by) == user_id

    if not is_creator:
        if court.club_id:
            club = db.query(Club).filter(Club.id == court.club_id).first()
            if not club or str(club.admin_id) != user_id:
                member = db.query(ClubMember).filter(
                    ClubMember.club_id == str(court.club_id),
                    ClubMember.user_id == user_id,
                    ClubMember.role.in_(["admin", "owner"]),
                ).first()
                if not member:
                    raise HTTPException(403, "Not authorized to upload photos for this court.")
        else:
            raise HTTPException(403, "Not authorized to upload photos for this court.")

    rel_path = _save(f"courts/{court_id}", data, _ext(file.content_type))
    url = _public_url(rel_path)
    setattr(court, "image_url", url)
    db.commit()

    return {"url": url}

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from app.database import get_db
from app.middleware.auth import get_current_user
from app.services.insights import generate_insight, get_latest_insight

router = APIRouter()


@router.post("/generate")
def generate_my_insight(
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    result = generate_insight(current_user["id"], db)
    if "error" in result:
        raise HTTPException(400, result["error"])
    return result


@router.get("/me")
def get_my_insight(
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    result = get_latest_insight(current_user["id"], db)
    if not result:
        return {"insight": None}
    return {"insight": result}

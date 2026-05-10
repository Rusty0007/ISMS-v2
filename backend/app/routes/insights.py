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


@router.get("/auto")
def get_or_generate_insight(
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """
    Return the latest insight for the player.
    If no insight exists and the player has at least 3 completed matches,
    silently generate one now and return it — no button click required.
    """
    from app.models.models import Match, PlayerInsight
    from sqlalchemy import or_

    user_id = current_user["id"]

    existing = get_latest_insight(user_id, db)
    if existing:
        return {"insight": existing, "generated_now": False}

    # Check match count before attempting generation
    match_count = db.query(Match).filter(
        or_(
            Match.player1_id == user_id,
            Match.player2_id == user_id,
            Match.team1_player1 == user_id,
            Match.team1_player2 == user_id,
            Match.team2_player1 == user_id,
            Match.team2_player2 == user_id,
        ),
        Match.status == "completed",
    ).count()

    if match_count < 3:
        return {"insight": None, "generated_now": False, "reason": "need_more_matches", "matches_played": match_count}

    result = generate_insight(user_id, db)
    if "error" in result:
        return {"insight": None, "generated_now": False, "reason": result["error"]}

    return {"insight": result, "generated_now": True}

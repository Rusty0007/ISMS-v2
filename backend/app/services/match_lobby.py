from sqlalchemy.orm import Session

from app.models.models import Match, MatchLobbyPlayer, MatchSet


def expected_lobby_assignments(match: Match) -> list[tuple[str, int]]:
    assignments: list[tuple[str, int]] = []
    seen: set[str] = set()

    def push(raw_user_id, team_no: int) -> None:
        if raw_user_id is None:
            return
        user_id = str(raw_user_id)
        if user_id in seen:
            return
        assignments.append((user_id, team_no))
        seen.add(user_id)

    explicit_team_slots = [
        (1, getattr(match, "team1_player1", None)),
        (1, getattr(match, "team1_player2", None)),
        (2, getattr(match, "team2_player1", None)),
        (2, getattr(match, "team2_player2", None)),
    ]
    if any(player_id is not None for _, player_id in explicit_team_slots):
        for team_no, player_id in explicit_team_slots:
            push(player_id, team_no)
        return assignments

    match_format = match.match_format.value if hasattr(match.match_format, "value") else str(match.match_format)
    if match_format in ("doubles", "mixed_doubles"):
        fallback_slots = [
            (1, getattr(match, "player1_id", None)),
            (1, getattr(match, "player3_id", None)),
            (2, getattr(match, "player2_id", None)),
            (2, getattr(match, "player4_id", None)),
        ]
    else:
        fallback_slots = [
            (1, getattr(match, "player1_id", None)),
            (2, getattr(match, "player2_id", None)),
        ]

    for team_no, player_id in fallback_slots:
        push(player_id, team_no)
    return assignments


def ensure_match_lobby_rows(db: Session, match: Match) -> None:
    for user_id, team_no in expected_lobby_assignments(match):
        existing_row = db.query(MatchLobbyPlayer).filter(
            MatchLobbyPlayer.match_id == match.id,
            MatchLobbyPlayer.user_id == user_id,
        ).first()
        if existing_row:
            if existing_row.team_no != team_no:
                setattr(existing_row, "team_no", team_no)
            continue

        db.add(MatchLobbyPlayer(
            match_id=match.id,
            user_id=user_id,
            team_no=team_no,
        ))


def ensure_initial_match_set(db: Session, match: Match) -> None:
    existing_set = db.query(MatchSet.id).filter(
        MatchSet.match_id == match.id,
        MatchSet.set_number == 1,
    ).first()
    if existing_set:
        return

    db.add(MatchSet(
        match_id=match.id,
        set_number=1,
        player1_score=0,
        player2_score=0,
        team1_score=0,
        team2_score=0,
    ))

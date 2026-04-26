from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import text
from pydantic import BaseModel
from typing import Optional
import uuid

from app.database import get_db
from app.models.models import FeedPost, PostComment, PostReaction, Profile, Club
from app.middleware.auth import get_current_user

router = APIRouter()

# ── Request models ─────────────────────────────────────────────────────────────

class CreatePostRequest(BaseModel):
    post_type:     str             = "manual"
    content:       Optional[str]  = None
    image_url:     Optional[str]  = None
    club_id:       Optional[str]  = None
    tournament_id: Optional[str]  = None
    match_id:      Optional[str]  = None
    open_play_id:  Optional[str]  = None
    meta:          Optional[dict] = None

class CreateCommentRequest(BaseModel):
    content:   str
    parent_id: Optional[str] = None

class ReactRequest(BaseModel):
    reaction: str = "like"

# ── Helpers ────────────────────────────────────────────────────────────────────

def _author_dict(p: Profile) -> dict:
    return {
        "id":         str(p.id),
        "first_name": p.first_name,
        "last_name":  p.last_name,
        "avatar_url": p.avatar_url,
    }

def _serialize_post(post: FeedPost, me_id: str, db: Session) -> dict:
    author = db.query(Profile).filter(Profile.id == post.author_id).first()

    club_name = club_logo = None
    if post.club_id:
        club = db.query(Club).filter(Club.id == post.club_id).first()
        if club:
            club_name = club.name
            club_logo = club.logo_url

    rows = db.execute(
        text("SELECT reaction, COUNT(*) AS cnt FROM post_reactions WHERE post_id = :pid GROUP BY reaction"),
        {"pid": str(post.id)},
    ).fetchall()
    reaction_counts = {r.reaction: int(r.cnt) for r in rows}

    my_row = db.execute(
        text("SELECT reaction FROM post_reactions WHERE post_id = :pid AND user_id = :uid"),
        {"pid": str(post.id), "uid": me_id},
    ).fetchone()
    my_reaction = my_row.reaction if my_row else None

    comment_count = db.execute(
        text("SELECT COUNT(*) FROM post_comments WHERE post_id = :pid"),
        {"pid": str(post.id)},
    ).scalar() or 0

    preview_rows = db.execute(
        text("""
            SELECT pc.id, pc.content, pc.created_at, pc.parent_id,
                   p.first_name, p.last_name, p.avatar_url
            FROM post_comments pc
            JOIN profiles p ON p.id = pc.author_id
            WHERE pc.post_id = :pid AND pc.parent_id IS NULL
            ORDER BY pc.created_at
            LIMIT 2
        """),
        {"pid": str(post.id)},
    ).fetchall()

    preview_comments = [
        {
            "id":         str(r.id),
            "content":    r.content,
            "created_at": r.created_at.isoformat() if r.created_at else None,
            "parent_id":  str(r.parent_id) if r.parent_id else None,
            "author": {
                "first_name": r.first_name,
                "last_name":  r.last_name,
                "avatar_url": r.avatar_url,
            },
        }
        for r in preview_rows
    ]

    return {
        "id":               str(post.id),
        "post_type":        post.post_type,
        "content":          post.content,
        "image_url":        post.image_url,
        "meta":             post.meta,
        "is_pinned":        post.is_pinned,
        "created_at":       post.created_at.isoformat() if post.created_at else None,
        "author":           _author_dict(author) if author else None,
        "club_id":          str(post.club_id)       if post.club_id       else None,
        "tournament_id":    str(post.tournament_id) if post.tournament_id else None,
        "match_id":         str(post.match_id)      if post.match_id      else None,
        "open_play_id":     str(post.open_play_id)  if post.open_play_id  else None,
        "club_name":        club_name,
        "club_logo":        club_logo,
        "reaction_counts":  reaction_counts,
        "my_reaction":      my_reaction,
        "comment_count":    int(comment_count),
        "preview_comments": preview_comments,
    }

# ── Routes ─────────────────────────────────────────────────────────────────────

@router.get("")
def get_feed(
    tab:    str           = Query("all"),
    limit:  int           = Query(20, le=50),
    before: Optional[str] = Query(None),
    db:     Session       = Depends(get_db),
    current_user: dict    = Depends(get_current_user),
):
    me_id = current_user["id"]

    q = db.query(FeedPost)

    if tab == "clubs":
        club_ids_rows = db.execute(
            text("SELECT club_id FROM club_members WHERE user_id = :uid"),
            {"uid": me_id},
        ).fetchall()
        club_ids = [r.club_id for r in club_ids_rows]
        if not club_ids:
            return {"posts": [], "has_more": False}
        q = q.filter(FeedPost.club_id.in_(club_ids))

    elif tab == "tournaments":
        q = q.filter(FeedPost.tournament_id.isnot(None))

    elif tab == "matches":
        q = q.filter(FeedPost.post_type == "match_result")

    elif tab == "following":
        friend_rows = db.execute(
            text("""
                SELECT CASE WHEN requester_id = :uid::uuid THEN addressee_id
                            ELSE requester_id END AS fid
                FROM friendships
                WHERE (requester_id = :uid::uuid OR addressee_id = :uid::uuid)
                  AND status = 'accepted'
            """),
            {"uid": me_id},
        ).fetchall()
        friend_ids = [r.fid for r in friend_rows]
        if not friend_ids:
            return {"posts": [], "has_more": False}
        q = q.filter(FeedPost.author_id.in_(friend_ids))

    if before:
        try:
            from datetime import datetime
            cursor_dt = datetime.fromisoformat(before.replace("Z", "+00:00"))
            q = q.filter(FeedPost.created_at < cursor_dt)
        except ValueError:
            pass

    posts = q.order_by(FeedPost.created_at.desc()).limit(limit + 1).all()
    has_more = len(posts) > limit
    posts = posts[:limit]

    return {
        "posts":    [_serialize_post(p, me_id, db) for p in posts],
        "has_more": has_more,
    }


@router.post("")
def create_post(
    body:         CreatePostRequest,
    db:           Session = Depends(get_db),
    current_user: dict    = Depends(get_current_user),
):
    if not body.content and not body.image_url and not body.meta:
        raise HTTPException(400, "Post must have content, an image, or structured data.")

    VALID = {"manual", "match_result", "tournament_update", "open_play_invite", "announcement"}
    if body.post_type not in VALID:
        raise HTTPException(400, f"Invalid post_type.")

    me_id = current_user["id"]
    post = FeedPost(
        author_id     = uuid.UUID(me_id),
        post_type     = body.post_type,
        content       = body.content,
        image_url     = body.image_url,
        club_id       = uuid.UUID(body.club_id)       if body.club_id       else None,
        tournament_id = uuid.UUID(body.tournament_id) if body.tournament_id else None,
        match_id      = uuid.UUID(body.match_id)      if body.match_id      else None,
        open_play_id  = uuid.UUID(body.open_play_id)  if body.open_play_id  else None,
        meta          = body.meta,
    )
    db.add(post)
    db.commit()
    db.refresh(post)
    return {"post": _serialize_post(post, me_id, db)}


@router.delete("/{post_id}")
def delete_post(
    post_id:      str,
    db:           Session = Depends(get_db),
    current_user: dict    = Depends(get_current_user),
):
    post = db.query(FeedPost).filter(FeedPost.id == uuid.UUID(post_id)).first()
    if not post:
        raise HTTPException(404, "Post not found.")
    if str(post.author_id) != current_user["id"]:
        raise HTTPException(403, "Not your post.")
    db.delete(post)
    db.commit()
    return {"message": "Deleted."}


@router.post("/{post_id}/react")
def react_to_post(
    post_id:      str,
    body:         ReactRequest,
    db:           Session = Depends(get_db),
    current_user: dict    = Depends(get_current_user),
):
    VALID = {"like", "hype", "respect", "strong", "skill"}
    if body.reaction not in VALID:
        raise HTTPException(400, "Invalid reaction.")

    pid   = uuid.UUID(post_id)
    me_id = uuid.UUID(current_user["id"])

    post = db.query(FeedPost).filter(FeedPost.id == pid).first()
    if not post:
        raise HTTPException(404, "Post not found.")

    existing = db.query(PostReaction).filter(
        PostReaction.post_id == pid,
        PostReaction.user_id == me_id,
    ).first()

    if existing:
        if existing.reaction == body.reaction:
            db.delete(existing)
            db.commit()
            return {"action": "removed", "reaction": body.reaction}
        existing.reaction = body.reaction
        db.commit()
        return {"action": "changed", "reaction": body.reaction}

    db.add(PostReaction(post_id=pid, user_id=me_id, reaction=body.reaction))
    db.commit()
    return {"action": "added", "reaction": body.reaction}


@router.get("/{post_id}/reactions")
def get_post_reactions(
    post_id:      str,
    db:           Session = Depends(get_db),
    current_user: dict    = Depends(get_current_user),
):
    try:
        pid = uuid.UUID(post_id)
    except ValueError:
        raise HTTPException(400, "Invalid post id.")

    post = db.query(FeedPost).filter(FeedPost.id == pid).first()
    if not post:
        raise HTTPException(404, "Post not found.")

    rows = db.execute(
        text("""
            SELECT
                pr.id,
                pr.reaction,
                pr.created_at,
                p.id AS user_id,
                p.first_name,
                p.last_name,
                p.avatar_url
            FROM post_reactions pr
            JOIN profiles p ON p.id = pr.user_id
            WHERE pr.post_id = :pid
            ORDER BY pr.created_at DESC
            LIMIT 100
        """),
        {"pid": str(pid)},
    ).fetchall()

    counts_rows = db.execute(
        text("SELECT reaction, COUNT(*) AS cnt FROM post_reactions WHERE post_id = :pid GROUP BY reaction"),
        {"pid": str(pid)},
    ).fetchall()

    return {
        "reactions": [
            {
                "id":         str(r.id),
                "reaction":   r.reaction,
                "created_at": r.created_at.isoformat() if r.created_at else None,
                "user": {
                    "id":         str(r.user_id),
                    "first_name": r.first_name,
                    "last_name":  r.last_name,
                    "avatar_url": r.avatar_url,
                },
            }
            for r in rows
        ],
        "counts": {r.reaction: int(r.cnt) for r in counts_rows},
    }


@router.get("/{post_id}/comments")
def get_comments(
    post_id:      str,
    db:           Session = Depends(get_db),
    current_user: dict    = Depends(get_current_user),
):
    rows = db.execute(
        text("""
            SELECT pc.id, pc.content, pc.created_at, pc.parent_id,
                   p.id AS author_id, p.first_name, p.last_name, p.avatar_url
            FROM post_comments pc
            JOIN profiles p ON p.id = pc.author_id
            WHERE pc.post_id = :pid
            ORDER BY pc.created_at
        """),
        {"pid": post_id},
    ).fetchall()

    return {
        "comments": [
            {
                "id":         str(r.id),
                "content":    r.content,
                "created_at": r.created_at.isoformat() if r.created_at else None,
                "parent_id":  str(r.parent_id) if r.parent_id else None,
                "author": {
                    "id":         str(r.author_id),
                    "first_name": r.first_name,
                    "last_name":  r.last_name,
                    "avatar_url": r.avatar_url,
                },
            }
            for r in rows
        ]
    }


@router.post("/{post_id}/comments")
def add_comment(
    post_id:      str,
    body:         CreateCommentRequest,
    db:           Session = Depends(get_db),
    current_user: dict    = Depends(get_current_user),
):
    if not body.content.strip():
        raise HTTPException(400, "Comment cannot be empty.")

    pid   = uuid.UUID(post_id)
    me_id = uuid.UUID(current_user["id"])

    post = db.query(FeedPost).filter(FeedPost.id == pid).first()
    if not post:
        raise HTTPException(404, "Post not found.")

    parent_id = None
    if body.parent_id:
        parent = db.query(PostComment).filter(
            PostComment.id == uuid.UUID(body.parent_id),
            PostComment.post_id == pid,
        ).first()
        if not parent:
            raise HTTPException(404, "Parent comment not found.")
        parent_id = parent.id

    comment = PostComment(
        post_id   = pid,
        author_id = me_id,
        parent_id = parent_id,
        content   = body.content.strip(),
    )
    db.add(comment)
    db.commit()
    db.refresh(comment)

    author = db.query(Profile).filter(Profile.id == me_id).first()
    return {
        "comment": {
            "id":         str(comment.id),
            "content":    comment.content,
            "created_at": comment.created_at.isoformat() if comment.created_at else None,
            "parent_id":  str(comment.parent_id) if comment.parent_id else None,
            "author":     _author_dict(author) if author else {"id": str(me_id), "username": "", "first_name": None, "last_name": None, "avatar_url": None},
        }
    }

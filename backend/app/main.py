from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import httpx
from app.config import settings
from app.routes import admin, auth, players, matches, courts, checkins, referee, views, clubs, insights, friends, rotations, tournaments, leaderboard
from app.database import engine
from app.models import models
from sqlalchemy import text

models.Base.metadata.create_all(bind=engine)

def _run_column_migrations():
    with engine.connect() as conn:
        # matches
        conn.execute(text("ALTER TABLE matches ADD COLUMN IF NOT EXISTS round_number INTEGER"))
        conn.execute(text("ALTER TABLE matches ADD COLUMN IF NOT EXISTS bracket_position INTEGER"))
        conn.execute(text("ALTER TABLE matches ADD COLUMN IF NOT EXISTS next_match_id UUID REFERENCES matches(id)"))
        conn.execute(text("ALTER TABLE matches ADD COLUMN IF NOT EXISTS bracket_side TEXT"))
        conn.execute(text("ALTER TABLE matches ADD COLUMN IF NOT EXISTS loser_next_match_id UUID REFERENCES matches(id)"))
        # clubs
        conn.execute(text("ALTER TABLE clubs ADD COLUMN IF NOT EXISTS category TEXT"))
        conn.execute(text("ALTER TABLE clubs ADD COLUMN IF NOT EXISTS membership_type TEXT DEFAULT 'open'"))
        conn.execute(text("ALTER TABLE clubs ADD COLUMN IF NOT EXISTS address TEXT"))
        conn.execute(text("ALTER TABLE clubs ADD COLUMN IF NOT EXISTS approval_mode TEXT DEFAULT 'auto'"))
        # courts
        conn.execute(text("ALTER TABLE courts ADD COLUMN IF NOT EXISTS surface TEXT"))
        conn.execute(text("ALTER TABLE courts ADD COLUMN IF NOT EXISTS is_indoor BOOLEAN DEFAULT TRUE"))
        conn.execute(text("ALTER TABLE courts ADD COLUMN IF NOT EXISTS lighting TEXT DEFAULT 'good'"))
        conn.execute(text("ALTER TABLE courts ADD COLUMN IF NOT EXISTS capacity INTEGER"))
        conn.execute(text("ALTER TABLE courts ADD COLUMN IF NOT EXISTS notes TEXT"))
        # club_members
        conn.execute(text("ALTER TABLE club_members ADD COLUMN IF NOT EXISTS role TEXT DEFAULT 'member'"))
        conn.execute(text("ALTER TABLE club_members ADD COLUMN IF NOT EXISTS duty_date DATE"))
        # profiles
        conn.execute(text("ALTER TABLE profiles ADD COLUMN IF NOT EXISTS referee_boost_until TIMESTAMPTZ"))
        conn.execute(text("ALTER TABLE profiles ADD COLUMN IF NOT EXISTS fcm_token TEXT"))
        conn.execute(text("ALTER TABLE profiles ADD COLUMN IF NOT EXISTS token_version INTEGER NOT NULL DEFAULT 0"))
        # matches — queue location snapshot
        conn.execute(text("ALTER TABLE matches ADD COLUMN IF NOT EXISTS queue_city_code TEXT"))
        conn.execute(text("ALTER TABLE matches ADD COLUMN IF NOT EXISTS queue_province_code TEXT"))
        conn.execute(text("ALTER TABLE matches ADD COLUMN IF NOT EXISTS queue_region_code TEXT"))
        # player_ratings — calibration phase
        conn.execute(text("ALTER TABLE player_ratings ADD COLUMN IF NOT EXISTS rating_status TEXT DEFAULT 'CALIBRATING'"))
        conn.execute(text("ALTER TABLE player_ratings ADD COLUMN IF NOT EXISTS calibration_matches_played INTEGER DEFAULT 0"))
        conn.execute(text("ALTER TABLE player_ratings ADD COLUMN IF NOT EXISTS is_leaderboard_eligible BOOLEAN DEFAULT FALSE"))
        conn.execute(text("ALTER TABLE player_ratings ADD COLUMN IF NOT EXISTS calibration_completed_at TIMESTAMPTZ"))
        # Back-fill: players with 10+ matches are already effectively rated
        conn.execute(text("""
            UPDATE player_ratings
            SET rating_status              = 'RATED',
                calibration_matches_played = LEAST(matches_played, 10),
                is_leaderboard_eligible    = TRUE,
                calibration_completed_at   = updated_at
            WHERE matches_played >= 10
              AND (rating_status IS NULL OR rating_status = 'CALIBRATING')
        """))
        conn.commit()

    # PostgreSQL enum ALTER must run outside a transaction (AUTOCOMMIT)
    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        conn.execute(text("ALTER TYPE match_status ADD VALUE IF NOT EXISTS 'pending_approval'"))

_run_column_migrations()


def _run_competitive_tier_setup():
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS ranking_levels (
                id                       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                level_name               TEXT NOT NULL UNIQUE,
                min_rating               NUMERIC NOT NULL,
                max_rating               NUMERIC,
                display_order            INTEGER NOT NULL,
                is_top_level             BOOLEAN DEFAULT FALSE,
                minimum_matches_required INTEGER DEFAULT 5,
                rd_threshold             NUMERIC DEFAULT 200,
                is_active                BOOLEAN DEFAULT TRUE
            )
        """))
        # Add rd_threshold to existing tables that predate this column
        conn.execute(text("ALTER TABLE ranking_levels ADD COLUMN IF NOT EXISTS rd_threshold NUMERIC DEFAULT 200"))
        # Widen bands to 200-point gaps; add RD threshold; update match requirements.
        # ON CONFLICT DO UPDATE so existing rows are corrected on every restart.
        conn.execute(text("""
            INSERT INTO ranking_levels
                (id, level_name, min_rating, max_rating, display_order, is_top_level, minimum_matches_required, rd_threshold)
            VALUES
                (gen_random_uuid(), 'Barangay',       1500, 1699, 1, FALSE,  3, 200),
                (gen_random_uuid(), 'City/Municipal', 1700, 1899, 2, FALSE,  5, 200),
                (gen_random_uuid(), 'Provincial',     1900, 2099, 3, FALSE,  8, 200),
                (gen_random_uuid(), 'Regional',       2100, 2299, 4, FALSE, 10, 200),
                (gen_random_uuid(), 'National',       2300, NULL, 5, TRUE,  12, 200)
            ON CONFLICT (level_name) DO UPDATE SET
                min_rating               = EXCLUDED.min_rating,
                max_rating               = EXCLUDED.max_rating,
                display_order            = EXCLUDED.display_order,
                is_top_level             = EXCLUDED.is_top_level,
                minimum_matches_required = EXCLUDED.minimum_matches_required,
                rd_threshold             = EXCLUDED.rd_threshold
        """))
        conn.commit()

_run_competitive_tier_setup()


def _run_stored_procedures():
    with engine.connect() as conn:
        conn.execute(text("""
            DROP FUNCTION IF EXISTS fn_complete_match(uuid,uuid,numeric,numeric,numeric,numeric,numeric,numeric);
        """))
        conn.commit()
        conn.execute(text("""
            CREATE OR REPLACE FUNCTION fn_complete_match(
                p_match_id    UUID,
                p_winner_id   UUID,
                p_r1          NUMERIC,
                p_rd1         NUMERIC,
                p_vol1        NUMERIC,
                p_r2          NUMERIC,
                p_rd2         NUMERIC,
                p_vol2        NUMERIC
            ) RETURNS VOID LANGUAGE plpgsql AS $$
            DECLARE
                v_player1_id       UUID;
                v_player2_id       UUID;
                v_sport            TEXT;
                v_format           TEXT;
                v_court_id         UUID;
                v_referee_id       UUID;
                v_next_match_id    UUID;
                v_bracket_pos      INTEGER;
                v_p1_wins          BOOLEAN;
                v_loser_id         UUID;
                v_loser_next_id    UUID;
            BEGIN
                SELECT player1_id, player2_id, sport::TEXT, match_format::TEXT,
                       court_id, referee_id, next_match_id, bracket_position,
                       loser_next_match_id
                INTO   v_player1_id, v_player2_id, v_sport, v_format,
                       v_court_id, v_referee_id, v_next_match_id, v_bracket_pos,
                       v_loser_next_id
                FROM matches WHERE id = p_match_id;

                -- 1. Mark match completed
                UPDATE matches
                SET status = 'completed', winner_id = p_winner_id, completed_at = NOW()
                WHERE id = p_match_id;

                -- 2. Release court
                IF v_court_id IS NOT NULL THEN
                    UPDATE courts SET status = 'available' WHERE id = v_court_id;
                END IF;

                v_p1_wins := (v_player1_id = p_winner_id);

                -- 3. Update player 1 rating
                UPDATE player_ratings SET
                    rating              = p_r1,
                    rating_deviation    = p_rd1,
                    volatility          = p_vol1,
                    matches_played      = matches_played + 1,
                    wins                = wins   + CASE WHEN v_p1_wins THEN 1 ELSE 0 END,
                    losses              = losses + CASE WHEN v_p1_wins THEN 0 ELSE 1 END,
                    current_win_streak  = CASE WHEN v_p1_wins THEN current_win_streak  + 1 ELSE 0 END,
                    current_loss_streak = CASE WHEN v_p1_wins THEN 0 ELSE current_loss_streak + 1 END
                WHERE user_id = v_player1_id
                  AND sport::TEXT = v_sport
                  AND match_format::TEXT = v_format;

                -- 4. Update player 2 rating
                UPDATE player_ratings SET
                    rating              = p_r2,
                    rating_deviation    = p_rd2,
                    volatility          = p_vol2,
                    matches_played      = matches_played + 1,
                    wins                = wins   + CASE WHEN v_p1_wins THEN 0 ELSE 1 END,
                    losses              = losses + CASE WHEN v_p1_wins THEN 1 ELSE 0 END,
                    current_win_streak  = CASE WHEN v_p1_wins THEN 0 ELSE current_win_streak  + 1 END,
                    current_loss_streak = CASE WHEN v_p1_wins THEN current_loss_streak + 1 ELSE 0 END
                WHERE user_id = v_player2_id
                  AND sport::TEXT = v_sport
                  AND match_format::TEXT = v_format;

                -- 5. Grant referee priority boost (2 hours)
                IF v_referee_id IS NOT NULL THEN
                    UPDATE profiles
                    SET referee_boost_until = NOW() + INTERVAL '2 hours'
                    WHERE id = v_referee_id;
                END IF;

                -- 6. Auto-advance tournament bracket (winner)
                IF v_next_match_id IS NOT NULL AND p_winner_id IS NOT NULL THEN
                    IF COALESCE(v_bracket_pos, 1) % 2 = 1 THEN
                        UPDATE matches SET player1_id = p_winner_id WHERE id = v_next_match_id;
                    ELSE
                        UPDATE matches SET player2_id = p_winner_id WHERE id = v_next_match_id;
                    END IF;
                END IF;

                -- 7. Route loser to losers bracket (double elimination)
                IF v_loser_next_id IS NOT NULL AND p_winner_id IS NOT NULL THEN
                    v_loser_id := CASE WHEN v_player1_id = p_winner_id THEN v_player2_id ELSE v_player1_id END;
                    IF v_loser_id IS NOT NULL THEN
                        IF EXISTS (SELECT 1 FROM matches WHERE id = v_loser_next_id AND player1_id IS NULL) THEN
                            UPDATE matches SET player1_id = v_loser_id WHERE id = v_loser_next_id;
                        ELSE
                            UPDATE matches SET player2_id = v_loser_id WHERE id = v_loser_next_id;
                        END IF;
                    END IF;
                END IF;

                -- 8. Calibration: increment counter; graduate to RATED after 10 verified matches
                UPDATE player_ratings SET
                    calibration_matches_played = calibration_matches_played + 1,
                    rating_status = CASE
                        WHEN rating_status = 'CALIBRATING'
                             AND calibration_matches_played + 1 >= 10
                        THEN 'RATED'
                        ELSE rating_status
                    END,
                    is_leaderboard_eligible = CASE
                        WHEN rating_status = 'CALIBRATING'
                             AND calibration_matches_played + 1 >= 10
                        THEN TRUE
                        ELSE is_leaderboard_eligible
                    END,
                    calibration_completed_at = CASE
                        WHEN rating_status = 'CALIBRATING'
                             AND calibration_matches_played + 1 >= 10
                        THEN NOW()
                        ELSE calibration_completed_at
                    END
                WHERE user_id IN (v_player1_id, v_player2_id)
                  AND sport::TEXT   = v_sport
                  AND match_format::TEXT = v_format;
            END;
            $$
        """))
        conn.commit()

_run_stored_procedures()


# ── App ──────────────────────────────────────────────────────────────────────
app = FastAPI(
    title="Intelligent Sports Management System - Backend API",
    description="ISMS Backend API",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    redirect_slashes=False,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.allowed_origins_list,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.exception_handler(httpx.ReadError)
async def httpx_read_error_handler(request, exc):
    return JSONResponse(
        status_code=503,
        content={"detail": "Service temporarily unavailable. Please retry"}
    )


# ── Routers ──────────────────────────────────────────────────────────────────
app.include_router(auth.router,         prefix="/auth",         tags=["Authentication"])
app.include_router(players.router,      prefix="/players",      tags=["Players"])
app.include_router(matches.router,      prefix="/matches",      tags=["Matches"])
app.include_router(courts.router,       prefix="/matches",      tags=["Courts"])
app.include_router(checkins.router,     prefix="/club check-ins", tags=["Club Check-ins"])
app.include_router(referee.router,      prefix="",              tags=["Referee"])
app.include_router(views.router,        prefix="/views",        tags=["Views"])
app.include_router(clubs.router,        prefix="/clubs",        tags=["Clubs"])
app.include_router(insights.router,     prefix="/insights",     tags=["Insights"])
app.include_router(friends.router,      prefix="/friends",      tags=["Friends"])
app.include_router(rotations.router,    prefix="/rotations",    tags=["Rotation"])
app.include_router(tournaments.router,  prefix="/tournaments",  tags=["Tournaments"])
app.include_router(leaderboard.router,  prefix="/leaderboard",  tags=["Leaderboard"])
app.include_router(admin.router,        prefix="/admin",         tags=["Admin"])


# ── Health ───────────────────────────────────────────────────────────────────
@app.get("/", tags=["Health"])
def root():
    return {
        "status":  "ok",
        "message": "ISMS API is running",
        "version": "1.0.0",
        "docs":    "/docs",
    }

@app.get("/health", tags=["Health"])
def health_check():
    return {"status": "healthy"}
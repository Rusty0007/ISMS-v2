from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
import httpx
import os
from pathlib import Path
from app.config import settings
from app.routes import admin, auth, players, matches, courts, checkins, referee, views, clubs, insights, friends, rotations, tournaments, leaderboard, open_play, upload, parties
from app.routes.lobby import router as lobby_router
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
        conn.execute(text("ALTER TABLE matches ADD COLUMN IF NOT EXISTS best_of INTEGER"))
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
        # profiles — gender
        conn.execute(text("ALTER TABLE profiles ADD COLUMN IF NOT EXISTS gender TEXT"))
        # court_bookings — rental support
        conn.execute(text("ALTER TABLE court_bookings ADD COLUMN IF NOT EXISTS booking_type TEXT DEFAULT 'match'"))
        conn.execute(text("ALTER TABLE court_bookings ADD COLUMN IF NOT EXISTS duration_hours NUMERIC DEFAULT 1"))
        # courts — rental pricing
        conn.execute(text("ALTER TABLE courts ADD COLUMN IF NOT EXISTS price_per_hour NUMERIC"))
        # clubs — cover image
        conn.execute(text("ALTER TABLE clubs ADD COLUMN IF NOT EXISTS cover_url TEXT"))
        # courts — image, creator, location, standalone support
        conn.execute(text("ALTER TABLE courts ADD COLUMN IF NOT EXISTS image_url TEXT"))
        conn.execute(text("ALTER TABLE courts ADD COLUMN IF NOT EXISTS created_by UUID REFERENCES profiles(id)"))
        conn.execute(text("ALTER TABLE courts ADD COLUMN IF NOT EXISTS address TEXT"))
        conn.execute(text("ALTER TABLE courts ADD COLUMN IF NOT EXISTS region_code TEXT"))
        conn.execute(text("ALTER TABLE courts ADD COLUMN IF NOT EXISTS province_code TEXT"))
        conn.execute(text("ALTER TABLE courts ADD COLUMN IF NOT EXISTS city_mun_code TEXT"))
        conn.execute(text("ALTER TABLE courts ALTER COLUMN club_id DROP NOT NULL"))
        # clubs — operating hours
        conn.execute(text("ALTER TABLE clubs ADD COLUMN IF NOT EXISTS opening_time TEXT DEFAULT '06:00'"))
        conn.execute(text("ALTER TABLE clubs ADD COLUMN IF NOT EXISTS closing_time TEXT DEFAULT '22:00'"))
        # court_bookings — allow standalone courts (no club)
        conn.execute(text("ALTER TABLE court_bookings ALTER COLUMN club_id DROP NOT NULL"))
        # tournaments — knockout best-of setting (1 = BO1, 3 = BO3)
        conn.execute(text("ALTER TABLE tournaments ADD COLUMN IF NOT EXISTS knockout_best_of INTEGER NOT NULL DEFAULT 3"))
        # matches — party support
        conn.execute(text("ALTER TABLE matches ADD COLUMN IF NOT EXISTS party_id UUID REFERENCES parties(id)"))
        # matches — per-match score limit override
        conn.execute(text("ALTER TABLE matches ADD COLUMN IF NOT EXISTS score_limit INTEGER"))
        # match lobby — pre-match readiness checkpoint
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS match_lobby_players (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                match_id UUID NOT NULL REFERENCES matches(id) ON DELETE CASCADE,
                user_id UUID NOT NULL REFERENCES profiles(id) ON DELETE CASCADE,
                team_no INTEGER NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                entered_at TIMESTAMPTZ,
                notified_at TIMESTAMPTZ DEFAULT NOW()
            )
        """))
        # De-duplicate any historical lobby rows per (match_id, user_id),
        # preferring 'entered' rows, then newest timestamp.
        conn.execute(text("""
            WITH ranked AS (
                SELECT
                    id,
                    ROW_NUMBER() OVER (
                        PARTITION BY match_id, user_id
                        ORDER BY
                            CASE WHEN status = 'entered' THEN 1 ELSE 0 END DESC,
                            COALESCE(entered_at, notified_at, NOW()) DESC,
                            id DESC
                    ) AS rn
                FROM match_lobby_players
            )
            DELETE FROM match_lobby_players m
            USING ranked r
            WHERE m.id = r.id
              AND r.rn > 1
        """))
        conn.execute(text("""
            CREATE UNIQUE INDEX IF NOT EXISTS ux_match_lobby_players_match_user
            ON match_lobby_players (match_id, user_id)
        """))
        conn.commit()

    # PostgreSQL enum ALTER must run outside a transaction (AUTOCOMMIT)
    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        conn.execute(text("ALTER TYPE match_status ADD VALUE IF NOT EXISTS 'pending_approval'"))
        conn.execute(text("ALTER TYPE match_status ADD VALUE IF NOT EXISTS 'invalidated'"))
        conn.execute(text("ALTER TYPE match_status ADD VALUE IF NOT EXISTS 'awaiting_players'"))
        conn.execute(text("ALTER TYPE match_type   ADD VALUE IF NOT EXISTS 'ranked'"))

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
        # parties — shared queue start timestamp so both players can sync their timer
        conn.execute(text("ALTER TABLE parties ADD COLUMN IF NOT EXISTS queue_started_at TIMESTAMPTZ"))
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
                v_team1_player1    UUID;
                v_team2_player1    UUID;
                v_player3_id       UUID;
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
                SELECT player1_id, player2_id, team1_player1, team2_player1, player3_id,
                       sport::TEXT, match_format::TEXT,
                       court_id, referee_id, next_match_id, bracket_position,
                       loser_next_match_id
                INTO   v_player1_id, v_player2_id, v_team1_player1, v_team2_player1, v_player3_id,
                       v_sport, v_format,
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

                -- Doubles anchors:
                -- team1 captain vs team2 captain, regardless of raw player slot layout.
                IF v_format IN ('doubles', 'mixed_doubles') THEN
                    v_player1_id := COALESCE(v_team1_player1, v_player1_id);
                    v_player2_id := COALESCE(v_team2_player1, v_player3_id, v_player2_id);
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
                    current_loss_streak = CASE WHEN v_p1_wins THEN 0 ELSE current_loss_streak + 1 END,
                    updated_at          = NOW()
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
                    current_loss_streak = CASE WHEN v_p1_wins THEN current_loss_streak + 1 ELSE 0 END,
                    updated_at          = NOW()
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


def _run_view_setup():
    with engine.connect() as conn:
        # Recreate views from scratch so schema evolutions that reorder/add
        # columns don't crash startup with PostgreSQL "cannot change name of
        # view column" errors on CREATE OR REPLACE VIEW.
        conn.execute(text("DROP VIEW IF EXISTS leaderboard_view"))
        conn.execute(text("DROP VIEW IF EXISTS player_profile_summary"))
        conn.execute(text("DROP VIEW IF EXISTS match_history_view"))
        conn.execute(text("DROP VIEW IF EXISTS active_queue_view"))
        conn.execute(text("DROP VIEW IF EXISTS tournament_standings_view"))
        conn.execute(text("""
            CREATE OR REPLACE VIEW leaderboard_view AS
            SELECT
                pr.user_id,
                p.username,
                p.first_name,
                p.last_name,
                p.avatar_url,
                p.region_code,
                p.province_code,
                p.city_mun_code,
                p.barangay_code,
                pr.sport,
                pr.match_format,
                pr.rating,
                pr.rating_deviation,
                pr.volatility,
                pr.wins,
                pr.losses,
                pr.matches_played,
                pr.current_win_streak,
                pr.current_loss_streak,
                pr.activeness_score,
                pr.is_leaderboard_eligible,
                pr.rating_status,
                CASE WHEN pr.matches_played > 0
                     THEN ROUND((pr.wins::NUMERIC / pr.matches_played) * 100, 1)
                     ELSE 0
                END AS win_rate_pct,
                COALESCE(rl.level_name, 'Unranked') AS skill_tier
            FROM player_ratings pr
            JOIN profiles p ON p.id = pr.user_id
            LEFT JOIN ranking_levels rl
                ON pr.rating >= rl.min_rating
                AND (pr.rating < rl.max_rating OR rl.max_rating IS NULL)
                AND rl.is_active = TRUE
        """))
        conn.execute(text("""
            CREATE OR REPLACE VIEW player_profile_summary AS
            SELECT
                p.id AS user_id,
                p.username,
                p.first_name,
                p.last_name,
                p.avatar_url,
                p.region_code,
                p.province_code,
                p.city_mun_code,
                p.barangay_code,
                p.profile_setup_complete,
                p.created_at AS member_since,
                ARRAY(SELECT role::TEXT FROM user_roles WHERE user_id = p.id) AS roles,
                pr.sport,
                pr.match_format,
                pr.rating,
                pr.rating_deviation,
                pr.wins,
                pr.losses,
                pr.matches_played,
                pr.current_win_streak,
                pr.activeness_score,
                CASE WHEN pr.matches_played > 0
                     THEN ROUND((pr.wins::NUMERIC / pr.matches_played) * 100, 1)
                     ELSE 0
                END AS win_rate_pct,
                COALESCE(rl.level_name, 'Unranked') AS skill_tier
            FROM profiles p
            LEFT JOIN player_ratings pr ON pr.user_id = p.id
            LEFT JOIN ranking_levels rl
                ON pr.rating >= rl.min_rating
                AND (pr.rating < rl.max_rating OR rl.max_rating IS NULL)
                AND rl.is_active = TRUE
        """))
        conn.execute(text("""
            CREATE OR REPLACE VIEW match_history_view AS
            SELECT
                m.*,
                p1.username AS player1_username,
                p1.avatar_url AS player1_avatar,
                p2.username AS player2_username,
                p2.avatar_url AS player2_avatar
            FROM matches m
            LEFT JOIN profiles p1 ON p1.id = m.player1_id
            LEFT JOIN profiles p2 ON p2.id = m.player2_id
        """))
        conn.execute(text("""
            CREATE OR REPLACE VIEW active_queue_view AS
            SELECT
                mq.*,
                p.username,
                p.avatar_url,
                p.region_code   AS profile_region_code,
                p.province_code AS profile_province_code,
                p.city_mun_code AS profile_city_mun_code
            FROM matchmaking_queue mq
            JOIN profiles p ON p.id = mq.user_id
            WHERE mq.status = 'waiting'
        """))
        conn.execute(text("""
            CREATE OR REPLACE VIEW tournament_standings_view AS
            SELECT
                tgs.*,
                tg.group_name,
                tr.player_id,
                tr.partner_id,
                p.username AS player_username,
                p.avatar_url AS player_avatar
            FROM tournament_group_standings tgs
            JOIN tournament_groups tg ON tg.id = tgs.group_id
            JOIN tournaments t ON t.id = tgs.tournament_id
            JOIN tournament_registrations tr ON tr.id = tgs.entry_id
            JOIN profiles p ON p.id = tr.player_id
        """))
        conn.commit()

_run_view_setup()


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
app.include_router(open_play.router,    prefix="",               tags=["Open Play"])
app.include_router(parties.router,      prefix="",               tags=["Parties"])
app.include_router(upload.router,       prefix="/upload",        tags=["Upload"])
app.include_router(lobby_router,        prefix="",               tags=["Lobby"])

# Serve uploaded files — must be mounted AFTER routers
_upload_dir = Path(os.environ.get("UPLOAD_DIR", "/app/uploads"))
_upload_dir.mkdir(parents=True, exist_ok=True)
app.mount("/static/uploads", StaticFiles(directory=str(_upload_dir)), name="uploads")


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

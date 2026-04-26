# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What is ISMS

Intelligent Sports Management System — a full-stack platform for racket sports (pickleball, badminton, lawn tennis, table tennis) covering matchmaking, clubs, courts, open play sessions, tournaments, and a social feed. Targeting Philippine players (location hierarchy: region → province → city/municipality → barangay).

## Dev Commands

### Start everything
```bash
docker compose up -d
```

### Rebuild a single service after code changes
```powershell
# PowerShell (Windows)
docker compose build --no-cache frontend; docker compose up -d frontend
docker compose build --no-cache backend; docker compose up -d backend
```

### Frontend dev server (outside Docker)
```bash
cd frontend && npm run dev      # http://localhost:3000
npm run build
npm run lint
```

### View logs
```bash
docker compose logs backend --since 5m
docker compose logs frontend --since 5m
```

### API docs
- Swagger: http://localhost:8000/docs
- Redoc: http://localhost:8000/redoc

## Architecture Overview

### Request Flow
Browser → Next.js (port 3000) → `/api/*` rewritten to FastAPI (port 8000) via `next.config.mjs` rewrites. No Next.js API routes exist — all backend logic is in FastAPI.

### Backend (`backend/`)
- **Framework**: FastAPI with synchronous SQLAlchemy ORM (`get_db` dependency injection)
- **Auth**: JWT Bearer tokens. `get_current_user` in `middleware/auth.py` validates the token AND checks `token_version` on the profile to invalidate old sessions on logout/device swap.
- **DB migrations**: No Alembic. Schema is evolved via `ADD COLUMN IF NOT EXISTS` raw SQL in `main.py` (`_run_column_migrations`), which runs on every startup. Add new columns there — never remove columns.
- **Stored procedures**: `fn_complete_match()` in PostgreSQL handles match completion atomically: updates ratings (Glicko-2), releases courts, advances tournament brackets, grants referee boosts, and updates calibration status.
- **Real-time**: Redis pub/sub. `broadcast.py` publishes events; routes use SSE (`EventSource`) to stream to clients. Match events go to channel `match:{id}`, tournament events to `isms:tournament:{id}`.
- **AI Insights**: `services/insights.py` calls OpenRouter (preferred) or Anthropic Claude directly. Configured via `OPENROUTER_API_KEY` / `ANTHROPIC_API_KEY` in `backend/.env`.
- **Push notifications**: Firebase FCM via `services/notifications.py`.
- **Matchmaking**: ML-assisted in `services/matchmaking.py`. Supports `quick`, `ranked`, and `club` queue modes with geo-proximity filters that relax over time.

### Router → URL prefix mapping (from `main.py`)
| File | Prefix |
|---|---|
| `auth.py` | `/auth` |
| `players.py` | `/players` |
| `matches.py` | `/matches` |
| `clubs.py` | `/clubs` |
| `courts.py` | `/matches` (court sub-resources) |
| `open_play.py` | `` (root, e.g. `/open-play/...`) |
| `parties.py` | `` (root) |
| `lobby.py` | `` (root) |
| `tournaments.py` | `/tournaments` |
| `leaderboard.py` | `/leaderboard` |
| `rotations.py` | `/rotations` |
| `feed.py` | `/feed` |
| `insights.py` | `/insights` |

### Frontend (`frontend/`)
- **Framework**: Next.js 14 App Router (`frontend/app/`)
- **Styling**: Tailwind CSS with a dark design system. Design tokens: `bg-[#071018]` base, cards `bg-[#0d1722]/90 border border-white/8 rounded-lg shadow-[0_18px_40px_rgba(0,0,0,0.22)]`, section labels `text-[11px] font-semibold uppercase tracking-[0.24em] text-cyan-100/70`.
- **Auth**: Access token stored in `localStorage` via `lib/auth.ts` (`getAccessToken`, `clearAuthSession`). All fetch calls add `Authorization: Bearer {token}`. `isUnauthorized(status)` checks for 401.
- **Real-time**: `EventSource` for SSE streams, `setInterval` polling for occupancy/queue status.
- **Reusable components**: `NavBar.tsx` (with notification bell, mobile nav, SSE for notifications), `QueueBanner.tsx`, `ImageUpload.tsx`.

### Key Domain Concepts
- **Rating system**: Glicko-2. Players start in `CALIBRATING` status (20 matches required to become `RATED` and leaderboard eligible). Rating tiers: Barangay (1500–1699) → City (1700–1899) → Provincial (1900–2099) → Regional (2100–2299) → National (2300+).
- **Open Play**: Club-hosted sessions with a live queue. Courts are tracked as `open_play_session_courts`. Players join a `open_play_queue_entries` queue; the system calls players via `open_play_assignments` with an ACK timeout.
- **Parties**: Two players queue together as a doubles team. Party state machine: `forming` → `ready` → `in_queue`.
- **Match lobby**: Pre-match readiness checkpoint (`match_lobby_players`) where all players and referee must check in before the match starts.
- **Court status**: `available` | `occupied`. Completing a match via `fn_complete_match` automatically sets the court back to `available`.

### Database
- PostgreSQL 15 with PostGIS extension (for future geo queries)
- No ORM migrations — only `ADD COLUMN IF NOT EXISTS` in `main.py`
- Key views: `leaderboard_view`, `player_profile_summary`, `match_history_view`, `active_queue_view`, `tournament_standings_view`
- pgAdmin available at http://localhost:5050 (admin@isms.com / admin)

## Key Constraints

- **Never remove DB columns** — only add with `IF NOT EXISTS` in `main.py`'s `_run_column_migrations`.
- **`Court.sport` is a plain `String`** column (not an enum) — don't call `.value` on it. `Match.sport` IS a `SAEnum`.
- **Frontend rewrites**: `/api/*` → `http://backend:8000/*`. There are no Next.js API route handlers.
- **NavBar `hideLogo` prop**: When `hideLogo=true`, the back button renders on the left. Don't also pass `backHref`/`backLabel` to the right-side nav link or it duplicates. The `!hideLogo` guard in NavBar handles this.
- **Docker on Windows**: Use `;` not `&&` to chain commands in PowerShell.

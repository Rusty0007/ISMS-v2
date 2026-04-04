import uuid
from datetime import datetime
from sqlalchemy import (
    Column, String, Boolean, Integer, Numeric,
    ForeignKey, Text, TIMESTAMP, Enum as SAEnum,
    Double, Date
)
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import relationship
from app.database import Base
import enum

# ── Enums ─────────────────────────────────────────────────────────────────────

class SportType(str, enum.Enum):
    pickleball   = "pickleball"
    badminton    = "badminton"
    lawn_tennis  = "lawn_tennis"
    table_tennis = "table_tennis"

class MatchType(str, enum.Enum):
    queue      = "queue"
    ranked     = "ranked"
    friendly   = "friendly"
    book       = "book"
    tournament = "tournament"

class MatchFormat(str, enum.Enum):
    singles       = "singles"
    doubles       = "doubles"
    mixed_doubles = "mixed_doubles"

class MatchStatus(str, enum.Enum):
    pending          = "pending"
    assembling       = "assembling"
    pending_approval = "pending_approval"
    awaiting_players = "awaiting_players"
    ongoing          = "ongoing"
    completed        = "completed"
    cancelled        = "cancelled"
    invalidated      = "invalidated"

class TournamentFormat(str, enum.Enum):
    single_elimination   = "single_elimination"
    double_elimination   = "double_elimination"
    round_robin          = "round_robin"
    swiss                = "swiss"
    group_stage_knockout = "group_stage_knockout"
    pool_play            = "pool_play"

class UserRole(str, enum.Enum):
    player               = "player"
    club_admin           = "club_admin"
    tournament_organizer = "tournament_organizer"
    referee              = "referee"
    system_admin         = "system_admin"

class EventType(str, enum.Enum):
    shot         = "shot"
    violation    = "violation"
    rally_outcome = "rally_outcome"
    momentum     = "momentum"

# ── Models ────────────────────────────────────────────────────────────────────

class Profile(Base):
    __tablename__ = "profiles"

    id                     = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    username               = Column(String, unique=True, nullable=False)
    email                  = Column(String, unique=True)
    hashed_password        = Column(Text)
    avatar_url             = Column(Text)
    first_name             = Column(String)
    last_name              = Column(String)
    region_code            = Column(String)
    province_code          = Column(String)
    city_mun_code          = Column(String)
    barangay_code          = Column(String)
    gender                 = Column(String)                  # male | female | other
    profile_setup_complete = Column(Boolean, default=False)
    referee_boost_until    = Column(TIMESTAMP(timezone=True))
    fcm_token              = Column(String)
    token_version          = Column(Integer, nullable=False, default=0)
    created_at             = Column(TIMESTAMP(timezone=True), default=datetime.utcnow)
    updated_at             = Column(TIMESTAMP(timezone=True), default=datetime.utcnow)

    roles                  = relationship("UserRoleModel", back_populates="user", cascade="all, delete")
    sport_registrations    = relationship("SportRegistration", back_populates="user", cascade="all, delete")
    ratings                = relationship("PlayerRating", back_populates="user", cascade="all, delete")


class UserRoleModel(Base):
    __tablename__ = "user_roles"

    id          = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id     = Column(UUID(as_uuid=True), ForeignKey("profiles.id", ondelete="CASCADE"), nullable=False)
    role        = Column(SAEnum(UserRole, name="user_role"), nullable=False, default=UserRole.player)
    assigned_at = Column(TIMESTAMP(timezone=True), default=datetime.utcnow)

    user        = relationship("Profile", back_populates="roles")


class SportRegistration(Base):
    __tablename__ = "sport_registrations"

    id            = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id       = Column(UUID(as_uuid=True), ForeignKey("profiles.id", ondelete="CASCADE"), nullable=False)
    sport         = Column(SAEnum(SportType, name="sport_type"), nullable=False)
    registered_at = Column(TIMESTAMP(timezone=True), default=datetime.utcnow)

    user          = relationship("Profile", back_populates="sport_registrations")


class PlayerRating(Base):
    __tablename__ = "player_ratings"

    id                  = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id             = Column(UUID(as_uuid=True), ForeignKey("profiles.id", ondelete="CASCADE"), nullable=False)
    sport               = Column(SAEnum(SportType, name="sport_type"), nullable=False)
    match_format        = Column(String, nullable=False, default="singles")
    rating              = Column(Numeric, default=1500)
    rating_deviation    = Column(Numeric, default=350)
    volatility          = Column(Numeric, default=0.06)
    activeness_score           = Column(Numeric, default=0)
    matches_played             = Column(Integer, default=0)
    wins                       = Column(Integer, default=0)
    losses                     = Column(Integer, default=0)
    current_win_streak         = Column(Integer, default=0)
    current_loss_streak        = Column(Integer, default=0)
    # Calibration
    rating_status              = Column(String, default="CALIBRATING")   # CALIBRATING | RATED
    calibration_matches_played = Column(Integer, default=0)
    is_leaderboard_eligible    = Column(Boolean, default=False)
    calibration_completed_at   = Column(TIMESTAMP(timezone=True))
    updated_at                 = Column(TIMESTAMP(timezone=True), default=datetime.utcnow)

    user                = relationship("Profile", back_populates="ratings")


class Club(Base):
    __tablename__ = "clubs"

    id              = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name            = Column(String, unique=True, nullable=False)
    description     = Column(Text)
    logo_url        = Column(Text)
    cover_url       = Column(Text)
    admin_id        = Column(UUID(as_uuid=True), ForeignKey("profiles.id"), nullable=False)
    sport           = Column(SAEnum(SportType, name="sport_type"))
    category        = Column(String)   # community | school | private | municipal | barangay | academy | venue
    membership_type = Column(String, default="open")  # open | invite_only
    address         = Column(Text)
    region_code     = Column(String)
    province_code   = Column(String)
    city_mun_code   = Column(String)
    approval_mode   = Column(String, default="auto")   # auto | manual
    opening_time    = Column(String, default="06:00")  # HH:MM 24-hour
    closing_time    = Column(String, default="22:00")  # HH:MM 24-hour
    is_active       = Column(Boolean, default=True)
    created_at      = Column(TIMESTAMP(timezone=True), default=datetime.utcnow)

    members       = relationship("ClubMember", back_populates="club", cascade="all, delete")


class ClubMember(Base):
    __tablename__ = "club_members"

    id         = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    club_id    = Column(UUID(as_uuid=True), ForeignKey("clubs.id", ondelete="CASCADE"), nullable=False)
    user_id    = Column(UUID(as_uuid=True), ForeignKey("profiles.id", ondelete="CASCADE"), nullable=False)
    role       = Column(String, default="member")   # member | admin | assistant
    duty_date  = Column(Date)                        # date assistant is on duty (nullable)
    joined_at  = Column(TIMESTAMP(timezone=True), default=datetime.utcnow)

    club       = relationship("Club", back_populates="members")

class ClubInvite(Base):
    __tablename__ = "club_invites"

    id           = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    club_id      = Column(UUID(as_uuid=True), ForeignKey("clubs.id", ondelete="CASCADE"), nullable=False)
    invited_by   = Column(UUID(as_uuid=True), ForeignKey("profiles.id"), nullable=False)
    invited_user = Column(UUID(as_uuid=True), ForeignKey("profiles.id"), nullable=False)
    status       = Column(String, nullable=False, default="pending")  # pending | accepted | declined
    message      = Column(Text)
    expires_at   = Column(TIMESTAMP(timezone=True))
    responded_at = Column(TIMESTAMP(timezone=True))
    created_at   = Column(TIMESTAMP(timezone=True), default=datetime.utcnow)


class OpenPlaySession(Base):
    __tablename__ = "open_play_sessions"

    id             = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    club_id        = Column(UUID(as_uuid=True), ForeignKey("clubs.id", ondelete="CASCADE"), nullable=False)
    court_id       = Column(UUID(as_uuid=True), ForeignKey("courts.id", ondelete="SET NULL"))
    created_by     = Column(UUID(as_uuid=True), ForeignKey("profiles.id"), nullable=False)
    title          = Column(String, nullable=False)
    description    = Column(Text)
    sport          = Column(String, nullable=False)
    session_date   = Column(TIMESTAMP(timezone=True), nullable=False)
    duration_hours = Column(Numeric, default=1)
    max_players    = Column(Integer, nullable=False)
    price_per_head = Column(Numeric, default=0)
    status         = Column(String, nullable=False, default="upcoming")  # upcoming|ongoing|completed|cancelled
    skill_min      = Column(Numeric)
    skill_max      = Column(Numeric)
    notes          = Column(Text)
    created_at     = Column(TIMESTAMP(timezone=True), default=datetime.utcnow)

    participants   = relationship("OpenPlayParticipant", back_populates="session", cascade="all, delete")


class OpenPlayParticipant(Base):
    __tablename__ = "open_play_participants"

    id         = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    session_id = Column(UUID(as_uuid=True), ForeignKey("open_play_sessions.id", ondelete="CASCADE"), nullable=False)
    user_id    = Column(UUID(as_uuid=True), ForeignKey("profiles.id", ondelete="CASCADE"), nullable=False)
    status     = Column(String, nullable=False, default="confirmed")  # confirmed|waitlisted|cancelled
    joined_at  = Column(TIMESTAMP(timezone=True), default=datetime.utcnow)

    session    = relationship("OpenPlaySession", back_populates="participants")


class Party(Base):
    __tablename__ = "parties"

    id               = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    leader_id        = Column(UUID(as_uuid=True), ForeignKey("profiles.id", ondelete="CASCADE"), nullable=False)
    sport            = Column(String, nullable=False)
    match_format     = Column(String, nullable=False, default="doubles")  # doubles|mixed_doubles
    status           = Column(String, nullable=False, default="forming")  # forming|ready|in_queue|match_found|disbanded
    match_id         = Column(UUID(as_uuid=True), ForeignKey("matches.id", ondelete="SET NULL"))
    queue_started_at = Column(TIMESTAMP(timezone=True))
    created_at       = Column(TIMESTAMP(timezone=True), default=datetime.utcnow)

    members      = relationship("PartyMember", back_populates="party", cascade="all, delete")
    invitations  = relationship("PartyInvitation", back_populates="party", cascade="all, delete")


class PartyMember(Base):
    __tablename__ = "party_members"

    id        = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    party_id  = Column(UUID(as_uuid=True), ForeignKey("parties.id", ondelete="CASCADE"), nullable=False)
    user_id   = Column(UUID(as_uuid=True), ForeignKey("profiles.id", ondelete="CASCADE"), nullable=False)
    role      = Column(String, nullable=False, default="member")  # leader|member
    joined_at = Column(TIMESTAMP(timezone=True), default=datetime.utcnow)

    party     = relationship("Party", back_populates="members")


class PartyInvitation(Base):
    __tablename__ = "party_invitations"

    id          = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    party_id    = Column(UUID(as_uuid=True), ForeignKey("parties.id", ondelete="CASCADE"), nullable=False)
    inviter_id  = Column(UUID(as_uuid=True), ForeignKey("profiles.id", ondelete="CASCADE"), nullable=False)
    invitee_id  = Column(UUID(as_uuid=True), ForeignKey("profiles.id", ondelete="CASCADE"), nullable=False)
    status      = Column(String, nullable=False, default="pending")  # pending|accepted|declined
    created_at  = Column(TIMESTAMP(timezone=True), default=datetime.utcnow)

    party       = relationship("Party", back_populates="invitations")


class ClubCheckin(Base):
    __tablename__ = "club_checkins"

    id             = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id        = Column(UUID(as_uuid=True), ForeignKey("profiles.id", ondelete="CASCADE"), nullable=False)
    club_id        = Column(UUID(as_uuid=True), ForeignKey("clubs.id", ondelete="CASCADE"), nullable=False)
    status         = Column(String, nullable=False, default="present")
    checked_in_at  = Column(TIMESTAMP(timezone=True), default=datetime.utcnow)
    checked_out_at = Column(TIMESTAMP(timezone=True))


class Court(Base):
    __tablename__ = "courts"

    id         = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    club_id    = Column(UUID(as_uuid=True), ForeignKey("clubs.id", ondelete="SET NULL"), nullable=True)
    name       = Column(String, nullable=False)
    sport      = Column(String)
    surface    = Column(String)   # sport-specific: Wooden, Clay, Acrylic, etc.
    is_indoor  = Column(Boolean, default=True)
    lighting   = Column(String, default="good")  # good | fair | poor
    capacity   = Column(Integer)
    notes      = Column(Text)
    address    = Column(Text)
    region_code     = Column(String)
    province_code   = Column(String)
    city_mun_code   = Column(String)
    status     = Column(String, nullable=False, default="available")
    created_at = Column(TIMESTAMP(timezone=True), default=datetime.utcnow)
    image_url  = Column(Text)
    created_by = Column(UUID(as_uuid=True), ForeignKey("profiles.id"))


class CourtBooking(Base):
    __tablename__ = "court_bookings"

    id           = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    court_id     = Column(UUID(as_uuid=True), ForeignKey("courts.id", ondelete="CASCADE"), nullable=False)
    match_id     = Column(UUID(as_uuid=True), ForeignKey("matches.id", ondelete="CASCADE"))
    club_id      = Column(UUID(as_uuid=True), ForeignKey("clubs.id", ondelete="SET NULL"), nullable=True)
    requested_by = Column(UUID(as_uuid=True), ForeignKey("profiles.id"), nullable=False)
    scheduled_at = Column(TIMESTAMP(timezone=True))
    status       = Column(String, nullable=False, default="pending_approval")
    admin_notes  = Column(Text)
    decided_at   = Column(TIMESTAMP(timezone=True))
    created_at   = Column(TIMESTAMP(timezone=True), default=datetime.utcnow)


class Tournament(Base):
    __tablename__ = "tournaments"

    id                = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name              = Column(String, nullable=False)
    description       = Column(Text)
    sport             = Column(SAEnum(SportType, name="sport_type"), nullable=False)
    format            = Column(String, nullable=False, default="single_elimination")
    match_format      = Column(String, nullable=False, default="singles")
    organizer_id      = Column(UUID(as_uuid=True), ForeignKey("profiles.id"), nullable=False)
    club_id           = Column(UUID(as_uuid=True), ForeignKey("clubs.id"))
    max_participants  = Column(Integer, default=32)
    status            = Column(String, default="upcoming")
    registration_open = Column(Boolean, default=True)
    starts_at         = Column(TIMESTAMP(timezone=True))
    ends_at           = Column(TIMESTAMP(timezone=True))
    region_code       = Column(String)
    province_code     = Column(String)
    # random | seeded | smart_tiered
    draw_method       = Column(String, default="random", nullable=False)
    # { group_count, balance_by_rating, separate_clubs, separate_locations }
    smart_tiered_config = Column(JSONB)
    # registration eligibility
    min_rating        = Column(Numeric)
    max_rating        = Column(Numeric)
    requires_approval  = Column(Boolean, default=False, nullable=False)
    # 1 = Best of 1 (single game), 3 = Best of 3 (first to win 2 games)
    knockout_best_of   = Column(Integer, default=3, nullable=False)
    created_at         = Column(TIMESTAMP(timezone=True), default=datetime.utcnow)


class Match(Base):
    __tablename__ = "matches"

    id             = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    sport          = Column(SAEnum(SportType, name="sport_type"), nullable=False)
    match_type     = Column(SAEnum(MatchType, name="match_type"), nullable=False)
    match_format   = Column(SAEnum(MatchFormat, name="match_format"), nullable=False, default=MatchFormat.singles)
    status         = Column(SAEnum(MatchStatus, name="match_status"), nullable=False, default=MatchStatus.pending)
    player1_id     = Column(UUID(as_uuid=True), ForeignKey("profiles.id"))
    player2_id     = Column(UUID(as_uuid=True), ForeignKey("profiles.id"))
    team1_player1  = Column(UUID(as_uuid=True), ForeignKey("profiles.id"))
    team1_player2  = Column(UUID(as_uuid=True), ForeignKey("profiles.id"))
    team2_player1  = Column(UUID(as_uuid=True), ForeignKey("profiles.id"))
    team2_player2  = Column(UUID(as_uuid=True), ForeignKey("profiles.id"))
    player3_id     = Column(UUID(as_uuid=True), ForeignKey("profiles.id"))
    player4_id     = Column(UUID(as_uuid=True), ForeignKey("profiles.id"))
    referee_id     = Column(UUID(as_uuid=True), ForeignKey("profiles.id"))
    court_id       = Column(UUID(as_uuid=True), ForeignKey("courts.id"))
    tournament_id    = Column(UUID(as_uuid=True), ForeignKey("tournaments.id"))
    club_id          = Column(UUID(as_uuid=True), ForeignKey("clubs.id"))
    winner_id        = Column(UUID(as_uuid=True), ForeignKey("profiles.id"))
    round_number          = Column(Integer)
    bracket_position      = Column(Integer)
    next_match_id         = Column(UUID(as_uuid=True), ForeignKey("matches.id"))
    bracket_side          = Column(String)   # W, L, GF, G0..Gn, K
    loser_next_match_id   = Column(UUID(as_uuid=True), ForeignKey("matches.id"))
    best_of             = Column(Integer)   # per-match override: 1, 3, or 5 (None = use tournament/sport default)
    score_limit         = Column(Integer)   # per-match points-per-set override: 11, 15, or 21
    ml_match_score      = Column(Numeric)
    queue_city_code     = Column(String)   # resolved play location snapshot at queue-join time
    queue_province_code = Column(String)
    queue_region_code   = Column(String)
    scheduled_at   = Column(TIMESTAMP(timezone=True))
    started_at     = Column(TIMESTAMP(timezone=True))
    completed_at   = Column(TIMESTAMP(timezone=True))
    created_at     = Column(TIMESTAMP(timezone=True), default=datetime.utcnow)

    sets           = relationship("MatchSet", back_populates="match", cascade="all, delete")
    rally_events   = relationship("RallyEvent", back_populates="match", cascade="all, delete")


class MatchSet(Base):
    __tablename__ = "match_sets"

    id            = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    match_id      = Column(UUID(as_uuid=True), ForeignKey("matches.id", ondelete="CASCADE"), nullable=False)
    set_number    = Column(Integer, nullable=False)
    player1_score = Column(Integer, default=0)
    player2_score = Column(Integer, default=0)
    team1_score   = Column(Integer, default=0)
    team2_score   = Column(Integer, default=0)
    is_completed  = Column(Boolean, default=False)
    completed_at  = Column(TIMESTAMP(timezone=True))

    match         = relationship("Match", back_populates="sets")


class MatchAcceptance(Base):
    __tablename__ = "match_acceptances"

    id          = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    match_id    = Column(UUID(as_uuid=True), ForeignKey("matches.id", ondelete="CASCADE"), nullable=False)
    user_id     = Column(UUID(as_uuid=True), ForeignKey("profiles.id", ondelete="CASCADE"), nullable=False)
    decision    = Column(String, nullable=False)
    decided_at  = Column(TIMESTAMP(timezone=True), default=datetime.utcnow)


class MatchLobbyPlayer(Base):
    __tablename__ = "match_lobby_players"

    id          = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    match_id    = Column(UUID(as_uuid=True), ForeignKey("matches.id", ondelete="CASCADE"), nullable=False)
    user_id     = Column(UUID(as_uuid=True), ForeignKey("profiles.id", ondelete="CASCADE"), nullable=False)
    team_no     = Column(Integer, nullable=False)   # 1 or 2
    status      = Column(String, nullable=False, default="pending")  # pending | entered
    entered_at  = Column(TIMESTAMP(timezone=True))
    notified_at = Column(TIMESTAMP(timezone=True), default=datetime.utcnow)


class ShotType(Base):
    __tablename__ = "shot_types"

    id    = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    sport = Column(SAEnum(SportType, name="sport_type"), nullable=False)
    name  = Column(String, nullable=False)
    code  = Column(String, nullable=False)


class ViolationType(Base):
    __tablename__ = "violation_types"

    id    = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    sport = Column(SAEnum(SportType, name="sport_type"), nullable=False)
    name  = Column(String, nullable=False)
    code  = Column(String, nullable=False)


class RallyEvent(Base):
    __tablename__ = "rally_events"

    id            = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    match_id      = Column(UUID(as_uuid=True), ForeignKey("matches.id", ondelete="CASCADE"), nullable=False)
    set_number    = Column(Integer, nullable=False)
    rally_number  = Column(Integer, nullable=False)
    scored_by     = Column(UUID(as_uuid=True), ForeignKey("profiles.id"))
    event_type    = Column(SAEnum(EventType, name="event_type"), nullable=False)
    event_code    = Column(String, nullable=False)
    tagged_player = Column(UUID(as_uuid=True), ForeignKey("profiles.id"))
    notes         = Column(Text)
    tagged_at     = Column(TIMESTAMP(timezone=True), default=datetime.utcnow)
    is_offline    = Column(Boolean, default=False)

    match         = relationship("Match", back_populates="rally_events")


class Friendship(Base):
    __tablename__ = "friendships"

    id           = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    requester_id = Column(UUID(as_uuid=True), ForeignKey("profiles.id", ondelete="CASCADE"), nullable=False)
    addressee_id = Column(UUID(as_uuid=True), ForeignKey("profiles.id", ondelete="CASCADE"), nullable=False)
    status       = Column(String, default="pending")
    created_at   = Column(TIMESTAMP(timezone=True), default=datetime.utcnow)


class Notification(Base):
    __tablename__ = "notifications"

    id         = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id    = Column(UUID(as_uuid=True), ForeignKey("profiles.id", ondelete="CASCADE"), nullable=False)
    type       = Column(String, nullable=False)
    title      = Column(String, nullable=False)
    body       = Column(Text)
    data       = Column(JSONB)
    is_read    = Column(Boolean, default=False)
    created_at = Column(TIMESTAMP(timezone=True), default=datetime.utcnow)


class TournamentRegistration(Base):
    __tablename__ = "tournament_registrations"

    id            = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    tournament_id = Column(UUID(as_uuid=True), ForeignKey("tournaments.id", ondelete="CASCADE"), nullable=False)
    player_id     = Column(UUID(as_uuid=True), ForeignKey("profiles.id"), nullable=False)
    partner_id    = Column(UUID(as_uuid=True), ForeignKey("profiles.id"))
    seed          = Column(Integer)
    # confirmed | invited | pending_approval | pending_partner | declined
    status        = Column(String, default="confirmed", nullable=False)
    # self_registered | organizer_invited
    source        = Column(String, default="self_registered", nullable=False)
    registered_at = Column(TIMESTAMP(timezone=True), default=datetime.utcnow)


class TournamentGroup(Base):
    __tablename__ = "tournament_groups"

    id            = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    tournament_id = Column(UUID(as_uuid=True), ForeignKey("tournaments.id", ondelete="CASCADE"), nullable=False)
    group_name    = Column(String, nullable=False)   # "Group A", "Group B", …
    group_order   = Column(Integer, nullable=False, default=0)
    group_size    = Column(Integer, nullable=False)
    created_at    = Column(TIMESTAMP(timezone=True), default=datetime.utcnow)


class TournamentGroupMember(Base):
    __tablename__ = "tournament_group_members"

    id                  = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    tournament_group_id = Column(UUID(as_uuid=True), ForeignKey("tournament_groups.id", ondelete="CASCADE"), nullable=False)
    entry_id            = Column(UUID(as_uuid=True), ForeignKey("tournament_registrations.id", ondelete="CASCADE"), nullable=False)
    seed_number         = Column(Integer)
    assigned_at         = Column(TIMESTAMP(timezone=True), default=datetime.utcnow)


class TournamentGroupStanding(Base):
    __tablename__ = "tournament_group_standings"

    id             = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    tournament_id  = Column(UUID(as_uuid=True), ForeignKey("tournaments.id", ondelete="CASCADE"), nullable=False)
    group_id       = Column(UUID(as_uuid=True), ForeignKey("tournament_groups.id", ondelete="CASCADE"), nullable=False)
    entry_id       = Column(UUID(as_uuid=True), ForeignKey("tournament_registrations.id", ondelete="CASCADE"), nullable=False)
    played         = Column(Integer, default=0, nullable=False)
    wins           = Column(Integer, default=0, nullable=False)
    losses         = Column(Integer, default=0, nullable=False)
    points_for     = Column(Integer, default=0, nullable=False)
    points_against = Column(Integer, default=0, nullable=False)
    point_diff     = Column(Integer, default=0, nullable=False)
    rank           = Column(Integer, default=0, nullable=False)
    updated_at     = Column(TIMESTAMP(timezone=True), default=datetime.utcnow)


class MatchmakingQueue(Base):
    __tablename__ = "matchmaking_queue"

    id           = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id      = Column(UUID(as_uuid=True), ForeignKey("profiles.id", ondelete="CASCADE"), nullable=False)
    sport        = Column(SAEnum(SportType, name="sport_type"), nullable=False)
    format       = Column(SAEnum(MatchFormat, name="match_format"), nullable=False)
    skill_rating = Column(Numeric, default=1500)
    joined_at    = Column(TIMESTAMP(timezone=True), default=datetime.utcnow)
    status       = Column(String, default="waiting")


class LeaderboardCache(Base):
    __tablename__ = "leaderboard_cache"

    id         = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id    = Column(UUID(as_uuid=True), ForeignKey("profiles.id"), nullable=False)
    sport      = Column(SAEnum(SportType, name="sport_type"), nullable=False)
    geo_level  = Column(String, nullable=False)
    geo_code   = Column(String, nullable=False)
    rating     = Column(Numeric, nullable=False)
    rank       = Column(Integer)
    updated_at = Column(TIMESTAMP(timezone=True), default=datetime.utcnow)


class GeographicData(Base):
    __tablename__ = "geographic_data"

    id          = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    level       = Column(String)
    code        = Column(String, unique=True, nullable=False)
    name        = Column(String, nullable=False)
    parent_code = Column(String)

class RefereeInvite(Base):
    __tablename__ = "referee_invites"

    id           = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    match_id     = Column(UUID(as_uuid=True), ForeignKey("matches.id", ondelete="CASCADE"), nullable=False)
    invited_by   = Column(UUID(as_uuid=True), ForeignKey("profiles.id"), nullable=False)
    invited_user = Column(UUID(as_uuid=True), ForeignKey("profiles.id"), nullable=False)
    status       = Column(String, nullable=False, default="pending")
    expires_at   = Column(TIMESTAMP(timezone=True))
    responded_at = Column(TIMESTAMP(timezone=True))
    created_at   = Column(TIMESTAMP(timezone=True), default=datetime.utcnow)


class RefereeOpenRequest(Base):
    __tablename__ = "referee_open_requests"

    id         = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    match_id   = Column(UUID(as_uuid=True), ForeignKey("matches.id", ondelete="CASCADE"), nullable=False)
    club_id    = Column(UUID(as_uuid=True), ForeignKey("clubs.id", ondelete="CASCADE"), nullable=False)
    posted_by  = Column(UUID(as_uuid=True), ForeignKey("profiles.id"), nullable=False)
    filled_by  = Column(UUID(as_uuid=True), ForeignKey("profiles.id"))
    status     = Column(String, nullable=False, default="open")
    created_at = Column(TIMESTAMP(timezone=True), default=datetime.utcnow)


class MatchHistory(Base):
    __tablename__ = "match_history"

    id             = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    match_id       = Column(UUID(as_uuid=True), ForeignKey("matches.id", ondelete="CASCADE"), nullable=False)
    # event_type: "point" | "violation" | "undo" | "set_end" | "match_start" | "match_end"
    event_type     = Column(String, nullable=False)
    team           = Column(String)                                    # "team1" | "team2" | null
    player_id      = Column(UUID(as_uuid=True), ForeignKey("profiles.id"))  # player attributed
    recorded_by    = Column(UUID(as_uuid=True), ForeignKey("profiles.id"))  # referee / participant
    description    = Column(String)                                    # human-readable summary
    set_number     = Column(Integer)
    team1_score    = Column(Integer)                                   # score snapshot after event
    team2_score    = Column(Integer)
    meta           = Column(JSONB)                                     # violation_code, cause, etc.
    created_at     = Column(TIMESTAMP(timezone=True), default=datetime.utcnow)

    match          = relationship("Match")


class PlayerInsight(Base):
    __tablename__ = "player_insights"

    id           = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id      = Column(UUID(as_uuid=True), ForeignKey("profiles.id", ondelete="CASCADE"), nullable=False)
    sport        = Column(String)               # sport the insight is for (or null = overall)
    insight_text = Column(Text, nullable=False) # LLM-generated markdown insight
    stats_snapshot = Column(JSONB)              # stats used to generate this insight
    generated_at = Column(TIMESTAMP(timezone=True), default=datetime.utcnow)


class CourtRotation(Base):
    __tablename__ = "court_rotations"

    id         = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    club_id    = Column(UUID(as_uuid=True), ForeignKey("clubs.id",   ondelete="SET NULL"))
    court_id   = Column(UUID(as_uuid=True), ForeignKey("courts.id",  ondelete="SET NULL"))
    sport      = Column(String, nullable=False)
    format     = Column(String, nullable=False, default="singles")   # singles | doubles
    created_by = Column(UUID(as_uuid=True), ForeignKey("profiles.id"), nullable=False)
    status     = Column(String, nullable=False, default="active")    # active | ended
    created_at = Column(TIMESTAMP(timezone=True), default=datetime.utcnow)

    members    = relationship(
        "CourtRotationMember", back_populates="rotation",
        cascade="all, delete",
        order_by="CourtRotationMember.queue_position",
    )


class CourtRotationMember(Base):
    __tablename__ = "court_rotation_members"

    id             = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    rotation_id    = Column(UUID(as_uuid=True), ForeignKey("court_rotations.id", ondelete="CASCADE"), nullable=False)
    user_id        = Column(UUID(as_uuid=True), ForeignKey("profiles.id", ondelete="SET NULL"))  # null = guest
    display_name   = Column(String, nullable=False)
    queue_position = Column(Integer, nullable=False)   # 1-indexed; 1..court_size = on court
    games_played   = Column(Integer, default=0)
    wins           = Column(Integer, default=0)
    joined_at      = Column(TIMESTAMP(timezone=True), default=datetime.utcnow)

    rotation       = relationship("CourtRotation", back_populates="members")


class SecurityAuditLog(Base):
    """Records security-relevant auth events (logins, dual-login detections, logouts)."""
    __tablename__ = "security_audit_logs"

    id         = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id    = Column(UUID(as_uuid=True), ForeignKey("profiles.id", ondelete="CASCADE"), nullable=False)
    # LOGIN | DUAL_LOGIN_DETECTED | LOGOUT
    event_type = Column(String, nullable=False)
    ip_address = Column(String)
    details    = Column(JSONB)
    created_at = Column(TIMESTAMP(timezone=True), default=datetime.utcnow)


class RankingLevel(Base):
    """Defines the rating-based competitive tier thresholds (Barangay → National)."""
    __tablename__ = "ranking_levels"

    id                       = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    level_name               = Column(String, nullable=False, unique=True)   # Barangay, City/Municipal, …
    min_rating               = Column(Numeric, nullable=False)
    max_rating               = Column(Numeric)                               # NULL for top level
    display_order            = Column(Integer, nullable=False)               # 1 = lowest
    is_top_level             = Column(Boolean, default=False)
    minimum_matches_required = Column(Integer, default=5)                   # recent matches (90 days)
    is_active                = Column(Boolean, default=True)
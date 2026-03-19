"""
Smart Tiered Draw — grouping engine + candidate scorer.

Flow:
  1. Receive a list of CompetitionEntry objects (player_id, rating, club_id, city_id)
  2. Generate N candidate group distributions
  3. Score each candidate (strength balance, club collision, competitiveness)
  4. Return the best-scored distribution + its scores for display
"""

import random
import statistics
from dataclasses import dataclass, field
from typing import Any


# ── Data model ────────────────────────────────────────────────────────────────

@dataclass
class CompetitionEntry:
    player_id:  str
    rating:     float
    club_id:    str | None = None
    city_id:    str | None = None
    partner_id: str | None = None   # for doubles


@dataclass
class GroupDistribution:
    groups: list[list[CompetitionEntry]]   # groups[i] = list of entries in group i
    scores: dict[str, float] = field(default_factory=dict)


# ── Config defaults ────────────────────────────────────────────────────────────

DEFAULT_CONFIG: dict[str, Any] = {
    "group_count":          4,
    "balance_by_rating":    True,
    "separate_clubs":       True,
    "separate_locations":   False,
    "num_candidates":       8,     # how many distributions to evaluate
}


# ── Main entry point ──────────────────────────────────────────────────────────

def generate_smart_tiered(
    entries: list[CompetitionEntry],
    config:  dict[str, Any] | None = None,
) -> GroupDistribution:
    """
    Generate the best bracket grouping for the given entries.
    Returns a GroupDistribution with .groups and .scores populated.
    """
    cfg = {**DEFAULT_CONFIG, **(config or {})}

    n_groups      = max(2, int(cfg["group_count"]))
    n_candidates  = max(1, int(cfg["num_candidates"]))
    bal_rating    = bool(cfg["balance_by_rating"])
    sep_clubs     = bool(cfg["separate_clubs"])
    sep_locations = bool(cfg["separate_locations"])

    if len(entries) < 2:
        raise ValueError("Need at least 2 entries to generate a bracket.")
    if len(entries) < n_groups:
        n_groups = len(entries)

    # Generate N candidates
    candidates: list[GroupDistribution] = []
    for _ in range(n_candidates):
        dist = _generate_candidate(entries, n_groups, bal_rating, sep_clubs, sep_locations)
        dist.scores = _score_distribution(dist, sep_clubs, sep_locations)
        candidates.append(dist)

    # Pick the one with the highest combined score
    best = max(candidates, key=lambda d: d.scores["combined_score"])
    return best


# ── Candidate generator ───────────────────────────────────────────────────────

def _generate_candidate(
    entries:       list[CompetitionEntry],
    n_groups:      int,
    bal_rating:    bool,
    sep_clubs:     bool,
    sep_locations: bool,
) -> GroupDistribution:
    """
    Create one candidate group distribution.

    Strategy:
    1. Sort by rating descending (for seeded placement)
    2. Snake-distribute into groups (ensures rating balance)
    3. Apply club-separation swaps if enabled
    4. Add a random perturbation so candidates differ
    """
    # Sort by rating desc, then shuffle ties slightly
    sorted_entries = sorted(entries, key=lambda e: e.rating + random.uniform(-15, 15), reverse=True)

    # Snake distribution: rows alternate direction
    groups: list[list[CompetitionEntry]] = [[] for _ in range(n_groups)]
    for i, entry in enumerate(sorted_entries):
        row = i // n_groups
        idx = i % n_groups if row % 2 == 0 else n_groups - 1 - (i % n_groups)
        groups[idx].append(entry)

    if bal_rating:
        groups = _rebalance_ratings(groups)

    if sep_clubs:
        groups = _separate_clubs(groups)

    if sep_locations:
        groups = _separate_locations(groups)

    return GroupDistribution(groups=groups)


# ── Rebalancing helpers ───────────────────────────────────────────────────────

def _group_avg_rating(group: list[CompetitionEntry]) -> float:
    if not group:
        return 0.0
    return sum(e.rating for e in group) / len(group)


def _rebalance_ratings(groups: list[list[CompetitionEntry]]) -> list[list[CompetitionEntry]]:
    """
    Iterative swap to reduce std deviation of group average ratings.
    Tries up to 200 swaps.
    """
    flat = [e for g in groups for e in g]
    sizes = [len(g) for g in groups]

    for _ in range(200):
        avgs = [_group_avg_rating(g) for g in groups]
        std  = statistics.pstdev(avgs) if len(avgs) > 1 else 0.0
        if std < 5:        # good enough
            break

        # Find highest-avg group and lowest-avg group
        hi_idx = max(range(len(groups)), key=lambda i: avgs[i])
        lo_idx = min(range(len(groups)), key=lambda i: avgs[i])
        if hi_idx == lo_idx:
            break

        # Try swapping best from hi with worst from lo
        if not groups[hi_idx] or not groups[lo_idx]:
            break

        hi_best = max(groups[hi_idx], key=lambda e: e.rating)
        lo_worst = min(groups[lo_idx], key=lambda e: e.rating)

        groups[hi_idx].remove(hi_best)
        groups[lo_idx].remove(lo_worst)
        groups[hi_idx].append(lo_worst)
        groups[lo_idx].append(hi_best)

    return groups


def _separate_clubs(groups: list[list[CompetitionEntry]]) -> list[list[CompetitionEntry]]:
    """
    Try to move same-club duplicates out of a group via swaps.
    Limited to 100 passes to keep it fast.
    """
    for _ in range(100):
        improved = False
        for g_idx, group in enumerate(groups):
            club_counts: dict[str, list[int]] = {}
            for pos, entry in enumerate(group):
                if entry.club_id is None:
                    continue
                club_counts.setdefault(entry.club_id, []).append(pos)

            for club_id, positions in club_counts.items():
                if len(positions) < 2:
                    continue
                # Try to swap the duplicate with an entry from another group
                dup_pos = positions[-1]
                dup_entry = group[dup_pos]
                for other_idx, other_group in enumerate(groups):
                    if other_idx == g_idx:
                        continue
                    for other_pos, other_entry in enumerate(other_group):
                        if other_entry.club_id != club_id:
                            # Swap
                            groups[g_idx][dup_pos] = other_entry
                            groups[other_idx][other_pos] = dup_entry
                            improved = True
                            break
                    if improved:
                        break
                if improved:
                    break
            if improved:
                break
        if not improved:
            break

    return groups


def _separate_locations(groups: list[list[CompetitionEntry]]) -> list[list[CompetitionEntry]]:
    """Same logic as club separation but by city_id."""
    for _ in range(100):
        improved = False
        for g_idx, group in enumerate(groups):
            city_counts: dict[str, list[int]] = {}
            for pos, entry in enumerate(group):
                if entry.city_id is None:
                    continue
                city_counts.setdefault(entry.city_id, []).append(pos)

            for city_id, positions in city_counts.items():
                if len(positions) < 2:
                    continue
                dup_pos = positions[-1]
                dup_entry = group[dup_pos]
                for other_idx, other_group in enumerate(groups):
                    if other_idx == g_idx:
                        continue
                    for other_pos, other_entry in enumerate(other_group):
                        if other_entry.city_id != city_id:
                            groups[g_idx][dup_pos] = other_entry
                            groups[other_idx][other_pos] = dup_entry
                            improved = True
                            break
                    if improved:
                        break
                if improved:
                    break
            if improved:
                break
        if not improved:
            break

    return groups


# ── Scoring engine ────────────────────────────────────────────────────────────

def _score_distribution(dist: GroupDistribution, sep_clubs: bool, sep_locations: bool) -> dict[str, float]:
    groups = dist.groups

    # ── Strength balance score ─────────────────────────────────────────────
    # Lower std dev of group averages → higher score
    avgs = [_group_avg_rating(g) for g in groups if g]
    if len(avgs) > 1:
        std = statistics.pstdev(avgs)
        # Map std to 0–100 (lower std = higher score). Cap at std=200 → score=0
        strength_balance_score = max(0.0, 100.0 - (std / 200.0) * 100.0)
    else:
        strength_balance_score = 100.0

    # ── Club collision penalty ──────────────────────────────────────────────
    total_collision = 0
    for group in groups:
        club_ids = [e.club_id for e in group if e.club_id]
        total_collision += len(club_ids) - len(set(club_ids))

    max_possible_collisions = max(1, sum(len(g) for g in groups) - len(groups))
    club_collision_penalty  = (total_collision / max_possible_collisions) * 100.0

    # ── Location collision penalty ─────────────────────────────────────────
    loc_collision = 0
    for group in groups:
        city_ids = [e.city_id for e in group if e.city_id]
        loc_collision += len(city_ids) - len(set(city_ids))
    loc_collision_penalty = (loc_collision / max_possible_collisions) * 100.0

    # ── Competitiveness score ──────────────────────────────────────────────
    # Cross-group R1 pairings: groups[0][i] vs groups[1][i], etc.
    # Lower rating gap → higher competitiveness
    matchup_gaps: list[float] = []
    for g_a, g_b in _group_pairs(groups):
        for ea, eb in zip(g_a, g_b):
            matchup_gaps.append(abs(ea.rating - eb.rating))

    if matchup_gaps:
        avg_gap = sum(matchup_gaps) / len(matchup_gaps)
        competitiveness_score = max(0.0, 100.0 - (avg_gap / 400.0) * 100.0)
    else:
        competitiveness_score = 100.0

    # ── Combined score (weighted) ──────────────────────────────────────────
    combined_score = (
        strength_balance_score * 0.40
        + competitiveness_score  * 0.30
        + (100.0 - club_collision_penalty)  * (0.20 if sep_clubs     else 0.0)
        + (100.0 - loc_collision_penalty)   * (0.10 if sep_locations  else 0.0)
        + (100.0 - club_collision_penalty)  * (0.30 if not sep_clubs and not sep_locations else 0.0)
    )

    return {
        "strength_balance_score": round(strength_balance_score, 1),
        "competitiveness_score":  round(competitiveness_score,  1),
        "club_collision_count":   total_collision,
        "location_collision_count": loc_collision,
        "combined_score":         round(combined_score, 2),
    }


def _group_pairs(groups: list[list[CompetitionEntry]]) -> list[tuple]:
    """
    Return pairs of groups for cross-group R1 matchup estimation.
    Groups 0&1, 2&3, 4&5, … (standard pairing map).
    """
    pairs = []
    for i in range(0, len(groups) - 1, 2):
        pairs.append((groups[i], groups[i + 1]))
    return pairs


# ── Utility: build entries from DB data ───────────────────────────────────────

def entries_from_registrations(
    registrations: list[Any],      # TournamentRegistration ORM objects (confirmed only)
    profiles_map:  dict[str, Any], # player_id → Profile ORM object
    ratings_map:   dict[str, float] | None = None,  # player_id → Glicko-2 rating
) -> list[CompetitionEntry]:
    """
    Convert registration ORM rows into CompetitionEntry objects.

    Profile schema:
      - city_mun_code  (location identifier)
      - no club_id directly on Profile; club membership is via ClubMembership table.
        Pass club_id via ratings_map if needed, or leave None.

    ratings_map: pre-fetched { player_id: float } from PlayerRating table.
    If omitted, defaults to 1500 for all entries.
    """
    result = []
    for reg in registrations:
        pid     = str(reg.player_id)
        profile = profiles_map.get(pid)
        rating  = float((ratings_map or {}).get(pid, 1500))
        # Use city_mun_code as the location grouping key
        city_id = getattr(profile, "city_mun_code", None) if profile else None
        result.append(CompetitionEntry(
            player_id  = pid,
            rating     = rating,
            club_id    = None,   # enriched separately if club separation needed
            city_id    = str(city_id) if city_id else None,
            partner_id = str(reg.partner_id) if reg.partner_id else None,
        ))
    return result

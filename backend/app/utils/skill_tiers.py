from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SkillTierDefinition:
    slug: str
    name: str
    min_rating: float
    max_rating: float | None
    display_order: int
    minimum_matches_required: int = 0
    rd_threshold: float = 200.0


RATING_FLOOR = 500.0
RATING_CEILING = 2700.0

# These thresholds follow the requested point ladder while covering the
# full Glicko-2 envelope used by ISMS. `max_rating` is exclusive.
SKILL_TIER_DEFINITIONS: tuple[SkillTierDefinition, ...] = (
    SkillTierDefinition("novice", "Novice", RATING_FLOOR, 1500.0, 1),
    SkillTierDefinition("advanced_beginner", "Advanced Beginner", 1500.0, 1700.0, 2),
    SkillTierDefinition("competent", "Competent", 1700.0, 1900.0, 3),
    SkillTierDefinition("proficient", "Proficient", 1900.0, 2200.0, 4),
    SkillTierDefinition("expert", "Expert", 2200.0, None, 5),
)

SKILL_TIERS_BY_SLUG = {tier.slug: tier for tier in SKILL_TIER_DEFINITIONS}
SKILL_TIERS_BY_NAME = {tier.name: tier for tier in SKILL_TIER_DEFINITIONS}


def is_rating_in_skill_tier(rating: float, tier: SkillTierDefinition) -> bool:
    if tier.max_rating is None:
        return rating >= tier.min_rating
    return tier.min_rating <= rating < tier.max_rating


def get_skill_tier(rating: float) -> SkillTierDefinition:
    for tier in SKILL_TIER_DEFINITIONS:
        if is_rating_in_skill_tier(rating, tier):
            return tier
    if rating < SKILL_TIER_DEFINITIONS[0].min_rating:
        return SKILL_TIER_DEFINITIONS[0]
    return SKILL_TIER_DEFINITIONS[-1]


def get_skill_tier_name(rating: float) -> str:
    return get_skill_tier(rating).name


def get_skill_tier_slug(rating: float) -> str:
    return get_skill_tier(rating).slug

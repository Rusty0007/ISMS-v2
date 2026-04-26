import unittest
from contextlib import contextmanager

import app.services.matchmaking as matchmaking
from app.services.matchmaking import can_join_doubles_lobby, is_mixed_doubles_team, run_matchmaking


def player(
    player_id: str,
    rating: float,
    gender: str,
    *,
    performance_rating: float = 50.0,
    performance_confidence: float = 0.0,
    performance_reliable: bool = False,
) -> dict:
    return {
        "player_id": player_id,
        "rating": rating,
        "rating_deviation": 120.0,
        "win_rate": 0.5,
        "activeness_score": 0.5,
        "current_streak": 0,
        "performance_rating": performance_rating,
        "performance_confidence": performance_confidence,
        "performance_reliable": performance_reliable,
        "city_code": "001",
        "province_code": "001",
        "region_code": "001",
        "gender": gender,
    }


@contextmanager
def fallback_matchmaking_mode():
    previous_state = (
        matchmaking._model,
        matchmaking._sport_enc,
        matchmaking._format_enc,
        matchmaking._model_info,
        matchmaking._model_load_attempted,
    )
    matchmaking._model = None
    matchmaking._sport_enc = None
    matchmaking._format_enc = None
    matchmaking._model_info = None
    matchmaking._model_load_attempted = True
    try:
        yield
    finally:
        (
            matchmaking._model,
            matchmaking._sport_enc,
            matchmaking._format_enc,
            matchmaking._model_info,
            matchmaking._model_load_attempted,
        ) = previous_state


class MixedDoublesMatchmakingTests(unittest.TestCase):
    def test_mixed_doubles_returns_male_female_teams(self):
        result = run_matchmaking(
            [
                player("m1", 1500, "male"),
                player("m2", 1510, "male"),
                player("f1", 1490, "female"),
                player("f2", 1520, "female"),
            ],
            sport="badminton",
            match_format="mixed_doubles",
            mode="ranked",
        )

        self.assertIsNotNone(result)
        self.assertTrue(is_mixed_doubles_team(result["team_a"]))
        self.assertTrue(is_mixed_doubles_team(result["team_b"]))

    def test_mixed_doubles_rejects_invalid_gender_pool(self):
        result = run_matchmaking(
            [
                player("m1", 1500, "male"),
                player("m2", 1510, "male"),
                player("m3", 1490, "male"),
                player("f1", 1520, "female"),
            ],
            sport="badminton",
            match_format="mixed_doubles",
            mode="ranked",
        )

        self.assertIsNone(result)

    def test_regular_doubles_does_not_require_mixed_team_gender(self):
        result = run_matchmaking(
            [
                player("m1", 1500, "male"),
                player("m2", 1510, "male"),
                player("m3", 1490, "male"),
                player("m4", 1520, "male"),
            ],
            sport="badminton",
            match_format="doubles",
            mode="ranked",
        )

        self.assertIsNotNone(result)

    def test_regular_doubles_uses_performance_to_avoid_lopsided_split_when_ratings_tie(self):
        result = run_matchmaking(
            [
                player("a", 1500, "male", performance_rating=90, performance_confidence=90, performance_reliable=True),
                player("b", 1500, "male", performance_rating=90, performance_confidence=90, performance_reliable=True),
                player("c", 1500, "male", performance_rating=10, performance_confidence=90, performance_reliable=True),
                player("d", 1500, "male", performance_rating=10, performance_confidence=90, performance_reliable=True),
            ],
            sport="badminton",
            match_format="doubles",
            mode="ranked",
        )

        self.assertIsNotNone(result)
        team_a_ids = {member["player_id"] for member in result["team_a"]}
        self.assertIn("a", team_a_ids)
        self.assertNotEqual(team_a_ids, {"a", "b"})
        self.assertLessEqual(result["performance_diff"], 1.0)

    def test_ranked_fourth_player_gate_allows_small_role_gap_for_regular_doubles(self):
        with fallback_matchmaking_mode():
            result = can_join_doubles_lobby(
                incoming=player("d", 1300, "male"),
                lobby_players=[
                    player("a", 1200, "male"),
                    player("b", 1200, "male"),
                    player("c", 1200, "male"),
                ],
                sport="badminton",
                match_format="doubles",
                lobby_wait_seconds=0,
                mode="ranked",
            )

        self.assertTrue(result)

    def test_ranked_fourth_player_gate_rejects_large_role_gap_even_after_best_split(self):
        with fallback_matchmaking_mode():
            result = can_join_doubles_lobby(
                incoming=player("d", 1500, "male"),
                lobby_players=[
                    player("a", 1200, "male"),
                    player("b", 1200, "male"),
                    player("c", 1200, "male"),
                ],
                sport="badminton",
                match_format="doubles",
                lobby_wait_seconds=0,
                mode="ranked",
            )

        self.assertFalse(result)

    def test_ranked_mixed_doubles_rejects_large_mirror_gap(self):
        with fallback_matchmaking_mode():
            result = can_join_doubles_lobby(
                incoming=player("f2", 1200, "female"),
                lobby_players=[
                    player("m1", 1800, "male"),
                    player("m2", 1200, "male"),
                    player("f1", 1800, "female"),
                ],
                sport="badminton",
                match_format="mixed_doubles",
                lobby_wait_seconds=0,
                mode="ranked",
            )

        self.assertFalse(result)


if __name__ == "__main__":
    unittest.main()

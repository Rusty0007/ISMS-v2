import unittest
from datetime import datetime, timezone
from types import SimpleNamespace

from app.services.performance_rating import (
    build_performance_snapshots,
    redistribute_match_ratings_by_performance,
)


def _match():
    return SimpleNamespace(
        id="match-1",
        sport="badminton",
        match_format="doubles",
        status="completed",
        winner_id="u1",
        completed_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        score_limit=21,
        team1_player1="u1",
        team1_player2="u2",
        team2_player1="u3",
        team2_player2="u4",
        player1_id="u1",
        player2_id="u3",
        player3_id="u2",
        player4_id="u4",
    )


def _history(
    event_type: str,
    *,
    team: str | None = None,
    player_id: str | None = None,
    team1_score: int | None = None,
    team2_score: int | None = None,
    meta: dict | None = None,
):
    return SimpleNamespace(
        event_type=event_type,
        team=team,
        player_id=player_id,
        team1_score=team1_score,
        team2_score=team2_score,
        meta=meta or {},
    )


class PerformanceRatingTests(unittest.TestCase):
    def test_build_snapshots_assigns_shared_pressure_credit_and_individual_errors(self):
        snapshots = build_performance_snapshots(
            [_match()],
            {
                "match-1": [
                    _history(
                        "point",
                        team="team1",
                        player_id="u1",
                        team1_score=18,
                        team2_score=17,
                        meta={"attribution_type": "winning_shot", "cause": "smash"},
                    ),
                    _history(
                        "point",
                        team="team1",
                        team1_score=20,
                        team2_score=18,
                        meta={"attribution_type": "opponent_error", "actor_player_id": "u3", "reason_code": "NET_ERROR"},
                    ),
                    _history(
                        "point",
                        team="team2",
                        player_id="u4",
                        team1_score=20,
                        team2_score=19,
                        meta={"attribution_type": "winning_shot", "cause": "drop"},
                    ),
                    _history(
                        "point",
                        team="team1",
                        team1_score=21,
                        team2_score=19,
                        meta={"attribution_type": "other", "notes": "extended rally"},
                    ),
                    _history(
                        "serve_change",
                        team="team1",
                        player_id="u2",
                        meta={"event_type": "side_out", "fault_player_id": "u2"},
                    ),
                ]
            },
        )

        u1 = snapshots[("u1", "badminton", "doubles")]
        u2 = snapshots[("u2", "badminton", "doubles")]
        u3 = snapshots[("u3", "badminton", "doubles")]
        u4 = snapshots[("u4", "badminton", "doubles")]

        self.assertEqual(u1.performance_total_points, 4)
        self.assertEqual(u1.performance_attributed_points, 3)
        self.assertEqual(u1.performance_winning_shots, 1.0)
        self.assertEqual(u1.performance_forced_errors_drawn, 0.5)
        self.assertEqual(u1.performance_clutch_points_won, 0.5)
        self.assertGreater(u1.performance_rating, 50.0)

        self.assertEqual(u2.performance_serve_faults, 1.0)
        self.assertLess(u2.performance_rating, 50.0)

        self.assertEqual(u3.performance_errors_committed, 1.0)
        self.assertEqual(u3.performance_clutch_errors, 1.0)
        self.assertLess(u3.performance_rating, 50.0)

        self.assertEqual(u4.performance_winning_shots, 1.0)
        self.assertEqual(u4.performance_clutch_points_won, 1.0)
        self.assertGreater(u4.performance_rating, 50.0)

    def test_redistribute_team_rating_delta_rewards_better_teammate_without_changing_team_total(self):
        match = _match()
        history_rows = []

        for score in range(1, 9):
            history_rows.append(
                _history(
                    "point",
                    team="team1",
                    player_id="u1",
                    team1_score=score,
                    team2_score=0,
                    meta={"attribution_type": "winning_shot", "cause": "smash"},
                )
            )

        for score in range(1, 5):
            history_rows.append(
                _history(
                    "point",
                    team="team2",
                    team1_score=8,
                    team2_score=score,
                    meta={"attribution_type": "opponent_error", "actor_player_id": "u2"},
                )
            )

        adjusted = redistribute_match_ratings_by_performance(
            match,
            history_rows,
            {"u1": 1500.0, "u2": 1500.0, "u3": 1500.0, "u4": 1500.0},
            {"u1": 1512.0, "u2": 1512.0, "u3": 1488.0, "u4": 1488.0},
            winner_id="u1",
        )

        self.assertGreater(adjusted["u1"], 1512.0)
        self.assertLess(adjusted["u2"], 1512.0)
        self.assertAlmostEqual(adjusted["u1"] + adjusted["u2"], 3024.0)
        self.assertAlmostEqual(adjusted["u3"] + adjusted["u4"], 2976.0)
        self.assertGreater(adjusted["u1"] - adjusted["u2"], 0.0)


if __name__ == "__main__":
    unittest.main()

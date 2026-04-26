import unittest
from datetime import datetime, timezone
from types import SimpleNamespace

from app.services.rating_rebuilder import replay_match_into_snapshots


def _match(index: int, winner_id: str):
    return SimpleNamespace(
        id=f"match-{index}",
        sport="pickleball",
        match_format="singles",
        status="completed",
        player1_id="player-1",
        player2_id="player-2",
        player3_id=None,
        player4_id=None,
        team1_player1=None,
        team1_player2=None,
        team2_player1=None,
        team2_player2=None,
        winner_id=winner_id,
        completed_at=datetime(2026, 1, index, tzinfo=timezone.utc),
    )


def _doubles_match():
    return SimpleNamespace(
        id="doubles-1",
        sport="pickleball",
        match_format="doubles",
        status="completed",
        player1_id="player-1",
        player2_id="player-3",
        player3_id="player-2",
        player4_id="player-4",
        team1_player1="player-1",
        team1_player2="player-2",
        team2_player1="player-3",
        team2_player2="player-4",
        winner_id="player-1",
        completed_at=datetime(2026, 2, 1, tzinfo=timezone.utc),
        score_limit=21,
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


class RatingRebuilderTests(unittest.TestCase):
    def test_replay_rebuilds_records_and_rating_from_match_history(self):
        snapshots = {}
        winners = [
            "player-2",
            "player-2",
            "player-2",
            "player-1",
            "player-2",
            "player-2",
            "player-2",
            "player-2",
            "player-1",
            "player-2",
            "player-2",
        ]

        for index, winner_id in enumerate(winners, start=1):
            self.assertTrue(replay_match_into_snapshots(_match(index, winner_id), snapshots))

        p1 = snapshots[("player-1", "pickleball", "singles")]
        p2 = snapshots[("player-2", "pickleball", "singles")]

        self.assertEqual(p1.wins, 2)
        self.assertEqual(p1.losses, 9)
        self.assertEqual(p2.wins, 9)
        self.assertEqual(p2.losses, 2)
        self.assertEqual(len(p1.distinct_opponents), 1)
        self.assertEqual(len(p2.distinct_opponents), 1)
        self.assertTrue(p1.is_matchmaking_eligible)
        self.assertTrue(p2.is_matchmaking_eligible)
        self.assertFalse(p1.is_leaderboard_eligible)
        self.assertFalse(p2.is_leaderboard_eligible)
        self.assertLess(p1.rating, 1500)
        self.assertGreater(p2.rating, 1500)
        self.assertGreater(p2.rating - p1.rating, 250)

    def test_replay_uses_match_performance_to_split_doubles_teammate_ratings(self):
        history_rows = []

        for score in range(1, 9):
            history_rows.append(
                _history(
                    "point",
                    team="team1",
                    player_id="player-1",
                    team1_score=score,
                    team2_score=0,
                    meta={"attribution_type": "winning_shot", "cause": "drive"},
                )
            )

        for score in range(1, 5):
            history_rows.append(
                _history(
                    "point",
                    team="team2",
                    team1_score=8,
                    team2_score=score,
                    meta={"attribution_type": "opponent_error", "actor_player_id": "player-2"},
                )
            )

        with_history = {}
        without_history = {}

        self.assertTrue(replay_match_into_snapshots(_doubles_match(), with_history, history_rows))
        self.assertTrue(replay_match_into_snapshots(_doubles_match(), without_history))

        p1_with = with_history[("player-1", "pickleball", "doubles")]
        p2_with = with_history[("player-2", "pickleball", "doubles")]
        p1_without = without_history[("player-1", "pickleball", "doubles")]
        p2_without = without_history[("player-2", "pickleball", "doubles")]

        self.assertEqual(p1_with.wins, 1)
        self.assertEqual(p2_with.wins, 1)
        self.assertGreater(p1_with.rating, p2_with.rating)
        self.assertAlmostEqual(
            p1_with.rating + p2_with.rating,
            p1_without.rating + p2_without.rating,
            places=6,
        )
        self.assertEqual(p1_without.rating, p2_without.rating)


if __name__ == "__main__":
    unittest.main()

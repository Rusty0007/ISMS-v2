import unittest

from app.services.rating_policy import leaderboard_eligible, matchmaking_eligible


class RatingPolicyTests(unittest.TestCase):
    def test_matchmaking_unlocks_before_leaderboard(self):
        self.assertFalse(matchmaking_eligible(9))
        self.assertTrue(matchmaking_eligible(10))

        # ranked_matches_played is now first arg
        self.assertFalse(leaderboard_eligible(20, 1, 120.0))   # not enough opponents
        self.assertFalse(leaderboard_eligible(20, 3, 250.0))   # RD too high
        self.assertTrue(leaderboard_eligible(20, 3, 120.0))    # all conditions met
        self.assertFalse(leaderboard_eligible(5, 3, 120.0))    # not enough ranked matches


if __name__ == "__main__":
    unittest.main()

import unittest

from app.services.rating_policy import leaderboard_eligible, matchmaking_eligible


class RatingPolicyTests(unittest.TestCase):
    def test_matchmaking_unlocks_before_leaderboard(self):
        self.assertFalse(matchmaking_eligible(9))
        self.assertTrue(matchmaking_eligible(10))

        self.assertFalse(leaderboard_eligible(20, 1, 120.0))
        self.assertFalse(leaderboard_eligible(20, 3, 250.0))
        self.assertTrue(leaderboard_eligible(20, 3, 120.0))


if __name__ == "__main__":
    unittest.main()

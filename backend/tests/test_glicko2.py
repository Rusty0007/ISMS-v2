import unittest

from app.utils.glicko2 import MAX_VOLATILITY, update


def _play_series(outcomes, volatility=0.06):
    p1 = [1500.0, 350.0, volatility]
    p2 = [1500.0, 350.0, volatility]

    for p1_wins in outcomes:
        old_p1 = p1[:]
        old_p2 = p2[:]
        p1 = list(
            update(
                old_p1[0],
                old_p1[1],
                old_p1[2],
                old_p2[0],
                old_p2[1],
                1.0 if p1_wins else 0.0,
            )
        )
        p2 = list(
            update(
                old_p2[0],
                old_p2[1],
                old_p2[2],
                old_p1[0],
                old_p1[1],
                0.0 if p1_wins else 1.0,
            )
        )

    return p1, p2


class Glicko2UpdateTests(unittest.TestCase):
    def test_alternating_1v1_results_do_not_hit_rating_bounds(self):
        p1, p2 = _play_series([index % 2 == 0 for index in range(10)])

        self.assertGreater(p1[0], 1400.0)
        self.assertLess(p1[0], 1600.0)
        self.assertGreater(p2[0], 1400.0)
        self.assertLess(p2[0], 1600.0)
        self.assertLessEqual(p1[2], MAX_VOLATILITY)
        self.assertLessEqual(p2[2], MAX_VOLATILITY)

    def test_repeated_wins_move_rating_gradually(self):
        p1, p2 = _play_series([True] * 10)

        self.assertGreater(p1[0], 1800.0)
        self.assertLess(p1[0], 1900.0)
        self.assertGreater(p2[0], 1100.0)
        self.assertLess(p2[0], 1200.0)

    def test_polluted_volatility_is_sanitized(self):
        p1, p2 = _play_series([True, False, True], volatility=1000.0)

        self.assertGreater(p1[0], 1400.0)
        self.assertLess(p1[0], 1700.0)
        self.assertGreater(p2[0], 1300.0)
        self.assertLess(p2[0], 1600.0)
        self.assertLessEqual(p1[2], MAX_VOLATILITY)
        self.assertLessEqual(p2[2], MAX_VOLATILITY)


if __name__ == "__main__":
    unittest.main()

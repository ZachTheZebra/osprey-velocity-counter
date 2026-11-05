#!/usr/bin/env python3
"""Percentiles for the store benchmark. The load run is bench/harness.py."""

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from bench.stats import percentile  # noqa: E402


class PercentileTest(unittest.TestCase):
    def test_median_and_ends_of_a_sorted_sample(self):
        samples = [1, 2, 3, 4, 5]
        self.assertEqual(percentile(samples, 50), 3)
        self.assertEqual(percentile(samples, 0), 1)
        self.assertEqual(percentile(samples, 100), 5)

    def test_p99_interpolates_between_the_top_samples(self):
        samples = list(range(1, 101))
        self.assertEqual(percentile(samples, 99), 99)


if __name__ == "__main__":
    unittest.main()

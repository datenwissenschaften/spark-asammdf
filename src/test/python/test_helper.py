import unittest
from unittest import mock
import os
import sys

# Add src/main/python to path for tests
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../main/python")))

from mdf_spark import helper
from mdf_spark.helper import get_partitions, read_data

# Generic, synthetic channel names used by the test fixture generated in
# src/test/resources/generate_fixture.py — no real-world signal names.
CHANNEL_A = "ObstacleDetected"
CHANNEL_B = "LaneChangePossible"


class TestHelper(unittest.TestCase):
    def setUp(self):
        self.test_data_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../resources/data"))
        self.test_file = os.path.join(self.test_data_dir, "sample.mf4")
        if not os.path.exists(self.test_file):
            self.skipTest(
                f"Test fixture {self.test_file} not found; run "
                "`python src/test/resources/generate_fixture.py` first"
            )

    def test_get_partitions(self):
        partitions = get_partitions(self.test_file, "")
        self.assertGreater(len(partitions), 0)
        channel_names = [p['channel'] for p in partitions]
        self.assertIn(CHANNEL_A, channel_names)
        self.assertIn(CHANNEL_B, channel_names)
        # Every partition should carry the (group, index) occurrence it was
        # resolved from, so readers can go straight to it without a second
        # whereis() lookup.
        for p in partitions:
            self.assertIn('group', p)
            self.assertIn('index', p)
        # The "time" master channel should never surface as its own partition.
        self.assertNotIn("time", channel_names)

    def test_get_partitions_with_where_clause(self):
        partitions = get_partitions(self.test_file, f"channel = '{CHANNEL_A}'")
        self.assertEqual(len(partitions), 1)
        self.assertEqual(partitions[0]['channel'], CHANNEL_A)
        self.assertIsInstance(partitions[0]['group'], int)
        self.assertIsInstance(partitions[0]['index'], int)

    def test_get_partitions_is_cached(self):
        # Calling get_partitions twice with the same (file, where_clause)
        # should hit the lru_cache and only parse the file once, since
        # Spark re-invokes query planning on every action (and can invoke
        # it more than once per action under AQE).
        helper._get_partitions_cached.cache_clear()
        with mock.patch("mdf_spark.helper.MDF", wraps=helper.MDF) as mdf_spy:
            first = get_partitions(self.test_file, f"channel = '{CHANNEL_B}'")
            second = get_partitions(self.test_file, f"channel = '{CHANNEL_B}'")
        self.assertEqual(mdf_spy.call_count, 1)
        self.assertEqual(first, second)

    def test_read_data(self):
        path = read_data(self.test_file, CHANNEL_B, "")
        try:
            import pandas as pd
            df = pd.read_parquet(path)
            self.assertGreater(len(df), 0)
            self.assertTrue((df['channel'] == CHANNEL_B).all())
        finally:
            if os.path.exists(path):
                os.remove(path)

    def test_read_data_with_explicit_occurrence(self):
        # This is the path MDFPartitionReader actually drives: the driver
        # resolves (group, index) once in get_partitions, then each
        # executor reads exactly that occurrence instead of re-resolving
        # channel_name via whereis().
        [occurrence] = get_partitions(self.test_file, f"channel = '{CHANNEL_B}'")
        path = read_data(
            self.test_file, CHANNEL_B, "",
            group_index=occurrence['group'], channel_index=occurrence['index'],
        )
        try:
            import pandas as pd
            df = pd.read_parquet(path)
            self.assertGreater(len(df), 0)
            self.assertTrue((df['channel'] == CHANNEL_B).all())

            without_hint_path = read_data(self.test_file, CHANNEL_B, "")
            try:
                df_without_hint = pd.read_parquet(without_hint_path)
                self.assertEqual(len(df), len(df_without_hint))
            finally:
                if os.path.exists(without_hint_path):
                    os.remove(without_hint_path)
        finally:
            if os.path.exists(path):
                os.remove(path)

    def test_read_data_with_value_filter(self):
        path = read_data(self.test_file, CHANNEL_B, "valueCont = 1")
        try:
            import pandas as pd
            df = pd.read_parquet(path)
            self.assertGreater(len(df), 0)
            self.assertTrue((df['valueCont'] == 1).all())
        finally:
            if os.path.exists(path):
                os.remove(path)


if __name__ == '__main__':
    unittest.main()

import unittest
from unittest import mock
import os
import sys

# Add src/main/python to path for tests
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../main/python")))

from mdf_spark import helper
from mdf_spark.helper import get_partitions, read_data, _record_chunks

# Generic, synthetic channel names used by the test fixture generated in
# src/test/resources/generate_fixture.py — no real-world signal names.
CHANNEL_A = "engine_speed"
CHANNEL_B = "vehicle_speed"
CHANNEL_STRING = "traffic_light_state"


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

    def test_record_chunks_splits_evenly(self):
        self.assertEqual(list(_record_chunks(3000, 1000)), [(0, 1000), (1000, 1000), (2000, 1000)])

    def test_record_chunks_splits_unevenly(self):
        self.assertEqual(list(_record_chunks(3000, 800)), [(0, 800), (800, 800), (1600, 800), (2400, 600)])

    def test_record_chunks_noop_below_threshold(self):
        # No max, a non-positive max, or a max at/above the total all fall back to one chunk —
        # i.e. today's default (unchunked) behavior.
        self.assertEqual(list(_record_chunks(3000, None)), [(0, 3000)])
        self.assertEqual(list(_record_chunks(3000, 0)), [(0, 3000)])
        self.assertEqual(list(_record_chunks(3000, 5000)), [(0, 3000)])

    def test_get_partitions_with_max_records_per_partition(self):
        # engine_speed has 3000 samples (see generate_fixture.py); capping at 1000 records
        # per partition should split its single occurrence into 3 chunk partitions that
        # together cover every record exactly once, instead of 1 whole-occurrence partition.
        unchunked = get_partitions(self.test_file, f"channel = '{CHANNEL_A}'")
        self.assertEqual(len(unchunked), 1)
        self.assertEqual(unchunked[0]['offset'], 0)
        self.assertEqual(unchunked[0]['count'], 3000)

        chunked = get_partitions(self.test_file, f"channel = '{CHANNEL_A}'", max_records_per_partition=1000)
        self.assertEqual(len(chunked), 3)
        self.assertEqual([(p['offset'], p['count']) for p in chunked], [(0, 1000), (1000, 1000), (2000, 1000)])
        self.assertEqual(sum(p['count'] for p in chunked), unchunked[0]['count'])
        for p in chunked:
            self.assertEqual(p['group'], unchunked[0]['group'])
            self.assertEqual(p['index'], unchunked[0]['index'])

    def test_read_data_with_record_range_matches_full_read_slice(self):
        # Reading the [1000, 2000) record range directly (as MDFPartitionReader does for a
        # chunk partition) must match the corresponding slice of an unchunked full read —
        # i.e. chunking must not change which rows come back, only how many partitions do.
        [occurrence] = get_partitions(self.test_file, f"channel = '{CHANNEL_A}'")
        full_path = read_data(
            self.test_file, CHANNEL_A, "",
            group_index=occurrence['group'], channel_index=occurrence['index'],
        )
        chunk_path = read_data(
            self.test_file, CHANNEL_A, "",
            group_index=occurrence['group'], channel_index=occurrence['index'],
            record_offset=1000, record_count=1000,
        )
        try:
            import pandas as pd
            full_df = pd.read_parquet(full_path).reset_index(drop=True)
            chunk_df = pd.read_parquet(chunk_path).reset_index(drop=True)
            self.assertEqual(len(chunk_df), 1000)
            expected = full_df.iloc[1000:2000].reset_index(drop=True)
            pd.testing.assert_frame_equal(chunk_df, expected)
        finally:
            for p in (full_path, chunk_path):
                if os.path.exists(p):
                    os.remove(p)

    def test_read_data_timestamps_are_plausible_regardless_of_sample_rate(self):
        # Regression test: pandas picks the datetime64 storage resolution (us vs ns) based on
        # what the input timestamps need to be represented exactly — a whole-second-sampled
        # channel like battery_voltage (1 Hz) resolves to `us`, while the finer, sub-second
        # channels resolve to `ns`. read_data used to assume `ns` unconditionally, which
        # silently corrupted `time` for any channel landing on `us` resolution (values came
        # back around 1970 instead of the real measurement date). Every channel's epoch
        # microseconds must land in the same year regardless of its own sample rate.
        import pandas as pd
        years = {}
        for channel in ("battery_voltage", "engine_speed", "vehicle_speed"):
            path = read_data(self.test_file, channel, "")
            try:
                df = pd.read_parquet(path)
                years[channel] = pd.to_datetime(df['time'].iloc[0], unit='us').year
            finally:
                if os.path.exists(path):
                    os.remove(path)
        self.assertEqual(len(set(years.values())), 1, f"channels disagree on year: {years}")
        self.assertGreater(next(iter(years.values())), 2000)

    def test_read_data_for_string_channel(self):
        # traffic_light_state is the fixture's discrete/string channel: its samples must land
        # in valueText (as "red"/"green" strings), with valueNumeric left NaN — the pandas-level
        # counterpart to the Scala-level regression test in test_mdf_datasource.py, which caught
        # MDFPartitionReader defaulting a missing numeric value to 0.0 instead of null.
        path = read_data(self.test_file, CHANNEL_STRING, "")
        try:
            import pandas as pd
            df = pd.read_parquet(path)
            self.assertGreater(len(df), 0)
            self.assertEqual(set(df['valueText'].unique()), {"red", "green"})
            self.assertTrue(df['valueNumeric'].isna().all())
        finally:
            if os.path.exists(path):
                os.remove(path)

    def test_read_data_with_value_filter(self):
        # vehicle_speed is 0 km/h during the fixture's idle phases and rises
        # above 50 km/h during the cruise phase (see generate_fixture.py),
        # so this filter is guaranteed to both include and exclude rows.
        path = read_data(self.test_file, CHANNEL_B, "valueNumeric > 50")
        try:
            import pandas as pd
            df = pd.read_parquet(path)
            self.assertGreater(len(df), 0)
            self.assertTrue((df['valueNumeric'] > 50).all())
        finally:
            if os.path.exists(path):
                os.remove(path)


if __name__ == '__main__':
    unittest.main()

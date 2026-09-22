import os
import sys
import unittest
from pyspark.sql import SparkSession

# Generic, synthetic channel names used by the test fixture generated in
# src/test/resources/generate_fixture.py
CHANNEL_A = "ObstacleDetected"
CHANNEL_B = "LaneChangePossible"


class TestMDFDataSource(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Path to project root
        cls.project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.."))
        python_path = os.path.join(cls.project_root, "src/main/python")

        cls.jar_path = os.path.join(
            cls.project_root, "target/scala-2.13/spark-asammdf-scala_2.13-0.1.jar"
        )
        cls.test_mdf_file = os.path.join(
            cls.project_root, "src/test/resources/data/sample.mf4"
        )

        if not os.path.exists(cls.jar_path):
            raise unittest.SkipTest(
                f"Scala jar not found at {cls.jar_path}; run `sbt package` first"
            )
        if not os.path.exists(cls.test_mdf_file):
            raise unittest.SkipTest(
                f"Test fixture {cls.test_mdf_file} not found; run "
                "`python src/test/resources/generate_fixture.py` first"
            )

        # Ensure Spark and its subprocesses use the Python executable and path from the current virtual environment
        os.environ['PYSPARK_PYTHON'] = sys.executable
        os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

        if 'PYTHONPATH' in os.environ:
            os.environ['PYTHONPATH'] = python_path + os.pathsep + os.environ['PYTHONPATH']
        else:
            os.environ['PYTHONPATH'] = python_path

        # Add src/main/python to path for tests
        sys.path.insert(0, python_path)

        # Create Spark session. Bind explicitly to loopback so this also works
        # in sandboxed/CI environments where binding to the detected host
        # address is not permitted.
        cls.spark = (
            SparkSession.builder
            .appName("MDFTest")
            .master("local[*]")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.jars", cls.jar_path)
            # Ensure executors get the PYTHONPATH
            .config("spark.executorEnv.PYTHONPATH", os.environ['PYTHONPATH'])
            .getOrCreate()
        )

        from mdf_spark import init_bridge
        init_bridge(cls.spark)

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_hybrid_datasource(self):
        print("\nTesting Hybrid Scala-Python filter extraction with IN query...")
        df = (
            self.spark.read
            .format("com.datenwissenschaften.MDFDataSource")
            .option("path", self.test_mdf_file)
            .load()
        )

        df.groupBy("channel").count().show(10, False)

        df = df.where(f"channel IN ('{CHANNEL_A}', '{CHANNEL_B}')")

        # Trigger planning and execution
        df.show(20, False)

        # Verify data availability
        count = df.count()
        print(f"Total rows loaded: {count}")
        self.assertGreater(count, 0, "Expected some data to be loaded, but got 0 rows")

        # Verify channels
        channels = [row.channel for row in df.select("channel").distinct().collect()]
        print(f"Loaded channels: {channels}")
        self.assertIn(CHANNEL_A, channels, f"Expected channel {CHANNEL_A} to be loaded")

    def test_value_filter(self):
        # Test with value filter on specific channel (using the new schema)
        print(f"\nTesting Hybrid Scala-Python with value filter (value == 1) on {CHANNEL_B}...")
        df_filtered = (
            self.spark.read
            .format("com.datenwissenschaften.MDFDataSource")
            .option("path", self.test_mdf_file)
            .load()
            .where(f"channel = '{CHANNEL_B}'")
            .where("valueCont = 1")
        )
        df_filtered.show(20, False)

        count = df_filtered.count()
        self.assertGreater(count, 0, f"Expected some data for filtered {CHANNEL_B}")


if __name__ == "__main__":
    unittest.main()

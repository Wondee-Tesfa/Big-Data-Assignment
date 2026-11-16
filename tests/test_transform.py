import unittest
import pandas as pd
from src.transform import clean_chunk


class TestTransform(unittest.TestCase):

    def setUp(self):
        """Build sample raw DataFrame mimicking NYC Taxi CSV structure."""
        self.sample_data = {
            "VendorID": [1, 2, 99],  # 99 is invalid vendor
            "tpep_pickup_datetime": [
                "2015-01-10 14:00:00",
                "2015-01-15 08:30:00",
                "2014-12-31 23:59:59"  # Outside Jan 2015
            ],
            "tpep_dropoff_datetime": [
                "2015-01-10 14:15:00",
                "2015-01-15 08:50:00",
                "2015-01-01 00:10:00"
            ],
            "passenger_count": [2, 1, 0],  # 0 is invalid passenger count
            "trip_distance": [2.5, 4.0, -1.0],  # -1 is invalid distance
            "pickup_longitude": [-73.98, -73.95, -73.98],
            "pickup_latitude": [40.75, 40.73, 40.75],
            "RateCodeID": [1, 2, 1],
            "store_and_fwd_flag": ["N", "Y", "invalid"],
            "dropoff_longitude": [-73.97, -73.96, -73.97],
            "dropoff_latitude": [40.76, 40.74, 40.76],
            "payment_type": [1, 2, 1],
            "fare_amount": [12.0, 18.5, -5.0],
            "extra": [0.5, 0.0, 0.0],
            "mta_tax": [0.5, 0.5, 0.5],
            "tip_amount": [2.5, 0.0, 0.0],
            "tolls_amount": [0.0, 0.0, 0.0],
            "improvement_surcharge": [0.3, 0.3, 0.3],
            "total_amount": [15.8, 19.3, -4.2]
        }
        self.raw_df = pd.DataFrame(self.sample_data)

    def test_clean_chunk_filtering(self):
        """Verifies that clean_chunk retains valid rows and rejects invalid outliers."""
        clean_df, stats = clean_chunk(self.raw_df)
        
        # Row 3 must be filtered out due to invalid vendor, dates, and amounts
        self.assertEqual(len(clean_df), 2)
        self.assertEqual(stats["raw_rows"], 3)
        self.assertEqual(stats["clean_rows"], 2)

    def test_feature_engineering(self):
        """Verifies that trip_duration_min, pickup_hour, and pickup_dow are computed accurately."""
        clean_df, _ = clean_chunk(self.raw_df)
        
        # Check derived columns
        self.assertIn("trip_duration_min", clean_df.columns)
        self.assertIn("pickup_hour", clean_df.columns)
        self.assertIn("pickup_dow", clean_df.columns)

        # Duration for first row: 14:00 to 14:15 = 15.0 minutes
        first_row = clean_df.iloc[0]
        self.assertAlmostEqual(first_row["trip_duration_min"], 15.0)
        self.assertEqual(first_row["pickup_hour"], 14)


if __name__ == "__main__":
    unittest.main()
